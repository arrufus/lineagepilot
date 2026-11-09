"""What the request handlers need from one loaded graph, and the data derived from it.

A ``GraphContext`` lives as long as its graph: when the indexer hands over a new graph,
the server builds a new context, so nothing derived from an old graph survives.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeout
from pathlib import Path
from typing import Any

from lineagepilot_core.analysis.impact import Impact, impact
from lineagepilot_core.analysis.pii import PIIIndex, pii_index
from lineagepilot_core.graph.column_lineage import SQL_KINDS, Traced, catalog_schema, record, trace
from lineagepilot_core.graph.model import (
    ColumnLineageStatus,
    EdgeKind,
    MetadataGraph,
    Node,
)

from lineagepilot_lsp.locate import Locator, location_json
from lineagepilot_lsp.project import DbtProject, relative_to

logger = logging.getLogger(__name__)

# Free text from the workspace is cut to this many characters in protocol messages
TEXT_LIMIT = 4000
# Time a single request may spend computing column lineage on demand (design section 11)
ON_DEMAND_BUDGET_S = 0.25
MAX_COMPILED_SQL_BYTES = 5_000_000


def clip(text: str | None, limit: int = TEXT_LIMIT) -> str | None:
    if text is None or len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


class _Tracer:
    """Where on-demand SQL is traced: one model at a time, on a thread of its own.

    sqlglot's cost on a crafted compiled file has no useful bound, and a request handler runs on
    the server's event loop (audit LP-SEC-05). So a request waits for a trace only within its
    budget; past it, the trace finishes in the background for a later request, and nothing else
    is traced meanwhile. The thread is a daemon: one still tracing never holds up shutdown.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._busy = False

    def submit(self, work: Callable[[], Traced]) -> Future[Traced] | None:
        """``work`` started on its own thread, or None if a trace is still running."""
        with self._lock:
            if self._busy:
                return None
            self._busy = True
        future: Future[Traced] = Future()

        def run() -> None:
            try:
                future.set_result(work())
            except BaseException as e:  # reported to the waiting request, if any
                future.set_exception(e)
            finally:
                with self._lock:
                    self._busy = False

        threading.Thread(target=run, name="lineagepilot-on-demand", daemon=True).start()
        return future


_TRACER = _Tracer()


class OnDemandColumns:
    """Column lineage for one model at a time, before the indexer has computed it.

    The indexer adds column lineage for every model after handing over the model-level
    graph; at 5,000 models that takes 20-30 s. Until it is done, a query that needs a
    model's columns computes that model here, from its compiled file and a schema of its
    direct upstream relations (milliseconds), within a time budget per request. Past the
    budget, or without a compiled file, the model is left to the model-level fallback,
    which the impact result marks as such. The SQL is traced off the event loop
    (``_Tracer``); the result is put in the graph here, on it.
    """

    def __init__(
        self, graph: MetadataGraph, project: DbtProject, budget_s: float = ON_DEMAND_BUDGET_S
    ):
        self.graph = graph
        self.project = project
        self.budget_s = budget_s
        self.computed = 0
        self._unavailable: set[str] = set()
        self._deadline = 0.0
        # Traces that finished after their request stopped waiting: (SQL, result)
        self._late: dict[str, tuple[str, Traced]] = {}

    def start_request(self) -> None:
        self._deadline = time.perf_counter() + self.budget_s

    def status(self, node_id: str) -> ColumnLineageStatus | None:
        known = self.graph.column_lineage.get(node_id)
        if known is not None or node_id in self._unavailable:
            return known
        late = self._late.pop(node_id, None)
        if late is not None:
            return self._record(node_id, *late)
        remaining = self._deadline - time.perf_counter()
        if remaining <= 0:
            return None
        node = self.graph.nodes.get(node_id)
        sql = self._compiled(node) if node is not None and node.kind in SQL_KINDS else None
        if sql is None:
            self._unavailable.add(node_id)
            return None
        upstream = {e.upstream for e in self.graph.in_edges(node_id, [EdgeKind.DEPENDS_ON])}
        # A source reads the table a seed or model builds; the catalog may list either
        for upstream_id in list(upstream):
            relation = self.graph.nodes[upstream_id].relation_id
            if relation:
                upstream |= {n.id for n in self.graph.logical_nodes_for(relation)}
        schema = catalog_schema(self.graph, sorted(upstream))
        adapter = self.graph.adapter_type
        future = _TRACER.submit(lambda: trace(sql, schema, adapter))
        if future is None:
            return None  # another model is still being traced
        try:
            traced = future.result(timeout=remaining)
        except FutureTimeout:
            future.add_done_callback(lambda f: self._keep(node_id, sql, f))
            return None
        except Exception:
            logger.exception("Column lineage for %s could not be computed", node_id)
            self._unavailable.add(node_id)
            return None
        return self._record(node_id, sql, traced)

    def _keep(self, node_id: str, sql: str, future: Future[Traced]) -> None:
        """On the tracer's thread: a result for the next request that asks."""
        if future.exception() is None:
            self._late[node_id] = (sql, future.result())

    def _record(self, node_id: str, sql: str, traced: Traced) -> ColumnLineageStatus:
        status = record(self.graph, node_id, sql, traced)
        self.computed += 1
        return status

    def _compiled(self, node: Node) -> str | None:
        if not node.compiled_path:
            return None
        path = self.project.root / node.compiled_path
        if relative_to(path, self.project.root) is None:  # the manifest is untrusted
            return None
        try:
            if not path.is_file() or path.stat().st_size > MAX_COMPILED_SQL_BYTES:
                return None
            return path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            return None


class GraphContext:
    def __init__(
        self,
        graph: MetadataGraph,
        project: DbtProject,
        columns_computing: Callable[[], bool] = lambda: False,
        workspace: Path | None = None,
    ) -> None:
        self.graph = graph
        self.project = project
        self.locator = Locator(project, workspace)
        self.on_demand = OnDemandColumns(graph, project)
        self._columns_computing = columns_computing
        self._pii: PIIIndex | None = None
        self._pii_nodes: set[str] | None = None
        self._downstream: dict[str, dict[str, int]] = {}

    def forget_derived(self) -> None:
        """A DAG scan changed what is downstream of what."""
        self._downstream.clear()

    def columns_computing(self) -> bool:
        """Whether the indexer is still adding column lineage to this graph."""
        return self._columns_computing()

    # Derived data, computed on first use

    @property
    def pii(self) -> PIIIndex:
        if self._pii is None:
            self._pii = pii_index(self.graph)
        return self._pii

    def has_pii(self, node_id: str) -> bool:
        if self._pii_nodes is None:
            self._pii_nodes = self.pii.nodes()
        return node_id in self._pii_nodes

    def impact(self, targets: list[str], max_depth: int | None = None) -> Impact:
        """Impact, computing column lineage on demand while the indexer still is."""
        if not self.columns_computing():
            return impact(self.graph, targets, max_depth=max_depth)
        self.on_demand.start_request()
        return impact(
            self.graph, targets, max_depth=max_depth, column_lineage=self.on_demand.status
        )

    def downstream_counts(self, node_id: str) -> dict[str, int]:
        """Impacted nodes per kind, at model level (CodeLens)."""
        counts = self._downstream.get(node_id)
        if counts is None:
            counts = self._downstream[node_id] = impact(self.graph, [node_id]).counts()
        return counts

    # Files

    def nodes_for_file(self, path: Path) -> list[Node]:
        relative = relative_to(path, self.project.root)
        if relative is None:
            return []
        nodes = self.graph.nodes_for_file(relative)
        packages = self.project.packages_path.rstrip("/") + "/"
        if not nodes and relative.startswith(packages):
            package, _, inner = relative[len(packages) :].partition("/")
            nodes = [n for n in self.graph.nodes_for_file(inner) if n.package == package]
        return nodes

    def location(self, ref: str) -> dict[str, Any] | None:
        if ref in self.graph.columns:
            column = self.graph.columns[ref]
            return location_json(self.locator.column(self.graph.nodes[column.node_id], ref))
        node = self.graph.nodes.get(ref)
        return location_json(self.locator.node(node)) if node is not None else None

    # Protocol shapes

    def node_json(self, ref: str, side: str, depth: int) -> dict[str, Any]:
        """A GraphNode for a node or a column."""
        column = self.graph.columns.get(ref)
        if column is not None:
            node = self.graph.nodes[column.node_id]
            return {
                "id": ref,
                "kind": "column",
                "name": clip(column.name),
                "side": side,
                "depth": depth,
                "parent": node.id,
                "location": self.location(ref),
                "layer": node.layer,
                "materialization": None,
                "owner": node.owner,
                "description": clip(column.description),
                "columnLineage": None,
                "columnLineageReason": None,
                "pii": bool(column.pii_type) or ref in self.pii.indicators,
            }
        node = self.graph.node(ref)
        status = self.graph.column_lineage.get(ref)
        return {
            "id": ref,
            "kind": node.kind.value,
            "name": clip(node.name),
            "side": side,
            "depth": depth,
            "parent": None,
            "location": self.location(ref),
            "layer": node.layer,
            "materialization": node.materialization,
            "owner": clip(node.owner),
            "description": clip(node.description),
            "columnLineage": status.state.value if status else None,
            "columnLineageReason": clip(status.reason) if status else None,
            "pii": self.has_pii(ref),
        }

    def unknowns_json(self, refs: list[str]) -> list[dict[str, Any]]:
        wanted = set(refs)
        return [
            {"subject": u.subject, "reference": clip(u.reference), "reason": clip(u.reason)}
            for u in self.graph.unresolved
            if u.subject in wanted
        ]
