"""A baseline manifest: what the changed nodes looked like before (design section 8, step 3).

Typically the production manifest used for dbt's ``--state``. It is untrusted input like any
other artifact, and is only read for the few nodes a review needs, because compiling the output
columns of every model would take as long as a cold index.

A baseline that cannot be read is reported as an ``error``: the review then falls back to the
next baseline in priority order and says so, instead of reporting that nothing changed.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lineagepilot_core.graph.build import build_dbt_graph
from lineagepilot_core.graph.column_lineage import catalog_schema
from lineagepilot_core.graph.ids import normalise_identifier
from lineagepilot_core.graph.model import EdgeKind, MetadataGraph
from lineagepilot_core.indexer import DEFAULT_MAX_ARTIFACT_BYTES, locate_artifacts
from lineagepilot_core.parsers.base import ParseErrorSeverity
from lineagepilot_core.parsers.dbt_parser import DbtParser
from lineagepilot_core.parsers.sql_lineage import sql_output_columns, sqlglot_dialect


@dataclass(frozen=True, slots=True)
class BaselineNode:
    id: str
    kind: str
    name: str
    owner: str | None = None
    # What the node's compiled SQL selected; None when that cannot be known, and ``reason`` says
    # why, so the review moves on to the next baseline for this node
    columns: tuple[str, ...] | None = None
    # Column types the baseline states, by name folded to the adapter's case rules
    types: dict[str, str] = field(default_factory=dict)
    reason: str | None = None


@dataclass
class ManifestBaseline:
    name: str  # the file's name, for showing: never its path, which differs per machine
    sha256: str | None = None
    generated_at: str | None = None
    dbt_version: str | None = None
    # The nodes asked for that the baseline has. A node it does not have did not exist then.
    nodes: dict[str, BaselineNode] = field(default_factory=dict)
    # The nodes each file asked about defined or documented, by file
    files: dict[str, list[str]] = field(default_factory=dict)
    error: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "sha256": self.sha256,
            "generatedAt": self.generated_at,
            "dbtVersion": self.dbt_version,
            "nodes": {
                node.id: {
                    "kind": node.kind,
                    "name": node.name,
                    "owner": node.owner,
                    "columns": None if node.columns is None else list(node.columns),
                    "types": node.types,
                    "reason": node.reason,
                }
                for node in self.nodes.values()
            },
            "files": self.files,
            "error": self.error,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> ManifestBaseline:
        return cls(
            name=data["name"],
            sha256=data["sha256"],
            generated_at=data["generatedAt"],
            dbt_version=data["dbtVersion"],
            nodes={
                node_id: BaselineNode(
                    id=node_id,
                    kind=node["kind"],
                    name=node["name"],
                    owner=node["owner"],
                    columns=None if node["columns"] is None else tuple(node["columns"]),
                    types=node["types"],
                    reason=node["reason"],
                )
                for node_id, node in data["nodes"].items()
            },
            files=data["files"],
            error=data["error"],
        )


def load_baseline_manifest(
    manifest: str | Path,
    node_ids: Iterable[str],
    files: Iterable[str] = (),
    max_artifact_bytes: int | None = DEFAULT_MAX_ARTIFACT_BYTES,
) -> ManifestBaseline:
    """Read a baseline manifest, and ``catalog.json`` beside it if there is one.

    Args:
        node_ids: the nodes to read columns for
        files: also find what these files defined or documented in the baseline, for files
            the current graph has nothing on (a model that has since been deleted)
    """
    path = Path(manifest)
    baseline = ManifestBaseline(name=path.name)
    try:
        if not path.is_file():
            baseline.error = f"no file at {path}"
            return baseline
        size = path.stat().st_size
    except OSError as e:
        baseline.error = f"{path} cannot be read: {e}"
        return baseline
    if max_artifact_bytes is not None and size > max_artifact_bytes:
        baseline.error = (
            f"{path.name} is {size / 2**20:.0f} MB, over the "
            f"{max_artifact_bytes / 2**20:.0f} MB limit for dbt artifacts"
        )
        return baseline

    _, catalog = locate_artifacts(path.parent, path)
    config = {"manifest_path": str(path)}
    if catalog is not None:
        config["catalog_path"] = str(catalog)
    parse = DbtParser().parse(config)
    baseline.sha256 = parse.manifest_sha256
    baseline.generated_at = parse.generated_at
    baseline.dbt_version = parse.dbt_version
    if parse.has_errors:
        problems = [e.message for e in parse.errors if e.severity is ParseErrorSeverity.ERROR]
        baseline.error = f"{path.name} cannot be trusted: {'; '.join(problems)}"
        return baseline

    graph = build_dbt_graph(parse)
    compiled = {n.unique_id: n.compiled_code for n in parse.nodes}
    wanted = dict.fromkeys(node_ids)
    for file in files:
        found = [n.id for n in graph.nodes_for_file(file)]
        if found:
            baseline.files[file] = found
            wanted.update(dict.fromkeys(found))
    for node_id in wanted:
        if node_id in graph.nodes:
            baseline.nodes[node_id] = _read_node(graph, node_id, compiled.get(node_id))
    return baseline


def _read_node(graph: MetadataGraph, node_id: str, sql: str | None) -> BaselineNode:
    node = graph.nodes[node_id]
    upstream = {e.upstream for e in graph.in_edges(node_id, (EdgeKind.DEPENDS_ON,))}
    # A source reads the table a seed or model builds; the catalog may list either
    for upstream_id in list(upstream):
        relation = graph.nodes[upstream_id].relation_id
        if relation:
            upstream |= {n.id for n in graph.logical_nodes_for(relation)}
    names, reason = (
        sql_output_columns(
            sql, catalog_schema(graph, sorted(upstream)), sqlglot_dialect(graph.adapter_type)
        )
        if sql
        else (None, "the baseline has no compiled SQL for it")
    )
    adapter = graph.adapter_type
    return BaselineNode(
        id=node_id,
        kind=node.kind.value,
        name=node.name,
        owner=node.owner or node.group,
        columns=tuple(names) if names else None,
        types={
            normalise_identifier(c.name, adapter): c.data_type
            for c in graph.columns_of(node_id)
            if c.data_type
        },
        reason=reason,
    )
