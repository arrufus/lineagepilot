"""Column lineage in the graph: ``derives`` edges from compiled SQL (task 1.9).

Column lineage is computed per model, not as part of the cold index: at about 3 ms a
model it takes 16 s for 5,000 models, against a 5 s budget for the whole index. The
indexer adds it after the model-level graph, or for just the models a query needs, and
``add_column_lineage`` can recompute any model on its own.

For each model or snapshot the graph records a ``ColumnLineageStatus``:

- ``resolved``: every output column was traced;
- ``partial``: some references did not resolve, and each is an ``Unresolved`` record;
- ``unknown``: the model was not analysed, with the reason (no compiled SQL, no
  catalog, a Python model, SQL that does not parse, ...). Never "no lineage".
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterable, Mapping
from pathlib import Path

from sqlglot.schema import MappingSchema

from lineagepilot_core.graph.ids import column_id, relation_id, split_relation_name
from lineagepilot_core.graph.model import (
    ArtifactVersion,
    ColumnLineageStatus,
    Confidence,
    EdgeKind,
    Evidence,
    LineageState,
    Location,
    MetadataGraph,
    NodeKind,
    Origin,
    Unresolved,
)
from lineagepilot_core.parsers.base import ParseResult
from lineagepilot_core.parsers.sql_lineage import (
    MAX_SQL_CHARS,
    TableRef,
    build_schema,
    sql_column_lineage,
    sqlglot_dialect,
)

logger = logging.getLogger(__name__)

SQL_KINDS = frozenset({NodeKind.MODEL, NodeKind.SNAPSHOT})

_LINEAGE_REFERENCE = "column lineage"


def compiled_sql(result: ParseResult, project_root: Path | None = None) -> dict[str, str]:
    """Compiled SQL per model and snapshot.

    Taken from the manifest's ``compiled_code``, which ``dbt compile`` and ``dbt docs
    generate`` write, or else from the node's ``compiled_path`` under ``project_root``
    (``target/compiled/...``). A path that leaves the project root is ignored: the
    manifest is untrusted input (design section 10).
    """
    root = project_root.resolve() if project_root else None
    found: dict[str, str] = {}
    for node in result.nodes:
        if node.resource_type not in ("model", "snapshot") or node.language == "python":
            continue
        if node.compiled_code:
            found[node.unique_id] = node.compiled_code
        elif root and node.compiled_path:
            path = (root / node.compiled_path).resolve()
            if not path.is_relative_to(root) or not path.is_file():
                continue
            try:
                found[node.unique_id] = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError) as e:
                logger.warning("Cannot read compiled SQL %s: %s", path, e)
    return found


def catalog_schema(graph: MetadataGraph) -> MappingSchema:
    """A sqlglot schema of every relation the catalog describes, with its columns."""
    dialect = sqlglot_dialect(graph.adapter_type)
    tables = []
    seen: set[str] = set()
    for node in graph.nodes.values():
        if not node.in_catalog or not node.relation_id or node.relation_id in seen:
            continue
        relation = graph.nodes.get(node.relation_id)
        parts = split_relation_name(
            (relation.properties if relation else {}).get("relation_name", "")
        )
        if not parts:
            continue
        columns = sorted(
            (c for c in graph.columns_of(node.id) if c.in_catalog),
            key=lambda c: (c.ordinal is None, c.ordinal or 0),
        )
        tables.append((parts, [c.name for c in columns]))
        seen.add(node.relation_id)
    return build_schema(tables, dialect)


def add_column_lineage(
    graph: MetadataGraph,
    sql: Mapping[str, str],
    node_ids: Iterable[str] | None = None,
    schema: MappingSchema | None = None,
    max_chars: int = MAX_SQL_CHARS,
) -> dict[str, ColumnLineageStatus]:
    """Compute column lineage for models and snapshots, replacing any earlier result.

    Args:
        graph: the graph, with the catalog's columns already in it
        sql: compiled SQL by node ID (see ``compiled_sql``)
        node_ids: the nodes to compute; every model and snapshot by default
        schema: a prebuilt ``catalog_schema(graph)``, to reuse across calls
    """
    if node_ids is None:
        node_ids = sorted(n.id for n in graph.nodes.values() if n.kind in SQL_KINDS)
    has_catalog = any(n.in_catalog for n in graph.nodes.values())
    if schema is None and has_catalog:
        schema = catalog_schema(graph)
    dialect = sqlglot_dialect(graph.adapter_type)

    statuses: dict[str, ColumnLineageStatus] = {}
    for node_id in node_ids:
        node = graph.node(node_id)
        if node.kind not in SQL_KINDS:
            raise ValueError(
                f"{node_id} is a {node.kind.value}; only models and snapshots have SQL"
            )
        _clear(graph, node_id)

        text = sql.get(node_id)
        if node.language == "python":
            status = _unknown("Python models are not analysed")
        elif not text:
            status = _unknown("no compiled SQL; run dbt compile or dbt docs generate")
        elif schema is None:
            status = _unknown("no catalog.json; run dbt docs generate")
        else:
            status = _apply(graph, node_id, text, schema, dialect, max_chars)

        graph.column_lineage[node_id] = status
        if status.state is LineageState.UNKNOWN:
            graph.add_unresolved(
                Unresolved(
                    subject=node_id,
                    reference=_LINEAGE_REFERENCE,
                    reason=status.reason or "unknown",
                    evidence=_evidence(graph, node_id, text),
                )
            )
        statuses[node_id] = status
    return statuses


def _unknown(reason: str) -> ColumnLineageStatus:
    return ColumnLineageStatus(LineageState.UNKNOWN, reason)


def _clear(graph: MetadataGraph, node_id: str) -> None:
    """Remove the derives edges into a node's columns and its lineage records."""
    column_ids = [c.id for c in graph.columns_of(node_id)]
    for cid in column_ids:
        for edge in graph.in_edges(cid, (EdgeKind.DERIVES,)):
            graph.remove_edge(edge)
    graph.remove_unresolved([node_id, *column_ids], origin=Origin.COMPILED_SQL)
    graph.column_lineage.pop(node_id, None)


def _evidence(graph: MetadataGraph, node_id: str, sql: str | None) -> Evidence:
    node = graph.nodes[node_id]
    manifest = _manifest_version(graph, node_id)
    artifact = ArtifactVersion(
        path=node.compiled_path or (manifest.path if manifest else "manifest.json"),
        sha256=hashlib.sha256(sql.encode("utf-8")).hexdigest() if sql else None,
        generated_at=manifest.generated_at if manifest else None,
        dbt_version=manifest.dbt_version if manifest else None,
    )
    return Evidence(
        origin=Origin.COMPILED_SQL,
        confidence=Confidence.INFERRED,
        artifact=artifact,
        location=Location(node.file_path) if node.file_path else None,
    )


def _manifest_version(graph: MetadataGraph, node_id: str) -> ArtifactVersion | None:
    """The manifest version, from the node's materialises edge (any manifest edge would do)."""
    for edge in graph.out_edges(node_id, (EdgeKind.MATERIALISES,)) + graph.in_edges(
        node_id, (EdgeKind.DEPENDS_ON,)
    ):
        for evidence in edge.evidence:
            if evidence.origin is Origin.MANIFEST and evidence.artifact:
                return evidence.artifact
    return None


def _apply(
    graph: MetadataGraph,
    node_id: str,
    sql: str,
    schema: MappingSchema,
    dialect: str | None,
    max_chars: int,
) -> ColumnLineageStatus:
    traced = sql_column_lineage(sql, schema, dialect, max_chars)
    if traced.unknown:
        return _unknown(traced.reason or "unknown")

    adapter = graph.adapter_type
    evidence = _evidence(graph, node_id, sql)
    upstream = {e.upstream for e in graph.in_edges(node_id, (EdgeKind.DEPENDS_ON,))}
    unresolved = 0

    for output, sources in traced.columns.items():
        target = column_id(node_id, output, adapter)
        if target not in graph.columns:
            unresolved += 1
            graph.add_unresolved(
                Unresolved(
                    subject=node_id,
                    reference=output,
                    reason="the column is in the compiled SQL but not in the catalog or the "
                    "docs; run dbt docs generate",
                    evidence=evidence,
                )
            )
            continue
        for source in sources:
            found, reason = _resolve(graph, node_id, upstream, source.table, source.column)
            if found is None:
                unresolved += 1
                graph.add_unresolved(
                    Unresolved(
                        subject=target,
                        reference=_describe(source.table, source.column),
                        reason=reason,
                        evidence=evidence,
                    )
                )
            elif found != target:  # an incremental model reading itself
                graph.add_edge(
                    EdgeKind.DERIVES, found, target, evidence, transformation=source.transformation
                )

    if unresolved:
        return ColumnLineageStatus(
            LineageState.PARTIAL, f"{unresolved} column references did not resolve"
        )
    return ColumnLineageStatus(LineageState.RESOLVED)


def _resolve(
    graph: MetadataGraph, node_id: str, upstream: set[str], table: TableRef, column: str
) -> tuple[str | None, str]:
    """The graph column a traced source refers to, or None and why not."""
    relation = relation_id(_relation_name(table), graph.adapter_type)
    if relation is None or relation not in graph.nodes:
        return None, "the table is not part of the dbt project"
    candidates = graph.logical_nodes_for(relation)
    preferred = [n for n in candidates if n.id in upstream or n.id == node_id]
    chosen = preferred or candidates
    if len(chosen) != 1:
        names = ", ".join(sorted(n.id for n in chosen))
        return None, f"the table is materialised by several nodes: {names}"
    source = column_id(chosen[0].id, column, graph.adapter_type)
    if source not in graph.columns:
        return None, f"column {column!r} is not in {chosen[0].id}"
    return source, ""


def _relation_name(table: TableRef) -> str:
    return ".".join(
        '"' + name.replace('"', '""') + '"' if quoted else name for name, quoted in table.parts
    )


def _describe(table: TableRef, column: str) -> str:
    return f"{'.'.join(name for name, _ in table.parts)}.{column}"
