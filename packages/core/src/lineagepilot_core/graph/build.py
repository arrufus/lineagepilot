"""Build a MetadataGraph from parsed dbt artifacts."""

from __future__ import annotations

from lineagepilot_core.graph.ids import column_id, relation_id
from lineagepilot_core.graph.model import (
    ArtifactVersion,
    Column,
    Confidence,
    EdgeKind,
    Evidence,
    GraphError,
    Location,
    MetadataGraph,
    Node,
    NodeKind,
    Origin,
    Unresolved,
)
from lineagepilot_core.parsers.base import ParseErrorSeverity, ParseResult, RawNode


class UntrustedParseResult(GraphError):
    """The parse result has errors, so a graph built from it would look falsely complete.

    The index must be shown as ``unknown`` instead (design section 6.1).
    """


def build_dbt_graph(result: ParseResult, graph: MetadataGraph | None = None) -> MetadataGraph:
    """Add the dbt nodes, columns, relations, edges and unresolved records of a parse.

    Every manifest fact gets ``manifest`` / ``declared`` evidence pointing at the manifest
    version and at the file of the node it concerns.
    """
    if result.has_errors:
        messages = "; ".join(
            e.message for e in result.errors if e.severity is ParseErrorSeverity.ERROR
        )
        raise UntrustedParseResult(f"the dbt artifacts could not be trusted: {messages}")

    graph = graph if graph is not None else MetadataGraph(adapter_type=result.adapter_type)
    adapter = result.adapter_type
    manifest = ArtifactVersion(
        path=result.manifest_path or "manifest.json",
        sha256=result.manifest_sha256,
        generated_at=result.generated_at,
        dbt_version=result.dbt_version,
    )
    graph.manifest = manifest

    def evidence(file_path: str | None) -> Evidence:
        return Evidence(
            origin=Origin.MANIFEST,
            confidence=Confidence.DECLARED,
            artifact=manifest,
            location=Location(file_path) if file_path else None,
        )

    for raw in result.nodes:
        node = graph.add_node(_node(raw, relation_id(raw.relation_name, adapter)))
        if node.relation_id:
            graph.add_node(
                Node(
                    id=node.relation_id,
                    kind=NodeKind.RELATION,
                    name=node.relation_id.rsplit(".", 1)[-1],
                    database=raw.database_name,
                    schema=raw.schema_name,
                    properties={"relation_name": raw.relation_name},
                )
            )
            graph.add_edge(
                EdgeKind.MATERIALISES, node.id, node.relation_id, evidence(node.file_path)
            )

    for raw_column in result.columns:
        cid = column_id(raw_column.node_id, raw_column.name, adapter)
        if cid in graph.columns:
            # Two spellings that fold to one name under the adapter's case rules
            graph.add_unresolved(
                Unresolved(
                    subject=raw_column.node_id,
                    reference=raw_column.name,
                    reason=f"column name collides with {graph.columns[cid].name!r} "
                    "under the adapter's case rules",
                    evidence=evidence(graph.nodes[raw_column.node_id].file_path),
                )
            )
            continue
        graph.add_column(
            Column(
                id=cid,
                node_id=raw_column.node_id,
                name=raw_column.name,
                data_type=raw_column.data_type,
                ordinal=raw_column.ordinal_position,
                description=raw_column.description,
                semantic_type=raw_column.semantic_type,
                pii_type=raw_column.pii_type,
                pii_detected_by=raw_column.pii_detected_by,
                tags=raw_column.tags,
                meta=raw_column.meta,
                test_count=raw_column.test_count,
                test_types=raw_column.test_types,
                stats=raw_column.stats,
                in_catalog=raw_column.in_catalog,
            )
        )

    for raw_edge in result.edges:
        downstream = graph.nodes[raw_edge.downstream_id]
        graph.add_edge(
            EdgeKind.DEPENDS_ON,
            raw_edge.upstream_id,
            raw_edge.downstream_id,
            evidence(downstream.file_path),
            transformation=raw_edge.transformation,
        )

    for raw_unresolved in result.unresolved:
        subject = graph.nodes.get(raw_unresolved.node_id)
        graph.add_unresolved(
            Unresolved(
                subject=raw_unresolved.node_id,
                reference=raw_unresolved.reference,
                reason=raw_unresolved.reason,
                evidence=evidence(subject.file_path if subject else None),
            )
        )

    return graph


def _node(raw: RawNode, relation: str | None) -> Node:
    return Node(
        id=raw.unique_id,
        kind=NodeKind(raw.resource_type),
        name=raw.name,
        package=raw.package_name,
        database=raw.database_name,
        schema=raw.schema_name,
        relation_id=relation,
        alias=raw.alias,
        version=raw.version,
        layer=raw.layer,
        layer_source=raw.layer_source,
        domain=raw.domain_name,
        description=raw.description,
        materialization=raw.materialization,
        owner=raw.owner,
        group=raw.group,
        tags=raw.tags,
        meta=raw.meta,
        test_count=raw.test_count,
        test_types=raw.test_types,
        file_path=raw.original_file_path,
        patch_path=raw.patch_path,
        properties=raw.properties,
        language=raw.language,
        compiled_path=raw.compiled_path,
        in_catalog=raw.in_catalog,
        primary_key=raw.primary_key,
    )
