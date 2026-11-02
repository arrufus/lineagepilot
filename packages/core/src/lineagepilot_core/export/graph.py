"""Graph exports: JSON, Mermaid, DOT, GraphML and Cypher (design section 7, task 1.7).

Ported from data-capsule's ``services/graph_export.py`` (commit 5e57fcc) with the
escaping fixed before export ships (design section 10): node IDs in Mermaid and DOT are
generated (``n0``, ``n1``, ...), labels are escaped per format, GraphML is built with
ElementTree, and Cypher uses JSON-escaped literals with labels and relationship types
from fixed lists only. Output is deterministic: the same graph exports byte for byte the
same.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from enum import Enum
from xml.etree import ElementTree as ET

from lineagepilot_core.export.escape import cypher_string, dot_string, mermaid_label, xml_text
from lineagepilot_core.graph.model import EdgeKind, MetadataGraph, NodeKind

FORMAT_VERSION = 1


class ExportFormat(str, Enum):
    JSON = "json"
    MERMAID = "mermaid"
    DOT = "dot"
    GRAPHML = "graphml"
    CYPHER = "cypher"

    @property
    def extension(self) -> str:
        return _EXTENSIONS[self]


_EXTENSIONS = {
    ExportFormat.JSON: "json",
    ExportFormat.MERMAID: "mmd",
    ExportFormat.DOT: "dot",
    ExportFormat.GRAPHML: "graphml",
    ExportFormat.CYPHER: "cypher",
}


@dataclass(frozen=True, slots=True)
class ExportNode:
    id: str
    kind: str
    name: str
    layer: str | None = None
    domain: str | None = None
    owner: str | None = None
    file_path: str | None = None


@dataclass(frozen=True, slots=True)
class ExportEdge:
    kind: str
    upstream: str
    downstream: str
    confidence: str
    transformation: str | None = None


@dataclass
class GraphView:
    nodes: list[ExportNode] = field(default_factory=list)
    edges: list[ExportEdge] = field(default_factory=list)


# Exported edge kinds; materialises is left out unless relations are included
_NODE_EDGES = (EdgeKind.DEPENDS_ON, EdgeKind.ORCHESTRATES, EdgeKind.TASK_DEPENDS_ON)
_HAS_COLUMN = "has_column"


def graph_view(
    graph: MetadataGraph,
    node_ids: Iterable[str] | None = None,
    columns: bool = False,
    relations: bool = False,
) -> GraphView:
    """The nodes (all, or ``node_ids``) and the edges between them, ready to export.

    With ``columns``, each node's columns are nodes too, joined to it by ``has_column``
    edges, with ``derives`` edges between them. With ``relations``, physical relations
    and ``materialises`` edges are included.
    """
    wanted = set(node_ids) if node_ids is not None else set(graph.nodes)
    for node_id in wanted:
        graph.node(node_id)  # raises KeyError for an unknown ID
    if not relations:
        wanted = {n for n in wanted if graph.nodes[n].kind is not NodeKind.RELATION}
    else:
        wanted |= {graph.nodes[n].relation_id for n in wanted if graph.nodes[n].relation_id}

    view = GraphView()
    for node_id in sorted(wanted):
        node = graph.nodes[node_id]
        view.nodes.append(
            ExportNode(
                id=node.id,
                kind=node.kind.value,
                name=node.name,
                layer=node.layer,
                domain=node.domain,
                owner=node.owner,
                file_path=node.file_path,
            )
        )
    kinds = _NODE_EDGES + ((EdgeKind.MATERIALISES,) if relations else ())
    for edge in graph.edges:
        if edge.kind in kinds and edge.upstream in wanted and edge.downstream in wanted:
            view.edges.append(_edge(edge.kind.value, edge.upstream, edge.downstream, edge))

    if columns:
        column_ids: set[str] = set()
        for node_id in sorted(wanted):
            for column in graph.columns_of(node_id):
                column_ids.add(column.id)
                view.nodes.append(ExportNode(id=column.id, kind="column", name=column.name))
                view.edges.append(ExportEdge(_HAS_COLUMN, node_id, column.id, "declared"))
        for edge in graph.edges:
            if edge.kind is EdgeKind.DERIVES and {edge.upstream, edge.downstream} <= column_ids:
                view.edges.append(_edge(edge.kind.value, edge.upstream, edge.downstream, edge))

    view.edges.sort(key=lambda e: (e.kind, e.upstream, e.downstream))
    return view


def _edge(kind: str, upstream: str, downstream: str, edge) -> ExportEdge:
    return ExportEdge(kind, upstream, downstream, edge.confidence.value, edge.transformation)


def export_graph(view: GraphView, fmt: ExportFormat | str) -> str:
    fmt = ExportFormat(fmt)
    return {
        ExportFormat.JSON: to_json,
        ExportFormat.MERMAID: to_mermaid,
        ExportFormat.DOT: to_dot,
        ExportFormat.GRAPHML: to_graphml,
        ExportFormat.CYPHER: to_cypher,
    }[fmt](view)


def to_json(view: GraphView) -> str:
    return (
        json.dumps(
            {
                "format": "lineagepilot-graph",
                "version": FORMAT_VERSION,
                "nodes": [asdict(n) for n in view.nodes],
                "edges": [asdict(e) for e in view.edges],
            },
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        )
        + "\n"
    )


def _short_ids(view: GraphView) -> dict[str, str]:
    return {node.id: f"n{i}" for i, node in enumerate(view.nodes)}


def _group(view: GraphView) -> dict[str, list[ExportNode]]:
    groups: dict[str, list[ExportNode]] = {}
    for node in view.nodes:
        groups.setdefault(node.layer or node.kind, []).append(node)
    return dict(sorted(groups.items()))


_MERMAID_SHAPES = {
    "source": ("[(", ")]"),
    "seed": ("[(", ")]"),
    "exposure": ("{{", "}}"),
    "airflow_task": ("[[", "]]"),
    "airflow_dag": ("[[", "]]"),
    "column": (">", "]"),
}
_MERMAID_ARROWS = {"derives": "-.->", "has_column": "---", "materialises": "-.->"}


def to_mermaid(view: GraphView) -> str:
    """A Mermaid flowchart. IDs are generated; names appear only in escaped labels."""
    ids = _short_ids(view)
    lines = ["flowchart LR"]
    for index, (group, nodes) in enumerate(_group(view).items()):
        lines.append(f"  subgraph s{index}[{mermaid_label(group)}]")
        for node in nodes:
            opening, closing = _MERMAID_SHAPES.get(node.kind, ("[", "]"))
            lines.append(f"    {ids[node.id]}{opening}{mermaid_label(node.name)}{closing}")
        lines.append("  end")
    for edge in view.edges:
        arrow = _MERMAID_ARROWS.get(edge.kind, "-->")
        lines.append(f"  {ids[edge.upstream]} {arrow} {ids[edge.downstream]}")
    return "\n".join(lines) + "\n"


_DOT_SHAPES = {
    "source": "cylinder",
    "seed": "cylinder",
    "exposure": "hexagon",
    "airflow_task": "component",
    "airflow_dag": "component",
    "column": "plaintext",
    "relation": "cylinder",
}


def to_dot(view: GraphView) -> str:
    """A Graphviz digraph. IDs are generated; every string is quoted and escaped."""
    ids = _short_ids(view)
    lines = ["digraph lineage {", "  rankdir=LR;", "  node [shape=box];"]
    for index, (group, nodes) in enumerate(_group(view).items()):
        lines.append(f"  subgraph cluster_{index} {{")
        lines.append(f"    label={dot_string(group)};")
        for node in nodes:
            shape = _DOT_SHAPES.get(node.kind, "box")
            lines.append(
                f"    {ids[node.id]} [label={dot_string(node.name)}, "
                f"tooltip={dot_string(node.id)}, shape={shape}];"
            )
        lines.append("  }")
    for edge in view.edges:
        style = "dashed" if edge.kind in ("derives", "materialises", "has_column") else "solid"
        lines.append(
            f"  {ids[edge.upstream]} -> {ids[edge.downstream]} "
            f"[label={dot_string(edge.kind)}, style={style}];"
        )
    lines.append("}")
    return "\n".join(lines) + "\n"


_GRAPHML_NS = "http://graphml.graphdrawing.org/xmlns"
_NODE_KEYS = ("name", "kind", "layer", "domain", "owner", "file_path")
_EDGE_KEYS = ("kind", "confidence", "transformation")


def to_graphml(view: GraphView) -> str:
    """GraphML built with ElementTree, which escapes markup; invalid XML characters are
    removed first."""
    ET.register_namespace("", _GRAPHML_NS)
    root = ET.Element(f"{{{_GRAPHML_NS}}}graphml")
    for key in _NODE_KEYS:
        ET.SubElement(
            root,
            f"{{{_GRAPHML_NS}}}key",
            {"id": f"n_{key}", "for": "node", "attr.name": key, "attr.type": "string"},
        )
    for key in _EDGE_KEYS:
        ET.SubElement(
            root,
            f"{{{_GRAPHML_NS}}}key",
            {"id": f"e_{key}", "for": "edge", "attr.name": key, "attr.type": "string"},
        )
    graph = ET.SubElement(root, f"{{{_GRAPHML_NS}}}graph", {"id": "G", "edgedefault": "directed"})
    for node in view.nodes:
        element = ET.SubElement(graph, f"{{{_GRAPHML_NS}}}node", {"id": xml_text(node.id)})
        for key in _NODE_KEYS:
            value = getattr(node, key)
            if value is not None:
                data = ET.SubElement(element, f"{{{_GRAPHML_NS}}}data", {"key": f"n_{key}"})
                data.text = xml_text(value)
    for index, edge in enumerate(view.edges):
        element = ET.SubElement(
            graph,
            f"{{{_GRAPHML_NS}}}edge",
            {
                "id": f"e{index}",
                "source": xml_text(edge.upstream),
                "target": xml_text(edge.downstream),
            },
        )
        for key in _EDGE_KEYS:
            value = getattr(edge, key)
            if value is not None:
                data = ET.SubElement(element, f"{{{_GRAPHML_NS}}}data", {"key": f"e_{key}"})
                data.text = xml_text(value)
    ET.indent(root)
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="unicode") + "\n"


# Labels and relationship types come only from these maps, never from the data (LP-auw)
_CYPHER_LABELS = {
    "model": "Model",
    "seed": "Seed",
    "snapshot": "Snapshot",
    "source": "Source",
    "exposure": "Exposure",
    "relation": "Relation",
    "airflow_dag": "AirflowDag",
    "airflow_task": "AirflowTask",
    "column": "Column",
}
_CYPHER_RELS = {
    "depends_on": "FEEDS",
    "derives": "DERIVES",
    "materialises": "MATERIALISES",
    "orchestrates": "ORCHESTRATES",
    "task_depends_on": "TRIGGERS",
    "has_column": "HAS_COLUMN",
}
_CYPHER_NODE_PROPS = ("name", "layer", "domain", "owner", "file_path")


def to_cypher(view: GraphView) -> str:
    """A Cypher script (for cypher-shell or Neo4j Browser).

    Every value is a JSON-escaped string literal. Labels and relationship types are
    fixed names; a kind without one is skipped, never interpolated. ``FEEDS`` edges
    point the way data flows, upstream to downstream.
    """
    statements = ["CREATE CONSTRAINT lineagepilot_id IF NOT EXISTS "
                  "FOR (n:LineagePilot) REQUIRE n.id IS UNIQUE"]  # fmt: skip
    for node in view.nodes:
        label = _CYPHER_LABELS.get(node.kind)
        if label is None:
            continue
        props = [
            f"{key}: {cypher_string(getattr(node, key))}"
            for key in _CYPHER_NODE_PROPS
            if getattr(node, key) is not None
        ]
        statements.append(
            f"MERGE (n:LineagePilot {{id: {cypher_string(node.id)}}}) "
            f"SET n:{label}, n += {{{', '.join(props)}}}"
        )
    for edge in view.edges:
        rel = _CYPHER_RELS.get(edge.kind)
        if rel is None:
            continue
        props = [f"confidence: {cypher_string(edge.confidence)}"]
        if edge.transformation is not None:
            props.append(f"transformation: {cypher_string(edge.transformation)}")
        statements.append(
            f"MATCH (a:LineagePilot {{id: {cypher_string(edge.upstream)}}}), "
            f"(b:LineagePilot {{id: {cypher_string(edge.downstream)}}}) "
            f"MERGE (a)-[:{rel} {{{', '.join(props)}}}]->(b)"
        )
    return ";\n".join(statements) + ";\n"
