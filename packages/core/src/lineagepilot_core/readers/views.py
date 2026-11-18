"""Pipelines, consumers and readers, for any tool: protocol 2's views (task 7.5).

Plain JSON in the shapes of the protocol's ``PipelinesResult``, ``ConsumersResult`` and
``ReaderSummary``, apart from each pipeline's, task's and consumer's ``location``, which the
language server fills in. ``readers.airflow.view`` keeps protocol 1's Airflow view.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from lineagepilot_core.graph.ids import split_column_id
from lineagepilot_core.graph.model import (
    PIPELINE_KINDS,
    Confidence,
    Edge,
    EdgeKind,
    MetadataGraph,
    Node,
    NodeKind,
)
from lineagepilot_core.readers.registry import READERS

_STRENGTH = (Confidence.DECLARED, Confidence.INFERRED, Confidence.HEURISTIC)
# Free text from the workspace is cut to this many characters (design section 9)
_TEXT_LIMIT = 4000
# What a task does to a dbt node: runs it, or writes the table a source reads
_TASK_EDGES = (EdgeKind.ORCHESTRATES, EdgeKind.LOADS)


def _clip(text: str | None) -> str | None:
    if text is None or len(text) <= _TEXT_LIMIT:
        return text
    return text[: _TEXT_LIMIT - 1] + "…"


def reader_summaries(
    graph: MetadataGraph | None, kinds: Iterable[NodeKind]
) -> list[dict[str, Any]]:
    """What each reader that adds nodes of these kinds read into ``graph``, by tool."""
    wanted = frozenset(kinds)
    return [
        READERS[tool].summary_json(graph)
        for tool in sorted(READERS)
        if READERS[tool].kinds & wanted
    ]


# Pipelines


def pipelines_view(
    graph: MetadataGraph, nodes: Iterable[str] | None = None, tools: Iterable[str] | None = None
) -> dict[str, Any]:
    """Every pipeline and task, or with ``nodes`` only those that run or load one of them directly;
    with ``tools``, only those tools' (the Pipelines view)."""
    wanted = None if nodes is None else set(nodes)
    allowed = None if tools is None else set(tools)
    pipelines: dict[str, dict[str, Any]] = {}
    for node in sorted(_pipeline_nodes(graph, allowed), key=lambda n: n.id):
        edges = graph.out_edges(node.id, _TASK_EDGES)
        touching = edges if wanted is None else [e for e in edges if e.downstream in wanted]
        if wanted is not None and not touching:
            continue
        pipeline_id = _pipeline_of(node)
        pipeline = pipelines.setdefault(pipeline_id, _pipeline_json(graph, pipeline_id, node))
        if node.kind is NodeKind.TASK or touching:
            pipeline["tasks"].append(_task_json(node, touching))
    origins = frozenset(
        origin
        for tool, reader in READERS.items()
        if reader.kinds & PIPELINE_KINDS and (allowed is None or tool in allowed)
        for origin in reader.origins
    )
    unknowns = sorted(
        (
            {
                "subject": _clip(u.subject),
                "reference": _clip(u.reference),
                "reason": _clip(u.reason),
            }
            for u in graph.unresolved
            if u.evidence.origin in origins
        ),
        key=lambda u: (u["subject"], u["reference"], u["reason"]),
    )
    return {"pipelines": [pipelines[p] for p in sorted(pipelines)], "unknowns": unknowns}


def _pipeline_nodes(graph: MetadataGraph, tools: set[str] | None) -> list[Node]:
    return [
        n
        for n in graph.nodes.values()
        if n.kind in PIPELINE_KINDS and n.tool is not None and (tools is None or n.tool in tools)
    ]


def _pipeline_of(node: Node) -> str:
    """The pipeline a task is in, by its ID: ``<tool>:<pipeline>``, as Airflow's DAG IDs are."""
    if node.kind is NodeKind.PIPELINE:
        return node.id
    name = node.properties.get("pipeline") or node.properties.get("dag")
    if not name:  # read it from the task's ID, <tool>:<pipeline>.<task>
        prefix, suffix = f"{node.tool}:", f".{node.name}"
        inside = node.id.startswith(prefix) and node.id.endswith(suffix)
        name = node.id[len(prefix) : -len(suffix)] if inside else ""
    return f"{node.tool}:{name or node.name}"


def _pipeline_json(graph: MetadataGraph, pipeline_id: str, member: Node) -> dict[str, Any]:
    node = graph.nodes.get(pipeline_id)
    return {
        "id": pipeline_id,
        "tool": member.tool,
        "subkind": node.subkind if node else None,
        "name": _clip(node.name if node else pipeline_id.partition(":")[2]),
        "owner": _clip(node.owner) if node else None,
        "location": None,
        "tasks": [],
    }


def _task_json(node: Node, edges: list[Edge]) -> dict[str, Any]:
    tasks = node.properties.get("tasks", {})
    targets = sorted(e.downstream for e in edges)
    derived = sorted({t for target in targets for t in tasks.get(target, ())})
    return {
        "id": node.id,
        "kind": node.kind_name,
        "tool": node.tool,
        "subkind": node.subkind,
        "name": _clip(node.name),
        "operator": _clip(node.properties.get("operator")),
        "owner": _clip(node.owner),
        "how": sorted({_clip(e.transformation) for e in edges if e.transformation}),
        "nodes": targets,
        "derivedTasks": [_clip(t) for t in derived],
        "confidence": _weakest(edges),
        "origins": sorted({ev.origin.value for e in edges for ev in e.evidence})[:5],
        "notes": [_clip(n) for n in node.properties.get("notes", ())],
        "location": None,
    }


def _weakest(edges: list[Edge]) -> str | None:
    confidences = [e.confidence for e in edges]
    return max(confidences, key=_STRENGTH.index).value if confidences else None


# Consumers


def consumers_view(graph: MetadataGraph, nodes: Iterable[str]) -> dict[str, Any]:
    """What reads each of ``nodes`` directly, and how: the whole node, or which of its columns
    (the Consumers view). Through a view to an explore is the graph's job, not this one's."""
    reads: dict[str, dict[str, dict[str, Any]]] = {}

    def read(consumer: str, node_id: str, edge: Edge, column: str | None) -> None:
        entry = reads.setdefault(consumer, {}).setdefault(node_id, {"columns": set(), "edges": []})
        entry["edges"].append(edge)
        if column is not None:
            entry["columns"].add(column)

    for node_id in dict.fromkeys(nodes):
        for edge in graph.in_edges(node_id, (EdgeKind.CONSUMES,)):
            if edge.upstream in graph.nodes:
                read(edge.upstream, node_id, edge, None)
        for column in graph.columns_of(node_id):
            for edge in graph.in_edges(column.id, (EdgeKind.CONSUMES,)):
                owner = (
                    graph.columns[edge.upstream].node_id if edge.upstream in graph.columns else None
                )
                consumer = owner or edge.upstream
                if consumer in graph.nodes:
                    read(consumer, node_id, edge, split_column_id(column.id)[1])
    consumers = []
    for consumer_id in sorted(reads):
        node = graph.nodes[consumer_id]
        consumers.append(
            {
                "id": node.id,
                "tool": node.tool or node.kind_name,
                "subkind": node.subkind,
                "name": _clip(node.name),
                "owner": _clip(node.owner),
                "location": None,
                "reads": [
                    _read_json(node_id, entry)
                    for node_id, entry in sorted(reads[consumer_id].items())
                ],
            }
        )
    return {"consumers": consumers}


def _read_json(node_id: str, entry: dict[str, Any]) -> dict[str, Any]:
    edges: list[Edge] = entry["edges"]
    strongest = min((e.confidence for e in edges), key=_STRENGTH.index)
    return {
        "node": node_id,
        "columns": sorted(_clip(c) or "" for c in entry["columns"]),
        "confidence": strongest.value,
        "origins": sorted({ev.origin.value for e in edges for ev in e.evidence})[:5],
    }
