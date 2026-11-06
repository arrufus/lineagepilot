"""The DAGs and tasks touching some nodes, or all of them: the Airflow tree view (design section 7).

Plain JSON in the shape of the protocol's ``AirflowResult``, apart from the ``location`` each DAG
and task gets from the language server.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from lineagepilot_core.airflow.link import AIRFLOW_KINDS, AIRFLOW_ORIGINS
from lineagepilot_core.graph.model import (
    AirflowSummary,
    Confidence,
    Edge,
    EdgeKind,
    MetadataGraph,
    Node,
    NodeKind,
)

_STRENGTH = (Confidence.DECLARED, Confidence.INFERRED, Confidence.HEURISTIC)
# Free text from the workspace is cut to this many characters (design section 9)
_TEXT_LIMIT = 4000


def _clip(text: str | None) -> str | None:
    if text is None or len(text) <= _TEXT_LIMIT:
        return text
    return text[: _TEXT_LIMIT - 1] + "…"


def summary_json(summary: AirflowSummary | None) -> dict[str, Any]:
    if summary is None:
        return {
            "analysed": False,
            "reason": "the DAG files were not analysed",
            "dagFiles": 0,
            "dags": 0,
            "tasks": 0,
            "links": 0,
            "unknowns": 0,
            "opaqueTasks": 0,
            "folders": [],
            "problems": [],
        }
    return {
        "analysed": summary.analysed,
        "reason": _clip(summary.reason),
        "dagFiles": summary.dag_files,
        "dags": summary.dags,
        "tasks": summary.tasks,
        "links": summary.links,
        "unknowns": summary.unknowns,
        "opaqueTasks": summary.opaque_tasks,
        "folders": [_clip(f) for f in summary.folders],
        "problems": [_clip(p) for p in summary.problems],
    }


def airflow_view(graph: MetadataGraph, nodes: Iterable[str] | None = None) -> dict[str, Any]:
    """Every DAG and task, or with ``nodes`` only those that run or read one of them directly."""
    wanted = None if nodes is None else set(nodes)
    dags: dict[str, dict[str, Any]] = {}
    for node in sorted(_airflow_nodes(graph), key=lambda n: n.id):
        dag_id = node.properties.get("dag") or node.name
        edges = graph.out_edges(node.id, (EdgeKind.ORCHESTRATES,))
        touching = edges if wanted is None else [e for e in edges if e.downstream in wanted]
        if wanted is not None and not touching:
            continue
        dag = dags.setdefault(dag_id, _dag_json(graph, dag_id))
        if node.kind is NodeKind.AIRFLOW_TASK or touching:
            dag["tasks"].append(_task_json(node, touching))
    return {
        "dags": [dags[d] for d in sorted(dags)],
        "unknowns": sorted(
            (
                {
                    "subject": _clip(u.subject),
                    "reference": _clip(u.reference),
                    "reason": _clip(u.reason),
                }
                for u in graph.unresolved
                if u.evidence.origin in AIRFLOW_ORIGINS
            ),
            key=lambda u: (u["subject"], u["reference"], u["reason"]),
        ),
        "summary": summary_json(graph.airflow),
    }


def _airflow_nodes(graph: MetadataGraph) -> list[Node]:
    return [n for n in graph.nodes.values() if n.kind in AIRFLOW_KINDS]


def _dag_json(graph: MetadataGraph, dag_id: str) -> dict[str, Any]:
    node = graph.nodes.get(f"airflow:{dag_id}")
    return {
        "id": f"airflow:{dag_id}",
        "name": _clip(dag_id),
        "owner": _clip(node.owner) if node else None,
        "tasks": [],
    }


def _task_json(node: Node, edges: list[Edge]) -> dict[str, Any]:
    tasks = node.properties.get("tasks", {})
    targets = sorted(e.downstream for e in edges)
    derived = sorted({t for target in targets for t in tasks.get(target, ())})
    confidences = [e.confidence for e in edges]
    weakest = max(confidences, key=_STRENGTH.index) if confidences else None
    return {
        "id": node.id,
        "kind": node.kind.value,
        "name": _clip(node.name),
        "operator": _clip(node.properties.get("operator")),
        "owner": _clip(node.owner),
        "how": sorted({_clip(e.transformation) for e in edges if e.transformation}),
        "nodes": targets,
        "derivedTasks": [_clip(t) for t in derived],
        "confidence": weakest.value if weakest else None,
        "origins": sorted({ev.origin.value for e in edges for ev in e.evidence}),
        "notes": [_clip(n) for n in node.properties.get("notes", ())],
    }
