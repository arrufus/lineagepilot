"""Protocol 1, for a client that negotiated it (task 7.5, design section 9).

The server builds every answer in protocol 2. For a protocol 1 client, ``answer`` takes out what
protocol 1 does not have, so a client built against ``protocol/v1/`` never meets a key or a
value its validators would refuse:

- a node's ``tool`` and ``subkind``, and a review row's ``pipeline`` and ``exports``;
- an Airflow DAG's and task's kind, which protocol 1 wrote ``airflow_dag`` and ``airflow_task``;
- a pipeline, task or consumer of any other tool, and an edge of a kind protocol 1 did not have
  (``loads``, ``consumes`` and ``part_of``), which it cannot represent, so they are left out;
- the review's ``pipelines`` and ``consumers``.

The status is built for the client's version where it is made (``nodeKinds``, ``readers``), and
``lineage/airflow`` is protocol 1's shape in every version. Protocol 1 goes when protocol 3 comes.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from lineagepilot_core.graph.model import legacy_kind

V1_NODE_KINDS = frozenset(
    {
        "model",
        "seed",
        "snapshot",
        "source",
        "exposure",
        "relation",
        "airflow_dag",
        "airflow_task",
        "column",
    }
)
V1_EDGE_KINDS = frozenset(
    {"depends_on", "derives", "materialises", "orchestrates", "task_depends_on"}
)
# Keys protocol 2 added to a node or a review row
_NEW_KEYS = ("tool", "subkind", "pipeline", "exports")


class Unrepresentable(LookupError):
    """What was asked for is a node protocol 1 has no kind for."""

    def __init__(self, ref: str) -> None:
        super().__init__(ref)
        self.ref = ref


def node(value: dict[str, Any]) -> dict[str, Any] | None:
    """A node or a review row as protocol 1 wrote it, or None if protocol 1 had no such node."""
    kind = legacy_kind(value["kind"], value.get("tool"))
    if kind not in V1_NODE_KINDS:
        return None
    return {**{k: v for k, v in value.items() if k not in _NEW_KEYS}, "kind": kind}


def _nodes(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [n for n in (node(v) for v in values) if n is not None]


def _counts(nodes: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for n in nodes:
        counts[n["kind"]] = counts.get(n["kind"], 0) + 1
    return dict(sorted(counts.items()))


def _graph(result: dict[str, Any]) -> dict[str, Any]:
    nodes = _nodes(result["nodes"])
    kept = {n["id"] for n in nodes}
    if result["root"] not in kept:
        raise Unrepresentable(result["root"])
    edges = [
        e
        for e in result["edges"]
        if e["kind"] in V1_EDGE_KINDS and e["upstream"] in kept and e["downstream"] in kept
    ]
    return {**result, "nodes": nodes, "edges": edges}


def _impact(result: dict[str, Any]) -> dict[str, Any]:
    nodes = _nodes(result["nodes"])
    return {**result, "nodes": nodes, "counts": _counts(nodes)}


def _nodes_for_file(result: dict[str, Any]) -> dict[str, Any]:
    return {**result, "nodes": _nodes(result["nodes"])}


def _node(result: dict[str, Any]) -> dict[str, Any]:
    found = node(result["node"])
    if found is None:
        raise Unrepresentable(result["node"]["id"])
    return {**result, "node": found}


def _review(result: dict[str, Any]) -> dict[str, Any]:
    impacted = _nodes(result["impacted"])
    summary = {**result["summary"], "impacted": _counts(impacted)}
    kept = {k: v for k, v in result.items() if k not in ("pipelines", "consumers")}
    return {**kept, "impacted": impacted, "summary": summary}


_ANSWERS: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "lineage/graph": _graph,
    "lineage/impact": _impact,
    "lineage/nodesForFile": _nodes_for_file,
    "lineage/node": _node,
    "lineage/reviewChanges": _review,
}


def answer(method: str, result: dict[str, Any]) -> dict[str, Any]:
    """``result``, a protocol 2 answer to ``method``, as protocol 1 gave it. ``Unrepresentable``
    if the node asked about has no protocol 1 kind."""
    convert = _ANSWERS.get(method)
    return convert(result) if convert is not None else result
