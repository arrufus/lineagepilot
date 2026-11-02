"""Upstream and downstream traversal at model and column level (task 1.4).

A step from one node or column to the next is one edge, or two at a physical table: a
dbt source reads the table that a model, seed or snapshot builds, and both have a
``materialises`` edge into it. So a source is downstream of its table's builders, and
the junction counts as one step. Physical relations are never reported as reached.

Downstream of a dbt node also includes the Airflow tasks that run it (``orchestrates``
edges point from the task to the node).

Traversal is breadth first, keeps the smallest depth for each node, and visits each node
once, so cycles (impossible in a dbt DAG, possible in a crafted manifest or among Airflow
tasks) cannot make it loop.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from enum import Enum

from lineagepilot_core.graph.ids import split_column_id
from lineagepilot_core.graph.model import (
    Edge,
    EdgeKind,
    MetadataGraph,
    NodeKind,
)


class Direction(str, Enum):
    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"


class Granularity(str, Enum):
    MODEL = "model"
    COLUMN = "column"


@dataclass(frozen=True, slots=True)
class Step:
    """One step to ``ref``, over one edge or, through a physical table, two."""

    ref: str
    edges: tuple[Edge, ...]


@dataclass(slots=True)
class Reached:
    ref: str
    depth: int
    # Every step into this ref from another reached ref, from any depth
    via: list[tuple[str, Step]] = field(default_factory=list)


@dataclass
class Traversal:
    start: tuple[str, ...]
    direction: Direction
    granularity: Granularity
    # Reached refs in breadth-first order, the start refs first at depth 0
    reached: dict[str, Reached] = field(default_factory=dict)
    # True if a depth or size cap stopped the traversal before it was complete
    truncated: bool = False

    def at_depth(self, depth: int) -> list[str]:
        return [r.ref for r in self.reached.values() if r.depth == depth]

    def found(self) -> list[str]:
        """Everything reached except the start refs."""
        starts = set(self.start)
        return [ref for ref in self.reached if ref not in starts]


def steps(
    graph: MetadataGraph, ref: str, direction: Direction, granularity: Granularity
) -> Iterator[Step]:
    """The next steps from a node (model granularity) or a column (column granularity)."""
    if granularity is Granularity.COLUMN:
        yield from _column_steps(graph, ref, direction)
    else:
        yield from _node_steps(graph, ref, direction)


def _node_steps(graph: MetadataGraph, node_id: str, direction: Direction) -> Iterator[Step]:
    if direction is Direction.DOWNSTREAM:
        for edge in graph.out_edges(node_id, (EdgeKind.DEPENDS_ON,)):
            yield Step(edge.downstream, (edge,))
        for reader, edges in _junction(graph, node_id, direction):
            yield Step(reader, edges)
        for edge in graph.in_edges(node_id, (EdgeKind.ORCHESTRATES,)):
            yield Step(edge.upstream, (edge,))
    else:
        for edge in graph.in_edges(node_id, (EdgeKind.DEPENDS_ON,)):
            yield Step(edge.upstream, (edge,))
        for builder, edges in _junction(graph, node_id, direction):
            yield Step(builder, edges)


def _column_steps(graph: MetadataGraph, column: str, direction: Direction) -> Iterator[Step]:
    if direction is Direction.DOWNSTREAM:
        for edge in graph.out_edges(column, (EdgeKind.DERIVES,)):
            yield Step(edge.downstream, (edge,))
    else:
        for edge in graph.in_edges(column, (EdgeKind.DERIVES,)):
            yield Step(edge.upstream, (edge,))
    node_id, name = split_column_id(column)
    for other, edges in _junction(graph, node_id, direction):
        twin = f"{other}#{name}"  # the column ID is already folded to the adapter's rules
        if twin in graph.columns:
            yield Step(twin, edges)


def _junction(
    graph: MetadataGraph, node_id: str, direction: Direction
) -> Iterator[tuple[str, tuple[Edge, ...]]]:
    """Across a physical table: builders to the sources that read it, or back."""
    node = graph.nodes.get(node_id)
    if node is None or node.relation_id is None:
        return
    is_source = node.kind is NodeKind.SOURCE
    if direction is Direction.DOWNSTREAM and is_source:
        return  # a source reads its table; it does not build it
    if direction is Direction.UPSTREAM and not is_source:
        return
    own = graph.edge(EdgeKind.MATERIALISES, node_id, node.relation_id)
    for edge in graph.in_edges(node.relation_id, (EdgeKind.MATERIALISES,)):
        other = graph.nodes[edge.upstream]
        if other.id == node_id or (other.kind is NodeKind.SOURCE) == is_source:
            continue
        yield other.id, (own, edge) if own else (edge,)


def traverse(
    graph: MetadataGraph,
    start: Iterable[str],
    direction: Direction,
    granularity: Granularity = Granularity.MODEL,
    max_depth: int | None = None,
    max_nodes: int | None = None,
) -> Traversal:
    """Breadth-first traversal from ``start`` (node IDs, or column IDs at column level)."""
    starts = tuple(dict.fromkeys(start))
    known = graph.columns if granularity is Granularity.COLUMN else graph.nodes
    for ref in starts:
        if ref not in known:
            raise KeyError(f"unknown {granularity.value}: {ref}")

    result = Traversal(starts, direction, granularity)
    queue: deque[str] = deque()
    for ref in starts:
        result.reached[ref] = Reached(ref, 0)
        queue.append(ref)

    while queue:
        ref = queue.popleft()
        depth = result.reached[ref].depth
        for step in sorted(steps(graph, ref, direction, granularity), key=_step_order):
            seen = result.reached.get(step.ref)
            if seen is not None:
                if step.ref not in starts:
                    seen.via.append((ref, step))
                continue
            if max_depth is not None and depth >= max_depth:
                result.truncated = True
                continue
            if max_nodes is not None and len(result.reached) >= max_nodes:
                result.truncated = True
                continue
            result.reached[step.ref] = Reached(step.ref, depth + 1, [(ref, step)])
            queue.append(step.ref)
    return result


def _step_order(step: Step) -> tuple[str, str]:
    return (step.ref, step.edges[0].kind.value)
