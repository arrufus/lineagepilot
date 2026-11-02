"""Downstream impact of changing nodes or columns (design section 8, step 4; task 1.4).

From a changed column, impact follows ``derives`` edges wherever a downstream model's
column lineage is known (``resolved`` or ``partial``). Where it is not (``unknown``, or
not computed yet), the whole downstream model is impacted at model level, marked
``inferred``, and everything below it too. Exposures and Airflow tasks have no columns,
so they are impacted when any column of a node they use or run is.

From a changed node, impact is model level throughout.

Each impacted node carries a confidence: the strongest, over the paths that reach it, of
the weakest evidence on the path. Model-level fallback steps count as ``inferred`` at
best. The unknowns met on the way (unresolved references, and models whose column
lineage is unknown) are listed with the result, never dropped.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from lineagepilot_core.analysis.traversal import Direction, Granularity, Step, steps
from lineagepilot_core.graph.ids import split_column_id
from lineagepilot_core.graph.model import (
    ColumnLineageStatus,
    Confidence,
    Edge,
    LineageState,
    MetadataGraph,
    NodeKind,
    Unresolved,
)

_STRENGTH = (Confidence.DECLARED, Confidence.INFERRED, Confidence.HEURISTIC)
_SQL_KINDS = frozenset({NodeKind.MODEL, NodeKind.SNAPSHOT})


def weakest(*levels: Confidence) -> Confidence:
    return max(levels, key=_STRENGTH.index)


def strongest(*levels: Confidence) -> Confidence:
    return min(levels, key=_STRENGTH.index)


@dataclass(slots=True)
class ImpactedNode:
    id: str
    kind: NodeKind
    depth: int
    # "column" if only reached through column lineage, "model" if reached at model level
    granularity: Granularity
    confidence: Confidence
    # Impacted columns of this node (column granularity), sorted
    columns: list[str] = field(default_factory=list)
    # Why a model-level fallback was used, when it was
    reason: str | None = None
    # (from ref, step) pairs that reached this node or its columns
    via: list[tuple[str, Step]] = field(default_factory=list)


@dataclass
class Impact:
    targets: tuple[str, ...]
    nodes: dict[str, ImpactedNode] = field(default_factory=dict)
    # Unresolved records on the targets and on everything impacted
    unknowns: list[Unresolved] = field(default_factory=list)
    truncated: bool = False

    def counts(self) -> dict[str, int]:
        """Impacted nodes per kind, for summaries such as CodeLens."""
        counts: dict[str, int] = {}
        for node in self.nodes.values():
            counts[node.kind.value] = counts.get(node.kind.value, 0) + 1
        return dict(sorted(counts.items()))


@dataclass(slots=True)
class _Hit:
    depth: int
    fallback: str | None = None  # the fallback reason, for model-level hits from a column
    via: list[tuple[str, Step, bool]] = field(default_factory=list)  # (from, step, fallback)


def impact(
    graph: MetadataGraph,
    targets: Iterable[str],
    max_depth: int | None = None,
    column_lineage: Callable[[str], ColumnLineageStatus | None] | None = None,
) -> Impact:
    """Everything downstream of the changed ``targets`` (node IDs or column IDs).

    Args:
        max_depth: stop after this many steps; ``truncated`` says if anything was cut
        column_lineage: returns a model's column-lineage status, computing it if needed.
            Defaults to ``graph.column_lineage.get``, so models not yet computed fall
            back to model level.
    """
    targets = tuple(dict.fromkeys(targets))
    for ref in targets:
        if ref not in graph.nodes and ref not in graph.columns:
            raise KeyError(f"unknown node or column: {ref}")
    status_of = column_lineage or graph.column_lineage.get

    hits: dict[str, _Hit] = {ref: _Hit(0) for ref in targets}
    truncated = False
    queue = deque(targets)

    def visit(origin: str, step: Step, depth: int, fallback: str | None = None) -> None:
        nonlocal truncated
        hit = hits.get(step.ref)
        if hit is not None:
            if step.ref not in targets:
                hit.via.append((origin, step, fallback is not None))
            return
        if max_depth is not None and depth > max_depth:
            truncated = True
            return
        hits[step.ref] = _Hit(depth, fallback, [(origin, step, fallback is not None)])
        queue.append(step.ref)

    while queue:
        ref = queue.popleft()
        depth = hits[ref].depth + 1
        if ref in graph.columns:
            node_id, _ = split_column_id(ref)
            model_steps = sorted(
                steps(graph, node_id, Direction.DOWNSTREAM, Granularity.MODEL), key=_order
            )
            # Ask first: a status callable may compute a model's column lineage on demand,
            # adding the derives edges that the column steps below follow
            statuses = {
                s.ref: status_of(s.ref)
                for s in model_steps
                if graph.nodes[s.ref].kind in _SQL_KINDS
            }
            column_steps = sorted(
                steps(graph, ref, Direction.DOWNSTREAM, Granularity.COLUMN), key=_order
            )
            for step in column_steps:
                visit(ref, step, depth)
            reached_by_column = {split_column_id(s.ref)[0] for s in column_steps}
            for step in model_steps:
                downstream = graph.nodes[step.ref]
                if downstream.kind in _SQL_KINDS:
                    status = statuses[step.ref]
                    if status and status.state is not LineageState.UNKNOWN:
                        continue  # its derives edges say exactly which columns are hit
                    reason = (
                        f"column lineage unknown: {status.reason}"
                        if status
                        else "column lineage not computed"
                    )
                    visit(ref, step, depth, reason)
                elif downstream.kind is NodeKind.SOURCE:
                    if step.ref not in reached_by_column:
                        visit(ref, step, depth, "the source does not list this column")
                else:  # exposures and Airflow tasks use the whole node
                    visit(ref, step, depth, "uses the whole node")
        else:
            for step in sorted(
                steps(graph, ref, Direction.DOWNSTREAM, Granularity.MODEL), key=_order
            ):
                visit(ref, step, depth)

    result = Impact(targets, truncated=truncated)
    confidence = _confidences(graph, hits, targets)
    _collect(graph, hits, targets, confidence, result)
    _unknowns(graph, hits, result)
    return result


def _order(step: Step) -> tuple[str, str]:
    return (step.ref, step.edges[0].kind.value)


def _step_confidence(edges: tuple[Edge, ...], fallback: bool) -> Confidence:
    level = weakest(*(e.confidence for e in edges))
    return weakest(level, Confidence.INFERRED) if fallback else level


def _confidences(
    graph: MetadataGraph, hits: dict[str, _Hit], targets: tuple[str, ...]
) -> dict[str, Confidence]:
    """Strongest-weakest path confidence, by relaxation over the three levels."""
    level = {ref: Confidence.DECLARED for ref in targets}
    changed = True
    while changed:
        changed = False
        for ref, hit in hits.items():
            if ref in targets:
                continue
            for origin, step, fallback in hit.via:
                if origin not in level:
                    continue
                candidate = weakest(level[origin], _step_confidence(step.edges, fallback))
                current = level.get(ref)
                best = candidate if current is None else strongest(current, candidate)
                if best != current:
                    level[ref] = best
                    changed = True
    return level


def _collect(
    graph: MetadataGraph,
    hits: dict[str, _Hit],
    targets: tuple[str, ...],
    confidence: dict[str, Confidence],
    result: Impact,
) -> None:
    """Group hits by node; a node's columns and model-level hits become one entry."""
    changed_nodes = {split_column_id(t)[0] if t in graph.columns else t for t in targets}
    for ref, hit in sorted(hits.items(), key=lambda item: (item[1].depth, item[0])):
        if ref in targets:
            continue
        is_column = ref in graph.columns
        node_id = split_column_id(ref)[0] if is_column else ref
        if node_id in changed_nodes and is_column:
            continue  # another column of a changed node; the node is a target, not impacted
        entry = result.nodes.get(node_id)
        if entry is None:
            entry = ImpactedNode(
                id=node_id,
                kind=graph.nodes[node_id].kind,
                depth=hit.depth,
                granularity=Granularity.COLUMN if is_column else Granularity.MODEL,
                confidence=confidence[ref],
            )
            result.nodes[node_id] = entry
        else:
            entry.depth = min(entry.depth, hit.depth)
            entry.confidence = strongest(entry.confidence, confidence[ref])
            if not is_column:
                entry.granularity = Granularity.MODEL
        if is_column:
            entry.columns.append(ref)
        elif hit.fallback and entry.reason is None:
            entry.reason = hit.fallback
        entry.via.extend((origin, step) for origin, step, _ in hit.via)
    for entry in result.nodes.values():
        entry.columns.sort()


def _unknowns(graph: MetadataGraph, hits: dict[str, _Hit], result: Impact) -> None:
    """Unresolved records on any target or impacted node, or on any of their columns.

    An unresolved reference on a column of an impacted model may hide more impact, so it
    is listed even when that column was not reached.
    """
    nodes = {r.partition("#")[0] for r in hits}
    result.unknowns = sorted(
        (u for u in graph.unresolved if u.subject.partition("#")[0] in nodes),
        key=lambda u: (u.subject, u.reference, u.reason),
    )
