"""The lineage/* requests over a loaded graph (task 2.3, design section 9).

Each handler takes params already validated against the shared schema and returns a
plain dict in the shape the schema gives its result. Errors use the protocol's codes:
``NotReady`` when no graph is loaded, ``UnknownNode`` for an ID the graph does not have.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from lineagepilot_core.analysis.impact import weakest
from lineagepilot_core.analysis.traversal import Direction, Granularity, Step, traverse
from lineagepilot_core.export import (
    ExportFormat,
    export_graph,
    graph_view,
    pii_inventory_csv,
    pii_inventory_markdown,
    safe_filename,
)
from lineagepilot_core.graph.model import NodeKind
from pygls.exceptions import JsonRpcServerError
from pygls.uris import to_fs_path

from lineagepilot_lsp import protocol
from lineagepilot_lsp.context import GraphContext, clip

DEFAULT_MAX_NODES = 2000


def unknown_node(ref: str) -> JsonRpcServerError:
    return JsonRpcServerError(
        f"{ref} is not in the lineage graph; it may have changed since it was read",
        protocol.ErrorCode.UNKNOWN_NODE,
    )


def _directions(direction: str) -> list[Direction]:
    if direction == "both":
        return [Direction.UPSTREAM, Direction.DOWNSTREAM]
    return [Direction(direction)]


def _edge_json(ctx: GraphContext, origin: str, step: Step, direction: Direction) -> dict[str, Any]:
    """The step from ``origin`` to ``step.ref``, as an edge in data-flow order."""
    if direction is Direction.DOWNSTREAM:
        upstream, downstream = origin, step.ref
    else:
        upstream, downstream = step.ref, origin
    edges = step.edges
    relation = None
    if len(edges) > 1:  # through the physical table a source shares with its builder
        ends = {edges[0].upstream, edges[0].downstream} & {edges[1].upstream, edges[1].downstream}
        relation = next(iter(ends), None)
    return {
        "kind": edges[0].kind.value,
        "upstream": upstream,
        "downstream": downstream,
        "relation": relation,
        "confidence": weakest(*(e.confidence for e in edges)).value,
        "origins": sorted({ev.origin.value for e in edges for ev in e.evidence}),
        "transformation": clip(edges[0].transformation) if len(edges) == 1 else None,
    }


def graph(ctx: GraphContext, params: protocol.GraphParams) -> dict[str, Any]:
    granularity = Granularity(params.granularity)
    known = ctx.graph.columns if granularity is Granularity.COLUMN else ctx.graph.nodes
    if params.root not in known:
        raise unknown_node(params.root)
    nodes = {params.root: ctx.node_json(params.root, "root", 0)}
    edges: dict[tuple[str, str, str], dict[str, Any]] = {}
    truncated = False
    for direction in _directions(params.direction):
        found = traverse(
            ctx.graph,
            [params.root],
            direction,
            granularity,
            max_depth=params.depth,
            max_nodes=params.max_nodes or DEFAULT_MAX_NODES,
        )
        truncated |= found.truncated
        for ref, reached in found.reached.items():
            if ref not in nodes:
                nodes[ref] = ctx.node_json(ref, direction.value, reached.depth)
            for origin, step in reached.via:
                edge = _edge_json(ctx, origin, step, direction)
                edges[(edge["kind"], edge["upstream"], edge["downstream"])] = edge
    return {
        "root": params.root,
        "direction": params.direction,
        "granularity": params.granularity,
        "nodes": list(nodes.values()),
        "edges": [edges[key] for key in sorted(edges)],
        "truncated": truncated,
        "unknowns": ctx.unknowns_json(list(nodes)),
    }


def impact(ctx: GraphContext, params: protocol.ImpactParams, column_stage: str) -> dict[str, Any]:
    for ref in params.targets:
        if ref not in ctx.graph.nodes and ref not in ctx.graph.columns:
            raise unknown_node(ref)
    found = ctx.impact(params.targets, max_depth=params.max_depth)
    nodes = []
    for hit in found.nodes.values():
        node = ctx.graph.nodes[hit.id]
        nodes.append(
            {
                "id": hit.id,
                "kind": hit.kind.value,
                "name": clip(node.name),
                "depth": hit.depth,
                "granularity": hit.granularity.value,
                "confidence": hit.confidence.value,
                "columns": list(hit.columns),
                "reason": clip(hit.reason),
                "location": ctx.location(hit.id),
                "owner": clip(node.owner),
            }
        )
    return {
        "targets": list(found.targets),
        "truncated": found.truncated,
        "counts": found.counts(),
        "nodes": nodes,
        "unknowns": [
            {"subject": u.subject, "reference": clip(u.reference), "reason": clip(u.reason)}
            for u in found.unknowns
        ],
        "columnLineage": column_stage,
    }


def export(ctx: GraphContext, params: protocol.ExportParams) -> dict[str, Any]:
    project = ctx.graph.project_name or ctx.project.name or "lineage"
    if params.format in ("pii-markdown", "pii-csv"):
        markdown = params.format == "pii-markdown"
        content = (pii_inventory_markdown if markdown else pii_inventory_csv)(ctx.graph, ctx.pii)
        filename = safe_filename(f"{project}-pii-inventory", "md" if markdown else "csv")
        return {
            "format": params.format,
            "filename": filename,
            "content": content,
            "nodes": len(ctx.pii.nodes()),
            "edges": 0,
        }
    node_ids = None
    if params.root is not None:
        if params.root not in ctx.graph.nodes:
            raise unknown_node(params.root)
        node_ids = {params.root}
        for direction in _directions(params.direction or "both"):
            node_ids.update(
                traverse(ctx.graph, [params.root], direction, max_depth=params.depth or 3).reached
            )
    view = graph_view(ctx.graph, node_ids)
    fmt = ExportFormat(params.format)
    stem = f"{project}-{ctx.graph.nodes[params.root].name}" if params.root else f"{project}-lineage"
    return {
        "format": params.format,
        "filename": safe_filename(stem, fmt.extension),
        "content": export_graph(view, fmt),
        "nodes": len(view.nodes),
        "edges": len(view.edges),
    }


def nodes_for_file(ctx: GraphContext, params: protocol.NodesForFileParams) -> dict[str, Any]:
    path = to_fs_path(params.uri)  # None for an untitled or virtual document
    if not path:
        return {"nodes": []}
    nodes = [n for n in ctx.nodes_for_file(Path(path)) if n.kind is not NodeKind.RELATION]
    return {"nodes": [ctx.node_json(n.id, "root", 0) for n in nodes]}
