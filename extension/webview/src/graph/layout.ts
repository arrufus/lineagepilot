// Automatic layout (task 3.3) with dagre: a layered drawing in data-flow order, left to right
// or top to bottom. The legacy graphs placed nodes by a hand-rolled level count; dagre also
// orders each layer to reduce crossings.

import dagre from '@dagrejs/dagre';

import type { GraphEdge, GraphNode } from '../../../src/protocol';

export const NODE_SIZE = {
  model: { width: 240, height: 64 },
  column: { width: 220, height: 72 },
};

export interface LayoutOptions {
  direction: 'LR' | 'TB';
  nodeSpacing: number;
  levelSpacing: number;
}

export type Positions = Map<string, { x: number; y: number }>;

// dagre's default ranker draws the most compact layers but took 0.44 s for 500 nodes, on every
// filter change and spacing step; tight-tree took 0.08 s (Node 22, Apple M4 Max)
const LARGE_LAYOUT = 200;

export function sizeOf(node: GraphNode): { width: number; height: number } {
  return node.kind === 'column' ? NODE_SIZE.column : NODE_SIZE.model;
}

/** Top-left positions for every node. */
export function layout(nodes: GraphNode[], edges: GraphEdge[], options: LayoutOptions): Positions {
  const graph = new dagre.graphlib.Graph({ multigraph: false });
  graph.setGraph({
    rankdir: options.direction, nodesep: options.nodeSpacing, ranksep: options.levelSpacing, marginx: 20, marginy: 20,
    ranker: nodes.length > LARGE_LAYOUT ? 'tight-tree' : 'network-simplex',
  });
  graph.setDefaultEdgeLabel(() => ({}));
  // Sorted, so the same graph always lays out the same way. dagre writes the position into the
  // object it is given, so each node gets its own
  for (const node of [...nodes].sort((a, b) => a.id.localeCompare(b.id))) {
    graph.setNode(node.id, { ...sizeOf(node) });
  }
  for (const edge of edges) {
    if (graph.hasNode(edge.upstream) && graph.hasNode(edge.downstream) && edge.upstream !== edge.downstream) {
      graph.setEdge(edge.upstream, edge.downstream);
    }
  }
  dagre.layout(graph);
  const positions: Positions = new Map();
  for (const node of nodes) {
    const placed = graph.node(node.id);
    const size = sizeOf(node);
    positions.set(node.id, { x: placed.x - size.width / 2, y: placed.y - size.height / 2 });
  }
  return positions;
}
