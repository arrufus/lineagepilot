// The graph the webview shows (task 3.3): what the language server returned, merged as nodes
// are expanded, and filtered for display. Pure functions, no React.

import type { GraphEdge, GraphNode, GraphResult, NodeKind } from '../../../src/protocol';

export interface ViewGraph {
  rootId: string;
  nodes: Map<string, GraphNode>;
  edges: Map<string, GraphEdge>;
  // Nodes whose neighbours beyond the first request have been fetched
  expanded: Set<string>;
  // The depth the first request asked for: nodes at it may have more beyond
  depth: number;
  truncated: boolean;
  unknowns: GraphResult['unknowns'];
}

export interface Filters {
  hiddenKinds: Set<NodeKind>;
  search: string;
}

export const NO_FILTERS: Filters = { hiddenKinds: new Set(), search: '' };

export function edgeKey(edge: GraphEdge): string {
  return `${edge.kind}|${edge.upstream}|${edge.downstream}`;
}

export function fromResult(result: GraphResult, depth: number): ViewGraph {
  return {
    rootId: result.root,
    nodes: new Map(result.nodes.map((node) => [node.id, node])),
    edges: new Map(result.edges.map((edge) => [edgeKey(edge), edge])),
    expanded: new Set(),
    depth,
    truncated: result.truncated,
    unknowns: result.unknowns,
  };
}

/** Add the neighbours fetched for ``from`` (lazy expansion). Known nodes keep their place. */
export function merge(view: ViewGraph, from: string, result: GraphResult): ViewGraph {
  const origin = view.nodes.get(from);
  const nodes = new Map(view.nodes);
  for (const node of result.nodes) {
    if (!nodes.has(node.id)) {
      nodes.set(node.id, {
        ...node,
        side: origin?.side === 'root' ? node.side : origin?.side ?? node.side,
        depth: (origin?.depth ?? 0) + node.depth,
      });
    }
  }
  const edges = new Map(view.edges);
  for (const edge of result.edges) {
    edges.set(edgeKey(edge), edge);
  }
  const unknowns = [...view.unknowns];
  for (const unknown of result.unknowns) {
    if (!unknowns.some((u) => u.subject === unknown.subject && u.reference === unknown.reference)) {
      unknowns.push(unknown);
    }
  }
  return { ...view, nodes, edges, unknowns, expanded: new Set([...view.expanded, from]) };
}

/** Nodes at the edge of what was fetched, which may have more neighbours. */
export function canExpand(view: ViewGraph, node: GraphNode): boolean {
  return node.side !== 'root' && node.depth >= view.depth && !view.expanded.has(node.id);
}

export function matches(node: GraphNode, search: string): boolean {
  const query = search.trim().toLowerCase();
  return !query || node.name.toLowerCase().includes(query) || node.id.toLowerCase().includes(query);
}

/** The nodes and edges to draw: hidden kinds removed (never the root), edges between the rest.
 * Search only highlights, so it is not an input: typing must not lay the graph out again. */
export function visible(view: ViewGraph, hiddenKinds: ReadonlySet<NodeKind>): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const nodes = [...view.nodes.values()].filter((node) => node.id === view.rootId || !hiddenKinds.has(node.kind));
  const shown = new Set(nodes.map((node) => node.id));
  const edges = [...view.edges.values()].filter((edge) => shown.has(edge.upstream) && shown.has(edge.downstream));
  return { nodes, edges };
}

export function kindsIn(view: ViewGraph): NodeKind[] {
  return [...new Set([...view.nodes.values()].map((node) => node.kind))].sort();
}

/** The edges into and out of a node, in data-flow terms. */
export function neighbours(view: ViewGraph, id: string): { upstream: GraphEdge[]; downstream: GraphEdge[] } {
  const all = [...view.edges.values()];
  return {
    upstream: all.filter((edge) => edge.downstream === id),
    downstream: all.filter((edge) => edge.upstream === id),
  };
}
