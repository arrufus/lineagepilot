// Builders for protocol objects, and a fake VS Code webview API that records what the webview
// posts and lets a test answer it, as the extension would.

import type {
  GraphEdge, GraphNode, GraphResult, ImpactResult, ImpactedNode, NodeResult,
} from '../../src/protocol';
import { HostMessage, type WebviewMessage } from '../../src/webviewMessages';
import { Transport, type VsCodeApi } from '../src/transport';

export function node(id: string, fields: Partial<GraphNode> = {}): GraphNode {
  const column = id.includes('#');
  return {
    id,
    kind: column ? 'column' : 'model',
    name: column ? id.split('#')[1] : id.split('.').pop()!,
    side: 'root',
    depth: 0,
    parent: column ? id.split('#')[0] : null,
    location: { uri: `file:///project/models/${id.split('.').pop()}.sql`, range: { start: { line: 0, character: 0 }, end: { line: 9, character: 0 } } },
    layer: null,
    materialization: null,
    owner: null,
    description: null,
    columnLineage: null,
    columnLineageReason: null,
    pii: false,
    ...fields,
  };
}

export function edge(upstream: string, downstream: string, fields: Partial<GraphEdge> = {}): GraphEdge {
  return {
    kind: upstream.includes('#') ? 'derives' : 'depends_on',
    upstream,
    downstream,
    relation: null,
    confidence: 'declared',
    origins: ['manifest'],
    transformation: null,
    ...fields,
  };
}

export function graph(root: string, nodes: GraphNode[], edges: GraphEdge[], fields: Partial<GraphResult> = {}): GraphResult {
  return {
    root, direction: 'both', granularity: root.includes('#') ? 'column' : 'model', nodes, edges, truncated: false, unknowns: [], ...fields,
  };
}

/** stg_orders -> orders -> customers_report, focused on orders. */
export function ordersGraph(fields: Partial<GraphResult> = {}): GraphResult {
  return graph('model.shop.orders', [
    node('model.shop.orders', { layer: 'marts', materialization: 'table' }),
    node('model.shop.stg_orders', { side: 'upstream', depth: 1, layer: 'staging' }),
    node('source.shop.raw.orders', { kind: 'source', side: 'upstream', depth: 2, name: 'raw.orders' }),
    node('model.shop.customers_report', { side: 'downstream', depth: 1, pii: true }),
  ], [
    edge('source.shop.raw.orders', 'model.shop.stg_orders'),
    edge('model.shop.stg_orders', 'model.shop.orders'),
    edge('model.shop.orders', 'model.shop.customers_report'),
  ], fields);
}

/** A layered graph of about ``count`` models, each depending on two in the layer before. */
export function largeGraph(count: number, width = 25): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];
  for (let i = 0; i < count; i += 1) {
    const layer = Math.floor(i / width);
    nodes.push(node(`model.big.m${i}`, { side: i === 0 ? 'root' : 'downstream', depth: layer }));
    if (layer > 0) {
      const before = (layer - 1) * width;
      edges.push(edge(`model.big.m${before + (i % width)}`, `model.big.m${i}`));
      edges.push(edge(`model.big.m${before + ((i * 7) % width)}`, `model.big.m${i}`));
    }
  }
  return { nodes, edges };
}

export function nodeResult(graphNode: GraphNode, fields: Partial<NodeResult> = {}): NodeResult {
  return { node: graphNode, columns: [], upstream: 0, downstream: 0, ...fields };
}

export function impacted(id: string, fields: Partial<ImpactedNode> = {}): ImpactedNode {
  return {
    id, kind: 'model', name: id.split('.').pop()!, depth: 1, granularity: 'model', confidence: 'declared', columns: [], reason: null,
    location: null, owner: null, ...fields,
  };
}

export function impactResult(fields: Partial<ImpactResult> = {}): ImpactResult {
  return { targets: ['model.shop.orders'], truncated: false, counts: {}, nodes: [], unknowns: [], columnLineage: 'done', ...fields };
}

/** A webview's VS Code API and window, with the messages it posted, for a real Transport. */
export class FakeHost implements VsCodeApi {
  readonly posted: WebviewMessage[] = [];
  private listener: ((event: { data: unknown }) => void) | undefined;
  readonly transport: Transport;

  constructor(state?: unknown) {
    this.state = state;
    this.transport = new Transport(this, {
      addEventListener: (_type: string, listener: (event: { data: unknown }) => void) => { this.listener = listener; },
    } as unknown as Window);
  }

  postMessage(message: unknown): void {
    this.posted.push(message as WebviewMessage);
  }

  /** VS Code's webview state: kept while the page is destroyed and built again. */
  state: unknown = undefined;

  getState(): unknown {
    return this.state;
  }

  setState(state: unknown): void {
    this.state = state;
  }

  /** Send a message to the webview, as the extension does. Validated, like the extension's. */
  send(message: HostMessage): void {
    this.listener?.({ data: HostMessage.parse(message) });
  }

  sendRaw(message: unknown): void {
    this.listener?.({ data: message });
  }

  /** The last request of a type the webview made. */
  last<T extends WebviewMessage['type']>(type: T): Extract<WebviewMessage, { type: T }> {
    const found = this.posted.filter((m) => m.type === type).pop();
    if (!found) {
      throw new Error(`the webview made no ${type} request; it posted ${this.posted.map((m) => m.type).join(', ') || 'nothing'}`);
    }
    return found as Extract<WebviewMessage, { type: T }>;
  }

  all<T extends WebviewMessage['type']>(type: T): Extract<WebviewMessage, { type: T }>[] {
    return this.posted.filter((m) => m.type === type) as Extract<WebviewMessage, { type: T }>[];
  }
}
