// The Lineage tree view (task 2.6, design section 7): what is upstream and downstream of the
// open file's nodes, one level at a time from lineage/graph. "Analyze Impact" pins it to a node.

import * as vscode from 'vscode';

import type { GraphEdge, GraphNode } from '../protocol';
import type { ProjectServer } from '../server';

type Direction = 'upstream' | 'downstream';

export type LineageItem = NodeItem | GroupItem | MessageItem;

export class NodeItem {
  readonly type = 'node';
  constructor(
    readonly server: ProjectServer,
    readonly node: GraphNode,
    readonly parent: LineageItem | undefined,
    // The direction this node was reached in; undefined for a focused node
    readonly direction: Direction | undefined,
    readonly edge: GraphEdge | undefined,
  ) {}
}

export class GroupItem {
  readonly type = 'group';
  constructor(readonly server: ProjectServer, readonly parent: NodeItem, readonly direction: Direction) {}
}

export class MessageItem {
  readonly type = 'message';
  constructor(readonly text: string, readonly parent: LineageItem | undefined = undefined) {}
}

const ICONS: Record<string, string> = {
  model: 'symbol-class',
  snapshot: 'history',
  seed: 'file',
  source: 'database',
  exposure: 'graph',
  airflow_dag: 'workflow',
  airflow_task: 'clock',
  column: 'symbol-field',
};

function escapeMarkdown(text: string): string {
  return text.replace(/([\\`*_{}[\]()#+\-.!|<>~&])/g, '\\$1');
}

export class LineageTree implements vscode.TreeDataProvider<LineageItem> {
  private roots: LineageItem[] = [];
  private pinnedNode: string | undefined;
  private readonly changed = new vscode.EventEmitter<LineageItem | undefined>();
  readonly onDidChangeTreeData = this.changed.event;
  private generation = 0;

  constructor(private readonly serverFor: (file: string) => ProjectServer | undefined) {}

  get pinned(): boolean {
    return this.pinnedNode !== undefined;
  }

  /** Show the nodes a file defines or documents. Ignored while pinned to a node. */
  async showFile(uri: vscode.Uri | undefined): Promise<void> {
    if (this.pinned) {
      return;
    }
    const generation = ++this.generation;
    const server = uri?.scheme === 'file' ? this.serverFor(uri.fsPath) : undefined;
    if (!uri || !server) {
      this.setRoots([]);
      return;
    }
    try {
      const { nodes } = await server.request('lineage/nodesForFile', { uri: uri.toString() });
      if (generation === this.generation) {
        this.setRoots(nodes.map((node) => new NodeItem(server, node, undefined, undefined, undefined)));
      }
    } catch (error) {
      if (generation === this.generation) {
        this.setRoots([new MessageItem(messageOf(error))]);
      }
    }
  }

  /** Pin the view to one node, showing its downstream. */
  async pin(server: ProjectServer, nodeId: string): Promise<NodeItem | undefined> {
    this.pinnedNode = nodeId;
    const generation = ++this.generation;
    void vscode.commands.executeCommand('setContext', 'lineagePilot.lineagePinned', true);
    try {
      const { nodes } = await server.request(
        'lineage/graph', { root: nodeId, direction: 'downstream', depth: 1, granularity: 'model' });
      const root = nodes.find((n) => n.id === nodeId);
      if (generation !== this.generation || !root) {
        return undefined;
      }
      const item = new NodeItem(server, root, undefined, undefined, undefined);
      this.setRoots([item]);
      return item;
    } catch (error) {
      this.setRoots([new MessageItem(messageOf(error))]);
      return undefined;
    }
  }

  unpin(): void {
    this.pinnedNode = undefined;
    void vscode.commands.executeCommand('setContext', 'lineagePilot.lineagePinned', false);
  }

  refresh(): void {
    this.changed.fire(undefined);
  }

  private setRoots(roots: LineageItem[]): void {
    this.roots = roots;
    this.changed.fire(undefined);
  }

  getParent(item: LineageItem): LineageItem | undefined {
    return item.parent;
  }

  async getChildren(item?: LineageItem): Promise<LineageItem[]> {
    if (!item) {
      return this.roots;
    }
    if (item instanceof MessageItem) {
      return [];
    }
    if (item instanceof NodeItem && item.direction === undefined) {
      return [new GroupItem(item.server, item, 'upstream'), new GroupItem(item.server, item, 'downstream')];
    }
    const [server, from, direction, parent] = item instanceof GroupItem
      ? [item.server, item.parent.node, item.direction, item]
      : [item.server, item.node, item.direction!, item];
    try {
      const result = await server.request(
        'lineage/graph', { root: from.id, direction, depth: 1, granularity: 'model' });
      const next = result.nodes.filter((n) => n.depth === 1);
      if (next.length === 0) {
        return item instanceof GroupItem ? [new MessageItem(`Nothing ${direction}`, item)] : [];
      }
      return next.map((node) => {
        const edge = result.edges.find((e) => (direction === 'downstream'
          ? e.upstream === from.id && e.downstream === node.id
          : e.downstream === from.id && e.upstream === node.id));
        return new NodeItem(server, node, parent, direction, edge);
      });
    } catch (error) {
      return [new MessageItem(messageOf(error), item)];
    }
  }

  getTreeItem(item: LineageItem): vscode.TreeItem {
    if (item instanceof MessageItem) {
      const tree = new vscode.TreeItem(item.text, vscode.TreeItemCollapsibleState.None);
      tree.iconPath = new vscode.ThemeIcon('info');
      return tree;
    }
    if (item instanceof GroupItem) {
      const tree = new vscode.TreeItem(
        item.direction === 'upstream' ? 'Upstream' : 'Downstream',
        item.direction === 'downstream' && this.pinned
          ? vscode.TreeItemCollapsibleState.Expanded : vscode.TreeItemCollapsibleState.Collapsed,
      );
      tree.iconPath = new vscode.ThemeIcon(item.direction === 'upstream' ? 'arrow-up' : 'arrow-down');
      tree.contextValue = 'lineageGroup';
      return tree;
    }
    const { node, edge } = item;
    const tree = new vscode.TreeItem(node.name, vscode.TreeItemCollapsibleState.Collapsed);
    tree.id = pathOf(item);
    tree.iconPath = new vscode.ThemeIcon(ICONS[node.kind] ?? 'circle-outline');
    tree.description = [node.kind, edge?.confidence, node.pii ? 'PII' : undefined].filter(Boolean).join(' · ');
    const tooltip = new vscode.MarkdownString(undefined, false);
    tooltip.isTrusted = false;
    tooltip.appendMarkdown(`**${escapeMarkdown(node.name)}** (${node.kind})\n\n${escapeMarkdown(node.id)}`);
    if (edge) {
      const via = edge.relation ? `, via table ${escapeMarkdown(edge.relation)}` : '';
      tooltip.appendMarkdown(`\n\n${edge.kind}: ${edge.confidence}, ${edge.origins.join(', ')}${via}`);
    }
    if (node.description) {
      tooltip.appendMarkdown(`\n\n${escapeMarkdown(node.description.slice(0, 300))}`);
    }
    tree.tooltip = tooltip;
    tree.contextValue = 'lineageNode';
    if (node.location) {
      tree.command = { command: 'lineagePilot.openNode', title: 'Open', arguments: [node.location] };
    }
    return tree;
  }
}

function pathOf(item: LineageItem): string {
  if (item instanceof NodeItem) {
    return `${item.parent ? `${pathOf(item.parent)}/` : ''}${item.node.id}`;
  }
  if (item instanceof GroupItem) {
    return `${pathOf(item.parent)}/${item.direction}`;
  }
  return `${item.parent ? `${pathOf(item.parent)}/` : ''}message`;
}

function messageOf(error: unknown): string {
  const text = error instanceof Error ? error.message : String(error);
  return text.length > 200 ? `${text.slice(0, 197)}...` : text;
}

/** The Airflow view: empty until DAG analysis arrives in Phase 5; its welcome text says so. */
export class AirflowTree implements vscode.TreeDataProvider<never> {
  getChildren(): never[] {
    return [];
  }

  getTreeItem(item: never): vscode.TreeItem {
    return item;
  }
}
