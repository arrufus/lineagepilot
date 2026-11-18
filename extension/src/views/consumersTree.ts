// The Consumers tree view (task 7.6, design_next_phase section 7.4): by tool, what reads the open
// file's nodes from outside dbt (metrics, dashboards, reports), and which of their columns.

import * as vscode from 'vscode';

import { subkindLabel, toolLabel } from '../kinds';
import type { Consumer, ConsumerRead, GraphNode } from '../protocol';
import type { ProjectServer } from '../server';
import { DBT_KINDS, escapeMarkdown, messageOf, ViewMessage } from './common';

export type ConsumersItem = ConsumerToolItem | ConsumerItem | ReadItem | ViewMessage;

export class ConsumerToolItem {
  readonly type = 'tool';
  readonly parent = undefined;
  constructor(readonly tool: string, readonly consumers: Consumer[]) {}
}

export class ConsumerItem {
  readonly type = 'consumer';
  constructor(readonly consumer: Consumer, readonly parent: ConsumerToolItem) {}
}

/** How a consumer reads one of the file's nodes: the whole node, or some of its columns. */
export class ReadItem {
  readonly type = 'read';
  constructor(readonly read: ConsumerRead, readonly name: string, readonly parent: ConsumerItem) {}
}

/** A group for each tool, by name; a message when nothing reads the nodes. Kept apart from VS
 * Code's tree so each case can be tested. */
export function itemsOf(consumers: Consumer[]): ConsumersItem[] {
  if (consumers.length === 0) {
    return [new ViewMessage('Nothing outside dbt is known to read this.')];
  }
  const tools = [...new Set(consumers.map((c) => c.tool))]
    .sort((a, b) => toolLabel(a).localeCompare(toolLabel(b)) || a.localeCompare(b));
  return tools.map((tool) => new ConsumerToolItem(tool, consumers.filter((c) => c.tool === tool)));
}

export class ConsumersTree implements vscode.TreeDataProvider<ConsumersItem> {
  private roots: ConsumersItem[] = [];
  // The names of the nodes shown, for the reads under each consumer
  private names = new Map<string, string>();
  private readonly changed = new vscode.EventEmitter<ConsumersItem | undefined>();
  readonly onDidChangeTreeData = this.changed.event;
  private generation = 0;

  constructor(private readonly serverFor: (file: string) => ProjectServer | undefined) {}

  /** Show what reads a file's nodes. With no file of a project open, the view's welcome text. */
  async showFile(uri: vscode.Uri | undefined): Promise<void> {
    const generation = ++this.generation;
    const server = uri?.scheme === 'file' ? this.serverFor(uri.fsPath) : undefined;
    if (!uri || !server) {
      this.setRoots([], new Map(), generation);
      return;
    }
    try {
      const nodes: GraphNode[] = (await server.request('lineage/nodesForFile', { uri: uri.toString() })).nodes
        .filter((node) => DBT_KINDS.has(node.kind)).slice(0, 200);
      if (nodes.length === 0) {
        this.setRoots([], new Map(), generation);
        return;
      }
      const { consumers } = await server.request('lineage/consumers', { nodes: nodes.map((node) => node.id) });
      this.setRoots(itemsOf(consumers), new Map(nodes.map((node) => [node.id, node.name])), generation);
    } catch (error) {
      this.setRoots([new ViewMessage(messageOf(error))], new Map(), generation);
    }
  }

  /** What the view shows (integration tests read it). */
  get items(): readonly ConsumersItem[] {
    return this.roots;
  }

  private setRoots(roots: ConsumersItem[], names: Map<string, string>, generation: number): void {
    if (generation === this.generation) {
      this.roots = roots;
      this.names = names;
      this.changed.fire(undefined);
    }
  }

  getParent(item: ConsumersItem): ConsumersItem | undefined {
    return item.parent;
  }

  getChildren(item?: ConsumersItem): ConsumersItem[] {
    if (!item) {
      return this.roots;
    }
    if (item instanceof ConsumerToolItem) {
      return item.consumers.map((consumer) => new ConsumerItem(consumer, item));
    }
    if (item instanceof ConsumerItem) {
      return item.consumer.reads.map((read) => new ReadItem(read, this.names.get(read.node) ?? read.node, item));
    }
    return [];
  }

  getTreeItem(item: ConsumersItem): vscode.TreeItem {
    if (item instanceof ConsumerToolItem) {
      const tree = new vscode.TreeItem(toolLabel(item.tool), vscode.TreeItemCollapsibleState.Expanded);
      tree.id = `tool:${item.tool}`;
      tree.iconPath = new vscode.ThemeIcon('tools');
      tree.description = item.consumers.length === 1 ? '1 consumer' : `${item.consumers.length} consumers`;
      return tree;
    }
    if (item instanceof ConsumerItem) {
      return consumerTreeItem(item.consumer);
    }
    if (item instanceof ReadItem) {
      return readTreeItem(item);
    }
    return item.treeItem();
  }
}

function consumerTreeItem(consumer: Consumer): vscode.TreeItem {
  const tree = new vscode.TreeItem(consumer.name, vscode.TreeItemCollapsibleState.Expanded);
  tree.id = consumer.id;
  tree.iconPath = new vscode.ThemeIcon('dashboard');
  tree.description = [subkindLabel(consumer.subkind) ?? 'consumer', consumer.owner].filter(Boolean).join(' · ');
  const tooltip = new vscode.MarkdownString(undefined, false);
  tooltip.isTrusted = false;
  tooltip.appendMarkdown(`**${escapeMarkdown(consumer.name)}**, ${escapeMarkdown(toolLabel(consumer.tool))}`);
  if (consumer.owner) {
    tooltip.appendMarkdown(`\n\nOwner: ${escapeMarkdown(consumer.owner)}`);
  }
  tooltip.appendMarkdown(`\n\n${escapeMarkdown(consumer.id)}`);
  tree.tooltip = tooltip;
  if (consumer.location) {
    tree.command = { command: 'lineagePilot.openNode', title: 'Open', arguments: [consumer.location] };
  }
  return tree;
}

function readTreeItem(item: ReadItem): vscode.TreeItem {
  const { read } = item;
  const tree = new vscode.TreeItem(item.name, vscode.TreeItemCollapsibleState.None);
  tree.id = `${item.parent.consumer.id}|${read.node}`;
  tree.iconPath = new vscode.ThemeIcon(read.columns.length > 0 ? 'symbol-field' : 'symbol-class');
  const what = read.columns.length > 0 ? read.columns.join(', ') : 'the whole node';
  tree.description = `${what} · ${read.confidence}`;
  return tree;
}
