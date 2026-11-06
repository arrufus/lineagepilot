// The Airflow tree view (task 5.3, design section 7): the DAGs and tasks that run or read the open
// file's nodes, or all of them when the file has none, and what could not be read.

import * as vscode from 'vscode';

import type { AirflowDag, AirflowResult, AirflowTask, Unknown } from '../protocol';
import type { ProjectServer } from '../server';

export type AirflowItem = DagItem | TaskItem | UnknownsItem | UnknownItem | AirflowMessage;

export class DagItem {
  readonly type = 'dag';
  constructor(readonly dag: AirflowDag) {}
}

export class TaskItem {
  readonly type = 'task';
  constructor(readonly task: AirflowTask, readonly parent: DagItem) {}
}

export class UnknownsItem {
  readonly type = 'unknowns';
  constructor(readonly unknowns: Unknown[]) {}
}

export class UnknownItem {
  readonly type = 'unknown';
  constructor(readonly unknown: Unknown, readonly parent: UnknownsItem) {}
}

export class AirflowMessage {
  readonly type = 'message';
  constructor(readonly text: string) {}
}

function escapeMarkdown(text: string): string {
  return text.replace(/([\\`*_{}[\]()#+\-.!|<>~&])/g, '\\$1');
}

function messageOf(error: unknown): string {
  const text = error instanceof Error ? error.message : String(error);
  return text.length > 200 ? `${text.slice(0, 197)}...` : text;
}

/** What to show for a scan. Kept apart from VS Code's tree so each case can be tested. */
export function itemsOf(result: AirflowResult, filtered: boolean): AirflowItem[] {
  const items: AirflowItem[] = [];
  if (!result.summary.analysed) {
    items.push(new AirflowMessage(`Not analysed: ${result.summary.reason ?? 'the DAG files were not read'}`));
  } else if (result.dags.length === 0) {
    items.push(new AirflowMessage(filtered ? 'No task in the DAG files read runs this.' : 'No task in the DAG files read runs dbt.'));
  }
  items.push(...result.dags.map((dag) => new DagItem(dag)));
  if (result.unknowns.length > 0) {
    items.push(new UnknownsItem(result.unknowns));
  }
  return items;
}

export class AirflowTree implements vscode.TreeDataProvider<AirflowItem> {
  private roots: AirflowItem[] = [];
  private readonly changed = new vscode.EventEmitter<AirflowItem | undefined>();
  readonly onDidChangeTreeData = this.changed.event;
  private generation = 0;

  constructor(
    private readonly serverFor: (file: string) => ProjectServer | undefined,
    private readonly servers: () => ProjectServer[],
  ) {}

  /** Show what touches a file's nodes, or everything if it has none. */
  async showFile(uri: vscode.Uri | undefined): Promise<void> {
    const generation = ++this.generation;
    const server = (uri?.scheme === 'file' ? this.serverFor(uri.fsPath) : undefined) ?? this.servers()[0];
    if (!server) {
      this.setRoots([], generation);
      return;
    }
    try {
      const nodes = uri && uri.scheme === 'file' && this.serverFor(uri.fsPath)
        ? (await server.request('lineage/nodesForFile', { uri: uri.toString() })).nodes
          .filter((node) => !node.kind.startsWith('airflow_')).map((node) => node.id)
        : [];
      const result = await server.request('lineage/airflow', { nodes: nodes.length > 0 ? nodes : null });
      this.setRoots(itemsOf(result, nodes.length > 0), generation);
    } catch (error) {
      this.setRoots([new AirflowMessage(messageOf(error))], generation);
    }
  }

  /** What the view shows (integration tests read it). */
  get items(): readonly AirflowItem[] {
    return this.roots;
  }

  private setRoots(roots: AirflowItem[], generation: number): void {
    if (generation === this.generation) {
      this.roots = roots;
      this.changed.fire(undefined);
    }
  }

  getParent(item: AirflowItem): AirflowItem | undefined {
    return item instanceof TaskItem || item instanceof UnknownItem ? item.parent : undefined;
  }

  getChildren(item?: AirflowItem): AirflowItem[] {
    if (!item) {
      return this.roots;
    }
    if (item instanceof DagItem) {
      return item.dag.tasks.map((task) => new TaskItem(task, item));
    }
    if (item instanceof UnknownsItem) {
      return item.unknowns.map((unknown) => new UnknownItem(unknown, item));
    }
    return [];
  }

  getTreeItem(item: AirflowItem): vscode.TreeItem {
    if (item instanceof DagItem) {
      return dagTreeItem(item.dag);
    }
    if (item instanceof TaskItem) {
      return taskTreeItem(item.task);
    }
    if (item instanceof UnknownsItem) {
      const tree = new vscode.TreeItem(`Could not be read (${item.unknowns.length})`, vscode.TreeItemCollapsibleState.Collapsed);
      tree.iconPath = new vscode.ThemeIcon('question');
      return tree;
    }
    if (item instanceof UnknownItem) {
      const tree = new vscode.TreeItem(item.unknown.subject.replace(/^airflow:/, ''), vscode.TreeItemCollapsibleState.None);
      tree.description = item.unknown.reference;
      tree.iconPath = new vscode.ThemeIcon('question');
      const tooltip = new vscode.MarkdownString(undefined, false);
      tooltip.isTrusted = false;
      tooltip.appendMarkdown(escapeMarkdown(item.unknown.reason));
      tree.tooltip = tooltip;
      return tree;
    }
    const tree = new vscode.TreeItem(item.text, vscode.TreeItemCollapsibleState.None);
    tree.iconPath = new vscode.ThemeIcon('info');
    return tree;
  }
}

function dagTreeItem(dag: AirflowDag): vscode.TreeItem {
  const tree = new vscode.TreeItem(dag.name, vscode.TreeItemCollapsibleState.Expanded);
  tree.iconPath = new vscode.ThemeIcon('workflow');
  tree.description = dag.owner ?? undefined;
  if (dag.location) {
    tree.command = { command: 'lineagePilot.openNode', title: 'Open', arguments: [dag.location] };
  }
  return tree;
}

function taskTreeItem(task: AirflowTask): vscode.TreeItem {
  // A Cosmos DbtDag is one link as a whole, listed under its own DAG
  const name = task.kind === 'airflow_dag' ? 'The whole DAG' : task.name;
  const tree = new vscode.TreeItem(name, vscode.TreeItemCollapsibleState.None);
  tree.id = task.id;
  tree.iconPath = new vscode.ThemeIcon('clock');
  tree.description = [...task.how, task.confidence].filter(Boolean).join(' · ');
  const tooltip = new vscode.MarkdownString(undefined, false);
  tooltip.isTrusted = false;
  tooltip.appendMarkdown(`**${escapeMarkdown(name)}**`);
  if (task.operator) {
    tooltip.appendMarkdown(` (${escapeMarkdown(task.operator)})`);
  }
  if (task.owner) {
    tooltip.appendMarkdown(`\n\nOwner: ${escapeMarkdown(task.owner)}`);
  }
  if (task.derivedTasks.length > 0) {
    const shown = task.derivedTasks.slice(0, 10).map(escapeMarkdown).join(', ');
    const more = task.derivedTasks.length > 10 ? ` and ${task.derivedTasks.length - 10} more` : '';
    tooltip.appendMarkdown(`\n\nTasks Cosmos should render: ${shown}${more}`);
  }
  for (const note of task.notes) {
    tooltip.appendMarkdown(`\n\n${escapeMarkdown(note)}`);
  }
  tree.tooltip = tooltip;
  if (task.location) {
    tree.command = { command: 'lineagePilot.openNode', title: 'Open', arguments: [task.location] };
  }
  return tree;
}
