// The Pipelines tree view (tasks 5.3 and 7.6, design_next_phase section 7.4): by tool, the
// pipelines and tasks that run or load the open file's nodes, or all of them when the file has
// none, and what could not be read. It was the Airflow view; Airflow is now one tool in it.

import * as vscode from 'vscode';

import { pipelineWord, toolLabel, toolOfId, withoutTool } from '../kinds';
import type { Pipeline, PipelinesResult, PipelineTask, ReaderStatus, Unknown } from '../protocol';
import type { ProjectServer } from '../server';
import { DBT_KINDS, escapeMarkdown, messageOf, ViewMessage } from './common';

export type PipelinesItem = ToolItem | PipelineItem | TaskItem | UnknownsItem | UnknownItem | ViewMessage<ToolItem>;

/** One tool: what its reader read, and its pipelines and unknowns. */
export class ToolItem {
  readonly type = 'tool';
  constructor(
    readonly tool: string,
    readonly reader: ReaderStatus | undefined,
    readonly pipelines: Pipeline[],
    readonly unknowns: Unknown[],
    readonly filtered: boolean,
  ) {}
}

export class PipelineItem {
  readonly type = 'pipeline';
  constructor(readonly pipeline: Pipeline, readonly parent: ToolItem) {}
}

export class TaskItem {
  readonly type = 'task';
  constructor(readonly task: PipelineTask, readonly parent: PipelineItem) {}
}

export class UnknownsItem {
  readonly type = 'unknowns';
  constructor(readonly unknowns: Unknown[], readonly parent: ToolItem | undefined) {}
}

export class UnknownItem {
  readonly type = 'unknown';
  constructor(readonly unknown: Unknown, readonly parent: UnknownsItem) {}
}

/** What to show for a result: a group for each tool, by name. An unknown whose subject names no
 * tool, such as a file that could not be read, goes under the only tool there is, or else in a
 * group of its own at the end. Kept apart from VS Code's tree so each case can be tested. */
export function itemsOf(result: PipelinesResult, filtered: boolean): PipelinesItem[] {
  const tools = new Set([...result.readers.map((r) => r.tool), ...result.pipelines.map((p) => p.tool)]);
  const sorted = [...tools].sort((a, b) => toolLabel(a).localeCompare(toolLabel(b)) || a.localeCompare(b));
  const only = sorted.length === 1 ? sorted[0] : undefined;
  const toolOf = (unknown: Unknown) => {
    const tool = toolOfId(unknown.subject);
    return tool !== undefined && tools.has(tool) ? tool : only;
  };
  const items: PipelinesItem[] = sorted.map((tool) => new ToolItem(
    tool,
    result.readers.find((r) => r.tool === tool),
    result.pipelines.filter((p) => p.tool === tool),
    result.unknowns.filter((u) => toolOf(u) === tool),
    filtered,
  ));
  if (items.length === 0) {
    items.push(new ViewMessage('No reader of pipelines is turned on.'));
  }
  const loose = result.unknowns.filter((u) => toolOf(u) === undefined);
  if (loose.length > 0) {
    items.push(new UnknownsItem(loose, undefined));
  }
  return items;
}

/** A tool's group: why it has nothing to show, when it has not, then its pipelines and unknowns. */
export function childrenOf(item: ToolItem): PipelinesItem[] {
  const children: PipelinesItem[] = [];
  const summary = item.reader?.summary;
  const files = `${pipelineWord(item.tool, 1)} files`;
  if (item.reader?.stage === 'failed') {
    children.push(new ViewMessage(`Reading the ${files} failed: ${item.reader.message ?? 'see the output'}`, item));
  } else if (summary && !summary.analysed) {
    children.push(new ViewMessage(`Not analysed: ${summary.reason ?? `the ${files} were not read`}`, item));
  } else if (item.pipelines.length === 0) {
    children.push(new ViewMessage(item.filtered ? `No task in the ${files} read runs this.` : `No task in the ${files} read runs dbt.`, item));
  }
  children.push(...item.pipelines.map((pipeline) => new PipelineItem(pipeline, item)));
  if (item.unknowns.length > 0) {
    children.push(new UnknownsItem(item.unknowns, item));
  }
  return children;
}

export class PipelinesTree implements vscode.TreeDataProvider<PipelinesItem> {
  private roots: PipelinesItem[] = [];
  private readonly changed = new vscode.EventEmitter<PipelinesItem | undefined>();
  readonly onDidChangeTreeData = this.changed.event;
  private generation = 0;

  constructor(
    private readonly serverFor: (file: string) => ProjectServer | undefined,
    private readonly servers: () => ProjectServer[],
  ) {}

  /** Show what touches a file's nodes, or everything if it has none. */
  async showFile(uri: vscode.Uri | undefined): Promise<void> {
    const generation = ++this.generation;
    const fileServer = uri?.scheme === 'file' ? this.serverFor(uri.fsPath) : undefined;
    const server = fileServer ?? this.servers()[0];
    if (!server) {
      this.setRoots([], generation);
      return;
    }
    try {
      const nodes = uri && fileServer
        ? (await server.request('lineage/nodesForFile', { uri: uri.toString() })).nodes
          .filter((node) => DBT_KINDS.has(node.kind)).map((node) => node.id).slice(0, 200)
        : [];
      const result = await server.request('lineage/pipelines', { nodes: nodes.length > 0 ? nodes : null });
      this.setRoots(itemsOf(result, nodes.length > 0), generation);
    } catch (error) {
      this.setRoots([new ViewMessage(messageOf(error))], generation);
    }
  }

  /** What the view shows (integration tests read it). */
  get items(): readonly PipelinesItem[] {
    return this.roots;
  }

  private setRoots(roots: PipelinesItem[], generation: number): void {
    if (generation === this.generation) {
      this.roots = roots;
      this.changed.fire(undefined);
    }
  }

  getParent(item: PipelinesItem): PipelinesItem | undefined {
    return item instanceof ToolItem ? undefined : item.parent;
  }

  getChildren(item?: PipelinesItem): PipelinesItem[] {
    if (!item) {
      return this.roots;
    }
    if (item instanceof ToolItem) {
      return childrenOf(item);
    }
    if (item instanceof PipelineItem) {
      return item.pipeline.tasks.map((task) => new TaskItem(task, item));
    }
    if (item instanceof UnknownsItem) {
      return item.unknowns.map((unknown) => new UnknownItem(unknown, item));
    }
    return [];
  }

  getTreeItem(item: PipelinesItem): vscode.TreeItem {
    if (item instanceof ToolItem) {
      return toolTreeItem(item);
    }
    if (item instanceof PipelineItem) {
      return pipelineTreeItem(item.pipeline);
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
      const tree = new vscode.TreeItem(withoutTool(item.unknown.subject), vscode.TreeItemCollapsibleState.None);
      tree.description = item.unknown.reference;
      tree.iconPath = new vscode.ThemeIcon('question');
      const tooltip = new vscode.MarkdownString(undefined, false);
      tooltip.isTrusted = false;
      tooltip.appendMarkdown(escapeMarkdown(item.unknown.reason));
      tree.tooltip = tooltip;
      return tree;
    }
    return item.treeItem();
  }
}

function plural(count: number, one: string, many = `${one}s`): string {
  return `${count} ${count === 1 ? one : many}`;
}

/** ``3 DAGs · 5 tasks``, or where the reader is. */
export function toolDescription(item: ToolItem): string {
  const reader = item.reader;
  if (reader?.stage === 'pending' || reader?.stage === 'scanning') {
    return 'reading…';
  }
  if (reader?.stage === 'failed') {
    return 'failed';
  }
  if (reader && !reader.summary.analysed) {
    return 'not analysed';
  }
  const tasks = item.pipelines.reduce((sum, p) => sum + p.tasks.filter((t) => t.kind === 'task').length, 0);
  return [
    `${item.pipelines.length} ${pipelineWord(item.tool, item.pipelines.length)}`,
    plural(tasks, 'task'),
  ].join(' · ');
}

function toolTreeItem(item: ToolItem): vscode.TreeItem {
  const tree = new vscode.TreeItem(toolLabel(item.tool), vscode.TreeItemCollapsibleState.Expanded);
  tree.id = `tool:${item.tool}`;
  tree.iconPath = new vscode.ThemeIcon('tools');
  tree.description = toolDescription(item);
  const summary = item.reader?.summary;
  if (summary) {
    const tooltip = new vscode.MarkdownString(undefined, false);
    tooltip.isTrusted = false;
    tooltip.appendMarkdown(`**${escapeMarkdown(toolLabel(item.tool))}**: ${plural(summary.files, 'file')} read`);
    if (summary.paths.length > 0) {
      tooltip.appendMarkdown(` in ${summary.paths.slice(0, 5).map(escapeMarkdown).join(', ')}`);
    }
    tooltip.appendMarkdown(`, ${plural(summary.links, 'link')} to dbt, ${plural(summary.unknowns, 'unknown')}`);
    if (summary.opaqueTasks > 0) {
      tooltip.appendMarkdown(`\n\n${plural(summary.opaqueTasks, 'task')} could not be read and do not mention dbt, so they are not listed.`);
    }
    for (const problem of summary.problems.slice(0, 5)) {
      tooltip.appendMarkdown(`\n\n${escapeMarkdown(problem)}`);
    }
    tree.tooltip = tooltip;
  }
  return tree;
}

function pipelineTreeItem(pipeline: Pipeline): vscode.TreeItem {
  const tree = new vscode.TreeItem(pipeline.name, vscode.TreeItemCollapsibleState.Expanded);
  tree.id = pipeline.id;
  tree.iconPath = new vscode.ThemeIcon('workflow');
  tree.description = pipeline.owner ?? undefined;
  if (pipeline.location) {
    tree.command = { command: 'lineagePilot.openNode', title: 'Open', arguments: [pipeline.location] };
  }
  return tree;
}

function taskTreeItem(task: PipelineTask): vscode.TreeItem {
  // A pipeline that is one link as a whole, such as a Cosmos DbtDag, is listed under itself
  const name = task.kind === 'pipeline' ? `The whole ${pipelineWord(task.tool, 1)}` : task.name;
  const tree = new vscode.TreeItem(name, vscode.TreeItemCollapsibleState.None);
  tree.id = `task:${task.id}`;
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
