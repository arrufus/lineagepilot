// LineagePilot: lineage and change impact for dbt projects, from the artifacts in the
// workspace (design section 7). This file wires the language servers, commands, views and
// status bar together.

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as vscode from 'vscode';

import { pickSubcommand, refreshArtifacts, type RefreshOutcome, type Subcommand, SUBCOMMANDS } from './dbt';
import { forgetOldCounts } from './legacy';
import { ServerManager } from './manager';
import { inWorkspace, LineagePanel, openLocation } from './panel';
import { PROJECT_GLOB } from './projects';
import type { ExportFormat, GraphNode } from './protocol';
import { ReviewPanel } from './reviewPanel';
import { applySampleChange, type ApplyOutcome, copySample, isSample } from './sample';
import type { ProjectServer } from './server';
import { statusBar } from './status';
import { AirflowTree } from './views/airflowTree';
import { LineageTree, NodeItem } from './views/lineageTree';

const GRAPH_FORMATS: { format: ExportFormat; label: string }[] = [
  { format: 'json', label: 'JSON' },
  { format: 'mermaid', label: 'Mermaid' },
  { format: 'dot', label: 'Graphviz DOT' },
  { format: 'graphml', label: 'GraphML' },
  { format: 'cypher', label: 'Cypher' },
];
const PII_FORMATS: { format: ExportFormat; label: string }[] = [
  { format: 'pii-markdown', label: 'Markdown' },
  { format: 'pii-csv', label: 'CSV' },
];
const STATUS_REFRESH_MS = 60_000;

/** For integration tests, and for other extensions later. */
export interface LineagePilotApi {
  manager: ServerManager;
  lineageTree: LineageTree;
  airflowTree: AirflowTree;
  statusBarText(): string;
  /** The project's open lineage graph panel. */
  lineagePanel(root: string): LineagePanel | undefined;
  /** The project's open Change Impact Review. */
  reviewPanel(root: string): ReviewPanel | undefined;
}

type SampleOutcome = ApplyOutcome | { status: 'cancelled' } | { status: 'restricted' };

export async function activate(context: vscode.ExtensionContext): Promise<LineagePilotApi> {
  const output = vscode.window.createOutputChannel('LineagePilot', { log: true });
  const manager = new ServerManager(context, output);
  const statusItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 10);
  statusItem.name = 'LineagePilot';
  statusItem.command = 'lineagePilot.showStatus';
  const lineageTree = new LineageTree((file) => manager.serverFor(file));
  const lineageView = vscode.window.createTreeView('lineagePilot.lineage', {
    treeDataProvider: lineageTree, showCollapseAll: true,
  });
  const airflowTree = new AirflowTree((file) => manager.serverFor(file), () => manager.list());
  const airflowView = vscode.window.createTreeView('lineagePilot.airflow', { treeDataProvider: airflowTree });

  const updateStatus = () => {
    const bar = statusBar(
      manager.list().map((s) => ({ root: s.project.root, status: s.status, problem: s.problem })), Date.now());
    if (!manager.interpreter && manager.resolution) {
      bar.text = '$(error) Lineage: no Python';
      bar.tooltip = manager.resolution.error ?? 'LineagePilot needs Python 3.10 or later';
    }
    statusItem.text = bar.text;
    statusItem.tooltip = bar.tooltip;
    statusItem.backgroundColor = bar.level === 'error'
      ? new vscode.ThemeColor('statusBarItem.errorBackground')
      : bar.level === 'warning' ? new vscode.ThemeColor('statusBarItem.warningBackground') : undefined;
    statusItem.show();
  };
  let lastGenerations = '';
  manager.onDidChange(() => {
    updateStatus();
    // Re-read the tree when any project's graph changes
    const generations = manager.list()
      .map((s) => `${s.status?.generation}:${s.status?.state}:${s.status?.indexing}:${s.status?.airflow.generation}:${s.status?.airflow.stage}`)
      .join();
    if (generations !== lastGenerations) {
      lastGenerations = generations;
      lineageTree.refresh();
      void lineageTree.showFile(vscode.window.activeTextEditor?.document.uri);
      void airflowTree.showFile(vscode.window.activeTextEditor?.document.uri);
    }
  });
  const updateSampleContext = () => void vscode.commands.executeCommand('setContext', 'lineagePilot.sampleOpen',
    (vscode.workspace.workspaceFolders ?? []).some((folder) => isSample(folder.uri.fsPath)));
  const timer = setInterval(updateStatus, STATUS_REFRESH_MS);

  const activeServer = async (): Promise<ProjectServer | undefined> => {
    const file = vscode.window.activeTextEditor?.document.uri;
    const fromEditor = file?.scheme === 'file' ? manager.serverFor(file.fsPath) : undefined;
    if (fromEditor) {
      return fromEditor;
    }
    const servers = manager.list();
    if (servers.length <= 1) {
      return servers[0];
    }
    const picked = await vscode.window.showQuickPick(
      servers.map((server) => ({ label: server.name, description: server.project.root, server })),
      { title: 'Which dbt project?' });
    return picked?.server;
  };

  // The first node the active editor's file defines, and its project's server
  const editorNode = async (): Promise<{ server: ProjectServer; node: GraphNode } | undefined> => {
    const uri = vscode.window.activeTextEditor?.document.uri;
    const server = uri?.scheme === 'file' ? manager.serverFor(uri.fsPath) : undefined;
    if (!uri || !server) {
      void vscode.window.showInformationMessage('Open a model or properties file of a dbt project first.');
      return undefined;
    }
    const { nodes } = await server.request('lineage/nodesForFile', { uri: uri.toString() });
    if (!nodes[0]) {
      void vscode.window.showInformationMessage('This file defines no node in the lineage graph.');
      return undefined;
    }
    return { server, node: nodes[0] };
  };

  const showImpact = async (nodeId?: string, projectRoot?: string): Promise<NodeItem | undefined> => {
    let server = projectRoot ? manager.serverForRoot(projectRoot) : undefined;
    if (!nodeId) {
      const found = await editorNode();
      if (!found) {
        return undefined;
      }
      ({ server, node: { id: nodeId } } = found);
    }
    server ??= await activeServer();
    if (!server) {
      return undefined;
    }
    const item = await lineageTree.pin(server, nodeId);
    if (item) {
      await lineageView.reveal(item, { select: true, focus: true, expand: 2 });
    }
    return item;
  };

  // From a Lineage tree item, a node ID (with its project, for tests), or the active editor
  const showLineage = async (target?: NodeItem | string, projectRoot?: string): Promise<LineagePanel | undefined> => {
    let server: ProjectServer | undefined;
    let node: Pick<GraphNode, 'id' | 'kind'> | undefined;
    if (target instanceof NodeItem) {
      ({ server, node } = target);
    } else if (typeof target === 'string') {
      server = projectRoot ? manager.serverForRoot(projectRoot) : await activeServer();
      node = { id: target, kind: target.includes('#') ? 'column' : 'model' };
    } else {
      ({ server, node } = (await editorNode()) ?? {});
    }
    if (!server || !node) {
      return undefined;
    }
    return LineagePanel.show(context, server, output, { id: node.id, granularity: node.kind === 'column' ? 'column' : 'model' });
  };

  const exportTo = async (formats: { format: ExportFormat; label: string }[], title: string, target?: vscode.Uri, format?: ExportFormat) => {
    const server = await activeServer();
    if (!server) {
      return undefined;
    }
    format ??= (await vscode.window.showQuickPick(formats.map((f) => ({ label: f.label, format: f.format })), { title }))?.format;
    if (!format) {
      return undefined;
    }
    const result = await server.request('lineage/export', { format });
    const folder = vscode.workspace.workspaceFolders?.[0]?.uri ?? vscode.Uri.file(server.project.root);
    target ??= await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.joinPath(folder, result.filename), title });
    if (!target) {
      return undefined;
    }
    await vscode.workspace.fs.writeFile(target, new TextEncoder().encode(result.content));
    void vscode.window.showInformationMessage(`Exported ${result.nodes} nodes to ${vscode.workspace.asRelativePath(target)}`, 'Open')
      .then((choice) => choice && vscode.window.showTextDocument(target!));
    return target;
  };

  context.subscriptions.push(
    output, statusItem, lineageView, airflowView, manager, { dispose: () => clearInterval(timer) },
    vscode.commands.registerCommand('lineagePilot.showImpact', showImpact),
    vscode.commands.registerCommand('lineagePilot.showLineage', showLineage),
    vscode.commands.registerCommand('lineagePilot.reviewChanges', async (projectRoot?: string) => {
      const server = typeof projectRoot === 'string' ? manager.serverForRoot(projectRoot) : await activeServer();
      if (!server) {
        void vscode.window.showInformationMessage('Open a workspace with a dbt project to review its changes.');
        return undefined;
      }
      return ReviewPanel.show(context, server, output);
    }),
    // For tests: a parent folder skips the dialog, and open=false leaves the window as it is
    vscode.commands.registerCommand('lineagePilot.openSample', async (parent?: string, open = true): Promise<string | undefined> => {
      if (typeof parent !== 'string') {
        const picked = await vscode.window.showOpenDialog({
          canSelectFolders: true, canSelectFiles: false, canSelectMany: false, defaultUri: vscode.Uri.file(os.homedir()),
          title: 'Choose a folder for the LineagePilot sample project', openLabel: 'Copy the Sample Here',
        });
        if (!picked?.[0]) {
          return undefined;
        }
        parent = picked[0].fsPath;
      }
      let folder: string;
      try {
        folder = await copySample(context.asAbsolutePath('sample'), parent);
      } catch (error) {
        void vscode.window.showErrorMessage(`LineagePilot could not copy its sample project: ${(error as Error).message}`);
        return undefined;
      }
      output.info(`Copied the sample project to ${folder}`);
      if (open !== false) {
        await vscode.commands.executeCommand('vscode.openFolder', vscode.Uri.file(folder),
          { forceNewWindow: Boolean(vscode.workspace.workspaceFolders?.length) });
      }
      return folder;
    }),
    // For tests: a root skips the questions. Restricted Mode only reads the workspace, so it is refused
    vscode.commands.registerCommand('lineagePilot.applySampleChange', async (root?: string): Promise<SampleOutcome> => {
      const interactive = typeof root !== 'string';
      if (!vscode.workspace.isTrusted) {
        if (interactive) {
          void vscode.window.showInformationMessage('Trust this folder to apply the sample change. In Restricted Mode '
            + 'LineagePilot only reads the project, and the review needs Git, which Restricted Mode turns off.');
        }
        return { status: 'restricted' };
      }
      const candidates = [
        ...(vscode.workspace.workspaceFolders ?? []).map((folder) => folder.uri.fsPath),
        ...manager.list().map((server) => server.project.root),
      ];
      const sample = interactive ? candidates.find(isSample) : root;
      if (!sample || !isSample(sample)) {
        if (interactive) {
          const choice = await vscode.window.showInformationMessage(
            'Open the sample project first: LineagePilot makes this change only in a copy of its sample.',
            'Open Sample Project');
          if (choice) {
            await vscode.commands.executeCommand('lineagePilot.openSample');
          }
        }
        return { status: 'not-sample' };
      }
      if (interactive && !fs.existsSync(path.join(sample, '.git'))) {
        const choice = await vscode.window.showWarningMessage(
          'The review compares your work with the last commit, and this copy of the sample has none yet. '
          + 'In Source Control, initialize a repository and commit everything, then apply the change.',
          'Open Source Control', 'Apply Anyway');
        if (choice !== 'Apply Anyway') {
          if (choice) {
            await vscode.commands.executeCommand('workbench.view.scm');
          }
          return { status: 'cancelled' };
        }
      }
      const outcome = await applySampleChange(sample, context.asAbsolutePath('sample'));
      if (outcome.status === 'modified' || outcome.status === 'unsafe') {
        output.warn(`Did not apply the sample change in ${sample}: ${JSON.stringify(outcome)}`);
      }
      if (interactive && (outcome.status === 'modified' || outcome.status === 'unsafe')) {
        void vscode.window.showWarningMessage(outcome.status === 'modified'
          ? `LineagePilot did not apply the sample change: ${outcome.files.join(', ')} changed since the sample was `
            + 'copied. Open Sample Project makes a fresh copy.'
          : `LineagePilot did not apply the sample change: ${outcome.file} is a symbolic link, or is reached through one. `
            + 'It writes only to the files of a copy of its sample.', 'Open Sample Project')
          .then((choice) => choice && vscode.commands.executeCommand('lineagePilot.openSample'));
      } else if (interactive && outcome.status === 'applied') {
        await vscode.window.showTextDocument(vscode.Uri.file(path.join(sample, outcome.files.find((f) => f.endsWith('.sql')) ?? outcome.files[0])));
        void vscode.window.showInformationMessage(
          'Renamed order_total to total_usd in models/marts/orders.sql, and put the manifest dbt compile would write '
          + 'in target/. Review Changes shows what the rename reaches.', 'Review Changes')
          .then((choice) => choice && vscode.commands.executeCommand('lineagePilot.reviewChanges'));
      } else if (interactive && outcome.status === 'already-applied') {
        void vscode.window.showInformationMessage('The sample change is already made. Review Changes shows what it reaches.',
          'Review Changes').then((choice) => choice && vscode.commands.executeCommand('lineagePilot.reviewChanges'));
      }
      return outcome;
    }),
    vscode.commands.registerCommand('lineagePilot.followEditor', () => {
      lineageTree.unpin();
      void lineageTree.showFile(vscode.window.activeTextEditor?.document.uri);
    }),
    vscode.commands.registerCommand('lineagePilot.openNode', async (location: unknown) => {
      if (!inWorkspace(location)) {
        output.warn('LineagePilot was asked to open a location outside the workspace folders; refused');
        return;
      }
      await openLocation(location);
    }),
    vscode.commands.registerCommand('lineagePilot.refreshArtifacts',
      async (subcommand?: Subcommand, projectRoot?: string): Promise<RefreshOutcome> => {
        const server = projectRoot ? manager.serverForRoot(projectRoot) : await activeServer();
        if (!server) {
          return { status: 'no-project' };
        }
        const chosen = subcommand && SUBCOMMANDS.includes(subcommand) ? subcommand : await pickSubcommand();
        if (!chosen) {
          return { status: 'cancelled' };
        }
        const outcome = await refreshArtifacts(server.project.root, chosen, output);
        if (outcome.status === 'done' && server.running) {
          await server.request('lineage/refresh', {});
        }
        return outcome;
      }),
    vscode.commands.registerCommand('lineagePilot.reindex', async () => {
      await Promise.all(manager.list().filter((s) => s.running).map((s) => s.request('lineage/refresh', {})));
    }),
    vscode.commands.registerCommand('lineagePilot.exportGraph', (target?: vscode.Uri, format?: ExportFormat) =>
      exportTo(GRAPH_FORMATS, 'Export lineage graph', target, format)),
    vscode.commands.registerCommand('lineagePilot.exportPiiInventory', (target?: vscode.Uri, format?: ExportFormat) =>
      exportTo(PII_FORMATS, 'Export PII inventory', target, format)),
    vscode.commands.registerCommand('lineagePilot.showStatus', async () => {
      const items = manager.list().map((server) => ({
        label: server.name,
        description: server.problem ?? server.status?.state,
        detail: server.status
          ? `${server.status.counts?.nodes ?? 0} nodes · Python ${server.status.server.python} · `
            + `rules ${server.status.rules.restricted ? 'built-in only (Restricted Mode)' : 'built-in and pack'}`
          : server.project.root,
      }));
      await vscode.window.showQuickPick(
        [...items, { label: '$(output) Show Output', description: '', detail: undefined }],
        { title: 'LineagePilot' },
      ).then((choice) => choice?.label.includes('Show Output') && output.show());
    }),
    vscode.commands.registerCommand('lineagePilot.restartServer', () => manager.restartAll(true)),
    vscode.commands.registerCommand('lineagePilot.showOutput', () => output.show()),
    vscode.window.onDidChangeActiveTextEditor((editor) => {
      void lineageTree.showFile(editor?.document.uri);
      void airflowTree.showFile(editor?.document.uri);
    }),
    vscode.workspace.onDidChangeWorkspaceFolders(() => {
      updateSampleContext();
      void manager.rediscover();
    }),
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (event.affectsConfiguration('lineagePilot.python') || event.affectsConfiguration('lineagePilot.index')
          || event.affectsConfiguration('lineagePilot.airflow')) {
        void manager.restartAll();
      }
    }),
    // Trust changes what the servers may do (rule packs) and which interpreters may run
    vscode.workspace.onDidGrantWorkspaceTrust(() => void manager.restartAll()),
  );
  const projectWatcher = vscode.workspace.createFileSystemWatcher(PROJECT_GLOB, false, true, false);
  projectWatcher.onDidCreate(() => void manager.rediscover());
  projectWatcher.onDidDelete(() => void manager.rediscover());
  context.subscriptions.push(projectWatcher);

  updateStatus();
  updateSampleContext();
  void forgetOldCounts(context.globalState);  // the usage counts 0.1.x kept, if any
  await manager.start(true);
  updateStatus();
  void lineageTree.showFile(vscode.window.activeTextEditor?.document.uri);
  void airflowTree.showFile(vscode.window.activeTextEditor?.document.uri);
  return {
    manager,
    lineageTree,
    airflowTree,
    statusBarText: () => statusItem.text,
    lineagePanel: (root) => LineagePanel.forProject(root),
    reviewPanel: (root) => ReviewPanel.forProject(root),
  };
}

export function deactivate(): void {
  // Servers are stopped by their disposables in context.subscriptions
}
