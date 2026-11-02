// LineagePilot: lineage and change impact for dbt projects, from the artifacts in the
// workspace (design section 7). This file wires the language servers, commands, views and
// status bar together.

import * as vscode from 'vscode';

import { pickSubcommand, refreshArtifacts, type RefreshOutcome, type Subcommand, SUBCOMMANDS } from './dbt';
import { ServerManager } from './manager';
import { PROJECT_GLOB } from './projects';
import type { ExportFormat, Location } from './protocol';
import type { ProjectServer } from './server';
import { statusBar } from './status';
import { AirflowTree, LineageTree, type NodeItem } from './views/lineageTree';

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
  statusBarText(): string;
}

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
  const airflowView = vscode.window.createTreeView('lineagePilot.airflow', { treeDataProvider: new AirflowTree() });

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
    const generations = manager.list().map((s) => `${s.status?.generation}:${s.status?.state}:${s.status?.indexing}`).join();
    if (generations !== lastGenerations) {
      lastGenerations = generations;
      lineageTree.refresh();
      void lineageTree.showFile(vscode.window.activeTextEditor?.document.uri);
    }
  });
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

  const showImpact = async (nodeId?: string, projectRoot?: string): Promise<NodeItem | undefined> => {
    let server = projectRoot ? manager.serverForRoot(projectRoot) : undefined;
    if (!nodeId) {
      const uri = vscode.window.activeTextEditor?.document.uri;
      server = uri?.scheme === 'file' ? manager.serverFor(uri.fsPath) : undefined;
      if (!uri || !server) {
        void vscode.window.showInformationMessage('Open a model or properties file of a dbt project first.');
        return undefined;
      }
      const { nodes } = await server.request('lineage/nodesForFile', { uri: uri.toString() });
      nodeId = nodes[0]?.id;
      if (!nodeId) {
        void vscode.window.showInformationMessage('This file defines no node in the lineage graph.');
        return undefined;
      }
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
    vscode.commands.registerCommand('lineagePilot.followEditor', () => {
      lineageTree.unpin();
      void lineageTree.showFile(vscode.window.activeTextEditor?.document.uri);
    }),
    vscode.commands.registerCommand('lineagePilot.openNode', async (location: Location) => {
      const document = await vscode.workspace.openTextDocument(vscode.Uri.parse(location.uri));
      const range = location.range
        ? new vscode.Range(location.range.start.line, location.range.start.character,
          location.range.end.line, location.range.end.character)
        : undefined;
      await vscode.window.showTextDocument(document, { selection: range, preview: true });
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
    vscode.window.onDidChangeActiveTextEditor((editor) => void lineageTree.showFile(editor?.document.uri)),
    vscode.workspace.onDidChangeWorkspaceFolders(() => void manager.rediscover()),
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
  await manager.start(true);
  updateStatus();
  void lineageTree.showFile(vscode.window.activeTextEditor?.document.uri);
  return { manager, lineageTree, statusBarText: () => statusItem.text };
}

export function deactivate(): void {
  // Servers are stopped by their disposables in context.subscriptions
}
