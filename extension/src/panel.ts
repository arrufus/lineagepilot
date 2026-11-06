// The lineage graph panel (task 3.1): hosts the webview, validates everything it sends, forwards
// its requests to the project's language server, and opens files for it.

import * as vscode from 'vscode';

import { isInside } from './interpreter';
import type { Location, Status } from './protocol';
import type { ProjectServer } from './server';
import {
  DEFAULT_SETTINGS, type Focus, HostMessage, ViewSettings, WebviewMessage,
} from './webviewMessages';
import { webviewHtml } from './webviewHtml';

export const VIEW_TYPE = 'lineagePilot.graph';
const SETTINGS_KEY = 'lineagePilot.graphSettings';

/** Open a protocol Location in an editor, selecting its range when it has one. */
export async function openLocation(
  location: Location, options: { viewColumn?: vscode.ViewColumn; preserveFocus?: boolean } = {},
): Promise<vscode.TextEditor> {
  const document = await vscode.workspace.openTextDocument(vscode.Uri.parse(location.uri));
  const range = location.range
    ? new vscode.Range(location.range.start.line, location.range.start.character,
      location.range.end.line, location.range.end.character)
    : undefined;
  return vscode.window.showTextDocument(document, { ...options, selection: range, preview: true });
}

/** Open a file a webview asked for: only file URIs inside the project. Never in the panel's own
 * group, where the file would hide the panel (and VS Code would destroy its page). */
export async function openOutsidePanel(
  panel: vscode.WebviewPanel, location: Location, root: string, output: vscode.LogOutputChannel, preserveFocus = false,
): Promise<void> {
  const uri = vscode.Uri.parse(location.uri);
  if (uri.scheme !== 'file' || !isInside(uri.fsPath, root, process.platform)) {
    output.warn(`A LineagePilot view asked to open ${location.uri}, which is outside the workspace folder; refused`);
    return;
  }
  const viewColumn = panel.viewColumn === vscode.ViewColumn.One ? vscode.ViewColumn.Two : vscode.ViewColumn.One;
  await openLocation(location, { viewColumn, preserveFocus });
}

function scanKey(status: Status | undefined): string {
  return status ? `${status.airflow.generation}:${status.airflow.stage}` : '';
}

export class LineagePanel implements vscode.Disposable {
  private static readonly panels = new Map<string, LineagePanel>();
  private readonly disposables: vscode.Disposable[] = [];
  private readonly posted = new vscode.EventEmitter<HostMessage>();
  /** Every message sent to the webview (integration tests listen). */
  readonly onDidPost = this.posted.event;
  private ready = false;
  private generation = -1;
  private scan = '';

  /** Show the project's panel, focused on ``focus``, creating it if needed. */
  static show(
    context: vscode.ExtensionContext, server: ProjectServer, output: vscode.LogOutputChannel, focus: Focus | null,
  ): LineagePanel {
    const existing = LineagePanel.panels.get(server.project.root);
    if (existing) {
      // Where it is: moving it to another group would rebuild its page
      existing.panel.reveal();
      if (focus) {
        existing.focus(focus);
      }
      return existing;
    }
    const panel = new LineagePanel(context, server, output, focus);
    LineagePanel.panels.set(server.project.root, panel);
    return panel;
  }

  static forProject(root: string): LineagePanel | undefined {
    return LineagePanel.panels.get(root);
  }

  readonly panel: vscode.WebviewPanel;

  private constructor(
    private readonly context: vscode.ExtensionContext,
    readonly server: ProjectServer,
    private readonly output: vscode.LogOutputChannel,
    private pendingFocus: Focus | null,
  ) {
    const bundle = vscode.Uri.joinPath(context.extensionUri, 'dist', 'webview');
    this.panel = vscode.window.createWebviewPanel(VIEW_TYPE, `Lineage: ${server.name}`, vscode.ViewColumn.Beside, {
      enableScripts: true,
      // Only the webview's own bundle can be loaded: nothing from the workspace
      localResourceRoots: [bundle],
    });
    const webview = this.panel.webview;
    webview.html = webviewHtml({
      cspSource: webview.cspSource,
      scriptUri: webview.asWebviewUri(vscode.Uri.joinPath(bundle, 'webview.js')).toString(),
      styleUri: webview.asWebviewUri(vscode.Uri.joinPath(bundle, 'webview.css')).toString(),
      title: `Lineage: ${server.name}`,
    });
    this.disposables.push(
      webview.onDidReceiveMessage((message) => void this.handle(message)),
      server.onDidChange(() => this.indexChanged()),
      // A hidden panel's page is destroyed, and built again when it is shown: until the new page
      // says it is ready, messages would be lost, so a focus waits for its init
      this.panel.onDidChangeViewState(() => {
        if (!this.panel.visible) {
          this.ready = false;
        }
      }),
      this.panel.onDidDispose(() => this.dispose()),
      this.posted,
    );
  }

  get settings(): ViewSettings {
    const stored = ViewSettings.safeParse(this.context.globalState.get(SETTINGS_KEY));
    return stored.success ? stored.data : DEFAULT_SETTINGS;
  }

  focus(focus: Focus): void {
    if (this.ready) {
      void this.post({ type: 'focus', focus });
    } else {
      this.pendingFocus = focus;
    }
  }

  /** Handle one message from the webview. Anything that does not match the schema is dropped. */
  async handle(raw: unknown): Promise<void> {
    const parsed = WebviewMessage.safeParse(raw);
    if (!parsed.success) {
      this.output.warn(`Ignoring a malformed message from the lineage view: ${parsed.error.issues[0]?.message}`);
      return;
    }
    const message = parsed.data;
    switch (message.type) {
      case 'ready':
        this.ready = true;
        this.generation = this.server.status?.generation ?? 0;
        this.scan = scanKey(this.server.status);
        await this.post({
          type: 'init',
          project: { name: this.server.name, root: this.server.project.root },
          focus: this.pendingFocus,
          settings: this.settings,
          generation: this.generation,
        });
        this.pendingFocus = null;
        return;
      case 'graph':
        return this.forward(message.id, () => this.server.request('lineage/graph', message.params),
          (result) => ({ type: 'graphResult', id: message.id, result }));
      case 'impact':
        return this.forward(message.id, () => this.server.request('lineage/impact', message.params),
          (result) => ({ type: 'impactResult', id: message.id, result }));
      case 'node':
        return this.forward(message.id, () => this.server.request('lineage/node', message.params),
          (result) => ({ type: 'nodeResult', id: message.id, result }));
      case 'openFile':
        // The workspace folder, not the project: a DAG file is often beside the project
        await openOutsidePanel(this.panel, message.location, this.server.project.folder, this.output, message.preserveFocus);
        return;
      case 'saveSettings':
        await this.context.globalState.update(SETTINGS_KEY, message.settings);
        return;
    }
  }

  private async forward<T>(id: number, call: () => Promise<T>, wrap: (result: T) => HostMessage): Promise<void> {
    try {
      await this.post(wrap(await call()));
    } catch (error) {
      const code = (error as { code?: unknown }).code;
      await this.post({
        type: 'error',
        id,
        message: error instanceof Error ? error.message : String(error),
        code: typeof code === 'number' && Number.isInteger(code) ? code : null,
      });
    }
  }

  private indexChanged(): void {
    const status = this.server.status;
    if (!this.ready || !status || status.indexing) {
      return;
    }
    // A DAG scan arriving changes what the graph shows, with the same index
    if (status.generation === this.generation && scanKey(status) === this.scan) {
      return;
    }
    this.generation = status.generation;
    this.scan = scanKey(status);
    void this.post({ type: 'indexUpdated', generation: status.generation, ready: status.state === 'ready' });
  }

  async post(message: HostMessage): Promise<boolean> {
    const checked = HostMessage.parse(message);
    this.posted.fire(checked);
    return this.panel.webview.postMessage(checked);
  }

  dispose(): void {
    LineagePanel.panels.delete(this.server.project.root);
    for (const disposable of this.disposables.splice(0)) {
      disposable.dispose();
    }
    this.panel.dispose();
  }
}
