// The Change Impact Review panel (task 4.4): reads what changed through vscode.git, asks the
// project's language server for the review, and shows it. The review runs again when the index or
// the working tree changes. "Copy as Markdown" copies the text the server rendered, which is what
// lineagepilot review prints for the same inputs.

import * as vscode from 'vscode';

import { baseChoices, collectChanges, GitProblem, type GitRepository, RefType } from './git';
import { NoRepository, repositoryFor } from './gitExtension';
import { isNotReady } from './lineage';
import { openOutsidePanel } from './panel';
import type { ReviewResult } from './protocol';
import { ReviewHostMessage, type ReviewProblem, ReviewWebviewMessage } from './reviewMessages';
import type { ProjectServer } from './server';
import { webviewHtml } from './webviewHtml';

export const REVIEW_VIEW_TYPE = 'lineagePilot.review';
const BASE_KEY = 'lineagePilot.reviewBase';
// A commit or a save changes the state in several steps; wait for it to settle
const WORKING_TREE_DEBOUNCE_MS = 1000;
const REF_LIMIT = 300;

type Run = Extract<ReviewHostMessage, { type: 'reviewResult' | 'reviewFailed' }>;

export class ReviewPanel implements vscode.Disposable {
  private static readonly panels = new Map<string, ReviewPanel>();

  /** Show the project's review, creating it if needed, and bring it up to date. */
  static show(context: vscode.ExtensionContext, server: ProjectServer, output: vscode.LogOutputChannel): ReviewPanel {
    const existing = ReviewPanel.panels.get(server.project.root);
    if (existing) {
      existing.panel.reveal();
      void existing.run();
      return existing;
    }
    const panel = new ReviewPanel(context, server, output);
    ReviewPanel.panels.set(server.project.root, panel);
    return panel;
  }

  static forProject(root: string): ReviewPanel | undefined {
    return ReviewPanel.panels.get(root);
  }

  readonly panel: vscode.WebviewPanel;
  private readonly disposables: vscode.Disposable[] = [];
  private readonly posted = new vscode.EventEmitter<ReviewHostMessage>();
  /** Every message sent to the webview (integration tests listen). */
  readonly onDidPost = this.posted.event;
  private ready = false;
  private runs = 0;
  private last: Run | undefined;
  private dirty = false;
  private indexKey: string | undefined;
  private repository: GitRepository | undefined;
  private repositoryWatch: { dispose(): void } | undefined;
  private debounce: NodeJS.Timeout | undefined;

  private constructor(
    private readonly context: vscode.ExtensionContext,
    readonly server: ProjectServer,
    private readonly output: vscode.LogOutputChannel,
  ) {
    const bundle = vscode.Uri.joinPath(context.extensionUri, 'dist', 'webview');
    const title = `Change impact: ${server.name}`;
    this.panel = vscode.window.createWebviewPanel(REVIEW_VIEW_TYPE, title, vscode.ViewColumn.Active, {
      enableScripts: true,
      // Only the webview's own bundle can be loaded: nothing from the workspace
      localResourceRoots: [bundle],
    });
    const webview = this.panel.webview;
    webview.html = webviewHtml({
      cspSource: webview.cspSource,
      scriptUri: webview.asWebviewUri(vscode.Uri.joinPath(bundle, 'review.js')).toString(),
      styleUri: webview.asWebviewUri(vscode.Uri.joinPath(bundle, 'review.css')).toString(),
      title,
    });
    this.indexKey = this.keyOfIndex();
    this.disposables.push(
      webview.onDidReceiveMessage((message) => void this.handle(message)),
      server.onDidChange(() => this.indexChanged()),
      vscode.workspace.onDidChangeConfiguration((event) => {
        if (event.affectsConfiguration('lineagePilot.baseline')) {
          void this.run();
        }
      }),
      // A hidden panel's page is destroyed, and built again when it is shown: it says ready again
      this.panel.onDidChangeViewState(() => {
        if (!this.panel.visible) {
          this.ready = false;
        } else if (this.dirty) {
          void this.run();
        }
      }),
      this.panel.onDidDispose(() => this.dispose()),
      this.posted,
    );
  }

  /** The base chosen for this project, or null for the default branch. */
  private get chosenBase(): string | null {
    return this.context.workspaceState.get<string | null>(`${BASE_KEY}:${this.server.project.root}`) ?? null;
  }

  private async chooseBase(ref: string | null): Promise<void> {
    await this.context.workspaceState.update(`${BASE_KEY}:${this.server.project.root}`, ref);
  }

  /** Handle one message from the webview. Anything that does not match the schema is dropped. */
  async handle(raw: unknown): Promise<void> {
    const parsed = ReviewWebviewMessage.safeParse(raw);
    if (!parsed.success) {
      this.output.warn(`Ignoring a malformed message from the review: ${parsed.error.issues[0]?.message}`);
      return;
    }
    const message = parsed.data;
    switch (message.type) {
      case 'ready':
        this.ready = true;
        await this.post({
          type: 'init',
          project: { name: this.server.name, root: this.server.project.root },
          trusted: vscode.workspace.isTrusted,
        });
        // A page rebuilt after being hidden gets what it showed, unless that has gone stale
        if (this.last && !this.dirty) {
          await this.post(this.last);
        } else {
          await this.run();
        }
        return;
      case 'refresh':
        await this.run();
        return;
      case 'pickBase':
        await this.pickBase();
        return;
      case 'copyMarkdown':
        await this.copy();
        return;
      case 'openFile':
        await openOutsidePanel(this.panel, message.location, this.server.project.root, this.output);
        return;
      case 'showInGraph':
        await vscode.commands.executeCommand('lineagePilot.showLineage', message.id, this.server.project.root);
        return;
      case 'runCompile':
        await this.compile();
        return;
      case 'manageTrust':
        await vscode.commands.executeCommand('workbench.trust.manage');
        return;
    }
  }

  /** Review what has changed now. A run that a later one overtakes is dropped. */
  async run(): Promise<void> {
    if (!this.panel.visible) {
      this.dirty = true;
      return;
    }
    this.dirty = false;
    const run = ++this.runs;
    if (this.ready) {
      await this.post({ type: 'reviewStarted' });
    }
    const outcome = await this.review();
    if (run !== this.runs) {
      return;
    }
    this.last = outcome;
    if (this.ready) {
      await this.post(outcome);
    }
  }

  private async review(): Promise<Run> {
    try {
      const repository = await repositoryFor(this.server.project.root);
      this.watch(repository);
      const { base, files } = await collectChanges(repository, this.chosenBase);
      const result = await this.server.request('lineage/reviewChanges', {
        changedFiles: files,
        baseline: { ref: base.ref, commit: base.commit, manifestPath: this.baselineManifest() },
      });
      return { type: 'reviewResult', result };
    } catch (error) {
      return { type: 'reviewFailed', problem: problemOf(error), message: error instanceof Error ? error.message : String(error) };
    }
  }

  private baselineManifest(): string | null {
    const folder = vscode.Uri.file(this.server.project.folder);
    const path = vscode.workspace.getConfiguration('lineagePilot', folder).get<string>('baseline.manifestPath', '');
    return path.trim() || null;
  }

  private watch(repository: GitRepository): void {
    if (repository === this.repository) {
      return;
    }
    this.repositoryWatch?.dispose();
    this.repository = repository;
    this.repositoryWatch = repository.state.onDidChange(() => {
      clearTimeout(this.debounce);
      this.debounce = setTimeout(() => void this.run(), WORKING_TREE_DEBOUNCE_MS);
    });
  }

  private keyOfIndex(): string | undefined {
    const status = this.server.status;
    return status && !status.indexing ? `${status.generation}:${status.state}:${status.columnLineage}` : undefined;
  }

  private indexChanged(): void {
    const key = this.keyOfIndex();
    if (key !== undefined && key !== this.indexKey) {
      this.indexKey = key;
      void this.run();
    }
  }

  private async pickBase(): Promise<void> {
    const repository = this.repository ?? await repositoryFor(this.server.project.root).catch(() => undefined);
    const refs = repository ? await repository.getRefs({ sort: 'committerdate', count: REF_LIMIT }).catch(() => []) : [];
    const icon = (type: number) => (type === RefType.Tag ? '$(tag)' : type === RefType.RemoteHead ? '$(cloud)' : '$(git-branch)');
    const chosen = this.chosenBase;
    const picked = await vscode.window.showQuickPick(
      [
        { label: '$(git-merge) Default branch', description: chosen ? undefined : 'current', ref: null as string | null },
        ...baseChoices(refs).map(({ name, type }) => ({
          label: `${icon(type)} ${name}`, description: name === chosen ? 'current' : undefined, ref: name as string | null,
        })),
      ],
      { title: 'Compare with the merge-base of HEAD and', matchOnDescription: true },
    );
    if (picked && picked.ref !== chosen) {
      await this.chooseBase(picked.ref);
      await this.run();
    }
  }

  private async copy(): Promise<void> {
    const markdown = this.last?.type === 'reviewResult' ? this.last.result.markdown : undefined;
    if (markdown === undefined) {
      return;
    }
    await vscode.env.clipboard.writeText(markdown);
    await this.post({ type: 'copied', characters: markdown.length });
  }

  /** dbt runs only through the guarded command, which refuses in Restricted Mode (task 2.5). */
  private async compile(): Promise<void> {
    if (!vscode.workspace.isTrusted) {
      this.output.warn('The review asked to run dbt compile in Restricted Mode; refused');
      return;
    }
    await vscode.commands.executeCommand('lineagePilot.refreshArtifacts', 'compile', this.server.project.root);
  }

  /** The latest review (integration tests read it). */
  get latest(): ReviewResult | undefined {
    return this.last?.type === 'reviewResult' ? this.last.result : undefined;
  }

  async post(message: ReviewHostMessage): Promise<boolean> {
    const checked = ReviewHostMessage.parse(message);
    this.posted.fire(checked);
    return this.panel.webview.postMessage(checked);
  }

  dispose(): void {
    ReviewPanel.panels.delete(this.server.project.root);
    clearTimeout(this.debounce);
    this.repositoryWatch?.dispose();
    for (const disposable of this.disposables.splice(0)) {
      disposable.dispose();
    }
    this.panel.dispose();
  }
}

export function problemOf(error: unknown): ReviewProblem {
  if (error instanceof NoRepository || error instanceof GitProblem) {
    return error.kind;
  }
  return isNotReady(error) ? 'not_ready' : 'failed';
}
