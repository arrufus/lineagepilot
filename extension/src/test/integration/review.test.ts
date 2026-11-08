// The Change Impact Review in a real VS Code, with the real vscode.git (tasks 4.1 and 4.4). The
// page's own bundle loads under the CSP and asks for the review; the panel reads git, asks the
// server, copies the server's text, and refuses everything else. Restricted Mode has no git, so it
// must say so rather than report that nothing changed.
import { strict as assert } from 'assert';
import { execFileSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as vscode from 'vscode';

import type { ReviewResult } from '../../protocol';
import type { ReviewHostMessage } from '../../reviewMessages';
import type { ReviewPanel } from '../../reviewPanel';
import { api, expectTrusted, J, label, ran, readyServer, root, until } from './helpers';

type Posted<T extends ReviewHostMessage['type']> = Extract<ReviewHostMessage, { type: T }>;
const USAGE = 'lineagePilot.usageCounts.enabled';

function git(...args: string[]): void {
  execFileSync('git', ['-c', 'user.name=t', '-c', 'user.email=t@example.com', ...args], { cwd: root(), stdio: 'pipe' });
}

function reviewOf(panel: ReviewPanel, match: (result: ReviewResult) => boolean): Promise<ReviewResult> {
  return until('the review to show the change', 60_000, async () => {
    const latest = panel.latest;
    if (latest && match(latest)) {
      return latest;
    }
    await panel.run();
    return undefined;
  });
}

suite(`change impact review (${label})`, () => {
  let panel: ReviewPanel;
  const posted: ReviewHostMessage[] = [];
  const find = <T extends ReviewHostMessage['type']>(type: T) =>
    posted.find((m): m is Posted<T> => m.type === type);

  async function open(): Promise<void> {
    panel = (await vscode.commands.executeCommand<ReviewPanel>('lineagePilot.reviewChanges', root()))!;
    assert.ok(panel, 'the command returns the panel');
    panel.onDidPost((message) => posted.push(message));
    assert.equal((await api()).reviewPanel(root()), panel);
  }

  suiteTeardown(async () => {
    panel?.dispose();
    await vscode.commands.executeCommand('workbench.action.closeAllEditors');
  });

  if (!expectTrusted) {
    test('Restricted Mode has no git: the review says so, and lineage keeps working', async function () {
      this.timeout(120_000);
      const server = await readyServer();
      await open();
      const failed = await until('the page to report the failure', 90_000, () => find('reviewFailed'));
      assert.equal(failed.problem, 'restricted');
      assert.match(failed.message, /Restricted Mode/);
      assert.equal(panel.latest, undefined);
      const graph = await server.request('lineage/graph', { root: `${J}.orders`, direction: 'both', depth: 1, granularity: 'model' });
      assert.ok(graph.nodes.some((n) => n.name === 'customers'));
    });

    test('Restricted Mode: the page cannot run dbt', async () => {
      await panel.handle({ type: 'runCompile' });
      assert.equal(ran('user-dbt-ran'), undefined);
      assert.equal(ran('planted-dbt-ran'), undefined);
    });
    return;
  }

  const model = () => path.join(root(), 'models', 'marts', 'orders.sql');
  const newModel = () => path.join(root(), 'models', 'marts', 'added_by_this_branch.sql');
  let original: string;
  let times: fs.Stats;

  suiteSetup(async function () {
    this.timeout(180_000);
    await readyServer();
    original = fs.readFileSync(model(), 'utf8');
    times = fs.statSync(model());
    fs.writeFileSync(path.join(root(), '.gitignore'), 'target/\n.venv/\n.vscode/\n.lineagepilot/\n');
    git('init', '-q', '-b', 'main');
    git('add', '-A');
    git('commit', '-q', '-m', 'base');
  });

  suiteTeardown(() => {
    fs.writeFileSync(model(), original);
    fs.utimesSync(model(), times.atime, times.mtime);
    fs.rmSync(newModel(), { force: true });
    // The repository stays: this is a temporary copy, and on Windows VS Code's Git extension holds
    // .git open, so it cannot be deleted from here
    fs.rmSync(path.join(root(), '.gitignore'), { force: true });
  });

  test('Review Changes opens the page; its script loads under the CSP and gets a review', async function () {
    this.timeout(120_000);
    await open();
    const answer = await until('the page to ask for the review', 90_000, () => find('reviewResult'));
    assert.equal(answer.result.base?.ref, 'main');
    assert.match(answer.result.base?.commit ?? '', /^[0-9a-f]{40}$/);
    assert.equal(answer.result.summary.changedFiles, 0);
    assert.equal(panel.panel.title, 'Change impact: jaffle_shop');
    const { html, options } = panel.panel.webview;
    assert.match(html, /<script nonce="[^"]+" src="[^"]+\/dist\/webview\/review\.js"><\/script>/);
    assert.doesNotMatch(html, /unsafe-eval/);
    const roots = options.localResourceRoots?.map((r) => r.path.replace(/\\/g, '/').split('/').slice(-2).join('/'));
    assert.deepEqual(roots, ['dist/webview']);
  });

  test('a changed model and an untracked file reach the server through vscode.git', async () => {
    fs.appendFileSync(model(), '\n-- a change under review\n');
    fs.writeFileSync(newModel(), 'select 1 as id\n');
    const result = await reviewOf(panel, (r) => r.summary.changedFiles >= 2);
    const orders = result.changedNodes.find((n) => n.id === `${J}.orders`);
    assert.ok(orders, JSON.stringify(result.changedNodes.map((n) => n.id)));
    assert.equal(orders.status, 'modified');
    // The file is newer than the manifest, so its columns wait for dbt compile
    assert.equal(orders.columns.state, 'unknown');
    assert.equal(orders.columns.stale, true);
    assert.equal(result.summary.staleNodes, 1);
    // Nothing is lost for that: everything below is reached at model level
    const customers = result.impacted.find((n) => n.id === `${J}.customers`);
    assert.equal(customers?.reach, 'model');
    assert.equal(customers?.confidence, 'inferred');
    const added = result.unknowns.find((u) => u.subject === 'models/marts/added_by_this_branch.sql');
    assert.match(added?.reason ?? '', /not in the manifest yet/);
    assert.ok(result.locations[`${J}.customers`]?.uri.endsWith('customers.sql'));
  });

  test('Copy as Markdown copies the text the server rendered, and takes none from the page', async () => {
    // With usage counts on, the copy is counted (task 6.3), and only the one that copied
    const { usage } = await api();
    const config = vscode.workspace.getConfiguration();
    await config.update(USAGE, true, vscode.ConfigurationTarget.Global);
    try {
      await until('counting to be on', 10_000, () => usage.isEnabled);
      await vscode.env.clipboard.writeText('before');
      const markdown = panel.latest!.markdown;
      assert.match(markdown, /^## Change impact review: `jaffle_shop`/);
      await panel.handle({ type: 'copyMarkdown', text: 'from the page' });
      assert.equal(await vscode.env.clipboard.readText(), 'before', 'a message with text is malformed, so it is dropped');
      await panel.handle({ type: 'copyMarkdown' });
      assert.equal(await vscode.env.clipboard.readText(), markdown);
      assert.equal(find('copied')?.characters, markdown.length);
      await until('the copy to be counted', 10_000, () => Object.values(usage.state().weeks)[0]?.copied === 1);
    } finally {
      await config.update(USAGE, undefined, vscode.ConfigurationTarget.Global);
    }
    await until('the counts to be forgotten', 10_000, () => Object.keys(usage.state().weeks).length === 0);
  });

  test('a node opens its file beside the review, and in the lineage graph', async () => {
    const location = panel.latest!.locations[`${J}.customers`];
    await panel.handle({ type: 'openFile', location });
    await until('the file to open', 20_000, () => vscode.window.visibleTextEditors.some((e) => e.document.uri.fsPath.endsWith('customers.sql')));
    await panel.handle({ type: 'showInGraph', id: `${J}.customers` });
    const graph = await until('the graph to open', 20_000, async () => (await api()).lineagePanel(root()));
    graph.dispose();
  });

  test('it refuses to open anything outside the workspace folder', async () => {
    const outside = path.join(root(), '..', 'outside-the-project.sql'); // root() is the workspace folder
    fs.writeFileSync(outside, 'select 1');
    try {
      await vscode.commands.executeCommand('workbench.action.closeAllEditors');
      await open();
      await panel.handle({ type: 'openFile', location: { uri: vscode.Uri.file(outside).toString(), range: null } });
      await panel.handle({ type: 'openFile', location: { uri: 'https://example.com/x.sql', range: null } });
      await new Promise((resolve) => setTimeout(resolve, 500));
      assert.equal(vscode.window.visibleTextEditors.some((e) => e.document.uri.fsPath.endsWith('outside-the-project.sql')), false);
    } finally {
      fs.rmSync(outside, { force: true });
    }
  });

  test('the working tree changing brings the review up to date by itself', async function () {
    this.timeout(60_000);
    await reviewOf(panel, (r) => r.summary.changedFiles >= 2);
    fs.rmSync(newModel(), { force: true });
    git('status');
    await until('the review to drop the deleted file', 45_000, () =>
      panel.latest?.unknowns.every((u) => !u.subject.includes('added_by_this_branch')) || undefined);
  });

  test('a page rebuilt after being hidden gets the review again', async () => {
    const from = posted.length;
    await panel.handle({ type: 'ready' });
    await until('the init and the review', 30_000, () =>
      posted.slice(from).some((m) => m.type === 'init') && posted.slice(from).some((m) => m.type === 'reviewResult'));
  });

  test('malformed messages are dropped', async () => {
    const before = posted.length;
    for (const message of [null, 'copyMarkdown', { type: 'runCommand', command: 'x' }, { type: 'openFile' }]) {
      await panel.handle(message);
    }
    assert.equal(posted.length, before);
  });
});
