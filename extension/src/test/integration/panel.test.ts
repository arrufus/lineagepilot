// The lineage graph panel in a real VS Code (Phase 3: task 3.1 and the exit criteria). The
// webview's own bundle loads under the CSP and asks for the graph; the panel forwards requests
// to the server, opens the right file and range for a node, and refuses everything else.
import { strict as assert } from 'assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as vscode from 'vscode';

import type { LineagePanel } from '../../panel';
import { ErrorCode, type GraphNode, type GraphResult, type Location } from '../../protocol';
import type { NodeItem } from '../../views/lineageTree';
import { DEFAULT_SETTINGS, type HostMessage } from '../../webviewMessages';
import { api, J, readyServer, root, until, uri } from './helpers';

type Posted<T extends HostMessage['type']> = Extract<HostMessage, { type: T }>;

suite('lineage graph panel', () => {
  // The extension runs from its bundle, so the panel is used through its interface only
  let panel: LineagePanel;
  const posted: HostMessage[] = [];
  let nextId = 9000;

  const find = <T extends HostMessage['type']>(type: T, match: (m: Posted<T>) => boolean = () => true, from = 0) =>
    posted.slice(from).find((m): m is Posted<T> => m.type === type && match(m as Posted<T>));

  /** Send the panel a message as its webview would, and wait for the answer to it. */
  async function ask<T extends 'graphResult' | 'impactResult' | 'nodeResult' | 'error'>(
    message: Record<string, unknown>, type: T): Promise<Posted<T>> {
    const id = nextId++;
    await panel.handle({ ...message, id });
    return until(`${type} ${id}`, 30_000, () => find(type, (m) => 'id' in m && m.id === id));
  }

  async function graph(rootId: string, depth = 2): Promise<GraphResult> {
    const answer = await ask({ type: 'graph', params: { root: rootId, direction: 'both', depth, granularity: 'model' } }, 'graphResult');
    return answer.result;
  }

  function nodeIn(result: GraphResult, id: string): GraphNode {
    const found = result.nodes.find((n) => n.id === id);
    assert.ok(found, `${id} is not in the graph of ${result.root}`);
    return found;
  }

  suiteSetup(async function () {
    this.timeout(180_000);
    await readyServer();
    await vscode.commands.executeCommand('workbench.action.closeAllEditors');
  });

  suiteTeardown(async () => {
    panel?.dispose();
    await vscode.commands.executeCommand('workbench.action.closeAllEditors');
  });

  test('Show Lineage Graph opens the view; its script loads under the CSP and asks for the graph', async function () {
    this.timeout(120_000);
    panel = (await vscode.commands.executeCommand<LineagePanel>('lineagePilot.showLineage', `${J}.orders`, root()))!;
    assert.ok(panel, 'the command returns the panel');
    panel.onDidPost((message) => posted.push(message));
    assert.equal((await api()).lineagePanel(root()), panel);
    // Only the real webview sends ready, and only once its bundle has run: the init that answers
    // it carries the focus, and the graph it then asks for comes back from the server
    const answer = await until('the webview to ask for the graph of orders', 90_000,
      () => find('graphResult', (m) => m.result.root === `${J}.orders` && m.id < 9000));
    assert.ok(answer.result.nodes.some((n) => n.name === 'stg_orders'));
    assert.equal(panel.panel.title, 'Lineage: pilot_shop');
  });

  test('the page allows only its own script, by nonce, and its own files', () => {
    const { html, options } = panel.panel.webview;
    const csp = /http-equiv="Content-Security-Policy" content="([^"]+)"/.exec(html)?.[1].replace(/&#39;|&apos;/g, "'");
    assert.ok(csp, 'the page has a CSP');
    assert.match(csp, /default-src 'none'/);
    const nonce = /script-src 'nonce-([A-Za-z0-9+/=]+)'/.exec(csp)?.[1];
    assert.ok(nonce, csp);
    assert.doesNotMatch(csp, /unsafe-eval|script-src[^;]*(https?:|\*)/);
    const scripts = [...html.matchAll(/<script\b([^>]*)>/g)].map((m) => m[1]);
    assert.equal(scripts.length, 1);
    assert.match(scripts[0], new RegExp(`nonce="${nonce.replace(/[+/=]/g, '\\$&')}"`));
    assert.match(scripts[0], /src="[^"]+\/dist\/webview\/webview\.js"/);
    assert.equal(options.enableScripts, true);
    const roots = options.localResourceRoots?.map((r) => r.path) ?? [];
    assert.deepEqual(roots.map((r) => r.replace(/\\/g, '/').split('/').slice(-2).join('/')), ['dist/webview']);
  });

  test('graph, node and impact requests are answered by the server', async () => {
    const orders = await graph(`${J}.orders`);
    assert.ok(orders.nodes.some((n) => n.side === 'downstream' && n.name === 'customers'));
    assert.ok(orders.nodes.some((n) => n.side === 'upstream' && n.name === 'stg_orders'));
    const detail = await ask({ type: 'node', params: { id: `${J}.orders` } }, 'nodeResult');
    assert.equal(detail.result.upstream, 2);
    assert.ok(detail.result.columns.some((c) => c.name === 'order_total'));
    const impact = await ask({ type: 'impact', params: { targets: [`${J}.orders`] } }, 'impactResult');
    assert.ok(impact.result.nodes.some((n) => n.id === `${J}.customers`));
  });

  test("the server's errors reach the webview with their codes", async () => {
    const error = await ask({ type: 'node', params: { id: `${J}.no_such_model` } }, 'error');
    assert.equal(error.code, ErrorCode.UnknownNode);
    assert.match(error.message, /no_such_model/);
  });

  test('clicking a node in the graph previews its file and range beside the graph', async () => {
    // Exit criterion: the node's location comes from the graph the webview was given, and a
    // click previews it (preserveFocus), keeping the keyboard in the graph
    const sources = await graph('source.pilot_shop.ecom.raw_orders', 1);
    const source = nodeIn(sources, 'source.pilot_shop.ecom.raw_orders');
    assert.ok(source.location?.range, 'a source has a range in its YAML file');
    await panel.handle({ type: 'openFile', location: source.location, preserveFocus: true });
    const editor = await until('the YAML entry to open', 30_000, () =>
      vscode.window.visibleTextEditors.find((e) => /__sources\.yml$/.test(e.document.uri.fsPath)));
    assert.equal(editor.selection.start.line, source.location.range.start.line);
    assert.equal(editor.selection.start.character, source.location.range.start.character);
    assert.equal(editor.selection.end.line, source.location.range.end.line);
    // In another group: the graph is still on screen, and still has the keyboard
    assert.notEqual(editor.viewColumn, panel.panel.viewColumn);
    assert.ok(panel.panel.visible);
    assert.ok(panel.panel.active, 'the graph keeps the focus');
  });

  test('opening a node goes to its file, beside the graph', async () => {
    const stg = nodeIn(await graph(`${J}.orders`), `${J}.stg_orders`);
    await panel.handle({ type: 'openFile', location: stg.location, preserveFocus: false });
    const editor = await until('stg_orders.sql to open', 30_000, () => {
      const active = vscode.window.activeTextEditor;
      return active && active.document.uri.fsPath === uri('models', 'staging', 'stg_orders.sql').fsPath ? active : undefined;
    });
    if (stg.location?.range) {
      assert.equal(editor.selection.start.line, stg.location.range.start.line);
    }
    assert.notEqual(editor.viewColumn, panel.panel.viewColumn);
    assert.ok(panel.panel.visible, 'the graph is still on screen');
  });

  test('it never opens anything outside the project', async () => {
    const outside = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'lp-outside-')), 'secrets.txt');
    fs.writeFileSync(outside, 'not yours\n');
    const sibling = path.join(path.dirname(root()), 'beside-the-project.txt');
    fs.writeFileSync(sibling, 'not yours either\n');
    // Closing all editors would close the panel too: a known editor is the baseline instead
    const baseline = uri('models', 'marts', 'customers.sql');
    await vscode.window.showTextDocument(baseline, {
      viewColumn: panel.panel.viewColumn === vscode.ViewColumn.One ? vscode.ViewColumn.Two : vscode.ViewColumn.One,
    });
    const refused: Location[] = [
      { uri: vscode.Uri.file(outside).toString(), range: null },
      { uri: `${vscode.Uri.file(root()).toString()}/../beside-the-project.txt`, range: null },
      { uri: 'untitled:evil', range: null },
      { uri: 'https://example.com/models/orders.sql', range: null },
      { uri: 'vscode-userdata:/settings.json', range: null },
    ];
    for (const location of refused) {
      await panel.handle({ type: 'openFile', location, preserveFocus: false });
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
    assert.equal(vscode.window.activeTextEditor?.document.uri.fsPath, baseline.fsPath);
    const open = vscode.workspace.textDocuments.map((d) => d.uri.fsPath);
    for (const file of [outside, sibling]) {
      assert.ok(!open.includes(file), `${file} was opened`);
    }
    assert.ok(!vscode.workspace.textDocuments.some((d) => ['untitled', 'https', 'vscode-userdata'].includes(d.uri.scheme)));
  });

  test('malformed messages are dropped without an answer', async () => {
    const before = posted.length;
    for (const message of [
      { type: 'exec', command: 'dbt run' },
      'ready',
      null,
      { type: 'graph', id: 9500, params: { root: `${J}.orders` } },
      { type: 'graph', id: 9501, params: { root: `${J}.orders`, direction: 'both', depth: 2, granularity: 'model' }, extra: 1 },
      { type: 'openFile', location: { uri: 'file:///etc/hosts' } },
    ]) {
      await panel.handle(message);
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
    assert.deepEqual(posted.slice(before).filter((m) => 'id' in m && m.id >= 9000), []);
  });

  test('view settings are kept, and only valid ones', async () => {
    await panel.handle({ type: 'saveSettings', settings: { ...DEFAULT_SETTINGS, layout: 'TB', showMiniMap: false } });
    assert.deepEqual(panel.settings, { ...DEFAULT_SETTINGS, layout: 'TB', showMiniMap: false });
    await panel.handle({ type: 'saveSettings', settings: { ...DEFAULT_SETTINGS, nodeSpacing: 5 } });
    assert.equal(panel.settings.layout, 'TB');
    await panel.handle({ type: 'saveSettings', settings: DEFAULT_SETTINGS });
    assert.deepEqual(panel.settings, DEFAULT_SETTINGS);
  });

  /** The webview was told to focus on ``id``: by a focus message, or, if its page was being
   * built again (hidden panels lose theirs), by the init that answers the new page's ready. */
  async function focusedAfter(from: number, id: string, granularity = 'model'): Promise<void> {
    const told = () => posted.slice(from).some((m) => (m.type === 'focus' || m.type === 'init')
      && m.focus?.id === id && m.focus.granularity === granularity);
    try {
      await until(`a focus on ${id}`, 20_000, told);
    } catch (error) {
      const seen = posted.slice(from).map((m) => (m.type === 'focus' || m.type === 'init' ? `${m.type}(${m.focus?.id})` : m.type));
      throw new Error(`${(error as Error).message}; posted since: ${seen.join(', ') || 'nothing'}; `
        + `visible ${panel.panel.visible}; editor ${vscode.window.activeTextEditor?.document.uri.fsPath}`);
    }
  }

  test('one panel per project: showing it again moves its focus', async () => {
    const focusAfter = (_what: string, from: number, id: string, granularity = 'model') => focusedAfter(from, id, granularity);
    let mark = posted.length;
    const again = await vscode.commands.executeCommand<LineagePanel>('lineagePilot.showLineage', `${J}.stg_orders`, root());
    assert.equal(again, panel);
    await focusAfter('a focus on stg_orders', mark, `${J}.stg_orders`);
    // From a Lineage tree item, as its context menu does
    const item = await vscode.commands.executeCommand<NodeItem>('lineagePilot.showImpact', `${J}.customers`, root());
    mark = posted.length;
    await vscode.commands.executeCommand('lineagePilot.showLineage', item);
    await focusAfter('a focus on customers', mark, `${J}.customers`);
    await vscode.commands.executeCommand('lineagePilot.followEditor');
    // From the active editor
    await vscode.window.showTextDocument(uri('models', 'marts', 'orders.sql'));
    mark = posted.length;
    await vscode.commands.executeCommand('lineagePilot.showLineage');
    await focusAfter('a focus on orders', mark, `${J}.orders`);
    // A column, by its ID
    mark = posted.length;
    await vscode.commands.executeCommand('lineagePilot.showLineage', `${J}.orders#order_total`, root());
    await focusAfter('a focus on a column', mark, `${J}.orders#order_total`, 'column');
    // ... and the webview acts on it: it asks for the column's graph
    await until('the column graph', 30_000, () => find('graphResult',
      (m) => m.id < 9000 && m.result.root === `${J}.orders#order_total` && m.result.granularity === 'column', mark));
  });

  test('hidden and shown again, the view comes back as it was', async function () {
    this.timeout(120_000);
    // The previous test left it on a column. Another editor in its group hides it: VS Code
    // destroys the page
    await vscode.window.showTextDocument(uri('models', 'marts', 'customers.sql'), { viewColumn: panel.panel.viewColumn });
    await until('the panel to be hidden', 10_000, () => !panel.panel.visible);
    let mark = posted.length;
    panel.panel.reveal();
    try {
      await until('the rebuilt page to ask for the column it showed', 60_000, () => find('graphResult',
        (m) => m.id < 9000 && m.result.root === `${J}.orders#order_total` && m.result.granularity === 'column', mark));
    } catch (error) {
      const seen = posted.slice(mark).map((m) => (m.type === 'graphResult' ? `graphResult(${m.id}, ${m.result.root})`
        : m.type === 'init' || m.type === 'focus' ? `${m.type}(${m.focus?.id})` : m.type));
      const before = posted.slice(Math.max(0, mark - 6), mark).map((m) => (m.type === 'graphResult' ? `graphResult(${m.id}, ${m.result.root})`
        : m.type === 'init' || m.type === 'focus' ? `${m.type}(${m.focus?.id})` : m.type));
      throw new Error(`${(error as Error).message}; before: ${before.join(', ')}; since: ${seen.join(', ') || 'nothing'}; visible ${panel.panel.visible}`);
    }
    assert.ok(find('init', () => true, mark), 'the new page said it was ready');
    // A focus asked for while it is hidden reaches the new page
    await vscode.window.showTextDocument(uri('models', 'marts', 'customers.sql'), { viewColumn: panel.panel.viewColumn });
    await until('the panel to be hidden again', 10_000, () => !panel.panel.visible);
    mark = posted.length;
    await vscode.commands.executeCommand('lineagePilot.showLineage', `${J}.customers`, root());
    await until('the init to carry the new focus', 60_000, () => find('init', (m) => m.focus?.id === `${J}.customers`, mark));
    await until('the graph of customers', 60_000, () => find('graphResult', (m) => m.id < 9000 && m.result.root === `${J}.customers`, mark));
  });

  test('a re-index tells the webview, which loads the graph again', async function () {
    this.timeout(120_000);
    assert.ok(panel.panel.visible);
    const before = posted.filter((m) => m.type === 'graphResult' && m.id < 9000).length;
    await vscode.commands.executeCommand('lineagePilot.reindex');
    await until('indexUpdated', 60_000, () => find('indexUpdated', (m) => m.ready));
    await until('the webview to reload', 60_000, () => posted.filter((m) => m.type === 'graphResult' && m.id < 9000).length > before);
  });

  test('closing the panel forgets it', async () => {
    panel.dispose();
    assert.equal((await api()).lineagePanel(root()), undefined);
    const fresh = await vscode.commands.executeCommand<LineagePanel>('lineagePilot.showLineage', `${J}.orders`, root());
    assert.notEqual(fresh, panel);
    panel = fresh!;
  });
});
