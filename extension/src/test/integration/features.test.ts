// What a user sees, in every configuration, trusted or not (tasks 2.2, 2.4 and 2.6).
import { strict as assert } from 'assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as vscode from 'vscode';

import { AirflowMessage } from '../../views/airflowTree';
import type { GroupItem, NodeItem } from '../../views/lineageTree';
import { api, codeOf, J, position, readyServer, root, until, uri } from './helpers';

suite('LineagePilot features', () => {
  suiteSetup(async function () {
    this.timeout(180_000);
    await readyServer();
  });

  test('the project is indexed, model level first and then columns', async () => {
    const server = await readyServer();
    assert.equal(server.status.project.name, 'jaffle_shop');
    assert.ok((server.status.counts?.nodes ?? 0) > 20);
    assert.deepEqual(server.status.columnStates, { resolved: 13, partial: 0, unknown: 0 });
    assert.equal(server.status.artifacts.manifest?.schemaVersion, 12);
  });

  test('the Airflow view says a DAG file was read and that no task in it runs dbt', async () => {
    const { airflowTree } = await api();
    const server = await readyServer();
    await until('the DAG files to be read', 100_000, () => server.status.airflow.stage === 'done');
    await airflowTree.showFile(undefined);
    const [item] = airflowTree.items;
    assert.ok(item instanceof AirflowMessage);
    assert.equal(airflowTree.getTreeItem(item).label, 'No task in the DAG files read runs dbt.');
    await airflowTree.showFile(uri('models', 'marts', 'orders.sql'));
    assert.equal(airflowTree.getTreeItem(airflowTree.items[0]).label, 'No task in the DAG files read runs this.');
    assert.deepEqual(airflowTree.getChildren(airflowTree.items[0]), []);
  });

  test('CodeLens shows what is downstream of a model', async () => {
    const orders = uri('models', 'marts', 'orders.sql');
    // The CodeLens command needs the document loaded
    await vscode.workspace.openTextDocument(orders);
    const lens = await until('the CodeLens', 60_000, async () => {
      const lenses = await vscode.commands.executeCommand<vscode.CodeLens[]>('vscode.executeCodeLensProvider', orders);
      return lenses?.find((l) => l.command?.command === 'lineagePilot.showImpact');
    });
    assert.equal(lens.command!.title, '↓ 1 model');
    assert.deepEqual(lens.command!.arguments, [`${J}.orders`, root()]);
    const sources = await vscode.workspace.openTextDocument(uri('models', 'staging', '__sources.yml'));
    const yaml = await vscode.commands.executeCommand<vscode.CodeLens[]>('vscode.executeCodeLensProvider', sources.uri);
    assert.equal(yaml.filter((l) => l.command?.command === 'lineagePilot.showImpact').length, 6);
  });

  test('hovering a ref() shows its lineage with evidence', async () => {
    const document = await vscode.workspace.openTextDocument(uri('models', 'marts', 'orders.sql'));
    const hovers = await until('a hover', 30_000, async () => {
      const found = await vscode.commands.executeCommand<vscode.Hover[]>(
        'vscode.executeHoverProvider', document.uri, position(document, "ref('stg_orders')", 6));
      return found?.length ? found : undefined;
    });
    const text = hovers.flatMap((h) => h.contents.map((c) => (typeof c === 'string' ? c : c.value))).join('\n');
    assert.match(text, /### stg\\_orders/);
    assert.match(text, /ref · declared, manifest/);
  });

  test('rules show in the Problems panel with their IDs and documentation', async () => {
    const customers = uri('models', 'marts', 'customers.sql');
    const diagnostics = await until('diagnostics', 60_000, () => {
      const found = vscode.languages.getDiagnostics(customers);
      return found.length ? found : undefined;
    });
    const gold = diagnostics.find((d) => codeOf(d) === 'MED_001');
    assert.ok(gold, JSON.stringify(diagnostics.map((d) => d.code)));
    assert.equal(gold.source, 'LineagePilot');
    assert.equal(gold.severity, vscode.DiagnosticSeverity.Error);
    assert.match(String((gold.code as { target: vscode.Uri }).target), /docs\/rules\.md#med_001$/);
  });

  test('Analyze Impact pins the Lineage view to a node and shows its downstream', async () => {
    // The extension runs from its bundle, so its classes are not the ones imported here:
    // items are told apart by their type field
    const item = await vscode.commands.executeCommand<NodeItem>('lineagePilot.showImpact', `${J}.stg_orders`, root());
    assert.ok(item?.type === 'node' && item.node.name === 'stg_orders');
    const { lineageTree } = await api();
    assert.ok(lineageTree.pinned);
    const [up, down] = (await lineageTree.getChildren(item)) as GroupItem[];
    assert.deepEqual([up.type, up.direction, down.type, down.direction], ['group', 'upstream', 'group', 'downstream']);
    const downstream = (await lineageTree.getChildren(down)) as NodeItem[];
    assert.deepEqual(downstream.map((n) => n.node.name).sort(), ['order_items', 'orders']);
    assert.equal(downstream[0].edge?.confidence, 'declared');
    const next = (await lineageTree.getChildren(downstream.find((n) => n.node.name === 'orders'))) as NodeItem[];
    assert.deepEqual(next.map((n) => n.node.name), ['customers']);
    const upstream = (await lineageTree.getChildren(up)) as NodeItem[];
    assert.deepEqual(upstream.map((n) => n.node.kind), ['source']);
    const tree = lineageTree.getTreeItem(downstream[0]);
    assert.equal(tree.command?.command, 'lineagePilot.openNode');
  });

  test('the Lineage view follows the active editor', async () => {
    await vscode.commands.executeCommand('lineagePilot.followEditor');
    const { lineageTree } = await api();
    await vscode.window.showTextDocument(uri('models', 'marts', 'customers.sql'));
    await until('the view to follow the editor', 30_000, async () => {
      const roots = (await lineageTree.getChildren()) as NodeItem[];
      return roots.length === 1 && roots[0].node.id === `${J}.customers`;
    });
    // Analyze Impact on the open file
    const item = await vscode.commands.executeCommand<NodeItem>('lineagePilot.showImpact');
    assert.equal(item?.node.id, `${J}.customers`);
    await vscode.commands.executeCommand('lineagePilot.followEditor');
  });

  test('opening a node goes to its YAML entry', async () => {
    const server = await readyServer();
    const { nodes } = await server.request('lineage/graph', {
      root: 'source.jaffle_shop.ecom.raw_orders', direction: 'downstream', depth: 1, granularity: 'model',
    });
    const source = nodes.find((n) => n.side === 'root')!;
    await vscode.commands.executeCommand('lineagePilot.openNode', source.location);
    const editor = vscode.window.activeTextEditor!;
    assert.match(editor.document.uri.fsPath, /__sources\.yml$/);
    assert.equal(editor.selection.start.line, source.location!.range!.start.line);
  });

  test('exports write files', async () => {
    const folder = fs.mkdtempSync(path.join(os.tmpdir(), 'lp-export-'));
    const graph = vscode.Uri.file(path.join(folder, 'lineage.mmd'));
    await vscode.commands.executeCommand('lineagePilot.exportGraph', graph, 'mermaid');
    assert.match(fs.readFileSync(graph.fsPath, 'utf8'), /^flowchart/);
    const pii = vscode.Uri.file(path.join(folder, 'pii.csv'));
    await vscode.commands.executeCommand('lineagePilot.exportPiiInventory', pii, 'pii-csv');
    assert.ok(fs.readFileSync(pii.fsPath, 'utf8').length > 0);
  });

  test('the status bar shows the node count and manifest age', async () => {
    const { statusBarText } = await api();
    await until('the status bar', 30_000, () => /Lineage: \d+ nodes · manifest .+ old|just now/.test(statusBarText()));
  });

  test('re-index and restart keep the lineage available', async () => {
    const before = (await readyServer()).status.generation;
    await vscode.commands.executeCommand('lineagePilot.reindex');
    const { manager } = await api();
    await until('a new index', 60_000, () => {
      const status = manager.list()[0].status;
      return status && status.generation > before && !status.indexing;
    });
    await vscode.commands.executeCommand('lineagePilot.restartServer');
    const restarted = await readyServer();
    assert.ok(restarted.status.fromCache, 'a restart loads the cached snapshot');
  });
});
