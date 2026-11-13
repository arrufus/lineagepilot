// The first-run walkthrough and the sample project, in a real VS Code (task 6.1), trusted and in
// Restricted Mode.
import { strict as assert } from 'assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as vscode from 'vscode';

import { api, EXTENSION_ID, expectTrusted, label, root } from './helpers';

interface Step {
  id: string;
  description: string;
  media: { markdown: string };
}

suite(`walkthrough and sample project (${label})`, () => {
  const parents: string[] = [];

  suiteTeardown(() => {
    for (const parent of parents) {
      fs.rmSync(parent, { recursive: true, force: true });
    }
  });

  function parent(): string {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lp-sample-'));
    parents.push(dir);
    return dir;
  }

  test('every step of the walkthrough has its page, and every button a command', async () => {
    await api();
    const extension = vscode.extensions.getExtension(EXTENSION_ID)!;
    const [walkthrough] = extension.packageJSON.contributes.walkthroughs as { steps: Step[] }[];
    assert.ok(walkthrough.steps.length >= 6);
    const commands = new Set(await vscode.commands.getCommands(true));
    for (const step of walkthrough.steps) {
      assert.ok(fs.existsSync(path.join(extension.extensionPath, step.media.markdown)), `${step.id}: ${step.media.markdown}`);
      const buttons = [...step.description.matchAll(/\(command:([\w.]+)/g)].map((m) => m[1]);
      assert.ok(buttons.length, `${step.id} has no button`);
      for (const command of buttons) {
        assert.ok(commands.has(command), `${step.id}: ${command} is not a command`);
      }
    }
  });

  test('Open Sample Project copies the sample beside any copy already there, dated as after dbt compile', async () => {
    const dir = parent();
    const folder = await vscode.commands.executeCommand<string>('lineagePilot.openSample', dir, false);
    assert.equal(folder, path.join(dir, 'lineagepilot-sample'));
    for (const file of ['dbt_project.yml', 'README.md', '.gitignore', path.join('.lineagepilot', 'sample.json'),
      path.join('models', 'marts', 'orders.sql'), path.join('dags', 'daily_marts.py'),
      path.join('target', 'manifest.json'), path.join('target', 'catalog.json')]) {
      assert.ok(fs.existsSync(path.join(folder, file)), file);
    }
    // Nothing starts out stale: the sources are older than the manifest
    const manifest = fs.statSync(path.join(folder, 'target', 'manifest.json')).mtimeMs;
    assert.ok(fs.statSync(path.join(folder, 'models', 'marts', 'orders.sql')).mtimeMs < manifest - 2000);
    assert.ok(fs.statSync(path.join(folder, 'dbt_project.yml')).mtimeMs < manifest - 2000);
    const second = await vscode.commands.executeCommand<string>('lineagePilot.openSample', dir, false);
    assert.equal(second, path.join(dir, 'lineagepilot-sample-2'));
  });

  test("Apply the Sample Change renames the mart's column, in a trusted copy of the sample only", async () => {
    const folder = (await vscode.commands.executeCommand<string>('lineagePilot.openSample', parent(), false))!;
    const orders = path.join(folder, 'models', 'marts', 'orders.sql');
    assert.match(fs.readFileSync(orders, 'utf8'), /as order_total/);
    const outcome = await vscode.commands.executeCommand<{ status: string }>('lineagePilot.applySampleChange', folder);
    if (!expectTrusted) {
      // Restricted Mode only reads the workspace
      assert.equal(outcome?.status, 'restricted');
      assert.match(fs.readFileSync(orders, 'utf8'), /as order_total/);
      return;
    }
    assert.equal(outcome?.status, 'applied');
    assert.match(fs.readFileSync(orders, 'utf8'), /as total_usd/);
    const manifest = path.join(folder, 'target', 'manifest.json');
    assert.ok(fs.readFileSync(manifest, 'utf8').includes('total_usd'), 'the manifest dbt compile writes after the edit');
    assert.ok(fs.statSync(manifest).mtimeMs >= fs.statSync(orders).mtimeMs);
    const again = await vscode.commands.executeCommand<{ status: string }>('lineagePilot.applySampleChange', folder);
    assert.equal(again?.status, 'already-applied');
    // The test workspace is not the sample, so nothing in it changes
    const workspaceModel = path.join(root(), 'models', 'marts', 'orders.sql');
    const before = fs.readFileSync(workspaceModel, 'utf8');
    const refused = await vscode.commands.executeCommand<{ status: string }>('lineagePilot.applySampleChange', root());
    assert.equal(refused?.status, 'not-sample');
    assert.equal(fs.readFileSync(workspaceModel, 'utf8'), before);
  });
});

