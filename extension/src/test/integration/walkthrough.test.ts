// The first-run walkthrough, the sample project and the beta's usage counts, in a real VS Code
// (tasks 6.1 and 6.3), trusted and in Restricted Mode. Every test workspace's settings turn usage
// counts on (.vscode-test.mjs): only the user's own settings may do that.
import { strict as assert } from 'assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as vscode from 'vscode';

import type { ReviewPanel } from '../../reviewPanel';
import { api, EXTENSION_ID, expectTrusted, label, readyServer, root, until } from './helpers';

const USAGE = 'lineagePilot.usageCounts.enabled';

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

suite(`usage counts (${label})`, () => {
  let panel: ReviewPanel | undefined;

  suiteTeardown(async () => {
    await vscode.workspace.getConfiguration().update(USAGE, undefined, vscode.ConfigurationTarget.Global);
    panel?.dispose();
    await vscode.commands.executeCommand('workbench.action.closeAllEditors');
  });

  test('a workspace cannot turn them on: nothing is counted', async function () {
    this.timeout(120_000);
    await readyServer();
    const { usage } = await api();
    assert.equal(usage.isEnabled, false);
    panel = await vscode.commands.executeCommand<ReviewPanel>('lineagePilot.reviewChanges', root());
    assert.ok(panel);
    assert.deepEqual(usage.state().weeks, {});
    assert.equal(await vscode.commands.executeCommand('lineagePilot.copyUsageCounts'), undefined);
  });

  test('turned on by the user, the review is counted; turned off, the counts are forgotten', async function () {
    this.timeout(120_000);
    const { usage } = await api();
    const config = vscode.workspace.getConfiguration();
    await config.update(USAGE, true, vscode.ConfigurationTarget.Global);
    await until('counting to be on', 10_000, () => usage.isEnabled);
    panel = await vscode.commands.executeCommand<ReviewPanel>('lineagePilot.reviewChanges', root());
    const week = () => Object.values(usage.state().weeks)[0];
    await until('the review to be counted', 10_000, () => week()?.opened === 1);
    // Copying is counted in review.test.ts, where there is always a review to copy
    assert.equal(week().days.length, 1);
    assert.equal(week().copied, 0);
    const text = await vscode.commands.executeCommand<string>('lineagePilot.copyUsageCounts');
    assert.equal(await vscode.env.clipboard.readText(), text);
    assert.match(text!, /^\| \d{4}-W\d{2} \(from \d{4}-\d{2}-\d{2}\) \| 1 \| 1 \| 0 \|$/m);
    // Nothing about the project
    assert.ok(!text!.includes(root()) && !text!.includes(path.basename(root())), text);
    assert.ok(!/jaffle|orders|customers/i.test(text!), text);
    await config.update(USAGE, false, vscode.ConfigurationTarget.Global);
    await until('the counts to be forgotten', 10_000, () => Object.keys(usage.state().weeks).length === 0);
  });
});
