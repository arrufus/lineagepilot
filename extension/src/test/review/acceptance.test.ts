// The full acceptance scenario (design section 14, the Phase 5 exit), in a real VS Code on
// fixtures/impact-shop, with real dbt's artifacts from before and after the rename. Every
// workspace is hostile, this one too, so the review also runs next to planted settings and scripts.
import { strict as assert } from 'assert';
import { execFileSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as vscode from 'vscode';

import type { ReviewResult } from '../../protocol';
import type { ReviewPanel } from '../../reviewPanel';
import type { PipelineItem, TaskItem } from '../../views/pipelinesTree';
import { api, ran, readerOf, readyServer, root, until } from '../integration/helpers';

const S = 'model.impact_shop';

function git(...args: string[]): void {
  execFileSync('git', ['-c', 'user.name=t', '-c', 'user.email=t@example.com', ...args], { cwd: root(), stdio: 'pipe' });
}

function overlay(from: string, to: string): void {
  for (const entry of fs.readdirSync(from, { withFileTypes: true })) {
    const source = path.join(from, entry.name);
    const target = path.join(to, entry.name);
    if (entry.isDirectory()) {
      overlay(source, target);
    } else {
      fs.copyFileSync(source, target);
    }
  }
}

suite('acceptance: rename a mart column', () => {
  let panel: ReviewPanel;
  let review: ReviewResult;
  const before = new Map<string, string>();

  suiteSetup(async function () {
    this.timeout(240_000);
    const server = await readyServer();
    const generation = server.status.generation;
    fs.writeFileSync(path.join(root(), '.gitignore'), 'target/\n.venv/\n.vscode/\n.lineagepilot/\nchanges/\nmake_fixture.py\nPROVENANCE.json\n');
    git('init', '-q', '-b', 'main');
    git('add', '-A');
    git('commit', '-q', '-m', 'the project as deployed');

    // The developer renames orders.order_total and runs dbt compile: the manifest dbt wrote then
    overlay(path.join(root(), 'changes', 'rename-order-total'), root());
    await until('the server to index the new manifest', 120_000, () => {
      const status = server.status;
      return status.generation > generation && !status.indexing && status.columnLineage === 'done' && readerOf(status)?.stage === 'done' ? status : undefined;
    });

    panel = (await vscode.commands.executeCommand<ReviewPanel>('lineagePilot.reviewChanges', root()))!;
    review = await until('the review of the rename', 90_000, async () => {
      const latest = panel.latest;
      if (latest?.pipelines?.find((r) => r.tool === 'airflow')?.analysed && latest.changedNodes.some((n) => n.columns.removed.length > 0)) {
        return latest;
      }
      await panel.run();
      return undefined;
    });
    for (const node of review.impacted) {
      before.set(node.id, node.reach);
    }
  });

  suiteTeardown(() => panel?.dispose());

  test('the rename is found by comparing the compiled SQL with the catalog', () => {
    assert.deepEqual(review.changedNodes.map((n) => n.id), [`${S}.orders`]);
    const { columns } = review.changedNodes[0];
    assert.deepEqual([columns.state, columns.baseline, columns.removed, columns.added],
      ['known', 'catalog', ['order_total'], ['total_usd']]);
    assert.equal(review.changedNodes[0].owner, 'finance-data');
    assert.equal(review.base?.ref, 'main');
  });

  test('the review lists the affected downstream models', () => {
    const revenue = review.impacted.find((n) => n.id === `${S}.customer_revenue`);
    assert.equal(revenue?.reach, 'column');
    assert.deepEqual(revenue?.columns, ['lifetime_value']);
    assert.equal(before.get(`${S}.revenue_tiers`), 'column');
    assert.equal(before.get(`${S}.order_counts`), 'model');
  });

  test('the review lists the exposure and the owners', () => {
    const exposure = review.impacted.find((n) => n.id === 'exposure.impact_shop.revenue_dashboard');
    assert.equal(exposure?.kind, 'exposure');
    assert.equal(exposure?.owner, 'Revenue Analytics');
    const owners = new Map(review.owners.map((o) => [o.owner, o.nodes]));
    assert.deepEqual(owners.get('growth-analytics'), [`${S}.customer_revenue`]);
    // A dbt group owns a model and the semantic layer's saved query
    assert.deepEqual(owners.get('operations'), [`${S}.order_counts`, 'saved_query.impact_shop.weekly_revenue']);
  });

  test('the review lists the metric and the saved query that use the column, with their owners', () => {
    const revenue = review.impacted.find((n) => n.id === 'metric.impact_shop.revenue');
    assert.deepEqual([revenue?.kind, revenue?.tool, revenue?.subkind], ['consumer', 'dbt_semantic', 'metric']);
    // Reached through the measure it reads
    assert.deepEqual([revenue?.reach, revenue?.columns], ['column', ['order_total']]);
    assert.equal(revenue?.owner, 'finance-analytics');
    const query = review.impacted.find((n) => n.id === 'saved_query.impact_shop.weekly_revenue');
    assert.equal(query?.owner, 'operations');
    assert.deepEqual(query?.exports, ['exports.weekly_revenue']);
    // The metric that only counts orders reads no renamed column
    assert.equal(review.impacted.find((n) => n.id === 'metric.impact_shop.order_count')?.reach, 'model');
    assert.match(review.markdown, /#### Semantic layer \(6\)/);
    assert.match(review.markdown, /\| `revenue` \(metric\) \| Field `order_total` \|/);
  });

  test('a deliberately unparseable model appears under Unknowns', () => {
    const unknown = review.unknowns.find((u) => u.subject === `${S}.broken_report`);
    assert.match(unknown?.reason ?? '', /could not be parsed/);
    assert.equal(before.get(`${S}.broken_report`), 'model');
  });

  test('the review lists the Airflow task and its owner', () => {
    assert.deepEqual(review.pipelines?.map((r) => [r.tool, r.analysed]), [['airflow', true]]);
    const group = review.impacted.find((n) => n.id === 'airflow:daily_marts.marts');
    // Protocol 2's words (D-9): a task, of Airflow, that Airflow calls a task group
    assert.deepEqual([group?.kind, group?.tool, group?.subkind], ['task', 'airflow', 'task_group']);
    assert.equal(group?.owner, 'data-platform');
    assert.equal(group?.pipeline?.pipeline, 'daily_marts');
    assert.deepEqual(group?.pipeline?.how, ['dbt, through Cosmos']);
    assert.ok(group?.pipeline?.tasks.includes('marts.orders.run'));
    const owners = new Map(review.owners.map((o) => [o.owner, o.nodes]));
    assert.ok(owners.get('data-platform')?.includes('airflow:daily_marts.marts'));
    assert.match(review.markdown, /\| `daily_marts` \| `marts`\. Derived: /);
  });

  test('heuristic links are shown, and labelled', () => {
    const shell = review.impacted.find((n) => n.id === 'airflow:operations.export_orders');
    assert.equal(shell?.confidence, 'heuristic');
    assert.match(review.markdown, /\| `operations` \| `export_orders` \| reads: `orders` \| heuristic \|/);
  });

  test('a task that runs after the changed one is listed too', () => {
    const after = review.impacted.find((n) => n.id === 'airflow:daily_marts.notify_finance');
    assert.deepEqual(after?.pipeline?.after, ['marts']);
  });

  test('a task created in a loop is an unknown at its call site', () => {
    const unknown = review.unknowns.find((u) => u.subject === 'airflow:backfill');
    assert.equal(unknown?.reference, 'loop or factory');
    assert.match(unknown?.reason ?? '', /^dags\/backfill\.py:12: /);
  });

  test('the task opens in its DAG file, at its line', async () => {
    const location = review.locations['airflow:daily_marts.marts'];
    await panel.handle({ type: 'openFile', location });
    await until('the DAG file to open', 10_000, () => vscode.window.visibleTextEditors.find(
      (editor) => editor.document.uri.fsPath.endsWith('daily_marts.py')));
    const editor = vscode.window.visibleTextEditors.find((e) => e.document.uri.fsPath.endsWith('daily_marts.py'));
    assert.equal(editor?.selection.active.line, 21);
  });

  test('the Pipelines view shows the DAGs and tasks of the open model, under Airflow', async () => {
    const { pipelinesTree } = await api();
    await pipelinesTree.showFile(vscode.Uri.file(path.join(root(), 'models', 'marts', 'order_counts.sql')));
    // Type-only imports and .type: these tests load out/, and the extension runs from its bundle
    const [airflow] = pipelinesTree.items;
    assert.equal(pipelinesTree.getTreeItem(airflow).label, 'Airflow');
    const dags = await until('the DAGs that run the model', 30_000, () => {
      const found = pipelinesTree.getChildren(pipelinesTree.items[0]).filter((item) => item.type === 'pipeline');
      return found.length > 0 ? found : undefined;
    });
    assert.deepEqual(dags.map((item) => (item as PipelineItem).pipeline.name), ['daily_marts', 'operations']);
    const tasks = pipelinesTree.getChildren(dags[1]);
    assert.deepEqual(tasks.map((item) => (item as TaskItem).task.name), ['refresh_counts']);
    const tree = pipelinesTree.getTreeItem(tasks[0]);
    assert.equal(tree.description, 'dbt run · heuristic');
    assert.equal(tree.command?.command, 'lineagePilot.openNode');
    const unknowns = pipelinesTree.getChildren(pipelinesTree.items[0]).find((item) => item.type === 'unknowns');
    assert.ok(unknowns);
    assert.equal(pipelinesTree.getTreeItem(unknowns).label, 'Could not be read (1)');
    const [unknown] = pipelinesTree.getChildren(unknowns);
    assert.equal(pipelinesTree.getTreeItem(unknown).label, 'backfill');
  });

  test('CodeLens counts the tasks', async () => {
    const orders = vscode.Uri.file(path.join(root(), 'models', 'marts', 'orders.sql'));
    await vscode.workspace.openTextDocument(orders);
    const lenses = await until('the lens', 30_000, async () => {
      const found = await vscode.commands.executeCommand<vscode.CodeLens[]>('vscode.executeCodeLensProvider', orders);
      return found?.find((lens) => /Airflow tasks/.test(lens.command?.title ?? ''));
    });
    assert.match(lenses.command?.title ?? '', /\d+ Airflow tasks/);
  });

  test('Copy as Markdown gives the review, ready for a pull request', async () => {
    await panel.handle({ type: 'copyMarkdown' });
    const text = await vscode.env.clipboard.readText();
    assert.equal(text, review.markdown);
    assert.match(text, /Removed: `order_total`/);
    assert.match(text, /\| `customer_revenue` \| Column `lifetime_value` \| inferred \| compiled SQL, from `orders\.order_total` \| `growth-analytics` \|/);
    assert.match(text, /### Unknowns \(2\)/);
  });

  // Through VS Code's own file watchers, not a simulated event: the demo script relies on these
  test('a changed DAG file is read again, and the view and the review follow', async () => {
    const server = await readyServer();
    const dag = path.join(root(), 'dags', 'operations.py');
    const before = fs.readFileSync(dag, 'utf8');
    const generation = readerOf(server.status)!.generation;
    fs.writeFileSync(dag, before.replace('--select order_counts', '--select nothing_called_this'));
    try {
      await until('the DAG file to be read again', 30_000, () =>
        readerOf(server.status)!.generation > generation && readerOf(server.status)!.stage === 'done');
      const edited = await until('the review to follow', 30_000, async () => {
        await panel.run();
        return panel.latest?.unknowns.some((u) => u.reference === 'selects nothing') ? panel.latest : undefined;
      });
      assert.match(edited.unknowns.find((u) => u.reference === 'selects nothing')?.reason ?? '', /nothing_called_this matches no node/);
      assert.ok(edited.otherFiles.some((f) => f.path === 'dags/operations.py'));
    } finally {
      fs.writeFileSync(dag, before);
    }
    await until('the DAG file to be restored', 30_000, async () => {
      await panel.run();
      return panel.latest?.unknowns.some((u) => u.reference === 'selects nothing') ? undefined : true;
    });
  });

  test('the annotation file answers an unknown, and the review follows', async () => {
    const server = await readyServer();
    const file = path.join(root(), '.lineagepilot', 'airflow.yml');
    const generation = readerOf(server.status)!.generation;
    fs.writeFileSync(file, [
      'acknowledged:',
      '  - file: dags/backfill.py',
      'pipelines:',
      '  - pipeline_id: backfill',
      '    owner: data-platform',
      '    tasks:',
      '      - task_id: backfill_all',
      `        consumes: [${S}.orders]`,
      '',
    ].join('\n'));
    try {
      await until('the annotation file to be read', 30_000, () =>
        readerOf(server.status)!.generation > generation && readerOf(server.status)!.stage === 'done');
      const answered = await until('the review to follow', 30_000, async () => {
        await panel.run();
        return panel.latest?.impacted.some((n) => n.id === 'airflow:backfill.backfill_all') ? panel.latest : undefined;
      });
      assert.equal(answered.unknowns.some((u) => u.subject === 'airflow:backfill'), false);
      assert.deepEqual(answered.impacted.find((n) => n.id === 'airflow:backfill.backfill_all')?.origins, ['annotation']);
    } finally {
      fs.rmSync(file, { force: true });
    }
    await until('the annotation file to be gone', 30_000, async () => {
      await panel.run();
      return panel.latest?.unknowns.some((u) => u.subject === 'airflow:backfill') ? true : undefined;
    });
  });

  test('nothing planted in the workspace ran', () => {
    assert.equal(ran('planted-python-ran'), undefined);
    assert.equal(ran('planted-dbt-ran'), undefined);
  });
});
