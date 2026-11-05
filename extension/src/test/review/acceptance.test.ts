// The dbt half of the acceptance scenario (design section 14, the Phase 4 exit), in a real VS Code
// on fixtures/impact-shop, with real dbt's artifacts from before and after the rename. Every
// workspace is hostile, this one too, so the review also runs next to planted settings and scripts.
import { strict as assert } from 'assert';
import { execFileSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as vscode from 'vscode';

import type { ReviewResult } from '../../protocol';
import type { ReviewPanel } from '../../reviewPanel';
import { ran, readyServer, root, until } from '../integration/helpers';

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
      return status.generation > generation && !status.indexing && status.columnLineage === 'done' ? status : undefined;
    });

    panel = (await vscode.commands.executeCommand<ReviewPanel>('lineagePilot.reviewChanges', root()))!;
    review = await until('the review of the rename', 90_000, async () => {
      const latest = panel.latest;
      if (latest?.changedNodes.some((n) => n.columns.removed.length > 0)) {
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
    assert.deepEqual(owners.get('operations'), [`${S}.order_counts`]);
  });

  test('a deliberately unparseable model appears under Unknowns', () => {
    const unknown = review.unknowns.find((u) => u.subject === `${S}.broken_report`);
    assert.match(unknown?.reason ?? '', /could not be parsed/);
    assert.equal(before.get(`${S}.broken_report`), 'model');
  });

  test('Airflow is said not to have been analysed: Phase 5 adds the task and its owner', () => {
    assert.equal(review.airflow.analysed, false);
    assert.match(review.markdown, /Airflow DAGs were not analysed in this version/);
  });

  test('Copy as Markdown gives the review, ready for a pull request', async () => {
    await panel.handle({ type: 'copyMarkdown' });
    const text = await vscode.env.clipboard.readText();
    assert.equal(text, review.markdown);
    assert.match(text, /Removed: `order_total`/);
    assert.match(text, /\| `customer_revenue` \| Column `lifetime_value` \| inferred \| compiled SQL, from `orders\.order_total` \| `growth-analytics` \|/);
    assert.match(text, /### Unknowns \(1\)/);
  });

  test('nothing planted in the workspace ran', () => {
    assert.equal(ran('planted-python-ran'), undefined);
    assert.equal(ran('planted-dbt-ran'), undefined);
  });
});
