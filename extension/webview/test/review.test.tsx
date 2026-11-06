// The Change Impact Review page against a fake extension (task 4.4): what it shows for a review,
// for each way of not having one, and what it asks the extension to do.
import { strict as assert } from 'assert';
import { act, fireEvent, render, screen, within } from '@testing-library/react';

import type {
  ChangedNode, ColumnDiff, ReviewImpacted, ReviewResult,
} from '../../src/protocol';
import { ReviewHostMessage, ReviewWebviewMessage } from '../../src/reviewMessages';
import ReviewApp, { shortRef } from '../src/review/ReviewApp';
import { ReviewTransport } from '../src/review/transport';

class Host {
  readonly posted: ReviewWebviewMessage[] = [];
  readonly transport: ReviewTransport;
  private listener: ((event: MessageEvent) => void) | undefined;

  constructor() {
    this.transport = new ReviewTransport({
      // Every message the page sends must be one the extension accepts
      postMessage: (message) => { this.posted.push(ReviewWebviewMessage.parse(message)); },
      getState: () => undefined,
      setState: () => undefined,
    }, { addEventListener: (_type: string, listener: unknown) => { this.listener = listener as (event: MessageEvent) => void; } } as never);
  }

  async send(message: unknown): Promise<void> {
    await act(async () => {
      this.listener?.({ data: message } as MessageEvent);
      await Promise.resolve();
    });
  }
}

function diff(fields: Partial<ColumnDiff> = {}): ColumnDiff {
  return { state: 'known', baseline: 'catalog', added: [], removed: [], typeChanged: [], stale: false, reason: null, ...fields };
}

function changedNode(fields: Partial<ChangedNode> = {}): ChangedNode {
  return {
    id: 'model.shop.orders',
    kind: 'model',
    name: 'orders',
    owner: 'finance-data',
    status: 'modified',
    files: [{ path: 'models/marts/orders.sql', status: 'modified', role: 'definition' }],
    columns: diff({ removed: ['order_total'], added: ['total_usd'] }),
    ...fields,
  };
}

function analysed(fields: Partial<ReviewResult['airflow']> = {}): ReviewResult['airflow'] {
  return { ...{ analysed: false, reason: 'the DAG files were not analysed', dagFiles: 0, dags: 0, tasks: 0, links: 0, unknowns: 0, opaqueTasks: 0, folders: [], problems: [] }, analysed: true, reason: null, dagFiles: 1, ...fields };
}

function task(fields: Partial<ReviewImpacted> = {}): ReviewImpacted {
  return impacted({
    id: 'airflow:z.t',
    kind: 'airflow_task',
    name: 't',
    file: 'dags/z.py',
    reach: 'model',
    confidence: 'heuristic',
    origins: ['dag_static'],
    columns: [],
    reason: null,
    via: [],
    airflow: { dag: 'z', operator: 'BashOperator', how: ['dbt run'], runs: ['orders'], runsMore: 0, tasks: [], tasksMore: 0, after: [], notes: [] },
    ...fields,
  });
}

function impacted(fields: Partial<ReviewImpacted> = {}): ReviewImpacted {
  return {
    id: 'model.shop.customer_revenue',
    kind: 'model',
    name: 'customer_revenue',
    owner: 'growth',
    file: 'models/marts/customer_revenue.sql',
    depth: 1,
    reach: 'column',
    confidence: 'inferred',
    origins: ['compiled_sql'],
    columns: ['lifetime_value'],
    reason: null,
    via: [{ source: 'model.shop.orders#order_total', target: 'model.shop.customer_revenue#lifetime_value', confidence: 'inferred', origins: ['compiled_sql'] }],
    viaMore: 0,
    airflow: null,
    ...fields,
  };
}

function review(fields: Partial<ReviewResult> = {}): ReviewResult {
  return {
    project: 'shop',
    base: { ref: 'origin/main', commit: '0123456789abcdef0123' },
    artifacts: { manifestGeneratedAt: null, dbtVersion: null, baselineManifest: null },
    columnLineage: 'done',
    summary: { changedFiles: 1, changedNodes: 1, changedColumns: 2, staleNodes: 0, impacted: { model: 1 }, impactedByColumn: 1, unknowns: 0 },
    changedNodes: [changedNode()],
    impacted: [impacted()],
    owners: [{ owner: 'growth', nodes: ['model.shop.customer_revenue'] }],
    airflow: { analysed: false, reason: 'the DAG files were not analysed', dagFiles: 0, dags: 0, tasks: 0, links: 0, unknowns: 0, opaqueTasks: 0, folders: [], problems: [] },
    unknowns: [],
    otherFiles: [],
    outsideProject: 0,
    notes: [],
    markdown: '## Review',
    locations: {
      'model.shop.orders': { uri: 'file:///w/models/marts/orders.sql', range: null },
      'model.shop.customer_revenue': { uri: 'file:///w/models/marts/customer_revenue.sql', range: null },
    },
    ...fields,
  };
}

async function start(trusted = true): Promise<Host> {
  const host = new Host();
  render(<ReviewApp transport={host.transport} />);
  assert.deepEqual(host.posted, [{ type: 'ready' }]);
  await host.send({ type: 'init', project: { name: 'shop', root: '/w' }, trusted });
  return host;
}

async function shown(result: ReviewResult = review(), trusted = true): Promise<Host> {
  const host = await start(trusted);
  // The fixture must be something the extension could really send
  await host.send(ReviewHostMessage.parse({ type: 'reviewResult', result }));
  return host;
}

suite('review page', () => {
  test('a node or column ID is shortened, and nothing else is', () => {
    assert.equal(shortRef('model.shop.orders'), 'orders');
    assert.equal(shortRef('model.shop.orders#total'), 'orders.total');
    assert.equal(shortRef('source.shop.raw.orders#id'), 'orders.id');
    assert.equal(shortRef('models/marts/orders.sql'), 'models/marts/orders.sql');
    assert.equal(shortRef('manifest.json'), 'manifest.json');
  });

  test('it asks for a review, and says it is working on it', async () => {
    await start();
    assert.ok(screen.getByRole('heading', { level: 1, name: 'Change impact: shop' }));
    assert.match(screen.getByRole('status').textContent ?? '', /Working out what changed/);
    assert.equal((screen.getByRole('button', { name: 'Copy as Markdown' }) as HTMLButtonElement).disabled, true);
    assert.equal((screen.getByRole('button', { name: 'Refresh' }) as HTMLButtonElement).disabled, true);
  });

  test('it shows the base, the summary and every section', async () => {
    await shown();
    assert.match(document.body.textContent ?? '', /merge-base of HEAD and origin\/main \(0123456789ab\)/);
    assert.ok(screen.getByText('1 changed node, 2 changed columns, 1 impacted, 1 of them traced by column, 0 unknowns.'));
    for (const name of ['Changed', 'Downstream impact', 'Unknowns (0)']) {
      assert.ok(screen.getByRole('heading', { level: 2, name }));
    }
  });

  test('changed columns read as a diff, and say so to a screen reader', async () => {
    await shown();
    const changes = within(screen.getByRole('article', { name: 'model orders' })).getByRole('list', { name: 'Column changes' });
    const [removed, added] = within(changes).getAllByRole('listitem');
    assert.equal(removed.textContent, '−Removed order_total');
    assert.equal(removed.className, 'lp-diff-removed');
    assert.equal(added.textContent, '+Added total_usd');
    assert.ok(screen.getByText('Compared with catalog.json.'));
    assert.ok(screen.getByText('model, modified, owned by finance-data'));
    assert.ok(screen.getByText('models/marts/orders.sql'));
  });

  test('a changed type, the absence of a change, and each baseline are said', async () => {
    await shown(review({
      changedNodes: [
        changedNode({ columns: diff({ baseline: 'baseline_manifest', typeChanged: [{ name: 'id', before: 'int', after: 'text' }] }) }),
        changedNode({ id: 'model.shop.same', name: 'same', owner: null, columns: diff() }),
        changedNode({ id: 'model.shop.fresh', name: 'fresh', status: 'untracked', columns: diff({ baseline: 'none', added: ['a'], reason: 'a new file' }) }),
      ],
    }));
    assert.match(screen.getByText(/id: int to text/).textContent ?? '', /Type changed/);
    assert.ok(screen.getByText('Compared with the baseline manifest.'));
    assert.ok(screen.getByText('No column added, removed or retyped, compared with catalog.json.'));
    assert.ok(screen.getByText('Compared with no baseline. a new file'));
    assert.ok(screen.getByText('model, new, owned by finance-data'));
  });

  test('columns that could not be compared say why, and a properties change says so', async () => {
    await shown(review({
      changedNodes: [
        changedNode({ columns: diff({ state: 'unknown', baseline: 'none', reason: 'no baseline has it' }) }),
        changedNode({ id: 'model.shop.docs', name: 'docs', status: 'properties', columns: diff({ state: 'not_compared', baseline: 'none', reason: 'only its properties file changed' }) }),
        changedNode({ id: 'model.shop.none', name: 'none', columns: diff({ state: 'not_compared', baseline: 'none', reason: null }) }),
      ],
    }));
    assert.match(screen.getByText('Columns unknown.').parentElement?.textContent ?? '', /no baseline has it/);
    assert.ok(screen.getByText('Only its properties file changed.'));
    assert.ok(screen.getByText('model, properties changed, owned by finance-data'));
  });

  test('impact is a table with how far, how sure, what says so and whose it is', async () => {
    await shown();
    const table = screen.getByRole('table', { name: 'Impacted models' });
    const headers = within(table).getAllByRole('columnheader').map((h) => h.textContent);
    assert.deepEqual(headers, ['Name', 'Reached', 'Confidence', 'Evidence', 'Owner']);
    const row = within(table).getAllByRole('row')[1];
    assert.equal(row.className, 'lp-reach-column');
    const [reached, confidence, evidence, owner] = within(row).getAllByRole('cell');
    assert.equal(reached.textContent, 'Column lifetime_value');
    assert.equal(confidence.textContent, 'inferred');
    assert.ok(confidence.querySelector('.lp-legend-line.lp-edge-inferred'), 'the graph\'s line for how sure it is');
    assert.equal(evidence.textContent, 'compiled SQLfrom orders.order_total');
    assert.equal(owner.textContent, 'growth');
  });

  test('a model-level row gives its reason, and every kind has its own table', async () => {
    await shown(review({
      impacted: [
        impacted({ reach: 'model', columns: [], via: [], reason: 'depends on a changed model', owner: null, origins: ['manifest'] }),
        impacted({ id: 'model.shop.both', name: 'both', reach: 'model', columns: ['a', 'b'], reason: null, viaMore: 3 }),
        impacted({ id: 'exposure.shop.dash', kind: 'exposure', name: 'dash', reach: 'model', confidence: 'declared', reason: 'uses the whole node' }),
        task(),
        impacted({ id: 'snapshot.shop.s', kind: 'snapshot', name: 's', confidence: 'declared', origins: ['manifest', 'catalog', 'annotation'] }),
      ],
      owners: [],
      airflow: analysed(),
    }));
    const models = screen.getByRole('table', { name: 'Impacted models' });
    const [first, second] = within(models).getAllByRole('row').slice(1);
    assert.equal(first.className, 'lp-reach-model');
    assert.equal(within(first).getAllByRole('cell')[0].textContent, 'Model level: depends on a changed model');
    assert.equal(within(first).getAllByRole('cell')[3].textContent, 'No owner');
    assert.equal(within(second).getAllByRole('cell')[0].textContent, 'Model levelColumns a, b');
    assert.match(within(second).getAllByRole('cell')[2].textContent ?? '', /and 3 more/);
    assert.ok(screen.getByRole('table', { name: 'Impacted exposures' }));
    const tasks = screen.getByRole('table', { name: 'Impacted airflow tasks' });
    assert.ok(tasks.querySelector('.lp-edge-heuristic'));
    assert.match(screen.getByRole('table', { name: 'Impacted snapshots' }).textContent ?? '', /the manifest, the catalog, an annotation/);
    assert.equal(screen.queryByText(/were not analysed/), null);
  });

  test('without Airflow analysis it says so rather than listing none', async () => {
    await shown();
    assert.ok(screen.getByRole('heading', { level: 3, name: 'Airflow tasks' }));
    assert.ok(screen.getByText(/Airflow DAGs were not analysed: the DAG files were not analysed/));
  });

  test('tasks are shown by DAG, with what they run, how sure that is, and why', async () => {
    const host = await shown(review({
      impacted: [
        impacted(),
        task({ id: 'airflow:z.last', name: 'last', owner: null }),
        task({
          id: 'airflow:a.marts',
          name: 'marts',
          owner: 'platform',
          airflow: {
            ...task().airflow!, dag: 'a', how: ['dbt, through Cosmos'], runs: ['orders', 'revenue'], runsMore: 3, tasks: ['marts.orders.run'], tasksMore: 2,
            notes: ['environment variable assumed unset'],
          },
        }),
        task({
          id: 'airflow:a.after',
          name: 'after',
          airflow: { ...task().airflow!, dag: 'a', how: [], runs: [], after: ['marts'] },
        }),
        task({ id: 'airflow:b', kind: 'airflow_dag', name: 'b', airflow: { ...task().airflow!, dag: 'b' } }),
      ],
      airflow: analysed({ opaqueTasks: 2 }),
      locations: { 'airflow:a.marts': { uri: 'file:///w/dags/a.py', range: { start: { line: 3, character: 0 }, end: { line: 3, character: 0 } } } },
    }));
    assert.ok(screen.getByRole('heading', { level: 3, name: 'Airflow tasks (4 tasks in 3 DAGs)' }));
    const table = screen.getByRole('table', { name: 'Impacted airflow tasks' });
    const rows = within(table).getAllByRole('row').slice(1);
    assert.deepEqual(rows.map((row) => row.querySelector('td')?.textContent), ['a', 'a', 'b', 'z']);
    const marts = rows[1];
    assert.match(marts.textContent ?? '', /Tasks Cosmos should render: marts\.orders\.run and 2 more/);
    assert.match(marts.textContent ?? '', /dbt, through Cosmos: orders, revenue and 3 more/);
    assert.match(marts.textContent ?? '', /environment variable assumed unset/);
    assert.match(marts.textContent ?? '', /platform/);
    assert.ok(marts.querySelector('.lp-edge-heuristic'));
    assert.match(rows[0].textContent ?? '', /Runs after marts/);
    assert.match(rows[2].textContent ?? '', /The whole DAG/);
    assert.match(rows[3].textContent ?? '', /No owner/);
    assert.ok(screen.getByText(/2 tasks could not be read and do not mention dbt/));
    fireEvent.click(within(marts).getByRole('button', { name: 'marts' }));
    assert.ok(host.posted.some((m) => m.type === 'openFile' && m.location.uri === 'file:///w/dags/a.py'));
  });

  test('DAG files that were read and touch nothing are said to', async () => {
    await shown(review({ airflow: analysed({ dagFiles: 3 }) }));
    assert.ok(screen.getByText(/No task in the 3 DAG files read runs a changed or downstream node/));
  });

  test('a reference to a task reads without its prefix', () => {
    assert.equal(shortRef('airflow:d.t'), 'd.t');
  });

  test('owners group what they own, by name, and nobody\'s nodes are shown', async () => {
    await shown(review({
      impacted: [impacted(), impacted({ id: 'model.shop.x', name: 'x', owner: null })],
      owners: [{ owner: 'growth', nodes: ['model.shop.customer_revenue'] }, { owner: null, nodes: ['model.shop.x', 'model.shop.gone'] }],
    }));
    const list = screen.getByRole('heading', { level: 3, name: 'Owners' }).nextElementSibling as HTMLElement;
    assert.match(list.textContent ?? '', /growthcustomer_revenue/);
    assert.match(list.textContent ?? '', /No ownerx, model\.shop\.gone/);
  });

  test('unknowns list what, and why', async () => {
    await shown(review({
      unknowns: [
        { subject: 'model.shop.broken', reference: 'column lineage', reason: 'SQL could not be parsed' },
        { subject: 'macros/m.sql', reference: 'changed file', reason: 'a macro' },
      ],
      summary: { changedFiles: 1, changedNodes: 1, changedColumns: 2, staleNodes: 0, impacted: {}, impactedByColumn: 0, unknowns: 2 },
      impacted: [],
      owners: [],
    }));
    assert.ok(screen.getByRole('heading', { level: 2, name: 'Unknowns (2)' }));
    assert.ok(screen.getByText('SQL could not be parsed'));
    assert.ok(screen.getByText('broken'));
    assert.ok(screen.getByText('macros/m.sql'), 'a path is shown whole, not cut at its last dot');
    assert.ok(screen.getByText('1 changed node, 2 changed columns, nothing downstream, 2 unknowns.'));
    assert.ok(screen.getByText('Nothing is known to be downstream of the changed nodes.'));
  });

  test('other files are out of the way, and files outside the project are counted', async () => {
    await shown(review({ otherFiles: [{ path: 'README.md', status: 'added' }], outsideProject: 2 }));
    assert.ok(screen.getByText('Other changed files (3)'));
    assert.ok(screen.getByText('README.md'));
    assert.ok(screen.getByText('2 files outside this dbt project'));
  });

  test('notes are shown', async () => {
    await shown(review({ notes: ['catalog.json lists what the warehouse held.'] }));
    assert.ok(screen.getByText('catalog.json lists what the warehouse held.'));
  });

  test('copying asks the extension, which holds the text, and says when it is done', async () => {
    const host = await shown();
    fireEvent.click(screen.getByRole('button', { name: 'Copy as Markdown' }));
    assert.deepEqual(host.posted.at(-1), { type: 'copyMarkdown' });
    await host.send({ type: 'copied', characters: 9 });
    assert.ok(screen.getByRole('button', { name: 'Copied' }));
    assert.equal(document.querySelector('[aria-live]')?.textContent, 'Copied the review as Markdown');
    await act(async () => { await new Promise((resolve) => setTimeout(resolve, 2100)); });
    assert.ok(screen.getByRole('button', { name: 'Copy as Markdown' }));
  });

  test('names open their file, and the graph is one more click away', async () => {
    const host = await shown();
    fireEvent.click(screen.getByRole('button', { name: 'customer_revenue' }));
    assert.deepEqual(host.posted.at(-1), { type: 'openFile', location: { uri: 'file:///w/models/marts/customer_revenue.sql', range: null } });
    fireEvent.click(screen.getByRole('button', { name: 'Show orders in the lineage graph' }));
    assert.deepEqual(host.posted.at(-1), { type: 'showInGraph', id: 'model.shop.orders' });
  });

  test('a node with nowhere to open is only named', async () => {
    await shown(review({ locations: {} }));
    assert.equal(screen.queryByRole('button', { name: 'orders' }), null);
    assert.equal(screen.queryByRole('button', { name: 'Show orders in the lineage graph' }), null);
    assert.ok(screen.getByText('orders'));
  });

  test('Refresh and Change base ask the extension', async () => {
    const host = await shown();
    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
    assert.deepEqual(host.posted.at(-1), { type: 'refresh' });
    fireEvent.click(screen.getByRole('button', { name: 'Change base' }));
    assert.deepEqual(host.posted.at(-1), { type: 'pickBase' });
  });

  test('while a new review runs the last one stays on screen', async () => {
    const host = await shown();
    await host.send({ type: 'reviewStarted' });
    assert.ok(screen.getByRole('heading', { level: 2, name: 'Changed' }));
    assert.equal((screen.getByRole('button', { name: 'Refresh' }) as HTMLButtonElement).disabled, true);
    assert.equal((screen.getByRole('button', { name: 'Copy as Markdown' }) as HTMLButtonElement).disabled, false);
  });

  test('a model newer than the manifest offers dbt compile, only in a trusted workspace', async () => {
    const stale = review({ summary: { ...review().summary, staleNodes: 1 }, changedNodes: [changedNode({ columns: diff({ state: 'unknown', baseline: 'none', stale: true, reason: 'run dbt compile' }) })] });
    const host = await shown(stale);
    assert.match(screen.getAllByRole('status')[0].textContent ?? '', /A changed model is newer than the manifest/);
    fireEvent.click(screen.getByRole('button', { name: 'Run dbt compile' }));
    assert.deepEqual(host.posted.at(-1), { type: 'runCompile' });
  });

  test('without trust there is no button that runs dbt', async () => {
    const stale = review({ summary: { ...review().summary, staleNodes: 2 } });
    await shown(stale, false);
    assert.match(screen.getAllByRole('status')[0].textContent ?? '', /2 changed models are newer/);
    assert.equal(screen.queryByRole('button', { name: 'Run dbt compile' }), null);
  });

  test('nothing changed says what to do next', async () => {
    await shown(review({
      summary: { changedFiles: 0, changedNodes: 0, changedColumns: 0, staleNodes: 0, impacted: {}, impactedByColumn: 0, unknowns: 0 },
      changedNodes: [],
      impacted: [],
      owners: [],
    }));
    assert.ok(screen.getByText(/Nothing differs from this base/));
    assert.equal(screen.queryByRole('heading', { name: 'Changed' }), null);
  });

  test('changed files that map to no node still get their sections', async () => {
    await shown(review({
      summary: { changedFiles: 1, changedNodes: 0, changedColumns: 0, staleNodes: 0, impacted: {}, impactedByColumn: 0, unknowns: 0 },
      changedNodes: [],
      impacted: [],
      owners: [],
    }));
    assert.ok(screen.getByText('No changed file maps to a node.'));
  });

  test('Restricted Mode explains itself and offers the way out, and no review', async () => {
    const host = await start(false);
    await host.send({ type: 'reviewFailed', problem: 'restricted', message: 'VS Code turns Git off in Restricted Mode.' });
    assert.match(screen.getByRole('alert').textContent ?? '', /turns Git off/);
    assert.equal((screen.getByRole('button', { name: 'Copy as Markdown' }) as HTMLButtonElement).disabled, true);
    fireEvent.click(screen.getByRole('button', { name: 'Manage Workspace Trust' }));
    assert.deepEqual(host.posted.at(-1), { type: 'manageTrust' });
  });

  test('no base offers to choose one', async () => {
    const host = await start();
    await host.send({ type: 'reviewFailed', problem: 'no_base', message: 'None of those exists.' });
    fireEvent.click(screen.getByRole('button', { name: 'Choose a base' }));
    assert.deepEqual(host.posted.at(-1), { type: 'pickBase' });
  });

  test('any other failure is said, Refresh is offered, and a later result replaces it', async () => {
    const host = await start();
    await host.send({ type: 'reviewFailed', problem: 'not_ready', message: 'lineage is not available' });
    assert.equal(screen.getByRole('alert').textContent, 'lineage is not available');
    assert.equal((screen.getByRole('button', { name: 'Refresh' }) as HTMLButtonElement).disabled, false);
    await host.send({ type: 'reviewResult', result: review() });
    assert.equal(screen.queryByRole('alert'), null);
    assert.ok(screen.getByRole('heading', { level: 2, name: 'Changed' }));
  });

  test('a failure replaces a review that is no longer true', async () => {
    const host = await shown();
    await host.send({ type: 'reviewFailed', problem: 'failed', message: 'the server stopped' });
    assert.equal(screen.queryByRole('heading', { level: 2, name: 'Changed' }), null);
  });

  test('workspace text stays text', async () => {
    const hostile = '<img src=x onerror=alert(1)>';
    await shown(review({
      changedNodes: [changedNode({ name: hostile, owner: hostile, columns: diff({ removed: [hostile] }) })],
      unknowns: [{ subject: hostile, reference: hostile, reason: hostile }],
    }));
    assert.equal(document.querySelector('img'), null);
    assert.ok(screen.getAllByText(hostile).length > 1);
  });

  test('a malformed message is dropped', async () => {
    const warned: unknown[] = [];
    const original = console.warn;
    console.warn = (...args) => warned.push(args);
    try {
      const host = await start();
      await host.send({ type: 'reviewResult', result: { project: 'not a review' } });
      assert.match(screen.getByRole('status').textContent ?? '', /Working out what changed/);
      assert.equal(warned.length, 1);
    } finally {
      console.warn = original;
    }
  });

  test('the transport listens to the window by default, and a listener can be removed', () => {
    let attached = false;
    const original = window.addEventListener;
    window.addEventListener = ((type: string) => { attached = type === 'message'; }) as typeof window.addEventListener;
    try {
      const transport = new ReviewTransport({ postMessage() {}, getState: () => undefined, setState() {} });
      assert.equal(attached, true);
      let received = 0;
      const stop = transport.onMessage(() => { received += 1; });
      transport.receive({ type: 'reviewStarted' });
      stop();
      transport.receive({ type: 'reviewStarted' });
      assert.equal(received, 1);
    } finally {
      window.addEventListener = original;
    }
  });
});
