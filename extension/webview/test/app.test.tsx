// The whole view against a fake extension (tasks 3.1-3.3): what it asks for, what it shows, and
// what it asks the extension to do, including opening the right file and range for a node.
import { strict as assert } from 'assert';
import { act, fireEvent, render, screen, within } from '@testing-library/react';

import type { GraphResult, ImpactResult, NodeResult } from '../../src/protocol';
import { DEFAULT_SETTINGS, type Focus } from '../../src/webviewMessages';
import App from '../src/App';
import { edge, FakeHost, graph, impacted, impactResult, node, nodeResult, ordersGraph } from './fixtures';

const ORDERS: Focus = { id: 'model.shop.orders', granularity: 'model' };

/** Send a message, and let everything it sets off finish, promise chains included. */
async function deliver(host: FakeHost, message: Parameters<FakeHost['send']>[0]): Promise<void> {
  await act(async () => {
    host.send(message);
    await new Promise((resolve) => setTimeout(resolve, 0));
  });
}

async function start(focus: Focus | null = ORDERS): Promise<FakeHost> {
  const host = new FakeHost();
  render(<App transport={host.transport} />);
  assert.deepEqual(host.posted, [{ type: 'ready' }]);
  await deliver(host, {
    type: 'init', project: { name: 'shop', root: '/project' }, focus, settings: DEFAULT_SETTINGS, generation: 1,
  });
  return host;
}

async function answer(host: FakeHost, type: 'graph', result: GraphResult): Promise<void>;
async function answer(host: FakeHost, type: 'node', result: NodeResult): Promise<void>;
async function answer(host: FakeHost, type: 'impact', result: ImpactResult): Promise<void>;
async function answer(host: FakeHost, type: 'graph' | 'node' | 'impact', result: GraphResult | NodeResult | ImpactResult): Promise<void> {
  const { id } = host.last(type);
  await deliver(host, { type: `${type}Result`, id, result } as never);
}

async function fail(host: FakeHost, type: 'graph' | 'node' | 'impact', message: string): Promise<void> {
  const { id } = host.last(type);
  await deliver(host, { type: 'error', id, message, code: -32001 });
}

async function nodeNamed(start: string): Promise<HTMLElement> {
  return screen.findByRole('group', { name: (name) => name.startsWith(start) });
}

async function showOrders(): Promise<FakeHost> {
  const host = await start();
  await answer(host, 'graph', ordersGraph());
  await nodeNamed('model orders');
  return host;
}

suite('app', () => {
  test('asks for the graph around the focus the extension gives, and shows it', async () => {
    const host = await start();
    assert.deepEqual(host.last('graph').params, { root: 'model.shop.orders', direction: 'both', depth: 2, granularity: 'model' });
    assert.equal(screen.getByRole('status').textContent, 'Loading lineage…');
    await answer(host, 'graph', ordersGraph());
    assert.ok(await nodeNamed('model orders, the focus of this view'));
    assert.equal(screen.getByText(/^Showing/).textContent, 'Showing 4 nodes around orders');
  });

  test('without a focus it says how to choose one, and asks for nothing', async () => {
    const host = await start(null);
    assert.ok(screen.getByText('Lineage for shop'));
    assert.ok(screen.getByText('LineagePilot: Show Lineage Graph'));
    assert.equal(host.all('graph').length, 0);
    assert.equal((screen.getByRole('button', { name: 'Columns' }) as HTMLButtonElement).disabled, true);
  });

  test('clicking a node shows its details and previews its file and range; Open opens it', async () => {
    const host = await showOrders();
    fireEvent.click(await nodeNamed('model stg_orders'));
    assert.deepEqual(host.last('node').params, { id: 'model.shop.stg_orders' });
    assert.deepEqual(host.last('openFile'), {
      type: 'openFile',
      location: { uri: 'file:///project/models/stg_orders.sql', range: { start: { line: 0, character: 0 }, end: { line: 9, character: 0 } } },
      preserveFocus: true,
    });
    const details = screen.getByRole('complementary', { name: 'Details of stg_orders' });
    await answer(host, 'node', nodeResult(node('model.shop.stg_orders', { side: 'upstream', depth: 1 }), { upstream: 1, downstream: 1 }));
    assert.ok(within(details).getByText('1 upstream, 1 downstream'));
    fireEvent.click(within(details).getByRole('button', { name: 'Open' }));
    assert.deepEqual(host.last('openFile'), { ...host.all('openFile')[0], preserveFocus: false });
    assert.equal(host.all('openFile').length, 2);
  });

  test('double-click, or O on the keyboard, opens a node directly', async () => {
    const host = await showOrders();
    fireEvent.doubleClick(await nodeNamed('model customers_report'));
    fireEvent.keyDown(await nodeNamed('source raw.orders'), { key: 'o' });
    assert.deepEqual(host.all('openFile').map((m) => [m.location.uri, m.preserveFocus]), [
      ['file:///project/models/customers_report.sql', false], ['file:///project/models/orders.sql', false],
    ]);
    // Enter, like a click, previews
    fireEvent.keyDown(await nodeNamed('model stg_orders'), { key: 'Enter' });
    assert.deepEqual([host.last('openFile').location.uri, host.last('openFile').preserveFocus], ['file:///project/models/stg_orders.sql', true]);
  });

  test('a node without a file opens nothing', async () => {
    const host = await start();
    await answer(host, 'graph', graph('model.shop.orders', [node('model.shop.orders', { location: null })], []));
    fireEvent.keyDown(await nodeNamed('model orders'), { key: 'o' });
    assert.equal(host.all('openFile').length, 0);
  });

  test('depth and direction ask again, and a late answer to an old request is ignored', async () => {
    const host = await showOrders();
    fireEvent.change(screen.getByRole('combobox', { name: 'Depth' }), { target: { value: '3' } });
    const deeper = host.last('graph');
    assert.equal(deeper.params.depth, 3);
    fireEvent.change(screen.getByRole('combobox', { name: 'Direction' }), { target: { value: 'upstream' } });
    const upstream = host.last('graph');
    assert.deepEqual(upstream.params, { root: 'model.shop.orders', direction: 'upstream', depth: 3, granularity: 'model' });
    await deliver(host, { type: 'graphResult', id: upstream.id, result: graph('model.shop.orders', [node('model.shop.orders')], []) });
    await deliver(host, { type: 'graphResult', id: deeper.id, result: ordersGraph() });
    assert.ok(await screen.findByText('1 node'));
    assert.equal(screen.queryByRole('group', { name: /^model stg_orders/ }), null);
  });

  test('a failed request is shown, and the next good answer clears it', async () => {
    const host = await start();
    await fail(host, 'graph', 'Unknown node model.shop.orders');
    assert.equal(screen.getByRole('alert').textContent, 'Unknown node model.shop.orders');
    fireEvent.click(screen.getByRole('button', { name: 'Reload' }));
    await answer(host, 'graph', ordersGraph());
    assert.equal(screen.queryByRole('alert'), null);
    assert.ok(await nodeNamed('model orders'));
  });

  test('+ on a node at the edge fetches one more step on its side, and merges it', async () => {
    const host = await showOrders();
    fireEvent.click(screen.getByRole('button', { name: 'Show more upstream of raw.orders' }));
    assert.deepEqual(host.last('graph').params, { root: 'source.shop.raw.orders', direction: 'upstream', depth: 1, granularity: 'model' });
    await answer(host, 'graph', graph('source.shop.raw.orders', [
      node('source.shop.raw.orders', { kind: 'source', name: 'raw.orders' }),
      node('seed.shop.raw_seed', { kind: 'seed', side: 'upstream', depth: 1 }),
    ], [edge('seed.shop.raw_seed', 'source.shop.raw.orders')]));
    assert.ok(await nodeNamed('seed raw_seed, upstream, 3 steps away'));
    assert.ok(await nodeNamed('model orders'));
    assert.equal(screen.getByText(/^Added/).textContent, 'Added 1 node upstream of raw.orders');
    assert.equal(screen.queryByRole('button', { name: 'Show more upstream of raw.orders' }), null);
  });

  test('a failed expansion is shown', async () => {
    const host = await showOrders();
    fireEvent.click(screen.getByRole('button', { name: 'Show more upstream of raw.orders' }));
    await fail(host, 'graph', 'The language server stopped');
    assert.equal(screen.getByRole('alert').textContent, 'The language server stopped');
    assert.ok(await nodeNamed('model orders'));
  });

  test('Impact shows what a change would reach, and its links refocus or open', async () => {
    const host = await showOrders();
    fireEvent.click(await nodeNamed('model orders'));
    fireEvent.click(screen.getByRole('button', { name: 'Impact' }));
    assert.deepEqual(host.last('impact').params, { targets: ['model.shop.orders'] });
    const location = { uri: 'file:///project/models/customers_report.sql', range: null };
    await answer(host, 'impact', impactResult({ counts: { model: 1 }, nodes: [impacted('model.shop.customers_report', { location })] }));
    const panel = within(screen.getByRole('complementary', { name: 'Impact of changing orders' }));
    fireEvent.click(panel.getByRole('button', { name: 'Open customers_report' }));
    assert.deepEqual(host.last('openFile').location, location);
    fireEvent.click(panel.getByRole('button', { name: 'customers_report' }));
    assert.equal(host.last('graph').params.root, 'model.shop.customers_report');
    assert.equal(screen.queryByRole('complementary', { name: /^Impact/ }), null);
  });

  test('impact and detail errors are shown in their panels', async () => {
    const host = await showOrders();
    fireEvent.click(await nodeNamed('model stg_orders'));
    await fail(host, 'node', 'Unknown node');
    assert.equal(within(screen.getByRole('complementary', { name: 'Details of stg_orders' })).getByRole('alert').textContent, 'Unknown node');
    fireEvent.click(screen.getByRole('button', { name: 'Impact' }));
    await fail(host, 'impact', 'Impact failed');
    assert.equal(within(screen.getByRole('complementary', { name: /^Impact/ })).getByRole('alert').textContent, 'Impact failed');
    fireEvent.click(screen.getByRole('button', { name: 'Close impact' }));
    assert.equal(screen.queryByRole('complementary'), null);
  });

  test('details: Focus here moves the graph, Close and the pane clear the selection', async () => {
    const host = await showOrders();
    fireEvent.click(await nodeNamed('model stg_orders'));
    fireEvent.click(screen.getByRole('button', { name: 'Close details' }));
    assert.equal(screen.queryByRole('complementary'), null);
    fireEvent.click(await nodeNamed('model stg_orders'));
    fireEvent.click(document.querySelector('.react-flow__pane')!);
    assert.equal(screen.queryByRole('complementary'), null);
    fireEvent.click(await nodeNamed('model stg_orders'));
    fireEvent.click(screen.getByRole('button', { name: 'Focus here' }));
    assert.deepEqual(host.last('graph').params, { root: 'model.shop.stg_orders', direction: 'both', depth: 2, granularity: 'model' });
  });

  test('column mode: choose a column of the focus, trace it, and go back to models', async () => {
    const host = await showOrders();
    fireEvent.click(screen.getByRole('button', { name: 'Columns' }));
    assert.deepEqual(host.last('node').params, { id: 'model.shop.orders' });
    await answer(host, 'node', nodeResult(node('model.shop.orders'), {
      columns: [{ id: 'model.shop.orders#order_total', name: 'order_total', dataType: 'numeric', description: null, pii: false, tests: 1 }],
    }));
    const picker = within(screen.getByRole('dialog', { name: 'Choose a column of orders' }));
    fireEvent.click(picker.getByRole('button', { name: 'order_total' }));
    assert.deepEqual(host.last('graph').params, { root: 'model.shop.orders#order_total', direction: 'both', depth: 2, granularity: 'column' });
    assert.equal(screen.queryByRole('dialog'), null);
    await answer(host, 'graph', graph('model.shop.orders#order_total', [
      node('model.shop.orders#order_total'),
      node('model.shop.stg_payments#amount', { side: 'upstream', depth: 1 }),
    ], [edge('model.shop.stg_payments#amount', 'model.shop.orders#order_total', { transformation: 'sum', confidence: 'inferred' })]));
    assert.ok(await nodeNamed('column order_total, the focus'));
    assert.equal(screen.getByRole('button', { name: 'Columns' }).getAttribute('aria-pressed'), 'true');
    // Column details trace to columns
    fireEvent.click(await nodeNamed('column amount'));
    fireEvent.click(screen.getByRole('button', { name: 'Focus here' }));
    assert.deepEqual(host.last('graph').params.granularity, 'column');
    await answer(host, 'graph', graph('model.shop.stg_payments#amount', [node('model.shop.stg_payments#amount')], []));
    await nodeNamed('column amount, the focus');
    fireEvent.click(screen.getByRole('button', { name: 'Models' }));
    assert.deepEqual(host.last('graph').params, { root: 'model.shop.stg_payments', direction: 'both', depth: 2, granularity: 'model' });
  });

  test('column mode from a selected model offers its columns; none known says why; cancel', async () => {
    const host = await showOrders();
    fireEvent.click(await nodeNamed('model stg_orders'));
    fireEvent.click(screen.getByRole('button', { name: 'Columns' }));
    assert.deepEqual(host.last('node').params, { id: 'model.shop.stg_orders' });
    await answer(host, 'node', nodeResult(node('model.shop.stg_orders')));
    const picker = within(screen.getByRole('dialog', { name: 'Choose a column of stg_orders' }));
    assert.ok(picker.getByText(/has no known columns. Run dbt docs generate/));
    fireEvent.click(picker.getByRole('button', { name: 'Cancel' }));
    assert.equal(screen.queryByRole('dialog'), null);
    fireEvent.click(screen.getByRole('button', { name: 'Columns' }));
    await fail(host, 'node', 'Unknown node');
    assert.equal(screen.getByRole('alert').textContent, 'Unknown node');
  });

  test('the columns in the details panel trace one column', async () => {
    const host = await showOrders();
    fireEvent.click(await nodeNamed('model orders'));
    await answer(host, 'node', nodeResult(node('model.shop.orders'), {
      columns: [{ id: 'model.shop.orders#order_id', name: 'order_id', dataType: null, description: null, pii: false, tests: 0 }],
    }));
    fireEvent.click(screen.getByRole('button', { name: 'order_id' }));
    assert.deepEqual(host.last('graph').params, { root: 'model.shop.orders#order_id', direction: 'both', depth: 2, granularity: 'column' });
  });

  test('settings are kept by the extension: switches, layout and spacing', async () => {
    const host = await showOrders();
    fireEvent.click(screen.getByRole('button', { name: 'View settings' }));
    fireEvent.click(screen.getByRole('switch', { name: /^Mini map/ }));
    assert.deepEqual(host.last('saveSettings').settings, { ...DEFAULT_SETTINGS, showMiniMap: false });
    assert.equal(document.querySelector('.react-flow__minimap'), null);
    fireEvent.change(screen.getByRole('combobox', { name: 'Layout' }), { target: { value: 'TB' } });
    assert.deepEqual(host.last('saveSettings').settings, { ...DEFAULT_SETTINGS, showMiniMap: false, layout: 'TB' });
    fireEvent.click(screen.getByRole('button', { name: 'View settings' }));
    assert.equal(screen.queryByRole('complementary', { name: 'View settings' }), null);
  });

  test('the extension decides the initial settings', async () => {
    const host = new FakeHost();
    render(<App transport={host.transport} />);
    await deliver(host, {
      type: 'init', project: { name: 'shop', root: '/project' }, focus: ORDERS, settings: { ...DEFAULT_SETTINGS, showLegend: false }, generation: 1,
    });
    await answer(host, 'graph', ordersGraph());
    await nodeNamed('model orders');
    assert.equal(screen.queryByLabelText('Legend'), null);
  });

  test('a finished re-index reloads; a new focus moves the view and keeps settings open', async () => {
    const host = await showOrders();
    const before = host.all('graph').length;
    await deliver(host, { type: 'indexUpdated', generation: 2, ready: false });
    assert.equal(host.all('graph').length, before);
    await deliver(host, { type: 'indexUpdated', generation: 3, ready: true });
    assert.equal(host.all('graph').length, before + 1);
    assert.equal(host.last('graph').params.root, 'model.shop.orders');
    fireEvent.click(screen.getByRole('button', { name: 'View settings' }));
    await deliver(host, { type: 'focus', focus: { id: 'model.shop.customers_report', granularity: 'model' } });
    assert.equal(host.last('graph').params.root, 'model.shop.customers_report');
    assert.ok(screen.getByRole('complementary', { name: 'View settings' }));
  });

  test('a new focus closes the details of the old one', async () => {
    const host = await showOrders();
    fireEvent.click(await nodeNamed('model stg_orders'));
    await deliver(host, { type: 'focus', focus: { id: 'model.shop.customers_report', granularity: 'model' } });
    assert.equal(screen.queryByRole('complementary'), null);
  });

  test('rebuilt after VS Code hid it, the view shows what it showed, without waiting', async () => {
    const host = new FakeHost({ focus: { id: 'model.shop.stg_orders', granularity: 'model' }, depth: 3, direction: 'upstream' });
    render(<App transport={host.transport} />);
    assert.deepEqual(host.last('graph').params, { root: 'model.shop.stg_orders', direction: 'upstream', depth: 3, granularity: 'model' });
    // The extension has no new focus for it
    await deliver(host, { type: 'init', project: { name: 'shop', root: '/project' }, focus: null, settings: DEFAULT_SETTINGS, generation: 2 });
    assert.equal(host.all('graph').length, 1);
    fireEvent.change(screen.getByRole('combobox', { name: 'Depth' }), { target: { value: '5' } });
    assert.deepEqual(host.state, { focus: { id: 'model.shop.stg_orders', granularity: 'model' }, depth: 5, direction: 'upstream' });
  });

  test('a focus sent while it was hidden wins over what it kept', async () => {
    const host = new FakeHost({ focus: { id: 'model.shop.stg_orders', granularity: 'model' }, depth: 3, direction: 'both' });
    render(<App transport={host.transport} />);
    await deliver(host, { type: 'init', project: { name: 'shop', root: '/project' }, focus: ORDERS, settings: DEFAULT_SETTINGS, generation: 2 });
    assert.deepEqual(host.last('graph').params, { root: 'model.shop.orders', direction: 'both', depth: 3, granularity: 'model' });
  });

  test('kept state that does not validate is ignored', async () => {
    const host = new FakeHost({ focus: { id: '', granularity: 'model' }, depth: 99, direction: 'sideways', extra: true });
    render(<App transport={host.transport} />);
    assert.equal(host.all('graph').length, 0);
    assert.equal((screen.getByRole('combobox', { name: 'Depth' }) as HTMLSelectElement).value, '2');
  });

  test('filters hide kinds, search highlights, recentre asks again', async () => {
    const host = await showOrders();
    fireEvent.click(within(screen.getByRole('group', { name: 'Show' })).getByRole('checkbox', { name: 'source' }));
    assert.equal(screen.queryByRole('group', { name: /^source raw.orders/ }), null);
    fireEvent.click(within(screen.getByRole('group', { name: 'Show' })).getByRole('checkbox', { name: 'source' }));
    assert.ok(await nodeNamed('source raw.orders'));
    fireEvent.change(screen.getByRole('searchbox', { name: 'Highlight nodes' }), { target: { value: 'customers' } });
    assert.equal(document.querySelectorAll('.lp-node-dimmed').length, 3);
    const before = host.all('graph').length;
    fireEvent.click(screen.getByRole('button', { name: 'Back to the focus' }));
    assert.equal(host.all('graph').length, before + 1);
  });
});
