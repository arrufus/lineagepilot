// The graph (tasks 3.2-3.4): nodes with accessible labels, selection, opening and expansion by
// mouse and keyboard, arrow keys along the edges, filters, edge styles, and the theme.
import { strict as assert } from 'assert';
import { act, fireEvent, render, screen, within } from '@testing-library/react';

import type { GraphNode, GraphResult } from '../../src/protocol';
import { DEFAULT_SETTINGS, type ViewSettings } from '../../src/webviewMessages';
import LineageGraph from '../src/components/LineageGraph';
import { handleNodeKey, type NodeActions } from '../src/components/ModelNode';
import { type Filters, fromResult, NO_FILTERS } from '../src/graph/model';
import { edge, graph, largeGraph, node, ordersGraph } from './fixtures';

type Calls = { select: (GraphNode | null)[]; open: GraphNode[]; expand: GraphNode[] };

function draw(result: GraphResult = ordersGraph(), options: { filters?: Filters; settings?: Partial<ViewSettings>; selectedId?: string; depth?: number } = {}) {
  const calls: Calls = { select: [], open: [], expand: [] };
  const props = {
    view: fromResult(result, options.depth ?? 2),
    filters: options.filters ?? NO_FILTERS,
    settings: { ...DEFAULT_SETTINGS, ...options.settings },
    selectedId: options.selectedId ?? null,
    onSelect: (n: GraphNode | null) => calls.select.push(n),
    onOpen: (n: GraphNode) => calls.open.push(n),
    onExpand: (n: GraphNode) => calls.expand.push(n),
  };
  const rendered = render(<LineageGraph {...props} />);
  return { ...rendered, calls, props };
}

/** A node's focusable wrapper, found by the start of what a screen reader announces. */
async function nodeNamed(start: string): Promise<HTMLElement> {
  return screen.findByRole('group', { name: (name) => name.startsWith(start) });
}

function translate(element: HTMLElement): { x: number; y: number } {
  const [, x, y] = /translate\(([-\d.]+)px, ?([-\d.]+)px\)/.exec(element.style.transform) ?? [];
  return { x: Number(x), y: Number(y) };
}

suite('lineage graph', () => {
  test('every node is focusable, with a label saying what and where it is', async () => {
    draw();
    const orders = await nodeNamed('model orders');
    assert.equal(orders.getAttribute('aria-label'), 'model orders, the focus of this view, 1 upstream, 1 downstream');
    assert.equal(orders.tabIndex, 0);
    assert.ok(await nodeNamed('source raw.orders, upstream, 2 steps away, 0 upstream, 1 downstream'));
    assert.ok(await nodeNamed('model customers_report, downstream, 1 step away, 1 upstream, 0 downstream, has PII indicators'));
    // React Flow describes the keys, from our configuration
    assert.match(document.body.textContent ?? '', /Arrow keys follow the edges\. Enter shows details and previews the file, O opens it, \+ shows more\./);
    assert.doesNotMatch(document.body.textContent ?? '', /move the node|delete/i);
  });

  test('a node shows its kind, layer, materialization and PII', async () => {
    draw(ordersGraph({
      nodes: [
        node('model.shop.orders', { layer: 'marts', materialization: 'table', columnLineage: 'unknown', columnLineageReason: 'Jinja in SQL' }),
        node('model.shop.customers_report', { side: 'downstream', depth: 1, pii: true }),
      ],
      edges: [edge('model.shop.orders', 'model.shop.customers_report')],
    }));
    const orders = within(await nodeNamed('model orders'));
    assert.ok(orders.getByText('marts'));
    assert.ok(orders.getByText('table'));
    assert.equal(orders.getByText('columns').closest('[title]')?.getAttribute('title'), 'Column lineage unknown: Jinja in SQL');
    assert.ok(within(await nodeNamed('model customers_report')).getByText('PII'));
  });

  test('click selects, the pane clears, double-click opens', async () => {
    const { calls, container } = draw();
    fireEvent.click(await nodeNamed('model stg_orders'));
    assert.deepEqual(calls.select.map((n) => n?.id), ['model.shop.stg_orders']);
    fireEvent.click(container.querySelector('.react-flow__pane')!);
    assert.deepEqual(calls.select.map((n) => n?.id ?? null), ['model.shop.stg_orders', null]);
    fireEvent.doubleClick(await nodeNamed('model customers_report'));
    assert.deepEqual(calls.open.map((n) => n.id), ['model.shop.customers_report']);
  });

  test('keys on the focused node: Enter and Space select, O opens, + expands where there is more', async () => {
    const { calls } = draw();
    const stg = await nodeNamed('model stg_orders');
    const source = await nodeNamed('source raw.orders');
    fireEvent.keyDown(stg, { key: 'Enter' });
    fireEvent.keyDown(source, { key: ' ' });
    fireEvent.keyDown(stg, { key: 'o' });
    fireEvent.keyDown(source, { key: 'O' });
    // stg_orders is inside the depth asked for, so there is nothing more to show
    fireEvent.keyDown(stg, { key: '+' });
    fireEvent.keyDown(source, { key: '+' });
    fireEvent.keyDown(source, { key: 'x' });
    assert.deepEqual(calls.select.map((n) => n?.id), ['model.shop.stg_orders', 'source.shop.raw.orders']);
    assert.deepEqual(calls.open.map((n) => n.id), ['model.shop.stg_orders', 'source.shop.raw.orders']);
    assert.deepEqual(calls.expand.map((n) => n.id), ['source.shop.raw.orders']);
  });

  test('the expand button shows more without selecting', async () => {
    const { calls } = draw();
    fireEvent.click(await screen.findByRole('button', { name: 'Show more upstream of raw.orders' }));
    assert.deepEqual(calls.expand.map((n) => n.id), ['source.shop.raw.orders']);
    assert.deepEqual(calls.select, []);
    assert.equal(screen.queryByRole('button', { name: /Show more .* of stg_orders/ }), null);
  });

  test('arrow keys follow the edges: downstream, upstream, and across a layer', async () => {
    const result = ordersGraph();
    result.nodes.push(node('model.shop.finance', { side: 'downstream', depth: 1 }));
    result.edges.push(edge('model.shop.orders', 'model.shop.finance'));
    draw(result);
    const orders = await nodeNamed('model orders');
    fireEvent.keyDown(orders, { key: 'ArrowRight' });
    const downstream = document.activeElement as HTMLElement;
    assert.match(downstream.getAttribute('aria-label') ?? '', /^model (customers_report|finance), downstream/);
    fireEvent.keyDown(downstream, { key: 'ArrowLeft' });
    assert.equal(document.activeElement, orders);
    fireEvent.keyDown(orders, { key: 'ArrowLeft' });
    assert.match((document.activeElement as HTMLElement).getAttribute('aria-label') ?? '', /^model stg_orders/);

    // The two downstream models share a layer: Down goes to the lower one, Up comes back
    const report = await nodeNamed('model customers_report');
    const finance = await nodeNamed('model finance');
    const [upper, lower] = translate(report).y < translate(finance).y ? [report, finance] : [finance, report];
    fireEvent.keyDown(upper, { key: 'ArrowDown' });
    assert.equal(document.activeElement, lower);
    fireEvent.keyDown(lower, { key: 'ArrowUp' });
    assert.equal(document.activeElement, upper);
    // Nothing beyond the last node: focus stays
    fireEvent.keyDown(upper, { key: 'ArrowRight' });
    assert.equal(document.activeElement, upper);
  });

  test('top to bottom, Down follows the flow', async () => {
    draw(ordersGraph(), { settings: { layout: 'TB' } });
    fireEvent.keyDown(await nodeNamed('model orders'), { key: 'ArrowDown' });
    assert.match((document.activeElement as HTMLElement).getAttribute('aria-label') ?? '', /^model customers_report/);
    fireEvent.keyDown(document.activeElement!, { key: 'ArrowUp' });
    assert.match((document.activeElement as HTMLElement).getAttribute('aria-label') ?? '', /^model orders/);
  });

  test('keys outside a node do nothing', async () => {
    const { calls, container } = draw();
    await nodeNamed('model orders');
    fireEvent.keyDown(container.querySelector('.react-flow__pane')!, { key: 'Enter' });
    const event = { key: 'Enter', preventDefault: () => {} };
    assert.deepEqual(calls.select, []);
    // The handler itself, for a key that means nothing
    const actions: NodeActions = { onSelect: () => assert.fail(), onOpen: () => assert.fail(), onExpand: () => assert.fail(), onNavigate: () => assert.fail() };
    handleNodeKey({ ...event, key: 'Tab' } as never, node('model.shop.orders'), actions, true);
  });

  test('the selected node is marked', async () => {
    draw(ordersGraph(), { selectedId: 'model.shop.stg_orders' });
    const stg = await nodeNamed('model stg_orders');
    assert.ok(stg.querySelector('.lp-node-selected'));
    assert.equal((await nodeNamed('model orders')).querySelector('.lp-node-selected'), null);
    assert.ok((await nodeNamed('model orders')).querySelector('.lp-node-root'));
  });

  test('hidden kinds are not drawn, and the summary says so; search dims the rest', async () => {
    const { container } = draw(ordersGraph(), { filters: { hiddenKinds: new Set(['source']), search: 'stg' } });
    await nodeNamed('model orders');
    assert.equal(screen.queryByRole('group', { name: /^source raw.orders/ }), null);
    assert.ok(screen.getByText('1 hidden by filters'));
    assert.ok(screen.getByText('3 nodes'));
    assert.ok(screen.getByText(/2 edges/));
    assert.equal(container.querySelectorAll('.lp-node-dimmed').length, 2);
    assert.equal((await nodeNamed('model stg_orders')).querySelector('.lp-node-dimmed'), null);
  });

  test('the summary notes what was cut off and what is unknown', async () => {
    draw(ordersGraph({ truncated: true, unknowns: [{ subject: 'model.shop.orders', reference: 'ref("gone")', reason: 'no such model' }] }));
    await nodeNamed('model orders');
    assert.ok(screen.getByText(/There is more beyond what is shown/));
    assert.ok(screen.getByText(/1 unknown: see the details panel/));
  });

  test('display settings: legend, summary, mini map and controls can each be hidden', async () => {
    const shown = draw();
    await nodeNamed('model orders');
    assert.ok(screen.getByLabelText('Legend'));
    assert.ok(shown.container.querySelector('.react-flow__minimap'));
    assert.ok(shown.container.querySelector('.react-flow__controls'));
    assert.ok(screen.getByText('4 nodes'));
    shown.unmount();
    const hidden = draw(ordersGraph(), { settings: { showLegend: false, showMiniMap: false, showControls: false, showInfoPanel: false } });
    await nodeNamed('model orders');
    assert.equal(screen.queryByLabelText('Legend'), null);
    assert.equal(hidden.container.querySelector('.react-flow__minimap'), null);
    assert.equal(hidden.container.querySelector('.react-flow__controls'), null);
    assert.equal(screen.queryByText('4 nodes'), null);
  });

  test('edges are styled by confidence; column edges say how the column was derived', async () => {
    const columns = graph('model.shop.orders#order_total', [
      node('model.shop.orders#order_total'),
      node('model.shop.stg_payments#amount', { side: 'upstream', depth: 1, pii: true }),
    ], [edge('model.shop.stg_payments#amount', 'model.shop.orders#order_total', { transformation: 'sum', confidence: 'inferred', origins: ['compiled_sql'] })]);
    const { container } = draw(columns);
    const root = within(await nodeNamed('column order_total, the focus'));
    assert.ok(root.getByText('sum'));
    assert.ok(root.getByText('inferred'));
    assert.ok(root.getByText('orders'));
    assert.ok(within(await nodeNamed('column amount')).getByText('PII'));
    assert.ok(container.querySelector('.react-flow__edge.lp-edge-inferred'));
    assert.ok(screen.getByText('sum · inferred'));
  });

  test('edges animate only when asked, for small graphs, without reduced motion', async () => {
    const still = draw();
    await nodeNamed('model orders');
    assert.equal(still.container.querySelector('.react-flow__edge.animated'), null);
    still.unmount();
    const animated = draw(ordersGraph(), { settings: { animateEdges: true } });
    await nodeNamed('model orders');
    assert.ok(animated.container.querySelector('.react-flow__edge.animated'));
    animated.unmount();
    const original = window.matchMedia;
    window.matchMedia = ((query: string) => ({ matches: query.includes('reduce'), media: query })) as unknown as typeof window.matchMedia;
    try {
      const reduced = draw(ordersGraph(), { settings: { animateEdges: true } });
      await nodeNamed('model orders');
      assert.equal(reduced.container.querySelector('.react-flow__edge.animated'), null);
    } finally {
      window.matchMedia = original;
    }
  });

  test('follows the VS Code theme as it changes', async () => {
    document.body.className = 'vscode-dark';
    const { container, unmount } = draw();
    try {
      await nodeNamed('model orders');
      assert.ok(container.querySelector('.react-flow.dark'));
      await act(async () => { document.body.className = 'vscode-light'; });
      assert.ok(container.querySelector('.react-flow.light'));
      await act(async () => { document.body.className = 'vscode-high-contrast'; });
      assert.ok(container.querySelector('.react-flow.dark'));
    } finally {
      unmount();
      document.body.className = '';
    }
  });

  test('500 nodes: only what is on screen is drawn, and selection stays quick', async () => {
    const big = largeGraph(500);
    const result = graph('model.big.m0', big.nodes, big.edges);
    const started = performance.now();
    const { container, calls } = draw(result, { depth: 20 });
    const first = await nodeNamed('model m0');
    // Let the nodes drawn after the first finish measuring
    await act(() => new Promise((resolve) => setTimeout(resolve, 0)));
    const drawn = container.querySelectorAll('.react-flow__node').length;
    const elapsed = performance.now() - started;
    assert.ok(drawn > 0 && drawn < 500, `${drawn} of 500 nodes drawn`);
    // jsdom is much slower than VS Code's Chromium; this guards against a regression, and the
    // browser check (docs/design.md section 14) measures the real thing
    assert.ok(elapsed < 10_000, `first draw took ${elapsed.toFixed(0)} ms`);
    const clicked = performance.now();
    fireEvent.click(first);
    assert.equal(calls.select.length, 1);
    assert.ok(performance.now() - clicked < 1000);
  });
});
