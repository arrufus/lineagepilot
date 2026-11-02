// The toolbar, and the details, impact and settings panels (tasks 3.2 and 3.3).
import { strict as assert } from 'assert';
import { fireEvent, render, screen, within } from '@testing-library/react';

import type { GraphNode, Location, NodeKind } from '../../src/protocol';
import { DEFAULT_SETTINGS, type ViewSettings } from '../../src/webviewMessages';
import DetailPanel from '../src/components/DetailPanel';
import ImpactPanel from '../src/components/ImpactPanel';
import SettingsPanel from '../src/components/SettingsPanel';
import Toolbar, { MAX_DEPTH, type ToolbarProps } from '../src/components/Toolbar';
import { edge, impacted, impactResult, node, nodeResult } from './fixtures';

/** Every callback a component takes, recording its calls. */
function recorder<T extends string>(...names: T[]): { calls: [T, ...unknown[]][]; handlers: Record<T, (...args: unknown[]) => void> } {
  const calls: [T, ...unknown[]][] = [];
  const handlers = Object.fromEntries(names.map((name) => [name, (...args: unknown[]) => { calls.push([name, ...args]); }])) as unknown as Record<T, (...args: unknown[]) => void>;
  return { calls, handlers };
}

suite('toolbar', () => {
  function toolbar(props: Partial<ToolbarProps> = {}) {
    const { calls, handlers } = recorder('onMode', 'onDepth', 'onDirection', 'onLayout', 'onToggleKind', 'onSearch', 'onRefresh', 'onRecentre', 'onSettings');
    render(<Toolbar mode="model" canUseColumns depth={2} direction="both" layout="LR" kinds={['model', 'source']}
      hiddenKinds={new Set<NodeKind>(['source'])} search="" {...handlers} {...props} />);
    return calls;
  }

  test('the mode buttons say which is on', () => {
    const calls = toolbar();
    assert.equal(screen.getByRole('button', { name: 'Models' }).getAttribute('aria-pressed'), 'true');
    assert.equal(screen.getByRole('button', { name: 'Columns' }).getAttribute('aria-pressed'), 'false');
    fireEvent.click(screen.getByRole('button', { name: 'Columns' }));
    assert.deepEqual(calls, [['onMode', 'column']]);
  });

  test('columns need a focus first', () => {
    toolbar({ canUseColumns: false });
    assert.equal((screen.getByRole('button', { name: 'Columns' }) as HTMLButtonElement).disabled, true);
  });

  test('depth, direction and layout', () => {
    const calls = toolbar();
    const depth = screen.getByRole('combobox', { name: 'Depth' }) as HTMLSelectElement;
    assert.equal(depth.options.length, MAX_DEPTH);
    fireEvent.change(depth, { target: { value: '5' } });
    fireEvent.change(screen.getByRole('combobox', { name: 'Direction' }), { target: { value: 'upstream' } });
    fireEvent.change(screen.getByRole('combobox', { name: 'Layout' }), { target: { value: 'TB' } });
    assert.deepEqual(calls, [['onDepth', 5], ['onDirection', 'upstream'], ['onLayout', 'TB']]);
  });

  test('kind filters, search and the buttons', () => {
    const calls = toolbar();
    const kinds = within(screen.getByRole('group', { name: 'Show' }));
    assert.equal((kinds.getByRole('checkbox', { name: 'model' }) as HTMLInputElement).checked, true);
    assert.equal((kinds.getByRole('checkbox', { name: 'source' }) as HTMLInputElement).checked, false);
    fireEvent.click(kinds.getByRole('checkbox', { name: 'source' }));
    fireEvent.change(screen.getByRole('searchbox', { name: 'Highlight nodes' }), { target: { value: 'orders' } });
    fireEvent.click(screen.getByRole('button', { name: 'Back to the focus' }));
    fireEvent.click(screen.getByRole('button', { name: 'Reload' }));
    fireEvent.click(screen.getByRole('button', { name: 'View settings' }));
    assert.deepEqual(calls, [['onToggleKind', 'source'], ['onSearch', 'orders'], ['onRecentre'], ['onRefresh'], ['onSettings']]);
  });

  test('no kind filter for a graph of one kind', () => {
    toolbar({ kinds: ['model'] });
    assert.equal(screen.queryByRole('group', { name: 'Show' }), null);
  });
});

suite('detail panel', () => {
  const stg = node('model.shop.stg_orders', {
    side: 'upstream', depth: 1, layer: 'staging', materialization: 'view', owner: 'data-eng', description: 'Cleaned orders',
    columnLineage: 'partial', columnLineageReason: 'one column from a macro', pii: true,
  });

  function panel(fields: Partial<Parameters<typeof DetailPanel>[0]> = {}) {
    const { calls, handlers } = recorder('onClose', 'onOpen', 'onFocus', 'onImpact');
    render(<DetailPanel node={stg} error={null} names={new Map([['model.shop.orders', 'orders']])} unknowns={[]}
      detail={nodeResult(stg, {
        upstream: 1, downstream: 1,
        columns: [
          { id: 'model.shop.stg_orders#order_id', name: 'order_id', dataType: 'integer', description: null, pii: false, tests: 2 },
          { id: 'model.shop.stg_orders#email', name: 'email', dataType: 'text', description: null, pii: true, tests: 0 },
        ],
      })}
      upstream={[edge('source.shop.raw.orders', 'model.shop.stg_orders', { relation: 'relation:raw.orders' })]}
      downstream={[edge('model.shop.stg_orders', 'model.shop.orders', { confidence: 'inferred', origins: ['compiled_sql'] })]}
      {...handlers} {...fields} />);
    return calls;
  }

  test('shows what is known about the node', () => {
    panel();
    const about = within(screen.getByRole('region', { name: 'About' }));
    for (const text of ['model', 'staging', 'view', 'data-eng', '1 upstream, 1 downstream', 'partial: one column from a macro', 'Cleaned orders']) {
      assert.ok(about.getByText(text), text);
    }
    assert.ok(about.getByText(/PII indicators: from tags, names or lineage, not verified exposure/));
    assert.ok(screen.getByText('model.shop.stg_orders'));
  });

  test('lists its columns, each one a way into column mode', () => {
    const calls = panel();
    const columns = within(screen.getByRole('region', { name: 'Columns' }));
    assert.ok(columns.getByText('2 columns'));
    assert.ok(columns.getByText('integer · 2 tests'));
    assert.ok(columns.getByText('text · PII'));
    fireEvent.click(columns.getByRole('button', { name: 'email' }));
    assert.deepEqual(calls, [['onFocus', 'model.shop.stg_orders#email', 'column']]);
  });

  test('lists its neighbours with how each link is known', () => {
    const calls = panel();
    const upstream = within(screen.getByRole('region', { name: 'Upstream' }));
    assert.ok(upstream.getByText(/declared · manifest · via table raw.orders/));
    const downstream = within(screen.getByRole('region', { name: 'Downstream' }));
    assert.ok(downstream.getByText('inferred · compiled sql'));
    fireEvent.click(downstream.getByRole('button', { name: 'orders' }));
    fireEvent.click(upstream.getByRole('button', { name: 'source.shop.raw.orders' }));
    assert.deepEqual(calls, [['onFocus', 'model.shop.orders', 'model'], ['onFocus', 'source.shop.raw.orders', 'model']]);
  });

  test('Open, Impact, Focus here and Close', () => {
    const calls = panel();
    fireEvent.click(screen.getByRole('button', { name: 'Open' }));
    fireEvent.click(screen.getByRole('button', { name: 'Impact' }));
    fireEvent.click(screen.getByRole('button', { name: 'Focus here' }));
    fireEvent.click(screen.getByRole('button', { name: 'Close details' }));
    assert.deepEqual(calls.map((c) => [c[0], c[0] === 'onFocus' ? c[1] : (c[1] as GraphNode | undefined)?.id]), [
      ['onOpen', 'model.shop.stg_orders'], ['onImpact', 'model.shop.stg_orders'], ['onFocus', 'model.shop.stg_orders'], ['onClose', undefined],
    ]);
  });

  test('the focus has no Focus here; a node without a file cannot be opened', () => {
    panel({ node: node('model.shop.orders', { location: null }), detail: null, upstream: [], downstream: [] });
    assert.equal(screen.queryByRole('button', { name: 'Focus here' }), null);
    assert.equal((screen.getByRole('button', { name: 'Open' }) as HTMLButtonElement).disabled, true);
    assert.equal(screen.queryByRole('region', { name: 'Upstream' }), null);
  });

  test('a column traces to columns; its own unknowns and errors are shown', () => {
    const column = node('model.shop.orders#order_total');
    const calls = panel({
      node: column,
      detail: null,
      error: 'The language server stopped',
      upstream: [edge('model.shop.stg_payments#amount', column.id, { transformation: 'sum', confidence: 'inferred', origins: ['compiled_sql'] })],
      downstream: [],
      unknowns: [
        { subject: 'model.shop.orders#order_total', reference: 'payments.amount', reason: 'ambiguous column' },
        { subject: 'model.shop.customers', reference: 'x', reason: 'not this node' },
      ],
    });
    assert.equal(screen.getByRole('alert').textContent, 'The language server stopped');
    assert.ok(screen.getByText('sum · inferred · compiled sql'));
    const unknowns = within(screen.getByRole('region', { name: 'Unknowns' }));
    assert.ok(unknowns.getByText('payments.amount'));
    assert.equal(unknowns.queryByText('x'), null);
    fireEvent.click(screen.getByRole('button', { name: 'model.shop.stg_payments#amount' }));
    assert.deepEqual(calls, [['onFocus', 'model.shop.stg_payments#amount', 'column']]);
  });
});

suite('impact panel', () => {
  const location: Location = { uri: 'file:///project/models/customers.sql', range: null };

  function panel(fields: Partial<Parameters<typeof ImpactPanel>[0]> = {}) {
    const { calls, handlers } = recorder('onClose', 'onOpen', 'onFocus');
    render(<ImpactPanel name="orders" result={null} error={null} {...handlers} {...fields} />);
    return calls;
  }

  test('says it is working until the answer comes', () => {
    panel();
    assert.match(screen.getByRole('status').textContent ?? '', /Working out the impact/);
  });

  test('an error', () => {
    panel({ error: 'Unknown node' });
    assert.equal(screen.getByRole('alert').textContent, 'Unknown node');
  });

  test('counts, then each kind with depth, confidence, columns and why', () => {
    const calls = panel({
      result: impactResult({
        counts: { model: 2, exposure: 1 },
        truncated: true,
        columnLineage: 'computing',
        nodes: [
          impacted('exposure.shop.dashboard', { kind: 'exposure', depth: 2, confidence: 'declared', owner: 'bi' }),
          impacted('model.shop.customers', { location, columns: ['model.shop.customers#lifetime_spend'], confidence: 'inferred', granularity: 'column' }),
          impacted('model.shop.finance', { reason: 'reached at model level: column lineage pending' }),
        ],
        unknowns: [{ subject: 'model.shop.finance', reference: 'macro()', reason: 'unrendered Jinja' }],
      }),
    });
    const summary = within(screen.getByRole('region', { name: 'Summary' }));
    assert.ok(summary.getByText('models'));
    assert.ok(summary.getByText('exposure'));
    assert.ok(summary.getByText(/Stopped at the depth limit/));
    assert.ok(summary.getByText(/Column lineage is still being computed/));
    // Models before exposures
    const headings = screen.getAllByRole('heading', { level: 3 }).map((h) => h.textContent);
    assert.deepEqual(headings.slice(0, 2), ['2 models', '1 exposure']);
    const models = within(screen.getByRole('region', { name: 'Impacted models' }));
    assert.ok(models.getByText('1 step · inferred · column level'));
    assert.ok(models.getByText('lifetime_spend'));
    assert.ok(models.getByText('reached at model level: column lineage pending'));
    assert.ok(screen.getByText('2 steps · declared · model level · owner bi'));
    assert.ok(within(screen.getByRole('region', { name: 'Unknowns' })).getByText('finance'));
    fireEvent.click(models.getByRole('button', { name: 'Open customers' }));
    fireEvent.click(models.getByRole('button', { name: 'finance' }));
    fireEvent.click(screen.getByRole('button', { name: 'Close impact' }));
    assert.deepEqual(calls, [['onOpen', location], ['onFocus', 'model.shop.finance'], ['onClose']]);
    assert.equal(models.queryByRole('button', { name: 'Open finance' }), null);
  });

  test('nothing downstream', () => {
    panel({ result: impactResult() });
    assert.ok(screen.getByText('Nothing downstream.'));
  });
});

suite('settings panel', () => {
  function panel(settings: ViewSettings = DEFAULT_SETTINGS) {
    const changes: ViewSettings[] = [];
    let closed = 0;
    render(<SettingsPanel settings={settings} onChange={(next) => changes.push(next)} onClose={() => { closed += 1; }} />);
    return { changes, closed: () => closed };
  }

  test('each display switch flips its setting', () => {
    const { changes } = panel();
    for (const [label, key] of [['Mini map', 'showMiniMap'], ['Zoom controls', 'showControls'], ['Legend', 'showLegend'], ['Summary', 'showInfoPanel'], ['Animate edges', 'animateEdges']] as const) {
      const toggle = screen.getByRole('switch', { name: new RegExp(`^${label}`) }) as HTMLInputElement;
      assert.equal(toggle.checked, DEFAULT_SETTINGS[key], label);
      fireEvent.click(toggle);
      assert.deepEqual(changes.pop(), { ...DEFAULT_SETTINGS, [key]: !DEFAULT_SETTINGS[key] }, label);
    }
  });

  test('spacing sliders, reset and close', () => {
    const { changes, closed } = panel({ ...DEFAULT_SETTINGS, nodeSpacing: 100 });
    assert.ok(screen.getByText('100 px'));
    fireEvent.change(screen.getByRole('slider', { name: /Node spacing/ }), { target: { value: '60' } });
    fireEvent.change(screen.getByRole('slider', { name: /Level spacing/ }), { target: { value: '200' } });
    fireEvent.click(screen.getByRole('button', { name: 'Reset to defaults' }));
    fireEvent.click(screen.getByRole('button', { name: 'Close settings' }));
    assert.deepEqual(changes, [
      { ...DEFAULT_SETTINGS, nodeSpacing: 60 }, { ...DEFAULT_SETTINGS, nodeSpacing: 100, levelSpacing: 200 }, DEFAULT_SETTINGS,
    ]);
    assert.equal(closed(), 1);
  });
});
