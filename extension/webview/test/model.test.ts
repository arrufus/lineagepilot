// The view's graph (task 3.3): built from lineage/graph, merged on expansion, filtered.
import { strict as assert } from 'assert';

import {
  canExpand, edgeKey, fromResult, kindsIn, matches, merge, neighbours, NO_FILTERS, visible,
} from '../src/graph/model';
import { edge, graph, node, ordersGraph } from './fixtures';

suite('graph model', () => {
  test('built from a lineage/graph result', () => {
    const view = fromResult(ordersGraph({ truncated: true }), 2);
    assert.equal(view.rootId, 'model.shop.orders');
    assert.equal(view.nodes.size, 4);
    assert.equal(view.edges.size, 3);
    assert.ok(view.edges.has(edgeKey(edge('model.shop.stg_orders', 'model.shop.orders'))));
    assert.equal(view.truncated, true);
    assert.equal(view.depth, 2);
    assert.equal(view.expanded.size, 0);
  });

  test('only nodes at the depth asked for can be expanded, once', () => {
    const view = fromResult(ordersGraph(), 2);
    const source = view.nodes.get('source.shop.raw.orders')!;
    assert.equal(canExpand(view, source), true);
    assert.equal(canExpand(view, view.nodes.get('model.shop.stg_orders')!), false);
    assert.equal(canExpand(view, view.nodes.get('model.shop.orders')!), false);
    const expanded = merge(view, source.id, graph(source.id, [node(source.id, { kind: 'source' })], []));
    assert.equal(canExpand(expanded, expanded.nodes.get(source.id)!), false);
  });

  test('merging an expansion places new nodes beyond the one expanded, on its side', () => {
    const view = fromResult(ordersGraph({ unknowns: [{ subject: 'model.shop.orders', reference: 'x', reason: 'unresolved' }] }), 2);
    const result = graph('source.shop.raw.orders', [
      node('source.shop.raw.orders', { kind: 'source' }),
      node('seed.shop.raw_seed', { kind: 'seed', side: 'upstream', depth: 1 }),
      // Already known: keeps its place
      node('model.shop.stg_orders', { side: 'downstream', depth: 1 }),
    ], [edge('seed.shop.raw_seed', 'source.shop.raw.orders'), edge('source.shop.raw.orders', 'model.shop.stg_orders')], {
      unknowns: [
        { subject: 'model.shop.orders', reference: 'x', reason: 'unresolved' },
        { subject: 'seed.shop.raw_seed', reference: 'y', reason: 'no file' },
      ],
    });
    const merged = merge(view, 'source.shop.raw.orders', result);
    assert.deepEqual(merged.nodes.get('seed.shop.raw_seed')!.side, 'upstream');
    assert.equal(merged.nodes.get('seed.shop.raw_seed')!.depth, 3);
    assert.equal(merged.nodes.get('model.shop.stg_orders')!.side, 'upstream');
    assert.equal(merged.nodes.get('model.shop.stg_orders')!.depth, 1);
    assert.equal(merged.edges.size, 4);
    assert.equal(merged.unknowns.length, 2);
    assert.ok(merged.expanded.has('source.shop.raw.orders'));
    // The original is unchanged
    assert.equal(view.nodes.size, 4);
  });

  test('expanding from the root keeps the sides the server gives', () => {
    const view = fromResult(graph('model.shop.a', [node('model.shop.a')], []), 1);
    const merged = merge(view, 'model.shop.a', graph('model.shop.a', [
      node('model.shop.a'), node('model.shop.b', { side: 'downstream', depth: 1 }),
    ], [edge('model.shop.a', 'model.shop.b')]));
    assert.equal(merged.nodes.get('model.shop.b')!.side, 'downstream');
  });

  test('filters hide kinds, never the root, and the edges of hidden nodes', () => {
    const view = fromResult(ordersGraph(), 2);
    assert.equal(visible(view, NO_FILTERS.hiddenKinds).nodes.length, 4);
    const withoutSources = visible(view, new Set(['source']));
    assert.deepEqual(withoutSources.nodes.map((n) => n.id).sort(), ['model.shop.customers_report', 'model.shop.orders', 'model.shop.stg_orders']);
    assert.equal(withoutSources.edges.length, 2);
    const withoutModels = visible(view, new Set(['model']));
    assert.deepEqual(withoutModels.nodes.map((n) => n.id).sort(), ['model.shop.orders', 'source.shop.raw.orders']);
    assert.equal(withoutModels.edges.length, 0);
  });

  test('search matches names and IDs, ignoring case and spaces', () => {
    const orders = node('model.shop.orders');
    assert.equal(matches(orders, ''), true);
    assert.equal(matches(orders, '  ORD '), true);
    assert.equal(matches(orders, 'shop.ord'), true);
    assert.equal(matches(orders, 'customers'), false);
  });

  test('kinds and neighbours', () => {
    const view = fromResult(ordersGraph(), 2);
    assert.deepEqual(kindsIn(view), ['model', 'source']);
    const { upstream, downstream } = neighbours(view, 'model.shop.orders');
    assert.deepEqual(upstream.map((e) => e.upstream), ['model.shop.stg_orders']);
    assert.deepEqual(downstream.map((e) => e.downstream), ['model.shop.customers_report']);
  });
});
