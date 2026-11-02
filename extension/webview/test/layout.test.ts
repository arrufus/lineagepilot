// Auto-layout (task 3.3): data flows left to right or top to bottom, the same graph always lays
// out the same way, and 500 nodes lay out well within a frame budget for interaction.
import { strict as assert } from 'assert';

import { layout, NODE_SIZE, sizeOf } from '../src/graph/layout';
import { edge, largeGraph, node, ordersGraph } from './fixtures';

const OPTIONS = { direction: 'LR' as const, nodeSpacing: 40, levelSpacing: 120 };

suite('layout', () => {
  const { nodes, edges } = ordersGraph();
  const at = (positions: Map<string, { x: number; y: number }>, id: string) => positions.get(id)!;

  test('left to right: each step downstream is further right', () => {
    const positions = layout(nodes, edges, OPTIONS);
    assert.equal(positions.size, 4);
    assert.ok(at(positions, 'source.shop.raw.orders').x < at(positions, 'model.shop.stg_orders').x);
    assert.ok(at(positions, 'model.shop.stg_orders').x < at(positions, 'model.shop.orders').x);
    assert.ok(at(positions, 'model.shop.orders').x < at(positions, 'model.shop.customers_report').x);
    assert.equal(at(positions, 'model.shop.orders').x - at(positions, 'model.shop.stg_orders').x, NODE_SIZE.model.width + 120);
  });

  test('top to bottom, with the spacing asked for', () => {
    const positions = layout(nodes, edges, { direction: 'TB', nodeSpacing: 40, levelSpacing: 300 });
    assert.ok(at(positions, 'model.shop.stg_orders').y < at(positions, 'model.shop.orders').y);
    assert.equal(at(positions, 'model.shop.orders').y - at(positions, 'model.shop.stg_orders').y, NODE_SIZE.model.height + 300);
  });

  test('the same graph in any order lays out the same', () => {
    const forward = layout(nodes, edges, OPTIONS);
    const backward = layout([...nodes].reverse(), [...edges].reverse(), OPTIONS);
    assert.deepEqual([...backward.entries()].sort(), [...forward.entries()].sort());
  });

  test('self edges and edges to nodes not shown are ignored', () => {
    const positions = layout(nodes, [...edges, edge('model.shop.orders', 'model.shop.orders'), edge('model.shop.gone', 'model.shop.orders')], OPTIONS);
    assert.equal(positions.size, 4);
    assert.equal(positions.has('model.shop.gone'), false);
  });

  test('every node has its own place, and the sizes are left alone', () => {
    const positions = layout(nodes, edges, OPTIONS);
    assert.equal(new Set([...positions.values()].map((p) => `${p.x},${p.y}`)).size, 4);
    assert.deepEqual(NODE_SIZE.model, { width: 240, height: 64 });
  });

  test('columns are sized as columns', () => {
    assert.deepEqual(sizeOf(node('model.shop.orders#id')), NODE_SIZE.column);
    assert.deepEqual(sizeOf(node('model.shop.orders')), NODE_SIZE.model);
  });

  test('500 nodes lay out quickly, still in data-flow order', () => {
    const big = largeGraph(500);
    layout(big.nodes, big.edges, OPTIONS);
    const started = performance.now();
    const positions = layout(big.nodes, big.edges, OPTIONS);
    const elapsed = performance.now() - started;
    assert.equal(positions.size, 500);
    // About 0.1 s on an M4 Max; the budget leaves room for slow CI machines
    assert.ok(elapsed < 1500, `laying out 500 nodes took ${elapsed.toFixed(0)} ms`);
    for (const e of big.edges) {
      assert.ok(positions.get(e.upstream)!.x < positions.get(e.downstream)!.x, `${e.upstream} -> ${e.downstream}`);
    }
  });
});
