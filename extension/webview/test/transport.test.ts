// The transport (task 3.1): requests and their answers matched by ID, errors with their codes,
// and everything malformed dropped.
import { strict as assert } from 'assert';

import { DEFAULT_SETTINGS } from '../../src/webviewMessages';
import { RequestError, Transport } from '../src/transport';
import { FakeHost, graph, node, nodeResult, ordersGraph } from './fixtures';

function silenced<T>(run: () => T): { result: T; warnings: unknown[][] } {
  const warnings: unknown[][] = [];
  const original = console.warn;
  console.warn = (...args: unknown[]) => { warnings.push(args); };
  try {
    return { result: run(), warnings };
  } finally {
    console.warn = original;
  }
}

suite('transport', () => {
  test('each request has its own ID, and answers resolve the request they name', async () => {
    const host = new FakeHost();
    const first = host.transport.graph({ root: 'model.shop.orders', direction: 'both', depth: 2, granularity: 'model' });
    const second = host.transport.node({ id: 'model.shop.orders' });
    assert.deepEqual(host.posted.map((m) => m.type === 'graph' || m.type === 'node' ? [m.type, m.id] : m.type), [['graph', 1], ['node', 2]]);
    // Answered out of order
    host.send({ type: 'nodeResult', id: 2, result: nodeResult(node('model.shop.orders'), { upstream: 1 }) });
    host.send({ type: 'graphResult', id: 1, result: ordersGraph() });
    assert.equal((await second).upstream, 1);
    assert.equal((await first).nodes.length, 4);
  });

  test('an error answer rejects with its message and code', async () => {
    const host = new FakeHost();
    const pending = host.transport.impact({ targets: ['model.shop.nope'] });
    host.send({ type: 'error', id: 1, message: 'Unknown node model.shop.nope', code: -32001 });
    await assert.rejects(pending, (error: unknown) => error instanceof RequestError
      && error.message === 'Unknown node model.shop.nope' && error.code === -32001);
  });

  test('an answer of the wrong type rejects', async () => {
    const host = new FakeHost();
    const pending = host.transport.node({ id: 'model.shop.orders' });
    host.send({ type: 'graphResult', id: 1, result: ordersGraph() });
    await assert.rejects(pending, /expected nodeResult, received graphResult/);
  });

  test('malformed messages and answers to nothing are dropped', async () => {
    const host = new FakeHost();
    const pending = host.transport.node({ id: 'model.shop.orders' });
    const { warnings } = silenced(() => {
      host.sendRaw({ type: 'nodeResult', id: 1, result: { node: 'not a node' } });
      host.sendRaw('<script>alert(1)</script>');
      host.sendRaw({ type: 'run', command: 'rm -rf /' });
    });
    assert.equal(warnings.length, 3);
    host.send({ type: 'graphResult', id: 99, result: graph('model.shop.a', [node('model.shop.a')], []) });
    // Still waiting for its own answer
    host.send({ type: 'nodeResult', id: 1, result: nodeResult(node('model.shop.orders')) });
    assert.equal((await pending).node.id, 'model.shop.orders');
  });

  test('notices go to every listener until it unsubscribes', () => {
    const host = new FakeHost();
    const seen: string[] = [];
    const stop = host.transport.onNotice((notice) => seen.push(`a:${notice.type}`));
    host.transport.onNotice((notice) => seen.push(`b:${notice.type}`));
    host.send({ type: 'indexUpdated', generation: 2, ready: true });
    stop();
    host.send({ type: 'focus', focus: { id: 'model.shop.orders', granularity: 'model' } });
    assert.deepEqual(seen, ['a:indexUpdated', 'b:indexUpdated', 'b:focus']);
  });

  test('ready, openFile and saveSettings post their messages', () => {
    const host = new FakeHost();
    const location = { uri: 'file:///project/models/orders.sql', range: null };
    host.transport.ready();
    host.transport.openFile(location);
    host.transport.openFile(location, true);
    host.transport.saveSettings(DEFAULT_SETTINGS);
    assert.deepEqual(host.posted, [
      { type: 'ready' }, { type: 'openFile', location, preserveFocus: false }, { type: 'openFile', location, preserveFocus: true },
      { type: 'saveSettings', settings: DEFAULT_SETTINGS },
    ]);
  });

  test('webview state: what is kept comes back, only if valid', () => {
    const host = new FakeHost();
    assert.equal(host.transport.restoreState(), null);
    const state = { focus: { id: 'model.shop.orders', granularity: 'column' as const }, depth: 4, direction: 'downstream' as const };
    host.transport.saveState(state);
    assert.deepEqual(host.transport.restoreState(), state);
    host.state = { ...state, depth: 0 };
    assert.equal(host.transport.restoreState(), null);
  });

  test('listens to the window by default', async () => {
    const posted: unknown[] = [];
    const transport = new Transport({ postMessage: (m) => posted.push(m), getState: () => undefined, setState: () => {} });
    const pending = transport.node({ id: 'model.shop.orders' });
    window.dispatchEvent(new window.MessageEvent('message', {
      data: { type: 'nodeResult', id: 1, result: nodeResult(node('model.shop.orders')) },
    }));
    assert.equal((await pending).node.name, 'orders');
    assert.equal(posted.length, 1);
  });
});
