// What screen readers announce for nodes and edges (task 3.4), and the message contract between
// the extension and the webview (task 3.1).
import { strict as assert } from 'assert';

import {
  groupWord, kindKey, kindLabel, pipelineWord, subkindLabel, toolLabel, toolOfId, withoutTool,
} from '../../src/kinds';
import { DEFAULT_SETTINGS, HostMessage, ViewSettings, WebviewMessage } from '../../src/webviewMessages';
import { edgeLabel, nodeLabel, plural, transformationLabel } from '../src/labels';
import { edge, node, ordersGraph } from './fixtures';

suite('labels', () => {
  test('a node says what it is, where it is, and what it links to', () => {
    assert.equal(nodeLabel(node('model.shop.orders'), 1, 2), 'model orders, the focus of this view, 1 upstream, 2 downstream');
    assert.equal(
      nodeLabel(node('source.shop.raw.orders', { kind: 'source', name: 'raw.orders', side: 'upstream', depth: 2, pii: true }), 0, 1),
      'source raw.orders, upstream, 2 steps away, 0 upstream, 1 downstream, has PII indicators');
    assert.equal(
      nodeLabel(node('model.shop.x', { side: 'downstream', depth: 1, columnLineage: 'unknown' }), 1, 0),
      'model x, downstream, 1 step away, 1 upstream, 0 downstream, column lineage unknown');
    assert.equal(nodeLabel(node('exposure.shop.d', { kind: 'exposure', name: 'd' }), 0, 0).startsWith('exposure d'), true);
  });

  test('an edge says how the link is known', () => {
    assert.equal(edgeLabel(edge('a#x', 'b#y', { transformation: 'window_function', confidence: 'inferred' })), 'window function · inferred');
    assert.equal(edgeLabel(edge('a', 'b')), 'declared');
    assert.equal(transformationLabel(null), null);
    assert.equal(transformationLabel(''), null);
  });

  test("a tool's node is announced with its tool", () => {
    const task = node('airflow:daily.marts', { kind: 'task', name: 'marts', tool: 'airflow', subkind: 'task_group', side: 'downstream', depth: 1 });
    assert.equal(nodeLabel(task, 1, 0), 'Airflow task group marts, downstream, 1 step away, 1 upstream, 0 downstream');
  });

  test('plural', () => {
    assert.equal(plural(1, 'node'), '1 node');
    assert.equal(plural(0, 'node'), '0 nodes');
    assert.equal(plural(2, 'query', 'queries'), '2 queries');
  });
});

suite('kinds and tools (task 7.6)', () => {
  test('tools are named in plain text, and one this version does not know by its own name', () => {
    assert.deepEqual(['airflow', 'powerbi', 'dbt_semantic', 'github_actions'].map(toolLabel), ['Airflow', 'Power BI', 'dbt semantic layer', 'GitHub Actions']);
    assert.equal(toolLabel('acme_bi'), 'acme_bi');
    assert.equal(toolLabel('constructor'), 'constructor');
    assert.equal(toolLabel(null), 'other');
  });

  test("what a tool calls its pipelines, and a subkind's words", () => {
    assert.deepEqual([pipelineWord('airflow', 1), pipelineWord('airflow', 2), pipelineWord('dagster', 1), pipelineWord(undefined, 0)],
      ['DAG', 'DAGs', 'pipeline', 'pipelines']);
    assert.deepEqual([subkindLabel('dag'), subkindLabel('task_group'), subkindLabel('saved_query'), subkindLabel(null)],
      ['DAG', 'task group', 'saved query', undefined]);
  });

  test('a kind, with its tool and what the tool calls it', () => {
    assert.equal(kindLabel({ kind: 'model' }), 'model');
    assert.equal(kindLabel({ kind: 'relation', tool: null }), 'table');
    assert.equal(kindLabel({ kind: 'task', tool: 'airflow', subkind: 'task' }), 'Airflow task');
    assert.equal(kindLabel({ kind: 'task', tool: 'airflow' }), 'Airflow task');
    assert.equal(kindLabel({ kind: 'pipeline', tool: 'airflow', subkind: 'dag' }), 'Airflow DAG');
    assert.equal(kindLabel({ kind: 'pipeline', tool: 'airflow' }), 'Airflow DAG');
    assert.equal(kindLabel({ kind: 'pipeline', tool: 'dagster' }), 'Dagster pipeline');
    // The review groups Dagster's rows by code location (task 8.9)
    assert.deepEqual([groupWord('dagster', 1), groupWord('dagster', 2), groupWord('airflow', 1)], ['code location', 'code locations', 'DAG']);
    assert.equal(kindLabel({ kind: 'pipeline', tool: 'dagster', subkind: 'job' }), 'Dagster job');
    assert.equal(kindLabel({ kind: 'consumer', tool: 'dbt_semantic', subkind: 'metric' }), 'dbt semantic layer metric');
    assert.equal(kindLabel({ kind: 'consumer', tool: 'looker' }), 'Looker consumer');
    assert.equal(kindLabel({ kind: 'task', tool: null }), 'task');
    assert.equal(kindLabel({ kind: 'airflow_task' }), 'Airflow task');
  });

  test('keys and IDs', () => {
    assert.deepEqual([kindKey({ kind: 'model', tool: null }), kindKey({ kind: 'task', tool: 'airflow' })], ['model', 'task:airflow']);
    assert.deepEqual(['airflow:daily.marts', 'dbt_semantic:revenue', 'model.shop.orders', 'dags/a.py', 'model.shop.o#x'].map(toolOfId),
      ['airflow', 'dbt_semantic', undefined, undefined, undefined]);
    assert.deepEqual(['looker:view.orders', 'model.shop.orders'].map(withoutTool), ['view.orders', 'model.shop.orders']);
  });
});

suite('webview messages', () => {
  test('the webview may ask only for what the contract lists', () => {
    const graph = { type: 'graph', id: 1, params: { root: 'model.shop.orders', direction: 'both', depth: 2, granularity: 'model' } };
    assert.equal(WebviewMessage.safeParse(graph).success, true);
    assert.equal(WebviewMessage.safeParse({ type: 'ready' }).success, true);
    const location = { uri: 'file:///project/models/orders.sql', range: null };
    assert.equal(WebviewMessage.safeParse({ type: 'openFile', location, preserveFocus: true }).success, true);
    assert.equal(WebviewMessage.safeParse({ type: 'openFile', location }).success, false);
    for (const bad of [
      { type: 'exec', command: 'dbt run' },
      { ...graph, extra: true },
      { ...graph, id: 0 },
      { ...graph, params: { ...graph.params, depth: 11 } },
      { type: 'openFile' },
      { type: 'openFile', location: { uri: '', range: null } },
      { type: 'saveSettings', settings: { ...DEFAULT_SETTINGS, nodeSpacing: 5 } },
      'ready',
      null,
    ]) {
      assert.equal(WebviewMessage.safeParse(bad).success, false, JSON.stringify(bad));
    }
  });

  test('the extension sends only what the contract lists', () => {
    assert.equal(HostMessage.safeParse({ type: 'graphResult', id: 3, result: ordersGraph() }).success, true);
    assert.equal(HostMessage.safeParse({ type: 'error', id: 3, message: 'no', code: null }).success, true);
    assert.equal(HostMessage.safeParse({ type: 'error', id: 3, message: 'no', code: 1.5 }).success, false);
    assert.equal(HostMessage.safeParse({ type: 'graphResult', id: 3, result: { ...ordersGraph(), nodes: [{ id: 'x' }] } }).success, false);
    assert.equal(HostMessage.safeParse({
      type: 'init', project: { name: 'shop', root: '/p' }, focus: null, settings: DEFAULT_SETTINGS, generation: 0,
    }).success, true);
  });

  test('settings stay within what the panel offers', () => {
    assert.equal(ViewSettings.safeParse(DEFAULT_SETTINGS).success, true);
    assert.equal(ViewSettings.safeParse({ ...DEFAULT_SETTINGS, layout: 'RL' }).success, false);
    assert.equal(ViewSettings.safeParse({ ...DEFAULT_SETTINGS, levelSpacing: 601 }).success, false);
    assert.equal(ViewSettings.safeParse({ ...DEFAULT_SETTINGS, theme: 'dark' }).success, false);
  });
});
