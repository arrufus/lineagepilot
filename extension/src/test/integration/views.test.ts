// The Pipelines and Consumers views (task 7.6), on answers a language server could give: several
// tools, a reader that failed or was not analysed, unknowns, and consumers that read columns.
// A fake server answers, so each case is here whatever the workspace holds.
import { strict as assert } from 'assert';
import * as vscode from 'vscode';

import type {
  Consumer, ConsumersResult, GraphNode, NodesForFileResult, Pipeline, PipelinesResult, PipelineTask, ReaderStatus,
} from '../../protocol';
import type { ProjectServer } from '../../server';
import { ConsumersTree } from '../../views/consumersTree';
import { PipelinesTree, type ToolItem } from '../../views/pipelinesTree';

const FILE = vscode.Uri.file('/w/shop/models/orders.sql');
const ORDERS = 'model.shop.orders';

function graphNode(id: string, fields: Partial<GraphNode> = {}): GraphNode {
  return {
    id, kind: 'model', name: id.split('.').pop()!, side: 'root', depth: 0, parent: null, location: null, layer: null,
    materialization: null, owner: null, description: null, columnLineage: null, columnLineageReason: null, pii: false,
    tool: null, subkind: null, ...fields,
  };
}

function reader(tool: string, fields: Partial<ReaderStatus> = {}, summary: Partial<ReaderStatus['summary']> = {}): ReaderStatus {
  return {
    tool, stage: 'done', generation: 1, message: null,
    summary: {
      tool, analysed: true, reason: null, paths: ['**/dags'], files: 2, pipelines: 1, tasks: 2, consumers: 0, links: 2,
      unknowns: 0, opaqueTasks: 0, problems: [], ...summary,
    },
    ...fields,
  };
}

function task(id: string, fields: Partial<PipelineTask> = {}): PipelineTask {
  return {
    id, kind: 'task', tool: 'airflow', subkind: 'task', name: id.split('.').pop()!, operator: 'BashOperator', owner: null,
    how: ['dbt run'], nodes: [ORDERS], derivedTasks: [], confidence: 'heuristic', origins: ['dag_static'], notes: [],
    location: null, ...fields,
  };
}

function pipeline(id: string, tasks: PipelineTask[], fields: Partial<Pipeline> = {}): Pipeline {
  const [tool, name] = id.split(':');
  return { id, tool, subkind: null, name, owner: null, location: null, tasks, ...fields };
}

/** A server that answers what it is given, and records what it was asked. */
function fakeServer(answers: { nodes?: GraphNode[]; pipelines?: PipelinesResult; consumers?: ConsumersResult | Error }) {
  const asked: [string, unknown][] = [];
  const server = {
    name: 'shop',
    async request(method: string, params: unknown): Promise<unknown> {
      asked.push([method, params]);
      if (method === 'lineage/nodesForFile') {
        return { nodes: answers.nodes ?? [] } satisfies NodesForFileResult;
      }
      const answer = method === 'lineage/pipelines' ? answers.pipelines : answers.consumers;
      if (answer instanceof Error || answer === undefined) {
        throw answer ?? new Error(`not answered: ${method}`);
      }
      return answer;
    },
  } as unknown as ProjectServer;
  return { server, asked };
}

suite('Pipelines view', () => {
  const result: PipelinesResult = {
    readers: [
      reader('dagster', {}, { paths: ['defs.yaml'] }),
      reader('airflow', {}, { opaqueTasks: 3, problems: ['dags/big.py is larger than the size limit'] }),
    ],
    pipelines: [
      pipeline('airflow:daily', [task('airflow:daily.marts', { subkind: 'task_group', derivedTasks: ['marts.orders.run'] })],
        { subkind: 'dag', owner: 'etl' }),
      pipeline('airflow:cosmos', [task('airflow:cosmos', { kind: 'pipeline', subkind: 'dag', how: ['dbt, through Cosmos'] })]),
      pipeline('dagster:nightly', [task('dagster:nightly.load', { tool: 'dagster', subkind: 'asset', operator: null })]),
    ],
    unknowns: [
      { subject: 'airflow:backfill', reference: 'not literal', reason: 'a loop' },
      { subject: 'dags/factory.py', reference: 'not literal', reason: 'a file of generated DAGs' },
    ],
  };

  test('each tool is a group, by name, with what its reader read', async () => {
    const { server, asked } = fakeServer({ nodes: [graphNode(ORDERS)], pipelines: result });
    const tree = new PipelinesTree(() => server, () => [server]);
    await tree.showFile(FILE);
    assert.deepEqual(asked.map(([method]) => method), ['lineage/nodesForFile', 'lineage/pipelines']);
    assert.deepEqual(asked[1][1], { nodes: [ORDERS] });
    const tools = tree.items.filter((item): item is ToolItem => item.type === 'tool');
    assert.deepEqual(tools.map((item) => tree.getTreeItem(item).label), ['Airflow', 'Dagster']);
    const airflow = tree.getTreeItem(tools[0]);
    assert.equal(airflow.description, '2 DAGs · 1 task');
    assert.match((airflow.tooltip as vscode.MarkdownString).value, /3 tasks could not be read/);
    assert.match((airflow.tooltip as vscode.MarkdownString).value, /larger than the size limit/);
    assert.equal(tree.getTreeItem(tools[1]).description, '1 pipeline · 1 task');
    // An unknown names its tool; one that names none goes in a group of its own
    const loose = tree.items.at(-1)!;
    assert.equal(tree.getTreeItem(loose).label, 'Could not be read (1)');
    assert.equal(tree.getParent(loose), undefined);
  });

  test("a tool's pipelines, tasks and unknowns, and a pipeline that is one link", async () => {
    const { server } = fakeServer({ pipelines: result });
    const tree = new PipelinesTree(() => undefined, () => [server]);
    await tree.showFile(undefined);
    const [airflow] = tree.items;
    const children = tree.getChildren(airflow);
    assert.deepEqual(children.map((item) => tree.getTreeItem(item).label), ['daily', 'cosmos', 'Could not be read (1)']);
    assert.equal(tree.getTreeItem(children[0]).description, 'etl');
    const [marts] = tree.getChildren(children[0]);
    assert.equal(tree.getParent(marts), children[0]);
    const mart = tree.getTreeItem(marts);
    assert.equal(mart.description, 'dbt run · heuristic');
    assert.match((mart.tooltip as vscode.MarkdownString).value, /Tasks Cosmos should render: marts\\.orders\\.run/);
    assert.equal(tree.getTreeItem(tree.getChildren(children[1])[0]).label, 'The whole DAG');
    const [unknown] = tree.getChildren(children[2]);
    assert.equal(tree.getTreeItem(unknown).label, 'backfill');
    assert.equal(tree.getParent(children[2]), airflow);
  });

  test('a reader not analysed, or failed, says so under its tool', async () => {
    const { server } = fakeServer({
      pipelines: {
        readers: [
          reader('airflow', {}, { analysed: false, reason: 'no folder matched **/dags' }),
          reader('dagster', { stage: 'failed', message: 'the reader ran out of time' }),
        ],
        pipelines: [],
        unknowns: [{ subject: 'dags/a.py', reference: 'x', reason: 'y' }],
      },
    });
    const tree = new PipelinesTree(() => undefined, () => [server]);
    await tree.showFile(undefined);
    const [airflow, dagster, loose] = tree.items;
    assert.equal(tree.getTreeItem(airflow).description, 'not analysed');
    assert.equal(tree.getTreeItem(tree.getChildren(airflow)[0]).label, 'Not analysed: no folder matched **/dags');
    assert.equal(tree.getTreeItem(dagster).description, 'failed');
    assert.equal(tree.getTreeItem(tree.getChildren(dagster)[0]).label, 'Reading the pipeline files failed: the reader ran out of time');
    assert.equal(tree.getTreeItem(loose).label, 'Could not be read (1)');
  });

  test('while a reader reads, and with no reader or no server', async () => {
    const reading = fakeServer({ pipelines: { readers: [reader('airflow', { stage: 'scanning' })], pipelines: [], unknowns: [] } });
    const tree = new PipelinesTree(() => undefined, () => [reading.server]);
    await tree.showFile(undefined);
    assert.equal(tree.getTreeItem(tree.items[0]).description, 'reading…');
    const none = fakeServer({ pipelines: { readers: [], pipelines: [], unknowns: [] } });
    const empty = new PipelinesTree(() => undefined, () => [none.server]);
    await empty.showFile(undefined);
    assert.equal(empty.getTreeItem(empty.items[0]).label, 'No reader of pipelines is turned on.');
    const nothing = new PipelinesTree(() => undefined, () => []);
    await nothing.showFile(undefined);
    assert.deepEqual(nothing.items, []);
  });

  test('a failed request is said in the view', async () => {
    const { server } = fakeServer({});
    const tree = new PipelinesTree(() => undefined, () => [server]);
    await tree.showFile(undefined);
    assert.equal(tree.getTreeItem(tree.items[0]).label, 'not answered: lineage/pipelines');
  });
});

suite('Consumers view', () => {
  const metric: Consumer = {
    id: 'metric.shop.revenue', tool: 'dbt_semantic', subkind: 'metric', name: 'revenue', owner: 'finance', location: null,
    reads: [{ node: ORDERS, columns: [], confidence: 'declared', origins: ['manifest'] }],
  };
  const view: Consumer = {
    id: 'looker:view.orders', tool: 'looker', subkind: 'view', name: 'orders', owner: null,
    location: { uri: 'file:///w/views/orders.view.lkml', range: null },
    reads: [
      { node: ORDERS, columns: ['amount', 'id'], confidence: 'heuristic', origins: ['annotation'] },
      { node: 'model.shop.items', columns: [], confidence: 'declared', origins: ['annotation'] },
    ],
  };

  test("what reads the file's dbt nodes, by tool, and how", async () => {
    const { server, asked } = fakeServer({
      nodes: [graphNode(ORDERS), graphNode('model.shop.items'), graphNode('airflow:d.t', { kind: 'task', tool: 'airflow' })],
      consumers: { consumers: [view, metric] },
    });
    const tree = new ConsumersTree(() => server);
    await tree.showFile(FILE);
    // Only dbt's nodes are asked about
    assert.deepEqual(asked[1], ['lineage/consumers', { nodes: [ORDERS, 'model.shop.items'] }]);
    const [semantic, looker] = tree.items;
    assert.deepEqual([semantic, looker].map((item) => tree.getTreeItem(item).label), ['dbt semantic layer', 'Looker']);
    assert.equal(tree.getTreeItem(looker).description, '1 consumer');
    const [orders] = tree.getChildren(looker);
    const item = tree.getTreeItem(orders);
    assert.equal(item.label, 'orders');
    assert.equal(item.description, 'view');
    assert.deepEqual(item.command?.arguments, [view.location]);
    const reads = tree.getChildren(orders);
    assert.deepEqual(reads.map((read) => [tree.getTreeItem(read).label, tree.getTreeItem(read).description]), [
      ['orders', 'amount, id · heuristic'],
      ['items', 'the whole node · declared'],
    ]);
    assert.equal(tree.getParent(reads[0]), orders);
    assert.equal(tree.getParent(orders), looker);
    const [revenue] = tree.getChildren(semantic);
    assert.equal(tree.getTreeItem(revenue).description, 'metric · finance');
    assert.match((tree.getTreeItem(revenue).tooltip as vscode.MarkdownString).value, /dbt semantic layer/);
  });

  test('nothing reads it, no dbt node in the file, no project, and a failure', async () => {
    const nobody = fakeServer({ nodes: [graphNode(ORDERS)], consumers: { consumers: [] } });
    const tree = new ConsumersTree(() => nobody.server);
    await tree.showFile(FILE);
    assert.equal(tree.getTreeItem(tree.items[0]).label, 'Nothing outside dbt is known to read this.');
    const dagFile = fakeServer({ nodes: [graphNode('airflow:d.t', { kind: 'task', tool: 'airflow' })] });
    const forDag = new ConsumersTree(() => dagFile.server);
    await forDag.showFile(FILE);
    assert.deepEqual(forDag.items, []);
    assert.deepEqual(dagFile.asked.map(([method]) => method), ['lineage/nodesForFile']);
    const outside = new ConsumersTree(() => undefined);
    await outside.showFile(FILE);
    assert.deepEqual(outside.items, []);
    const failing = fakeServer({ nodes: [graphNode(ORDERS)], consumers: new Error('the language server is not running') });
    const failed = new ConsumersTree(() => failing.server);
    await failed.showFile(FILE);
    assert.equal(failed.getTreeItem(failed.items[0]).label, 'the language server is not running');
  });
});
