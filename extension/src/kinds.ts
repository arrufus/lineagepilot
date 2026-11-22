// What node kinds and tools are called where people read them: the tree views, the graph and the
// review (task 7.6). Tools are named in plain text, never with a logo (design_next_phase section
// 7.4), and a tool missing here is shown by its own name. The language server keeps the same
// table of tools, for CodeLens, in lineagepilot_core/readers/tools.py; each phase that adds a
// reader adds its tool to both. Kept free of the vscode module, so the webviews use it too.

import type { NodeKind } from './protocol';

export const KIND_LABELS: Readonly<Record<NodeKind, string>> = {
  model: 'model',
  seed: 'seed',
  snapshot: 'snapshot',
  source: 'source',
  exposure: 'exposure',
  relation: 'table',
  // Protocol 1's words for an Airflow DAG and task; the extension speaks protocol 2
  airflow_dag: 'Airflow DAG',
  airflow_task: 'Airflow task',
  pipeline: 'pipeline',
  task: 'task',
  consumer: 'consumer',
  column: 'column',
};

// A Map, so a tool named like an Object property (constructor, __proto__) is just unknown
const TOOL_LABELS: ReadonlyMap<string, string> = new Map([
  ['airflow', 'Airflow'],
  ['dagster', 'Dagster'],
  ['databricks', 'Databricks'],
  ['github_actions', 'GitHub Actions'],
  ['dlt', 'dlt'],
  ['meltano', 'Meltano'],
  ['openlineage', 'OpenLineage'],
  ['dbt_semantic', 'dbt semantic layer'],
  ['looker', 'Looker'],
  ['powerbi', 'Power BI'],
  ['lightdash', 'Lightdash'],
  ['tableau', 'Tableau'],
]);

// What a tool calls its pipelines, where that is not "pipeline": [one, many]
const PIPELINE_WORDS: ReadonlyMap<string, readonly [string, string]> = new Map([['airflow', ['DAG', 'DAGs']]]);

// What the review groups a tool's rows by, where that is not its pipelines: Dagster's are grouped
// by code location, with its jobs and schedules among them (task 8.9). As GROUP_WORDS in the core.
const GROUP_WORDS: ReadonlyMap<string, readonly [string, string]> = new Map([['dagster', ['code location', 'code locations']]]);

/** A row in the review that is one of these is labelled with what it is, in this order within its
 * pipeline, as Dagster's are: its definitions, its jobs each with its schedules, and the assets
 * downstream (task 8.9). The core keeps the same table, as ROW_WORDS in readers/tools.py. */
export const ROW_WORDS: ReadonlyMap<string, readonly [number, string, string]> = new Map([
  ['dbt_assets', [0, 'dbt asset definition', 'dbt asset definitions']],
  ['job', [1, 'job', 'jobs']],
  ['schedule', [2, 'schedule', 'schedules']],
  ['asset', [3, 'asset', 'assets']],
]);

// A subkind as people write it, where that is not the subkind with spaces for underscores
const SUBKIND_LABELS: ReadonlyMap<string, string> = new Map([['dag', 'DAG']]);

/** What has a kind, and for a pipeline, task or consumer, a tool and what the tool calls it. */
export interface Kinded {
  kind: NodeKind;
  tool?: string | null;
  subkind?: string | null;
}

/** Whether this version names a tool: one that a reader reads, or that the design plans. */
export function isKnownTool(tool: string): boolean {
  return TOOL_LABELS.has(tool);
}

/** A tool's name as people write it: ``Power BI`` for ``powerbi``; ``other`` for none. */
export function toolLabel(tool: string | null | undefined): string {
  return tool ? TOOL_LABELS.get(tool) ?? tool : 'other';
}

/** What a tool calls ``count`` pipelines: ``DAGs`` for Airflow, ``pipelines`` otherwise. */
export function pipelineWord(tool: string | null | undefined, count: number): string {
  const [one, many] = PIPELINE_WORDS.get(tool ?? '') ?? ['pipeline', 'pipelines'];
  return count === 1 ? one : many;
}

/** What the review groups ``count`` of a tool's rows by: its pipelines, or Dagster's code locations. */
export function groupWord(tool: string | null | undefined, count: number): string {
  const words = GROUP_WORDS.get(tool ?? '');
  return words ? (count === 1 ? words[0] : words[1]) : pipelineWord(tool, count);
}

/** ``task group`` for ``task_group``, ``DAG`` for ``dag``. */
export function subkindLabel(subkind: string | null | undefined): string | undefined {
  return subkind ? SUBKIND_LABELS.get(subkind) ?? subkind.replace(/_/g, ' ') : undefined;
}

/** What a node is: ``model``; ``Airflow task``, ``Airflow DAG``, ``dbt semantic layer metric``.
 * Without a subkind, what its tool calls its kind, so tasks and task groups read alike. */
export function kindLabel(node: Kinded): string {
  if (!node.tool || !['pipeline', 'task', 'consumer'].includes(node.kind)) {
    return KIND_LABELS[node.kind] ?? node.kind;
  }
  const word = subkindLabel(node.subkind)
    ?? (node.kind === 'pipeline' ? pipelineWord(node.tool, 1) : KIND_LABELS[node.kind]);
  return `${toolLabel(node.tool)} ${word}`;
}

/** One key per kind and tool, for filters and groups: ``model``, ``task:airflow``. */
export function kindKey(node: Kinded): string {
  return node.tool ? `${node.kind}:${node.tool}` : node.kind;
}

/** The tool an ID starts with: ``airflow`` for ``airflow:daily.marts``; none for a dbt ID. */
export function toolOfId(id: string): string | undefined {
  return /^([a-z][a-z0-9_]*):/.exec(id)?.[1];
}

/** An ID without the tool in front: ``daily.marts`` for ``airflow:daily.marts``. */
export function withoutTool(id: string): string {
  const tool = toolOfId(id);
  return tool ? id.slice(tool.length + 1) : id;
}
