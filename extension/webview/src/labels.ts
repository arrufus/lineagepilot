// Words for what the graph shows: node kinds, confidence, transformations, and the accessible
// labels screen readers announce for each node (task 3.4).

import type { Confidence, GraphEdge, GraphNode, NodeKind } from '../../src/protocol';

export const KIND_LABELS: Record<NodeKind, string> = {
  model: 'model',
  seed: 'seed',
  snapshot: 'snapshot',
  source: 'source',
  exposure: 'exposure',
  relation: 'table',
  airflow_dag: 'Airflow DAG',
  airflow_task: 'Airflow task',
  column: 'column',
};

export const CONFIDENCE_LABELS: Record<Confidence, string> = {
  declared: 'declared',
  inferred: 'inferred',
  heuristic: 'heuristic',
};

/** sqlglot's expression kinds, as the column view shows them. */
export function transformationLabel(transformation: string | null | undefined): string | null {
  if (!transformation) {
    return null;
  }
  return transformation.replace(/_/g, ' ');
}

export function plural(count: number, one: string, many = `${one}s`): string {
  return `${count} ${count === 1 ? one : many}`;
}

/** What a screen reader announces for a node. */
export function nodeLabel(node: GraphNode, upstream: number, downstream: number): string {
  const parts = [`${KIND_LABELS[node.kind]} ${node.name}`];
  if (node.side === 'root') {
    parts.push('the focus of this view');
  } else {
    parts.push(`${node.side}, ${plural(node.depth, 'step')} away`);
  }
  parts.push(`${upstream} upstream, ${downstream} downstream`);
  if (node.pii) {
    parts.push('has PII indicators');
  }
  if (node.columnLineage === 'unknown') {
    parts.push('column lineage unknown');
  }
  return parts.join(', ');
}

export function edgeLabel(edge: GraphEdge): string {
  const how = transformationLabel(edge.transformation);
  return [how, CONFIDENCE_LABELS[edge.confidence]].filter(Boolean).join(' · ');
}
