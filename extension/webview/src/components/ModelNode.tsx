// A dbt node in the graph (task 3.2), ported from the dashboard's CapsuleNode: its kind, name,
// layer, PII indicator and column-lineage state. Colours come from the VS Code theme (task 3.4).

import { memo, type KeyboardEvent } from 'react';
import { Handle, type Node, type NodeProps, Position } from '@xyflow/react';
import { Camera, Clock, Database, FileText, Package, Plus, Shield, Table, TriangleAlert, Workflow } from 'lucide-react';

import type { GraphEdge, GraphNode } from '../../../src/protocol';
import { KIND_LABELS } from '../labels';

export interface NodeActions {
  onSelect(node: GraphNode): void;
  onOpen(node: GraphNode): void;
  onExpand(node: GraphNode): void;
  // Arrow keys follow edges to a neighbour
  onNavigate(node: GraphNode, key: string): void;
}

export type LineageNodeData = {
  node: GraphNode;
  // For a column: the edge it was derived through, to show the transformation
  incoming?: GraphEdge;
  canExpand: boolean;
  dimmed: boolean;
  horizontal: boolean;
  actions: NodeActions;
};

export type LineageFlowNode = Node<LineageNodeData, 'model' | 'column'>;

const ICONS = {
  model: Database,
  source: FileText,
  seed: Package,
  snapshot: Camera,
  exposure: Table,
  relation: Table,
  airflow_dag: Workflow,
  airflow_task: Clock,
  column: Table,
} as const;

/** A node's keys: Enter or Space selects, O opens, + expands, arrows follow edges. Tab moves
 * between nodes (React Flow). Selection is the view's state, not React Flow's, so it is handled
 * here, for the focused node's wrapper (see LineageGraph). */
export function handleNodeKey(event: KeyboardEvent, node: GraphNode, actions: NodeActions, canExpand: boolean): void {
  if (event.key === 'Enter' || event.key === ' ') {
    event.preventDefault();
    actions.onSelect(node);
  } else if (event.key === 'o' || event.key === 'O') {
    event.preventDefault();
    actions.onOpen(node);
  } else if ((event.key === '+' || event.key === '=') && canExpand) {
    event.preventDefault();
    actions.onExpand(node);
  } else if (event.key.startsWith('Arrow')) {
    event.preventDefault();
    actions.onNavigate(node, event.key);
  }
}

function ModelNode({ data, selected }: NodeProps<LineageFlowNode>) {
  const { node, actions, canExpand, dimmed, horizontal } = data;
  const Icon = ICONS[node.kind] ?? Database;
  const classes = ['lp-node', `lp-kind-${node.kind}`];
  if (node.side === 'root') classes.push('lp-node-root');
  if (selected) classes.push('lp-node-selected');
  if (dimmed) classes.push('lp-node-dimmed');
  return (
    <div className={classes.join(' ')}>
      <Handle type="target" position={horizontal ? Position.Left : Position.Top} isConnectable={false} />
      <div className="lp-node-body">
        <span className="lp-node-icon" aria-hidden="true"><Icon size={16} /></span>
        <div className="lp-node-text">
          <div className="lp-node-name" title={node.id}>{node.name}</div>
          <div className="lp-node-badges">
            <span className="lp-badge lp-badge-kind">{KIND_LABELS[node.kind]}</span>
            {node.layer && <span className="lp-badge">{node.layer}</span>}
            {node.materialization && <span className="lp-badge lp-badge-quiet">{node.materialization}</span>}
            {node.pii && (
              <span className="lp-badge lp-badge-pii" title="PII indicators (not verified exposure)">
                <Shield size={11} aria-hidden="true" /> PII
              </span>
            )}
            {node.columnLineage === 'unknown' && (
              <span className="lp-badge lp-badge-warn" title={`Column lineage unknown: ${node.columnLineageReason ?? ''}`}>
                <TriangleAlert size={11} aria-hidden="true" /> columns
              </span>
            )}
          </div>
        </div>
        {canExpand && (
          <button type="button" className="lp-expand nodrag" aria-label={`Show more ${node.side} of ${node.name}`}
            title={`Show more ${node.side}`} tabIndex={-1}
            onClick={(event) => { event.stopPropagation(); actions.onExpand(node); }}>
            <Plus size={14} aria-hidden="true" />
          </button>
        )}
      </div>
      <Handle type="source" position={horizontal ? Position.Right : Position.Bottom} isConnectable={false} />
    </div>
  );
}

export default memo(ModelNode);
