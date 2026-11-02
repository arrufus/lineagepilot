// A column in the column-level graph (task 3.2), ported from the dashboard's ColumnNode: the
// column, its model, and the transformation it was derived through, with that edge's
// confidence (declared, inferred, heuristic) instead of the old percentage.

import { memo } from 'react';
import { Handle, type NodeProps, Position } from '@xyflow/react';
import { Database, Plus, Shield, Table2 } from 'lucide-react';

import { CONFIDENCE_LABELS, transformationLabel } from '../labels';
import type { LineageFlowNode } from './ModelNode';

function parentName(id: string, parent: string | null): string {
  return (parent ?? id.split('#')[0]).split('.').pop() ?? '';
}

function ColumnNode({ data, selected }: NodeProps<LineageFlowNode>) {
  const { node, incoming, actions, canExpand, dimmed, horizontal } = data;
  const transformation = transformationLabel(incoming?.transformation);
  const classes = ['lp-node', 'lp-node-column'];
  if (node.side === 'root') classes.push('lp-node-root');
  if (selected) classes.push('lp-node-selected');
  if (dimmed) classes.push('lp-node-dimmed');
  return (
    <div className={classes.join(' ')}>
      <Handle type="target" position={horizontal ? Position.Left : Position.Top} isConnectable={false} />
      <div className="lp-node-body">
        <span className="lp-node-icon" aria-hidden="true"><Table2 size={16} /></span>
        <div className="lp-node-text">
          <div className="lp-node-name" title={node.id}>{node.name}</div>
          <div className="lp-node-parent">
            <Database size={11} aria-hidden="true" /> {parentName(node.id, node.parent)}
          </div>
          <div className="lp-node-badges">
            {transformation && <span className={`lp-badge lp-transform lp-transform-${incoming?.transformation}`}>{transformation}</span>}
            {incoming && <span className="lp-badge lp-badge-quiet">{CONFIDENCE_LABELS[incoming.confidence]}</span>}
            {node.pii && (
              <span className="lp-badge lp-badge-pii" title="PII indicator (not verified exposure)">
                <Shield size={11} aria-hidden="true" /> PII
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

export default memo(ColumnNode);
