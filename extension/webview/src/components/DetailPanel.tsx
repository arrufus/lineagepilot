// The selected node or column (task 3.2), ported from the dashboard's ColumnDetailPanel: its
// details, its columns, its direct upstream and downstream with how each link is known, and the
// unknowns met around it. Nothing here is estimated: confidence is declared, inferred or
// heuristic, never a percentage.

import { ArrowLeft, ArrowRight, Columns3, ExternalLink, Info, Shield, Target, TriangleAlert, X } from 'lucide-react';

import type { GraphEdge, GraphNode, NodeResult, Unknown } from '../../../src/protocol';
import { CONFIDENCE_LABELS, KIND_LABELS, plural, transformationLabel } from '../labels';

export interface DetailPanelProps {
  node: GraphNode;
  detail: NodeResult | null;
  error: string | null;
  upstream: GraphEdge[];
  downstream: GraphEdge[];
  names: Map<string, string>;
  unknowns: Unknown[];
  onClose(): void;
  onOpen(node: GraphNode): void;
  onFocus(id: string, granularity: 'model' | 'column'): void;
  onImpact(node: GraphNode): void;
}

function EdgeList({ title, edges, other, names, onFocus, column }: {
  title: string; edges: GraphEdge[]; other: 'upstream' | 'downstream'; names: Map<string, string>;
  onFocus: DetailPanelProps['onFocus']; column: boolean;
}) {
  if (edges.length === 0) {
    return null;
  }
  return (
    <section aria-label={title}>
      <h3>{other === 'upstream' ? <ArrowLeft size={14} aria-hidden="true" /> : <ArrowRight size={14} aria-hidden="true" />} {title}</h3>
      <ul className="lp-list">
        {edges.map((edge) => {
          const id = other === 'upstream' ? edge.upstream : edge.downstream;
          const how = transformationLabel(edge.transformation);
          return (
            <li key={`${edge.kind}|${edge.upstream}|${edge.downstream}`}>
              <button type="button" className="lp-link" onClick={() => onFocus(id, column ? 'column' : 'model')}>
                {names.get(id) ?? id}
              </button>
              <span className="lp-muted">
                {[how, CONFIDENCE_LABELS[edge.confidence], edge.origins.join(', ').replace(/_/g, ' ')].filter(Boolean).join(' · ')}
                {edge.relation ? ` · via table ${edge.relation.replace(/^relation:/, '')}` : ''}
              </span>
            </li>
          );
        })}
      </ul>
    </section>
  );
}

export default function DetailPanel(props: DetailPanelProps) {
  const { node, detail } = props;
  const column = node.kind === 'column';
  const own = props.unknowns.filter((u) => u.subject === node.id || u.subject.startsWith(`${node.id}#`));
  return (
    <aside className="lp-side" aria-label={`Details of ${node.name}`}>
      <header>
        <div>
          <h2>{node.name}</h2>
          <div className="lp-muted lp-mono" title={node.id}>{node.id}</div>
        </div>
        <button type="button" className="lp-icon-button" onClick={() => props.onClose()} aria-label="Close details" title="Close">
          <X size={16} aria-hidden="true" />
        </button>
      </header>
      <div className="lp-actions">
        <button type="button" onClick={() => props.onOpen(node)} disabled={!node.location}>
          <ExternalLink size={14} aria-hidden="true" /> Open
        </button>
        <button type="button" onClick={() => props.onImpact(node)}>
          <Target size={14} aria-hidden="true" /> Impact
        </button>
        {node.side !== 'root' && (
          <button type="button" onClick={() => props.onFocus(node.id, column ? 'column' : 'model')}>Focus here</button>
        )}
      </div>
      <section aria-label="About">
        <h3><Info size={14} aria-hidden="true" /> About</h3>
        <dl className="lp-facts">
          <dt>Kind</dt><dd>{KIND_LABELS[node.kind]}</dd>
          {node.layer && <><dt>Layer</dt><dd>{node.layer}</dd></>}
          {node.materialization && <><dt>Materialized as</dt><dd>{node.materialization}</dd></>}
          {node.owner && <><dt>Owner</dt><dd>{node.owner}</dd></>}
          {detail && <><dt>Direct</dt><dd>{detail.upstream} upstream, {detail.downstream} downstream</dd></>}
          {node.columnLineage && (
            <><dt>Column lineage</dt><dd>{node.columnLineage}{node.columnLineageReason ? `: ${node.columnLineageReason}` : ''}</dd></>
          )}
        </dl>
        {node.description && <p className="lp-description">{node.description}</p>}
        {node.pii && (
          <p className="lp-pii"><Shield size={13} aria-hidden="true" /> PII indicators: from tags, names or lineage, not verified exposure.</p>
        )}
      </section>
      {props.error && <p className="lp-error" role="alert">{props.error}</p>}
      {detail && detail.columns.length > 0 && (
        <section aria-label="Columns">
          <h3><Columns3 size={14} aria-hidden="true" /> {plural(detail.columns.length, 'column')}</h3>
          <ul className="lp-list">
            {detail.columns.map((c) => (
              <li key={c.id}>
                <button type="button" className="lp-link" onClick={() => props.onFocus(c.id, 'column')}
                  title="Trace this column">{c.name}</button>
                <span className="lp-muted">
                  {[c.dataType, c.tests ? plural(c.tests, 'test') : null, c.pii ? 'PII' : null].filter(Boolean).join(' · ')}
                </span>
              </li>
            ))}
          </ul>
        </section>
      )}
      <EdgeList title="Upstream" edges={props.upstream} other="upstream" names={props.names} onFocus={props.onFocus} column={column} />
      <EdgeList title="Downstream" edges={props.downstream} other="downstream" names={props.names} onFocus={props.onFocus} column={column} />
      {own.length > 0 && (
        <section aria-label="Unknowns">
          <h3><TriangleAlert size={14} aria-hidden="true" /> Unknowns</h3>
          <ul className="lp-list">
            {own.map((u) => <li key={`${u.subject}|${u.reference}`}><strong>{u.reference}</strong> <span className="lp-muted">{u.reason}</span></li>)}
          </ul>
        </section>
      )}
    </aside>
  );
}
