// What a change to the selected node or column reaches (task 3.2), ported from the dashboard's
// ImpactAnalysis. The old risk levels, recommendations and "proceed" button rested on
// unmeasured heuristics (review finding F13) and are gone: this shows lineage/impact as it is,
// with each node's confidence, the columns hit, why a model was reached at model level, and the
// unknowns.

import { ExternalLink, Loader2, Target, TriangleAlert, X } from 'lucide-react';

import type { ImpactedNode, ImpactResult, Location, NodeKind } from '../../../src/protocol';
import { CONFIDENCE_LABELS, KIND_LABELS, plural } from '../labels';

export interface ImpactPanelProps {
  name: string;
  result: ImpactResult | null;
  error: string | null;
  onClose(): void;
  onOpen(location: Location): void;
  onFocus(id: string): void;
}

const ORDER: NodeKind[] = ['model', 'snapshot', 'seed', 'source', 'exposure', 'airflow_task', 'airflow_dag'];

function groups(nodes: ImpactedNode[]): [NodeKind, ImpactedNode[]][] {
  const byKind = new Map<NodeKind, ImpactedNode[]>();
  for (const node of nodes) {
    byKind.set(node.kind, [...(byKind.get(node.kind) ?? []), node]);
  }
  return [...byKind.entries()].sort((a, b) => ORDER.indexOf(a[0]) - ORDER.indexOf(b[0]));
}

export default function ImpactPanel({ name, result, error, onClose, onOpen, onFocus }: ImpactPanelProps) {
  return (
    <aside className="lp-side" aria-label={`Impact of changing ${name}`}>
      <header>
        <div>
          <h2><Target size={16} aria-hidden="true" /> Impact of {name}</h2>
          <div className="lp-muted">Everything downstream that a change could reach</div>
        </div>
        <button type="button" className="lp-icon-button" onClick={() => onClose()} aria-label="Close impact" title="Close">
          <X size={16} aria-hidden="true" />
        </button>
      </header>
      {error && <p className="lp-error" role="alert">{error}</p>}
      {!result && !error && <p className="lp-muted" role="status"><Loader2 size={14} className="lp-spin" aria-hidden="true" /> Working out the impact…</p>}
      {result && (
        <>
          <section aria-label="Summary">
            <div className="lp-cards">
              {Object.entries(result.counts).map(([kind, count]) => (
                <div key={kind} className="lp-card">
                  <div className="lp-card-value">{count}</div>
                  <div className="lp-muted">{KIND_LABELS[kind as NodeKind] ?? kind}{count === 1 ? '' : 's'}</div>
                </div>
              ))}
              {result.nodes.length === 0 && <p>Nothing downstream.</p>}
            </div>
            {result.truncated && <p className="lp-muted">Stopped at the depth limit: there is more downstream.</p>}
            {result.columnLineage === 'computing' && (
              <p className="lp-muted">Column lineage is still being computed, so some models are reached at model level.</p>
            )}
          </section>
          {groups(result.nodes).map(([kind, nodes]) => (
            <section key={kind} aria-label={`Impacted ${KIND_LABELS[kind]}s`}>
              <h3>{plural(nodes.length, KIND_LABELS[kind])}</h3>
              <ul className="lp-list">
                {nodes.map((hit) => (
                  <li key={hit.id}>
                    <button type="button" className="lp-link" onClick={() => onFocus(hit.id)}>{hit.name}</button>
                    {hit.location && (
                      <button type="button" className="lp-icon-button lp-inline" aria-label={`Open ${hit.name}`}
                        title="Open" onClick={() => onOpen(hit.location!)}>
                        <ExternalLink size={13} aria-hidden="true" />
                      </button>
                    )}
                    <span className="lp-muted">
                      {[plural(hit.depth, 'step'), CONFIDENCE_LABELS[hit.confidence], `${hit.granularity} level`, hit.owner ? `owner ${hit.owner}` : null]
                        .filter(Boolean).join(' · ')}
                    </span>
                    {hit.columns.length > 0 && (
                      <div className="lp-columns">{hit.columns.map((c) => c.split('#').pop()).join(', ')}</div>
                    )}
                    {hit.reason && <div className="lp-muted">{hit.reason}</div>}
                  </li>
                ))}
              </ul>
            </section>
          ))}
          {result.unknowns.length > 0 && (
            <section aria-label="Unknowns">
              <h3><TriangleAlert size={14} aria-hidden="true" /> Unknowns ({result.unknowns.length})</h3>
              <ul className="lp-list">
                {result.unknowns.map((u) => (
                  <li key={`${u.subject}|${u.reference}`}><strong>{u.subject.split('.').pop()}</strong>: {u.reference} <span className="lp-muted">{u.reason}</span></li>
                ))}
              </ul>
            </section>
          )}
        </>
      )}
    </aside>
  );
}
