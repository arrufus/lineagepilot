// The Change Impact Review (task 4.4, design sections 7 and 8). It reads like a code review: the
// columns that changed, as a diff; then what that reaches, each row with how sure it is and what
// says so; then what could not be checked. All of it comes from the language server through the
// extension, and "Copy as Markdown" copies the server's text, not what this page shows.

import { useEffect, useRef, useState } from 'react';
import { Check, Copy, GitCompare, Loader2, Network, RefreshCw } from 'lucide-react';

import type {
  ChangedNode, ColumnDiff, Confidence, Location, NodeKind, Origin, ReviewImpacted, ReviewResult,
} from '../../../src/protocol';
import type { ReviewHostMessage, ReviewProblem } from '../../../src/reviewMessages';
import { KIND_LABELS, plural } from '../labels';
import type { ReviewTransport } from './transport';

const COPIED_MS = 2000;
const KIND_ORDER: NodeKind[] = ['model', 'snapshot', 'seed', 'source', 'exposure', 'airflow_task', 'airflow_dag'];
const KIND_HEADINGS: Partial<Record<NodeKind, string>> = {
  model: 'Models', snapshot: 'Snapshots', seed: 'Seeds', source: 'Sources', exposure: 'Exposures',
  airflow_task: 'Airflow tasks', airflow_dag: 'Airflow DAGs',
};
const ORIGIN_LABELS: Record<Origin, string> = {
  manifest: 'the manifest', compiled_sql: 'compiled SQL', catalog: 'the catalog', dag_static: 'the DAG file', annotation: 'an annotation',
};
const BASELINE_LABELS: Record<ColumnDiff['baseline'], string> = {
  baseline_manifest: 'the baseline manifest', catalog: 'catalog.json', none: 'no baseline',
};
const STATUS_LABELS: Record<ChangedNode['status'], string> = {
  added: 'added', modified: 'modified', deleted: 'deleted', renamed: 'renamed', untracked: 'new', properties: 'properties changed',
};

const NODE_ID = /^(model|seed|snapshot|source|exposure)\./;

/** ``model.shop.orders#total`` as ``orders.total``. Anything else, such as a path, as it is. */
export function shortRef(ref: string): string {
  if (!NODE_ID.test(ref)) {
    return ref;
  }
  const [node, column] = ref.split('#');
  const name = node.split('.').pop() ?? node;
  return column ? `${name}.${column}` : name;
}

function ConfidenceMark({ confidence }: { confidence: Confidence }) {
  return (
    <span className="lp-confidence">
      <span className={`lp-legend-line lp-edge-${confidence}`} aria-hidden="true" />
      {confidence}
    </span>
  );
}

interface NodeActions {
  locations: ReviewResult['locations'];
  onOpen(location: Location): void;
  onGraph(id: string): void;
}

function NodeName({ id, name, actions }: { id: string; name: string; actions: NodeActions }) {
  const location = actions.locations[id];
  return (
    <span className="lp-node-title">
      {location
        ? <button type="button" className="lp-link lp-identifier" onClick={() => actions.onOpen(location)} title={`Open ${name}`}>{name}</button>
        : <span className="lp-identifier">{name}</span>}
      {location && (
        <button type="button" className="lp-icon-button" aria-label={`Show ${name} in the lineage graph`} title="Show in the lineage graph"
          onClick={() => actions.onGraph(id)}>
          <Network size={13} aria-hidden="true" />
        </button>
      )}
    </span>
  );
}

function ColumnChanges({ diff }: { diff: ColumnDiff }) {
  if (diff.state === 'not_compared') {
    return diff.reason ? <p className="lp-muted">{diff.reason[0].toUpperCase() + diff.reason.slice(1)}.</p> : null;
  }
  if (diff.state === 'unknown') {
    return <p className="lp-column-unknown"><strong>Columns unknown.</strong> {diff.reason}</p>;
  }
  const changed = diff.removed.length + diff.added.length + diff.typeChanged.length > 0;
  return (
    <>
      {changed && (
        <ul className="lp-diff" aria-label="Column changes">
          {diff.removed.map((name) => (
            <li key={`-${name}`} className="lp-diff-removed">
              <span className="lp-diff-sign" aria-hidden="true">−</span>
              <span className="lp-visually-hidden">Removed </span>
              {name}
            </li>
          ))}
          {diff.added.map((name) => (
            <li key={`+${name}`} className="lp-diff-added">
              <span className="lp-diff-sign" aria-hidden="true">+</span>
              <span className="lp-visually-hidden">Added </span>
              {name}
            </li>
          ))}
          {diff.typeChanged.map((change) => (
            <li key={`~${change.name}`} className="lp-diff-retyped">
              <span className="lp-diff-sign" aria-hidden="true">~</span>
              <span className="lp-visually-hidden">Type changed </span>
              {change.name}: {change.before} to {change.after}
            </li>
          ))}
        </ul>
      )}
      <p className="lp-muted">
        {changed ? `Compared with ${BASELINE_LABELS[diff.baseline]}.` : `No column added, removed or retyped, compared with ${BASELINE_LABELS[diff.baseline]}.`}
        {diff.reason ? ` ${diff.reason}` : ''}
      </p>
    </>
  );
}

function ChangedSection({ nodes, actions }: { nodes: ChangedNode[]; actions: NodeActions }) {
  return (
    <section aria-labelledby="lp-changed">
      <h2 id="lp-changed">Changed</h2>
      {nodes.length === 0 && <p className="lp-muted">No changed file maps to a node.</p>}
      {nodes.map((node) => (
        <article key={node.id} className="lp-changed" aria-label={`${KIND_LABELS[node.kind]} ${node.name}`}>
          <h3>
            <NodeName id={node.id} name={node.name} actions={actions} />
            <span className="lp-muted lp-changed-meta">
              {KIND_LABELS[node.kind]}, {STATUS_LABELS[node.status]}{node.owner ? `, owned by ${node.owner}` : ''}
            </span>
          </h3>
          <div className="lp-muted lp-identifier lp-files">{node.files.map((file) => file.path).join(', ')}</div>
          <ColumnChanges diff={node.columns} />
        </article>
      ))}
    </section>
  );
}

function Reached({ node }: { node: ReviewImpacted }) {
  const columns = node.columns.length > 0 && (
    <span>{node.columns.length === 1 ? 'Column ' : 'Columns '}<span className="lp-identifier">{node.columns.join(', ')}</span></span>
  );
  if (node.reach === 'column') {
    return columns || null;
  }
  return (
    <>
      <div className="lp-reason" title={node.reason ?? undefined}>Model level{node.reason ? `: ${node.reason}` : ''}</div>
      {columns && <div>{columns}</div>}
    </>
  );
}

function Evidence({ node }: { node: ReviewImpacted }) {
  const sources = [...new Set(node.via.map((step) => shortRef(step.source)))];
  return (
    <>
      {node.origins.map((origin) => ORIGIN_LABELS[origin]).join(', ')}
      {sources.length > 0 && <div className="lp-muted">from <span className="lp-identifier">{sources.join(', ')}</span>{node.viaMore > 0 ? ` and ${node.viaMore} more` : ''}</div>}
    </>
  );
}

function ImpactTable({ kind, nodes, actions }: { kind: NodeKind; nodes: ReviewImpacted[]; actions: NodeActions }) {
  const heading = KIND_HEADINGS[kind] ?? kind;
  return (
    <>
      <h3>{heading} ({nodes.length})</h3>
      <table className="lp-impact" aria-label={`Impacted ${heading.toLowerCase()}`}>
        <thead>
          <tr><th scope="col">Name</th><th scope="col">Reached</th><th scope="col">Confidence</th><th scope="col">Evidence</th><th scope="col">Owner</th></tr>
        </thead>
        <tbody>
          {nodes.map((node) => (
            <tr key={node.id} className={node.reach === 'column' ? 'lp-reach-column' : 'lp-reach-model'}>
              <th scope="row"><NodeName id={node.id} name={node.name} actions={actions} /></th>
              <td><Reached node={node} /></td>
              <td><ConfidenceMark confidence={node.confidence} /></td>
              <td><Evidence node={node} /></td>
              <td>{node.owner ?? <span className="lp-muted">No owner</span>}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}

function DownstreamSection({ result, actions }: { result: ReviewResult; actions: NodeActions }) {
  const names = new Map(result.impacted.map((node) => [node.id, node.name]));
  return (
    <section aria-labelledby="lp-downstream">
      <h2 id="lp-downstream">Downstream impact</h2>
      {result.impacted.length === 0 && <p className="lp-muted">Nothing is known to be downstream of the changed nodes.</p>}
      {KIND_ORDER.map((kind) => {
        const nodes = result.impacted.filter((node) => node.kind === kind);
        return nodes.length > 0 ? <ImpactTable key={kind} kind={kind} nodes={nodes} actions={actions} /> : null;
      })}
      {!result.airflow.analysed && (
        <>
          <h3>Airflow tasks</h3>
          <p className="lp-muted">Airflow DAGs were not analysed in this version, so tasks are not listed.</p>
        </>
      )}
      {result.owners.length > 0 && (
        <>
          <h3>Owners</h3>
          <dl className="lp-owners">
            {result.owners.map((group) => (
              <div key={group.owner ?? ''}>
                <dt>{group.owner ?? <span className="lp-muted">No owner</span>}</dt>
                <dd className="lp-identifier">{group.nodes.map((id) => names.get(id) ?? id).join(', ')}</dd>
              </div>
            ))}
          </dl>
        </>
      )}
    </section>
  );
}

function UnknownsSection({ result }: { result: ReviewResult }) {
  return (
    <section aria-labelledby="lp-unknowns">
      <h2 id="lp-unknowns">Unknowns ({result.unknowns.length})</h2>
      {result.unknowns.length === 0
        ? <p className="lp-muted">Nothing on the way down could not be checked.</p>
        : (
          <ul className="lp-unknowns">
            {result.unknowns.map((unknown) => (
              <li key={`${unknown.subject}|${unknown.reference}|${unknown.reason}`}>
                <span className="lp-identifier">{shortRef(unknown.subject)}</span>
                <span className="lp-muted"> {unknown.reference}</span>
                <div>{unknown.reason}</div>
              </li>
            ))}
          </ul>
        )}
    </section>
  );
}

function OtherFiles({ result }: { result: ReviewResult }) {
  const total = result.otherFiles.length + result.outsideProject;
  if (total === 0) {
    return null;
  }
  return (
    <details className="lp-other">
      <summary>Other changed files ({total})</summary>
      <ul>
        {result.otherFiles.map((file) => <li key={file.path}><span className="lp-identifier">{file.path}</span> <span className="lp-muted">{file.status}</span></li>)}
        {result.outsideProject > 0 && <li className="lp-muted">{plural(result.outsideProject, 'file')} outside this dbt project</li>}
      </ul>
    </details>
  );
}

function headline(result: ReviewResult): string {
  const { summary } = result;
  const impacted = Object.values(summary.impacted).reduce((sum, count) => sum + count, 0);
  return [
    plural(summary.changedNodes, 'changed node'),
    plural(summary.changedColumns, 'changed column'),
    impacted === 0 ? 'nothing downstream' : `${impacted} impacted, ${summary.impactedByColumn} of them traced by column`,
    plural(summary.unknowns, 'unknown'),
  ].join(', ');
}

function nothingChanged(result: ReviewResult): boolean {
  return result.summary.changedFiles === 0 && result.outsideProject === 0;
}

const FAILURE_ACTIONS: Partial<Record<ReviewProblem, 'manageTrust' | 'pickBase'>> = {
  restricted: 'manageTrust', no_base: 'pickBase', too_many_files: 'pickBase',
};

export default function ReviewApp({ transport }: { transport: ReviewTransport }) {
  const [project, setProject] = useState<{ name: string; root: string } | null>(null);
  const [trusted, setTrusted] = useState(false);
  const [running, setRunning] = useState(true);
  const [result, setResult] = useState<ReviewResult | null>(null);
  const [failure, setFailure] = useState<{ problem: ReviewProblem; message: string } | null>(null);
  const [copied, setCopied] = useState(false);
  const [announcement, setAnnouncement] = useState('');
  const copiedTimer = useRef<ReturnType<typeof setTimeout>>(undefined);

  useEffect(() => {
    const stop = transport.onMessage((message: ReviewHostMessage) => {
      switch (message.type) {
        case 'init':
          setProject(message.project);
          setTrusted(message.trusted);
          break;
        case 'reviewStarted':
          setRunning(true);
          break;
        case 'reviewResult':
          setRunning(false);
          setResult(message.result);
          setFailure(null);
          setAnnouncement(headline(message.result));
          break;
        case 'reviewFailed':
          setRunning(false);
          setResult(null);
          setFailure({ problem: message.problem, message: message.message });
          break;
        case 'copied':
          setCopied(true);
          setAnnouncement('Copied the review as Markdown');
          clearTimeout(copiedTimer.current);
          copiedTimer.current = setTimeout(() => setCopied(false), COPIED_MS);
          break;
      }
    });
    transport.send({ type: 'ready' });
    return () => {
      stop();
      clearTimeout(copiedTimer.current);
    };
  }, [transport]);

  const actions: NodeActions = {
    locations: result?.locations ?? {},
    onOpen: (location) => transport.send({ type: 'openFile', location }),
    onGraph: (id) => transport.send({ type: 'showInGraph', id }),
  };
  const failureAction = failure && FAILURE_ACTIONS[failure.problem];
  const base = result?.base;

  return (
    <div className="lp-review">
      <header className="lp-review-header">
        <div className="lp-review-title">
          <h1><GitCompare size={16} aria-hidden="true" /> Change impact{project ? `: ${project.name}` : ''}</h1>
          {base?.ref && base.commit && (
            <p className="lp-muted">
              Compared with the merge-base of HEAD and <span className="lp-identifier">{base.ref}</span> (<span className="lp-identifier">{base.commit.slice(0, 12)}</span>)
            </p>
          )}
        </div>
        <div className="lp-review-actions">
          <button type="button" onClick={() => transport.send({ type: 'pickBase' })}>Change base</button>
          <button type="button" onClick={() => transport.send({ type: 'refresh' })} disabled={running}>
            <RefreshCw size={14} aria-hidden="true" className={running ? 'lp-spin' : undefined} /> Refresh
          </button>
          <button type="button" className="lp-primary" disabled={!result} onClick={() => transport.send({ type: 'copyMarkdown' })}>
            {copied ? <Check size={14} aria-hidden="true" /> : <Copy size={14} aria-hidden="true" />}
            {copied ? 'Copied' : 'Copy as Markdown'}
          </button>
        </div>
      </header>

      <main className="lp-review-body">
        {!result && !failure && (
          <p className="lp-muted" role="status"><Loader2 size={14} className="lp-spin" aria-hidden="true" /> Working out what changed…</p>
        )}
        {failure && (
          <div className="lp-review-failure" role="alert">
            <p>{failure.message}</p>
            {failureAction === 'manageTrust' && <button type="button" onClick={() => transport.send({ type: 'manageTrust' })}>Manage Workspace Trust</button>}
            {failureAction === 'pickBase' && <button type="button" onClick={() => transport.send({ type: 'pickBase' })}>Choose a base</button>}
          </div>
        )}
        {result && nothingChanged(result) && (
          <p className="lp-review-empty">Nothing differs from this base. Commit or change a file, or choose another base.</p>
        )}
        {result && !nothingChanged(result) && (
          <>
            <p className="lp-review-headline">{headline(result)}.</p>
            {result.summary.staleNodes > 0 && (
              <div className="lp-review-stale" role="status">
                <p>
                  {result.summary.staleNodes === 1 ? 'A changed model is' : `${result.summary.staleNodes} changed models are`} newer than the manifest, so column changes are unknown and impact is at model level.
                </p>
                {trusted && <button type="button" onClick={() => transport.send({ type: 'runCompile' })}>Run dbt compile</button>}
              </div>
            )}
            {result.notes.map((note) => <p key={note} className="lp-review-note">{note}</p>)}
            <ChangedSection nodes={result.changedNodes} actions={actions} />
            <DownstreamSection result={result} actions={actions} />
            <UnknownsSection result={result} />
            <OtherFiles result={result} />
          </>
        )}
      </main>
      <div className="lp-visually-hidden" aria-live="polite">{announcement}</div>
    </div>
  );
}
