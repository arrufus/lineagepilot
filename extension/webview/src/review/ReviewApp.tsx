// The Change Impact Review (task 4.4, design sections 7 and 8). It reads like a code review: the
// columns that changed, as a diff; then what that reaches, each row with how sure it is and what
// says so; then what could not be checked. All of it comes from the language server through the
// extension, and "Copy as Markdown" copies the server's text, not what this page shows.
//
// Its sections follow design_next_phase section 7.5 (task 7.6): what changed; dbt's own nodes,
// with the semantic layer; the pipelines that run them, by tool and then by pipeline; the
// consumers that read them, by tool; the owners across tools; and the unknowns, by tool.

import { useEffect, useRef, useState } from 'react';
import { Check, Copy, GitCompare, Loader2, Network, RefreshCw } from 'lucide-react';

import { isKnownTool, pipelineWord, subkindLabel, toolOfId, withoutTool } from '../../../src/kinds';
import type {
  ChangedNode, ColumnDiff, Confidence, Location, NodeKind, Origin, ReaderSummary, ReviewImpacted, ReviewResult, Unknown,
} from '../../../src/protocol';
import type { ReviewHostMessage, ReviewProblem } from '../../../src/reviewMessages';
import { KIND_LABELS, plural, toolLabel } from '../labels';
import type { ReviewTransport } from './transport';

const COPIED_MS = 2000;
// dbt's own nodes, in this order, each kind in a table of its own
const DBT_ORDER: NodeKind[] = ['model', 'snapshot', 'seed', 'source', 'exposure'];
const KIND_HEADINGS: Partial<Record<NodeKind, string>> = {
  model: 'Models', snapshot: 'Snapshots', seed: 'Seeds', source: 'Sources', exposure: 'Exposures',
};
const PIPELINE_KINDS: NodeKind[] = ['pipeline', 'task'];
// Consumers that are dbt's own, listed with dbt's nodes rather than with the other tools'
const DBT_TOOLS = new Set(['dbt_semantic']);
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

/** ``model.shop.orders#total`` as ``orders.total``, ``airflow:dag.task`` as ``dag.task``, for any
 * tool. Anything else, such as a path, as it is. */
export function shortRef(ref: string): string {
  if (toolOfId(ref) !== undefined) {
    return withoutTool(ref);
  }
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

/** ``Release notes.`` for ``Release notes``, as the review's Markdown writes a reason. */
function sentence(text: string): string {
  return /[.!?]$/.test(text) ? text : `${text}.`;
}

function Reached({ node }: { node: ReviewImpacted }) {
  // A consumer's columns are its fields: a metric's measures, a view's dimensions
  const word = node.kind === 'consumer' ? 'Field' : 'Column';
  const columns = node.columns.length > 0 && (
    <span>{node.columns.length === 1 ? `${word} ` : `${word}s `}<span className="lp-identifier">{node.columns.join(', ')}</span></span>
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
      {node.origins.map((origin) => ORIGIN_LABELS[origin] ?? origin).join(', ')}
      {sources.length > 0 && <div className="lp-muted">from <span className="lp-identifier">{sources.join(', ')}</span>{node.viaMore > 0 ? ` and ${node.viaMore} more` : ''}</div>}
    </>
  );
}

interface ImpactTableProps {
  heading: string;
  // What the table is called to a screen reader: "Impacted models"
  label: string;
  nodes: ReviewImpacted[];
  actions: NodeActions;
}

function ImpactTable({ heading, label, nodes, actions }: ImpactTableProps) {
  return (
    <>
      <h3>{heading} ({nodes.length})</h3>
      <table className="lp-impact" aria-label={label}>
        <thead>
          <tr><th scope="col">Name</th><th scope="col">Reached</th><th scope="col">Confidence</th><th scope="col">Evidence</th><th scope="col">Owner</th></tr>
        </thead>
        <tbody>
          {nodes.map((node) => (
            <tr key={node.id} className={node.reach === 'column' ? 'lp-reach-column' : 'lp-reach-model'}>
              <th scope="row">
                <NodeName id={node.id} name={node.name} actions={actions} />
                {node.tool && node.subkind && <div className="lp-muted">{subkindLabel(node.subkind)}</div>}
                {/* A saved query's exports: the tables it writes change too */}
                {node.exports && node.exports.length > 0 && (
                  <div className="lp-muted">exports <span className="lp-identifier">{node.exports.join(', ')}</span></div>
                )}
              </th>
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

function moreOf(shown: number, more: number): string {
  return shown > 0 && more > 0 ? ` and ${more} more` : '';
}

/** One task: what it has to do with the change, and why the link is as sure as it is. */
function TaskRow({ node, actions }: { node: ReviewImpacted; actions: NodeActions }) {
  const detail = node.pipeline;
  return (
    <tr className="lp-reach-model">
      <td className="lp-identifier">{detail?.pipeline}</td>
      <th scope="row">
        {node.kind === 'pipeline'
          ? <span>The whole {pipelineWord(node.tool, 1)}</span>
          : <NodeName id={node.id} name={node.name} actions={actions} />}
        {detail && detail.tasks.length > 0 && (
          <div className="lp-muted">
            Tasks Cosmos should render: <span className="lp-identifier">{detail.tasks.join(', ')}</span>{moreOf(detail.tasks.length, detail.tasksMore)}
          </div>
        )}
      </th>
      <td>
        {detail && detail.runs.length > 0 && (
          <div>{detail.how.join(', ') || 'Runs'}: <span className="lp-identifier">{detail.runs.join(', ')}</span>{moreOf(detail.runs.length, detail.runsMore)}</div>
        )}
        {detail && detail.after.length > 0 && <div>Runs after <span className="lp-identifier">{detail.after.join(', ')}</span></div>}
      </td>
      <td><ConfidenceMark confidence={node.confidence} /></td>
      <td>
        {node.origins.map((origin) => ORIGIN_LABELS[origin] ?? origin).join(', ')}
        {node.file && <div className="lp-muted lp-identifier">{node.file}</div>}
        {detail?.notes.map((note) => <div key={note} className="lp-muted lp-reason" title={note}>{note}</div>)}
      </td>
      <td>{node.owner ?? <span className="lp-muted">No owner</span>}</td>
    </tr>
  );
}

/** The tools in some reader summaries and rows, by name: a tool with rows and no reader too. */
function toolsOf(summaries: ReaderSummary[], nodes: ReviewImpacted[]): string[] {
  const tools = new Set([...summaries.map((s) => s.tool), ...nodes.map((n) => n.tool ?? '')]);
  tools.delete('');
  return [...tools].sort((a, b) => toolLabel(a).localeCompare(toolLabel(b)) || a.localeCompare(b));
}

/** One tool's tasks, by pipeline; or why there are none. */
function PipelineTool({ tool, summary, nodes, actions }: {
  tool: string; summary: ReaderSummary | undefined; nodes: ReviewImpacted[]; actions: NodeActions;
}) {
  const name = toolLabel(tool);
  const files = `${pipelineWord(tool, 1)} file`;
  if (summary && !summary.analysed) {
    return (
      <>
        <h3>{name}</h3>
        <p className="lp-muted">Not analysed: {sentence(summary.reason ?? `the ${files}s were not read`)}</p>
      </>
    );
  }
  const sorted = [...nodes].sort((a, b) => (a.pipeline?.pipeline ?? '').localeCompare(b.pipeline?.pipeline ?? '') || a.name.localeCompare(b.name));
  const pipelines = new Set(sorted.map((node) => node.pipeline?.pipeline));
  const counted = sorted.length > 0 ? ` (${plural(sorted.length, 'task')} in ${pipelines.size} ${pipelineWord(tool, pipelines.size)})` : '';
  return (
    <>
      <h3>{name}{counted}</h3>
      {sorted.length === 0 && summary && <p className="lp-muted">No task in the {plural(summary.files, files)} read runs a changed or downstream node.</p>}
      {sorted.length > 0 && (
        <table className="lp-impact lp-impact-pipeline" aria-label={`Impacted ${name} tasks`}>
          <thead>
            <tr><th scope="col">{pipelineWord(tool, 1)}</th><th scope="col">Task</th><th scope="col">What it has to do with this</th><th scope="col">Confidence</th><th scope="col">Evidence</th><th scope="col">Owner</th></tr>
          </thead>
          <tbody>
            {sorted.map((node) => <TaskRow key={node.id} node={node} actions={actions} />)}
          </tbody>
        </table>
      )}
      {summary && summary.opaqueTasks > 0 && <p className="lp-muted">{plural(summary.opaqueTasks, 'task')} could not be read and do not mention dbt, so they are not listed.</p>}
    </>
  );
}

function PipelinesSection({ result, actions }: { result: ReviewResult; actions: NodeActions }) {
  const summaries = result.pipelines ?? [];
  const nodes = result.impacted.filter((node) => PIPELINE_KINDS.includes(node.kind));
  const tools = toolsOf(summaries, nodes);
  if (tools.length === 0) {
    return null;
  }
  return (
    <section aria-labelledby="lp-pipelines">
      <h2 id="lp-pipelines">Pipelines</h2>
      {tools.map((tool) => (
        <PipelineTool key={tool} tool={tool} summary={summaries.find((s) => s.tool === tool)}
          nodes={nodes.filter((node) => node.tool === tool)} actions={actions} />
      ))}
    </section>
  );
}

/** What reads the changed nodes from outside dbt, by tool, with the fields of each that break.
 * Shown when a reader of consumers is turned on, or something declares a consumer. */
function ConsumersSection({ result, actions }: { result: ReviewResult; actions: NodeActions }) {
  const summaries = (result.consumers ?? []).filter((s) => !DBT_TOOLS.has(s.tool));
  const nodes = result.impacted.filter((node) => node.kind === 'consumer' && !DBT_TOOLS.has(node.tool ?? ''));
  const tools = toolsOf(summaries, nodes);
  if (tools.length === 0) {
    return null;
  }
  return (
    <section aria-labelledby="lp-consumers">
      <h2 id="lp-consumers">Consumers</h2>
      {tools.map((tool) => {
        const found = nodes.filter((node) => node.tool === tool);
        const summary = summaries.find((s) => s.tool === tool);
        if (summary && !summary.analysed) {
          return (
            <div key={tool}>
              <h3>{toolLabel(tool)}</h3>
              <p className="lp-muted">Not analysed: {sentence(summary.reason ?? 'its files were not read')}</p>
            </div>
          );
        }
        return found.length > 0
          ? <ImpactTable key={tool} heading={toolLabel(tool)} label={`Impacted ${toolLabel(tool)} consumers`} nodes={found} actions={actions} />
          : (
            <div key={tool}>
              <h3>{toolLabel(tool)}</h3>
              <p className="lp-muted">No consumer in the {plural(summary?.files ?? 0, 'file')} read uses a changed or downstream node.</p>
            </div>
          );
      })}
    </section>
  );
}

function DownstreamSection({ result, actions }: { result: ReviewResult; actions: NodeActions }) {
  const semantic = result.impacted.filter((node) => node.kind === 'consumer' && DBT_TOOLS.has(node.tool ?? ''));
  return (
    <section aria-labelledby="lp-downstream">
      <h2 id="lp-downstream">Downstream impact</h2>
      {result.impacted.length === 0 && <p className="lp-muted">Nothing is known to be downstream of the changed nodes.</p>}
      {DBT_ORDER.map((kind) => {
        const nodes = result.impacted.filter((node) => node.kind === kind);
        const heading = KIND_HEADINGS[kind] ?? kind;
        return nodes.length > 0
          ? <ImpactTable key={kind} heading={heading} label={`Impacted ${heading.toLowerCase()}`} nodes={nodes} actions={actions} />
          : null;
      })}
      {semantic.length > 0 && <ImpactTable heading="Semantic layer" label="Impacted semantic layer" nodes={semantic} actions={actions} />}
    </section>
  );
}

function OwnersSection({ result }: { result: ReviewResult }) {
  if (result.owners.length === 0) {
    return null;
  }
  const names = new Map(result.impacted.map((node) => [node.id, node.name]));
  return (
    <section aria-labelledby="lp-owners">
      <h2 id="lp-owners">Owners</h2>
      <dl className="lp-owners">
        {result.owners.map((group) => (
          <div key={group.owner ?? ''}>
            <dt>{group.owner ?? <span className="lp-muted">No owner</span>}</dt>
            <dd className="lp-identifier">{group.nodes.map((id) => names.get(id) ?? id).join(', ')}</dd>
          </div>
        ))}
      </dl>
    </section>
  );
}

/** The unknowns by tool, as the review's Markdown groups them. A review's unknowns are about a
 * node, a column or a changed file: a pipeline's, task's or consumer's ID starts with its tool,
 * and anything else, such as a path with a colon in it, is dbt's. dbt first. */
export function unknownGroups(result: ReviewResult): [string, Unknown[]][] {
  const tools = new Set([
    ...[...(result.pipelines ?? []), ...(result.consumers ?? [])].map((s) => s.tool),
    ...result.impacted.map((n) => n.tool ?? ''),
  ]);
  const groups = new Map<string, Unknown[]>();
  for (const unknown of result.unknowns) {
    const tool = toolOfId(unknown.subject);
    const name = tool !== undefined && (isKnownTool(tool) || tools.has(tool)) ? toolLabel(tool) : 'dbt';
    groups.set(name, [...(groups.get(name) ?? []), unknown]);
  }
  return [...groups].sort((a, b) => Number(b[0] === 'dbt') - Number(a[0] === 'dbt') || a[0].localeCompare(b[0]));
}

function UnknownList({ unknowns }: { unknowns: Unknown[] }) {
  return (
    <ul className="lp-unknowns">
      {unknowns.map((unknown) => (
        <li key={`${unknown.subject}|${unknown.reference}|${unknown.reason}`}>
          <span className="lp-identifier">{shortRef(unknown.subject)}</span>
          <span className="lp-muted"> {unknown.reference}</span>
          <div>{unknown.reason}</div>
        </li>
      ))}
    </ul>
  );
}

function UnknownsSection({ result }: { result: ReviewResult }) {
  const groups = unknownGroups(result);
  return (
    <section aria-labelledby="lp-unknowns">
      <h2 id="lp-unknowns">Unknowns ({result.unknowns.length})</h2>
      {result.unknowns.length === 0 && <p className="lp-muted">Nothing on the way down could not be checked.</p>}
      {groups.length === 1 && <UnknownList unknowns={groups[0][1]} />}
      {groups.length > 1 && groups.map(([name, unknowns]) => (
        <div key={name}>
          <h3>{name} ({unknowns.length})</h3>
          <UnknownList unknowns={unknowns} />
        </div>
      ))}
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
            <PipelinesSection result={result} actions={actions} />
            <ConsumersSection result={result} actions={actions} />
            <OwnersSection result={result} />
            <UnknownsSection result={result} />
            <OtherFiles result={result} />
          </>
        )}
      </main>
      <div className="lp-visually-hidden" aria-live="polite">{announcement}</div>
    </div>
  );
}
