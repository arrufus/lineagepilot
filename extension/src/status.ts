// The status bar text for all projects (task 2.4, design section 7), from the servers'
// lineage/status. Kept free of the vscode module so it can be unit-tested.
//   Lineage: 1,204 nodes · manifest 3 min old
//   Lineage: 1,204 nodes · 3 files newer than the manifest   (a warning)

import type { Status } from './protocol';

export type Level = 'ok' | 'busy' | 'warning' | 'error';

export interface ProjectView {
  root: string;
  // Set when the server is not running: why
  problem?: string;
  status?: Status;
}

export interface StatusBar {
  text: string;
  tooltip: string;
  level: Level;
}

const ICONS: Record<Level, string> = {
  ok: '$(type-hierarchy)',
  busy: '$(sync~spin)',
  warning: '$(warning)',
  error: '$(error)',
};

export function age(iso: string | null | undefined, now: number): string | undefined {
  if (!iso) {
    return undefined;
  }
  const then = Date.parse(iso);
  if (Number.isNaN(then)) {
    return undefined;
  }
  const minutes = Math.max(0, Math.round((now - then) / 60_000));
  if (minutes < 1) {
    return 'just now';
  }
  if (minutes < 60) {
    return `${minutes} min old`;
  }
  const hours = Math.round(minutes / 60);
  if (hours < 48) {
    return `${hours} h old`;
  }
  return `${Math.round(hours / 24)} days old`;
}

function plural(n: number, one: string, many: string): string {
  return `${n.toLocaleString('en-US')} ${n === 1 ? one : many}`;
}

/** One project's state, as a level and a short phrase. */
export function describe(project: ProjectView, now: number): { level: Level; phrase: string } {
  if (project.problem) {
    return { level: 'error', phrase: project.problem };
  }
  const status = project.status;
  if (!status || status.state === 'starting') {
    return { level: 'busy', phrase: 'indexing…' };
  }
  if (status.state === 'failed') {
    return { level: 'error', phrase: `index failed: ${status.message ?? 'see the output'}` };
  }
  if (status.state === 'unknown') {
    return { level: 'warning', phrase: `lineage unknown: ${status.message ?? 'the artifacts cannot be used'}` };
  }
  const nodes = plural(status.counts?.nodes ?? 0, 'node', 'nodes');
  if (status.stale.count > 0) {
    const files = plural(status.stale.count, 'file', 'files');
    return { level: 'warning', phrase: `${nodes} · ${files} newer than the manifest` };
  }
  const manifest = status.artifacts.manifest;
  const old = age(manifest?.generatedAt ?? manifest?.modifiedAt, now);
  const phrase = old ? `${nodes} · manifest ${old}` : nodes;
  return { level: status.indexing ? 'busy' : 'ok', phrase };
}

const SEVERITY: Level[] = ['ok', 'busy', 'warning', 'error'];

export function statusBar(projects: ProjectView[], now: number): StatusBar {
  if (projects.length === 0) {
    return { text: `${ICONS.warning} Lineage: no dbt project`, tooltip: 'No dbt_project.yml found', level: 'warning' };
  }
  const described = projects.map((p) => ({ project: p, ...describe(p, now) }));
  const level = described.reduce<Level>(
    (worst, d) => (SEVERITY.indexOf(d.level) > SEVERITY.indexOf(worst) ? d.level : worst), 'ok');
  let phrase: string;
  if (described.length === 1) {
    phrase = described[0].phrase;
  } else {
    const nodes = projects.reduce((sum, p) => sum + (p.status?.counts?.nodes ?? 0), 0);
    const notOk = described.filter((d) => d.level !== 'ok').length;
    phrase = `${projects.length} projects · ${plural(nodes, 'node', 'nodes')}`
      + (notOk ? ` · ${notOk} need attention` : '');
  }
  const tooltip = described
    .map((d) => `${d.project.status?.project.name ?? d.project.root}: ${d.phrase}`)
    .join('\n');
  return { text: `${ICONS[level]} Lineage: ${phrase}`, tooltip, level };
}
