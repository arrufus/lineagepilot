// Interpreter resolution for the language server (design section 4.4), kept free of the vscode
// module so it can be unit-tested. Order: the machine-scoped setting, then the Python extension's
// active environment, then python on PATH. The uv-managed fallback is not part of the spike.

import * as path from 'path';

export const MIN_VERSION: readonly [number, number] = [3, 10];

export type Source = 'setting' | 'python-extension' | 'path';

export interface Candidate {
  command: string;
  args: string[];
  source: Source;
}

export interface Probe {
  executable: string;
  version: [number, number, number];
}

export interface Interpreter extends Probe {
  source: Source;
}

export interface ResolverDeps {
  settingPath(): string | undefined;
  pythonExtensionPath(): Promise<string | undefined>;
  probe(candidate: Candidate): Promise<Probe | undefined>;
  platform: NodeJS.Platform;
  isTrusted: boolean;
  workspaceFolders: string[];
}

export interface Resolution {
  interpreter?: Interpreter;
  // Why each rejected candidate was skipped, for the output channel and error messages.
  attempts: string[];
  // Set when an explicit setting is unusable: an explicit choice is never silently replaced.
  error?: string;
}

// The Python script run by probe(). It prints one JSON line and imports nothing from the workspace,
// because the probe also runs with -I.
export const PROBE_SCRIPT =
  'import json,sys;print(json.dumps({"executable":sys.executable,"version":list(sys.version_info[:3])}))';

export function pathCandidates(platform: NodeJS.Platform): Candidate[] {
  if (platform === 'win32') {
    return [
      { command: 'py', args: ['-3'], source: 'path' },
      { command: 'python', args: [], source: 'path' },
    ];
  }
  return [
    { command: 'python3', args: [], source: 'path' },
    { command: 'python', args: [], source: 'path' },
  ];
}

export function parseProbe(stdout: string): Probe | undefined {
  const line = stdout.trim().split(/\r?\n/).pop() ?? '';
  try {
    const data = JSON.parse(line);
    const version = data.version;
    if (typeof data.executable !== 'string' || !Array.isArray(version) || version.length !== 3
        || !version.every((n: unknown) => Number.isInteger(n))) {
      return undefined;
    }
    return { executable: data.executable, version: version as [number, number, number] };
  } catch {
    return undefined;
  }
}

export function meetsMinimum(version: readonly number[]): boolean {
  return version[0] > MIN_VERSION[0] || (version[0] === MIN_VERSION[0] && version[1] >= MIN_VERSION[1]);
}

// True when child is parent or lies under it. macOS and Windows file systems are usually
// case-insensitive, so the comparison is too: a false "inside" only makes the check stricter.
export function isInside(child: string, parent: string, platform: NodeJS.Platform): boolean {
  const p = platform === 'win32' ? path.win32 : path.posix;
  const fold = (s: string) => (platform === 'linux' ? s : s.toLowerCase());
  const relative = p.relative(fold(p.resolve(parent)), fold(p.resolve(child)));
  return relative === '' || (!relative.startsWith('..') && !p.isAbsolute(relative));
}

function versionText(version: readonly number[]): string {
  return version.join('.');
}

export async function resolveInterpreter(deps: ResolverDeps): Promise<Resolution> {
  const attempts: string[] = [];
  const inWorkspace = (file: string) =>
    deps.workspaceFolders.some((folder) => isInside(file, folder, deps.platform));

  const setting = deps.settingPath()?.trim();
  if (setting) {
    const probe = await deps.probe({ command: setting, args: [], source: 'setting' });
    if (!probe) {
      return { attempts, error: `lineagePilot.python.interpreterPath (${setting}) did not run as Python.` };
    }
    if (!meetsMinimum(probe.version)) {
      return {
        attempts,
        error: `lineagePilot.python.interpreterPath is Python ${versionText(probe.version)}; `
          + `LineagePilot needs ${versionText(MIN_VERSION)} or later.`,
      };
    }
    return { interpreter: { ...probe, source: 'setting' }, attempts };
  }

  const candidates: Candidate[] = [];
  const fromExtension = await deps.pythonExtensionPath();
  if (fromExtension) {
    candidates.push({ command: fromExtension, args: [], source: 'python-extension' });
  }
  candidates.push(...pathCandidates(deps.platform));

  for (const candidate of candidates) {
    const label = `${candidate.source}: ${[candidate.command, ...candidate.args].join(' ')}`;
    // A cloned repository can commit its own .venv, which the Python extension may select.
    // In restricted mode that interpreter is never run, not even to probe it.
    if (!deps.isTrusted && path.isAbsolute(candidate.command) && inWorkspace(candidate.command)) {
      attempts.push(`${label}: skipped, inside the workspace and the workspace is not trusted`);
      continue;
    }
    const probe = await deps.probe(candidate);
    if (!probe) {
      attempts.push(`${label}: not found or did not run`);
      continue;
    }
    if (!deps.isTrusted && inWorkspace(probe.executable)) {
      attempts.push(`${label}: skipped, resolves to ${probe.executable} inside the untrusted workspace`);
      continue;
    }
    if (!meetsMinimum(probe.version)) {
      attempts.push(`${label}: Python ${versionText(probe.version)} is older than ${versionText(MIN_VERSION)}`);
      continue;
    }
    return { interpreter: { ...probe, source: candidate.source }, attempts };
  }
  return { attempts };
}
