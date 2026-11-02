// Interpreter resolution for the language server (design section 4.4), kept free of the vscode
// module so it can be unit-tested. In order:
//   1. the machine-scoped lineagePilot.python.interpreterPath setting (an unusable value is an
//      error, never silently replaced);
//   2. the Python extension's active environment;
//   3. python3 then python on PATH (py -3 then python on Windows);
//   4. a Python already installed by uv, then, with the user's consent, one uv downloads.
// In an untrusted workspace no candidate inside a workspace folder is ever run, not even to
// probe its version: candidates are resolved to absolute paths first, and the executable the
// probe reports is checked again afterwards (spike S1, finding 5).

import * as path from 'path';

export const MIN_VERSION: readonly [number, number] = [3, 10];

export type Source = 'setting' | 'python-extension' | 'path' | 'uv';

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
  // An absolute path for a command name, from PATH, without running anything
  which(command: string): string | undefined;
  probe(candidate: Candidate): Promise<Probe | undefined>;
  // A uv-managed Python 3.10+ already installed, found without downloading
  uvFind(): Promise<string | undefined>;
  // Ask the user, and if they agree, have uv download one; undefined if declined or failed
  uvInstall(): Promise<string | undefined>;
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
  const p = deps.platform === 'win32' ? path.win32 : path.posix;
  const inWorkspace = (file: string) =>
    deps.workspaceFolders.some((folder) => isInside(file, folder, deps.platform));

  const setting = deps.settingPath()?.trim();
  if (setting) {
    const command = p.isAbsolute(setting) ? setting : deps.which(setting);
    if (!command) {
      return { attempts, error: `lineagePilot.python.interpreterPath (${setting}) was not found.` };
    }
    // A user setting is a deliberate choice; it is still never run from inside an untrusted
    // workspace, because the user may have pointed it at a project's .venv.
    const insideMessage = (file: string) =>
      `lineagePilot.python.interpreterPath (${file}) is inside the workspace, which is not trusted. `
      + 'Trust the workspace or choose an interpreter outside it.';
    if (!deps.isTrusted && inWorkspace(command)) {
      return { attempts, error: insideMessage(command) };
    }
    const probe = await deps.probe({ command, args: [], source: 'setting' });
    if (!probe) {
      return { attempts, error: `lineagePilot.python.interpreterPath (${setting}) did not run as Python.` };
    }
    if (!deps.isTrusted && inWorkspace(probe.executable)) {
      return { attempts, error: insideMessage(probe.executable) };
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

  const tryCandidate = async (candidate: Candidate): Promise<Interpreter | undefined> => {
    const label = `${candidate.source}: ${[candidate.command, ...candidate.args].join(' ')}`;
    const command = p.isAbsolute(candidate.command) ? candidate.command : deps.which(candidate.command);
    if (!command) {
      attempts.push(`${label}: not found`);
      return undefined;
    }
    // A cloned repository can commit its own .venv, which the Python extension may select,
    // or put a directory of its own on PATH. In restricted mode that is never run.
    if (!deps.isTrusted && inWorkspace(command)) {
      attempts.push(`${label}: skipped, ${command} is inside the workspace and the workspace is not trusted`);
      return undefined;
    }
    const probe = await deps.probe({ ...candidate, command });
    if (!probe) {
      attempts.push(`${label}: did not run as Python`);
      return undefined;
    }
    if (!deps.isTrusted && inWorkspace(probe.executable)) {
      attempts.push(`${label}: skipped, resolves to ${probe.executable} inside the untrusted workspace`);
      return undefined;
    }
    if (!meetsMinimum(probe.version)) {
      attempts.push(`${label}: Python ${versionText(probe.version)} is older than ${versionText(MIN_VERSION)}`);
      return undefined;
    }
    return { ...probe, source: candidate.source };
  };

  for (const candidate of candidates) {
    const found = await tryCandidate(candidate);
    if (found) {
      return { interpreter: found, attempts };
    }
  }
  for (const [label, lookup] of [['uv: installed', deps.uvFind], ['uv: downloaded', deps.uvInstall]] as const) {
    const executable = await lookup();
    if (!executable) {
      attempts.push(`${label}: none`);
      continue;
    }
    const found = await tryCandidate({ command: executable, args: [], source: 'uv' });
    if (found) {
      return { interpreter: found, attempts };
    }
  }
  return { attempts };
}
