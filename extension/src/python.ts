// Finding Python for the language server: the VS Code side of interpreter.ts (task 2.4).

import { execFile } from 'child_process';
import * as os from 'os';
import * as path from 'path';
import * as vscode from 'vscode';
import { PythonExtension } from '@vscode/python-extension';

import {
  Candidate, isInside, parseProbe, Probe, PROBE_SCRIPT, Resolution, resolveInterpreter,
} from './interpreter';
import { which } from './which';

const PROBE_TIMEOUT_MS = 10_000;
const UV_FIND_TIMEOUT_MS = 30_000;
const UV_INSTALL_TIMEOUT_MS = 10 * 60_000;
// The version uv downloads when the user agrees to it
export const UV_PYTHON_VERSION = '3.13';

let uvDeclined = false;

// Machine-scoped, and only the user (global) value is read, so a workspace's .vscode/settings.json
// can never choose the executable even if VS Code passed a workspace value through.
export function userSetting(key: 'python.interpreterPath' | 'dbt.executablePath'): string | undefined {
  return vscode.workspace.getConfiguration('lineagePilot').inspect<string>(key)?.globalValue;
}

export function onPath(command: string): string | undefined {
  return which(command, { env: process.env, platform: process.platform });
}

async function pythonExtensionPath(output: vscode.LogOutputChannel, folder?: vscode.Uri): Promise<string | undefined> {
  if (!vscode.extensions.getExtension('ms-python.python')) {
    return undefined;
  }
  try {
    const api = await PythonExtension.api();
    const environment = await api.environments.resolveEnvironment(
      api.environments.getActiveEnvironmentPath(folder));
    return environment?.executable.uri?.fsPath;
  } catch (error) {
    output.warn(`Python extension did not provide an interpreter: ${error}`);
    return undefined;
  }
}

function run(command: string, args: string[], options: { timeout: number; env?: NodeJS.ProcessEnv; cwd?: string })
  : Promise<string | undefined> {
  return new Promise((resolve) => {
    execFile(command, args, { ...options, windowsHide: true }, (error, stdout) =>
      resolve(error ? undefined : stdout));
  });
}

async function probe(candidate: Candidate): Promise<Probe | undefined> {
  const stdout = await run(candidate.command, [...candidate.args, '-I', '-c', PROBE_SCRIPT],
    { timeout: PROBE_TIMEOUT_MS });
  return stdout === undefined ? undefined : parseProbe(stdout);
}

/** uv's executable: on PATH, or where its installer puts it. */
export function findUv(): string | undefined {
  const name = process.platform === 'win32' ? 'uv.exe' : 'uv';
  return onPath('uv') ?? [path.join(os.homedir(), '.local', 'bin', name), path.join(os.homedir(), '.cargo', 'bin', name)]
    .find((candidate) => which(candidate, { env: process.env, platform: process.platform }));
}

function uvEnvironment(downloads: boolean): NodeJS.ProcessEnv {
  // Only uv's own Pythons, and never a project's virtual environment
  const env: NodeJS.ProcessEnv = {
    ...process.env, UV_PYTHON_PREFERENCE: 'only-managed', UV_PYTHON_DOWNLOADS: downloads ? 'automatic' : 'never',
  };
  delete env.VIRTUAL_ENV;
  delete env.CONDA_PREFIX;
  return env;
}

export interface ResolveOptions {
  output: vscode.LogOutputChannel;
  extensionPath: string;
  folder?: vscode.Uri;
  // Whether to offer a uv download when nothing else is found; off for background restarts
  offerDownload: boolean;
}

export async function resolvePython(options: ResolveOptions): Promise<Resolution> {
  const folders = (vscode.workspace.workspaceFolders ?? []).map((f) => f.uri.fsPath);
  const isTrusted = vscode.workspace.isTrusted;
  const usableUv = (): string | undefined => {
    const uv = findUv();
    // uv itself is run, so the same rule as for interpreters applies
    return uv && (isTrusted || !folders.some((f) => isInside(uv, f, process.platform))) ? uv : undefined;
  };
  const uvFind = async (): Promise<string | undefined> => {
    const uv = usableUv();
    if (!uv) {
      return undefined;
    }
    const stdout = await run(uv, ['python', 'find', '--no-project', '>=3.10'],
      { timeout: UV_FIND_TIMEOUT_MS, env: uvEnvironment(false), cwd: options.extensionPath });
    return stdout?.trim().split(/\r?\n/).pop() || undefined;
  };
  const uvInstall = async (): Promise<string | undefined> => {
    const uv = usableUv();
    if (!options.offerDownload || uvDeclined || !uv) {
      return undefined;
    }
    const download = `Download Python ${UV_PYTHON_VERSION} with uv`;
    const answer = await vscode.window.showWarningMessage(
      'LineagePilot needs Python 3.10 or later and found none. uv can download a private copy '
      + `(about 30 MB) into its own folder. Set lineagePilot.python.interpreterPath to use another.`,
      download, 'Not Now');
    if (answer !== download) {
      uvDeclined = true;
      return undefined;
    }
    const installed = await vscode.window.withProgress(
      { location: vscode.ProgressLocation.Notification, title: `Downloading Python ${UV_PYTHON_VERSION} with uv` },
      () => run(uv, ['python', 'install', UV_PYTHON_VERSION],
        { timeout: UV_INSTALL_TIMEOUT_MS, env: uvEnvironment(true), cwd: options.extensionPath }));
    if (installed === undefined) {
      options.output.error('uv could not download Python; see `uv python install` for details');
      return undefined;
    }
    return uvFind();
  };
  return resolveInterpreter({
    settingPath: () => userSetting('python.interpreterPath'),
    pythonExtensionPath: () => pythonExtensionPath(options.output, options.folder),
    which: onPath,
    probe,
    uvFind,
    uvInstall,
    platform: process.platform,
    isTrusted,
    workspaceFolders: folders,
  });
}
