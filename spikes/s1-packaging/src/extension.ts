// S1 spike extension: resolve a Python 3.10+ interpreter, start the bundled pygls server over
// stdio, and expose its status. Everything else in the design waits for Phase 2.

import { execFile } from 'child_process';
import * as path from 'path';
import * as vscode from 'vscode';
import { PythonExtension } from '@vscode/python-extension';
import { LanguageClient, LanguageClientOptions, ServerOptions, TransportKind } from 'vscode-languageclient/node';

import { Candidate, Interpreter, parseProbe, Probe, PROBE_SCRIPT, resolveInterpreter } from './interpreter';

const PROBE_TIMEOUT_MS = 10_000;

let client: LanguageClient | undefined;
let interpreter: Interpreter | undefined;
let output: vscode.LogOutputChannel;
let statusItem: vscode.StatusBarItem;

export interface SpikeStatus {
  interpreter?: Interpreter;
  server?: unknown;
  extensionPath: string;
}

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  output = vscode.window.createOutputChannel('LineagePilot', { log: true });
  statusItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left);
  statusItem.command = 'lineagePilot.showStatus';
  context.subscriptions.push(
    output,
    statusItem,
    vscode.commands.registerCommand('lineagePilot.showStatus', () => showStatus(context)),
    vscode.commands.registerCommand('lineagePilot.restartServer', () => restart(context)),
    vscode.workspace.onDidChangeConfiguration((e) => {
      if (e.affectsConfiguration('lineagePilot.python')) {
        void restart(context);
      }
    }),
    vscode.workspace.onDidGrantWorkspaceTrust(() => void restart(context)),
    { dispose: () => void client?.stop() },
  );
  await start(context);
}

export async function deactivate(): Promise<void> {
  await client?.stop();
  client = undefined;
}

async function start(context: vscode.ExtensionContext): Promise<void> {
  const resolution = await resolveInterpreter({
    settingPath,
    pythonExtensionPath,
    probe,
    platform: process.platform,
    isTrusted: vscode.workspace.isTrusted,
    workspaceFolders: (vscode.workspace.workspaceFolders ?? []).map((f) => f.uri.fsPath),
  });
  resolution.attempts.forEach((a) => output.info(`interpreter ${a}`));
  interpreter = resolution.interpreter;
  if (!interpreter) {
    const message = resolution.error
      ?? 'LineagePilot needs Python 3.10 or later. Set lineagePilot.python.interpreterPath or install Python.';
    output.error(message);
    statusItem.text = '$(error) LineagePilot: no Python';
    statusItem.show();
    void vscode.window.showErrorMessage(message);
    return;
  }
  output.info(`using Python ${interpreter.version.join('.')} at ${interpreter.executable} (${interpreter.source})`);

  const serverScript = context.asAbsolutePath(path.join('bundled', 'tool', 'lsp_server.py'));
  const serverOptions: ServerOptions = {
    command: interpreter.executable,
    // -I: nothing from the workspace, the user site-packages or PYTHON* variables can shadow
    // the bundled libraries. The working directory is the extension's, never the workspace.
    args: ['-I', serverScript],
    transport: TransportKind.stdio,
    options: { cwd: path.dirname(serverScript) },
  };
  const clientOptions: LanguageClientOptions = {
    documentSelector: [{ scheme: 'file', pattern: '**/*.sql' }],
    outputChannel: output,
  };
  client = new LanguageClient('lineagePilot', 'LineagePilot', serverOptions, clientOptions);
  await client.start();
  statusItem.text = `$(pulse) LineagePilot: Python ${interpreter.version.slice(0, 2).join('.')}`;
  statusItem.show();
}

async function restart(context: vscode.ExtensionContext): Promise<void> {
  await client?.stop();
  client = undefined;
  await start(context);
}

async function showStatus(context: vscode.ExtensionContext): Promise<SpikeStatus> {
  // Always send a params object: pygls 2.1 cannot decode a request whose params field is absent,
  // and it fails without answering, so the request would hang.
  const server = client?.isRunning() ? await client.sendRequest('lineage/status', {}) : undefined;
  return { interpreter, server, extensionPath: context.extensionPath };
}

// Machine-scoped, and only the user (global) value is read, so a workspace's .vscode/settings.json
// can never choose the executable even if VS Code passed a workspace value through.
function settingPath(): string | undefined {
  const inspected = vscode.workspace.getConfiguration('lineagePilot').inspect<string>('python.interpreterPath');
  return inspected?.globalValue;
}

async function pythonExtensionPath(): Promise<string | undefined> {
  if (!vscode.extensions.getExtension('ms-python.python')) {
    return undefined;
  }
  try {
    const api = await PythonExtension.api();
    const folder = vscode.workspace.workspaceFolders?.[0]?.uri;
    const environment = await api.environments.resolveEnvironment(api.environments.getActiveEnvironmentPath(folder));
    return environment?.executable.uri?.fsPath;
  } catch (error) {
    output.warn(`Python extension did not provide an interpreter: ${error}`);
    return undefined;
  }
}

function probe(candidate: Candidate): Promise<Probe | undefined> {
  return new Promise((resolve) => {
    execFile(candidate.command, [...candidate.args, '-I', '-c', PROBE_SCRIPT],
      { timeout: PROBE_TIMEOUT_MS, windowsHide: true },
      (error, stdout) => resolve(error ? undefined : parseProbe(stdout)));
  });
}
