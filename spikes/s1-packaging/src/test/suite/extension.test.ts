// Integration tests that run inside a real VS Code against the fixture workspace.
// scripts/run_integration.mjs sets LP_EXPECT_SOURCE and LP_EXPECT_PYTHON_VERSION per run.
import { strict as assert } from 'assert';
import { execFileSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as vscode from 'vscode';

import type { SpikeStatus } from '../../extension';

const EXTENSION_ID = 'lineagepilot.lineagepilot-spike';
const PLANTED_INTERPRETER = '/nonexistent/workspace-chosen-python';
const expectedSource = process.env.LP_EXPECT_SOURCE;
const expectedVersion = process.env.LP_EXPECT_PYTHON_VERSION;

interface ServerStatus {
  python: { version: string; executable: string; isolated: boolean };
  libs: Record<'pygls' | 'sqlglot' | 'yaml', { file: string; withLibyaml?: boolean }>;
}

async function until<T>(what: string, timeoutMs: number, fn: () => Promise<T | undefined>): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn();
    if (value !== undefined) {
      return value;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`timed out after ${timeoutMs} ms waiting for ${what}`);
}

// The same candidates, in the same order, as pathCandidates() in src/interpreter.ts.
function firstPythonOnPath(): string {
  const candidates = process.platform === 'win32' ? [['py', '-3'], ['python']] : [['python3'], ['python']];
  for (const [command, ...args] of candidates) {
    try {
      return execFileSync(command, [...args, '-I', '-c', 'import sys; print(sys.executable)'],
        { encoding: 'utf8', windowsHide: true }).trim();
    } catch {
      // try the next candidate
    }
  }
  throw new Error('no Python on PATH in the extension host');
}

function inside(file: string, directory: string): boolean {
  const relative = path.relative(fs.realpathSync(directory), fs.realpathSync(file));
  return !relative.startsWith('..') && !path.isAbsolute(relative);
}

async function status(): Promise<SpikeStatus & { server: ServerStatus }> {
  return until('server status', 60_000, async () => {
    const result = await vscode.commands.executeCommand<SpikeStatus>('lineagePilot.showStatus');
    return result?.server ? (result as SpikeStatus & { server: ServerStatus }) : undefined;
  });
}

suite('S1 packaging spike', () => {
  let folder: vscode.Uri;

  suiteSetup(async () => {
    folder = vscode.workspace.workspaceFolders![0].uri;
    const extension = vscode.extensions.getExtension(EXTENSION_ID);
    assert.ok(extension, `${EXTENSION_ID} is not installed`);
    await extension.activate();
  });

  test('the bundled server returns a CodeLens for a dbt model', async () => {
    const uri = vscode.Uri.joinPath(folder, 'models', 'orders.sql');
    await vscode.window.showTextDocument(uri);
    const lens = await until('the LineagePilot CodeLens', 60_000, async () => {
      const lenses = await vscode.commands.executeCommand<vscode.CodeLens[]>('vscode.executeCodeLensProvider', uri);
      return lenses?.find((l) => l.command?.title.startsWith('LineagePilot spike'));
    });
    const title = lens.command!.title;
    assert.match(title, /2 upstream refs, 2 tables \| spike_shop \| Python \d+\.\d+\.\d+$/);
    if (expectedVersion) {
      assert.ok(title.includes(`Python ${expectedVersion}.`), title);
    }
  });

  test('the server runs on the expected interpreter, from the bundled libraries', async () => {
    const result = await status();
    console.log(`[S1] interpreter ${JSON.stringify(result.interpreter)}`);
    console.log(`[S1] server python ${JSON.stringify(result.server.python)}`);
    if (expectedSource) {
      assert.equal(result.interpreter?.source, expectedSource);
    }
    if (expectedVersion) {
      assert.ok(result.server.python.version.startsWith(`${expectedVersion}.`), result.server.python.version);
    }
    if (process.env.LP_EXPECT_PATH_LOOKUP) {
      const found = firstPythonOnPath();
      console.log(`[S1] PATH lookup in the extension host finds ${found}`);
      assert.equal(fs.realpathSync(result.server.python.executable), fs.realpathSync(found));
    }
    assert.equal(result.server.python.isolated, true);
    const libs = path.join(result.extensionPath, 'bundled', 'libs');
    for (const name of ['pygls', 'sqlglot', 'yaml'] as const) {
      assert.ok(inside(result.server.libs[name].file, libs), `${name} loaded from ${result.server.libs[name].file}`);
    }
    assert.equal(result.server.libs.yaml.withLibyaml, false);
  });

  test('a workspace setting cannot choose the interpreter', async () => {
    const inspected = vscode.workspace.getConfiguration('lineagePilot').inspect<string>('python.interpreterPath');
    console.log(`[S1] workspaceValue as reported by VS Code: ${JSON.stringify(inspected?.workspaceValue)}`);
    const result = await status();
    assert.notEqual(result.interpreter?.executable, PLANTED_INTERPRETER);
    assert.notEqual(result.server.python.executable, PLANTED_INTERPRETER);
  });
});
