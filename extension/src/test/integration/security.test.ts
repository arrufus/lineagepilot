// Interpreter choice and Workspace Trust (tasks 2.4 and 2.5; the Phase 2 exit's restricted-mode
// tests). Every workspace is hostile: its settings point the interpreter and dbt at planted
// scripts inside it that record if they ever run.
import { strict as assert } from 'assert';
import { execFileSync } from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as vscode from 'vscode';

import type { RefreshOutcome } from '../../dbt';
import { PROBE_SCRIPT } from '../../interpreter';
import { api, codeOf, expectTrusted, label, ran, readyServer, root, until, uri } from './helpers';

const expectedSource = process.env.LP_EXPECT_SOURCE;
const expectedVersion = process.env.LP_EXPECT_PYTHON_VERSION;
const extensionDir = process.env.LP_EXTENSION_DIR ?? '';

function inside(file: string, directory: string): boolean {
  const relative = path.relative(fs.realpathSync(directory), fs.realpathSync(file));
  return !relative.startsWith('..') && !path.isAbsolute(relative);
}

// The same candidates, in the same order, as pathCandidates() in src/interpreter.ts
function firstPythonOnPath(): string {
  const candidates = process.platform === 'win32' ? [['py', '-3'], ['python']] : [['python3'], ['python']];
  for (const [command, ...args] of candidates) {
    try {
      return execFileSync(command, [...args, '-I', '-c', 'import sys; print(sys.executable)'],
        { encoding: 'utf8', windowsHide: true }).trim();
    } catch {
      // try the next one
    }
  }
  throw new Error('no Python on PATH in the extension host');
}

suite(`interpreter (${label})`, () => {
  test('the server runs on the expected interpreter, isolated, from the bundled libraries', async () => {
    const server = await readyServer();
    const { manager } = await api();
    const python = server.status.server;
    if (expectedSource) {
      assert.equal(manager.interpreter?.source, expectedSource);
    }
    if (expectedVersion) {
      assert.ok(python.python.startsWith(`${expectedVersion}.`), python.python);
    }
    if (process.env.LP_EXPECT_PATH_LOOKUP) {
      assert.equal(fs.realpathSync(python.executable), fs.realpathSync(firstPythonOnPath()));
    }
    assert.equal(python.isolated, true);
    const libs = path.join(extensionDir, 'bundled', 'libs');
    for (const name of ['lineagepilot_lsp', 'lineagepilot_core', 'pygls', 'sqlglot', 'yaml']) {
      assert.ok(inside(python.modules[name], libs), `${name} was imported from ${python.modules[name]}`);
    }
  });

  test('workspace settings cannot choose the interpreter or dbt', async () => {
    const server = await readyServer();
    const config = vscode.workspace.getConfiguration('lineagePilot');
    // Machine-scoped: VS Code drops the workspace's values (S1, finding 6)
    assert.equal(config.inspect('python.interpreterPath')?.workspaceValue, undefined);
    assert.equal(config.inspect('dbt.executablePath')?.workspaceValue, undefined);
    assert.equal(config.inspect('trace.server')?.workspaceValue, undefined);
    assert.ok(!inside(server.status.server.executable, root()));
    // In a trusted workspace the Python extension runs the workspace's .venv to identify it;
    // what must never happen is LineagePilot probing it or starting its server with it.
    const ours = (ran('planted-python-ran') ?? '').split(/\r?\n/)
      .filter((line) => line.includes(PROBE_SCRIPT) || line.includes('lsp_server.py'));
    assert.deepEqual(ours, [], 'LineagePilot ran the planted interpreter');
  });
});

(expectTrusted ? suite : suite.skip)('a trusted workspace', () => {
  test('is trusted, and loads the project rule pack', async () => {
    assert.equal(vscode.workspace.isTrusted, true);
    const server = await readyServer();
    assert.equal(server.status.trusted, true);
    assert.equal(server.status.rules.packLoaded, true);
    // The pack turns DBT_009 off
    const customers = uri('models', 'marts', 'customers.yml');
    await until('diagnostics', 60_000, () => vscode.languages.getDiagnostics(customers).length > 0);
    const codes = vscode.languages.getDiagnostics(customers).map((d) => codeOf(d));
    assert.ok(!codes.includes('DBT_009'), codes.join());
  });

  test("Refresh dbt Artifacts runs the user's dbt, never the workspace's", async function () {
    if (!process.env.LP_USER_DBT) {
      this.skip();  // Windows: the fake dbt would be a .cmd file, which the extension refuses
    }
    const outcome = await vscode.commands.executeCommand<RefreshOutcome>('lineagePilot.refreshArtifacts', 'parse', root());
    assert.equal(outcome.status, 'done');
    assert.equal(ran('user-dbt-ran'), `parse --project-dir ${root()}`);
    assert.equal(ran('planted-dbt-ran'), undefined, 'the workspace dbt ran');
  });
});

suite('DAG files', () => {
  test('a DAG file is read and never run, trusted or not', async function () {
    this.timeout(120_000);
    const server = await readyServer();
    await until('the DAG files to be read', 100_000, () => server.status.airflow.stage === 'done');
    assert.ok(server.status.airflow.summary.dagFiles >= 1, 'no DAG file was read');
    assert.equal(ran('planted-dag-ran'), undefined, 'a DAG file was imported or run');
  });
});

suite('opening files', () => {
  test('Open Node opens files in the workspace only (audit LP-SEC-04)', async () => {
    await readyServer();
    const outsideDir = fs.mkdtempSync(path.join(os.tmpdir(), 'lp-outside-'));
    const outsideFile = path.join(outsideDir, 'secret.sql');
    fs.writeFileSync(outsideFile, 'select 1\n');
    const opened = (file: string) => vscode.workspace.textDocuments.some((d) => d.uri.fsPath === file);
    for (const location of [
      { uri: vscode.Uri.file(outsideFile).toString(), range: null },
      { uri: 'untitled:x', range: null },
      { uri: 42 },
      undefined,
    ]) {
      await vscode.commands.executeCommand('lineagePilot.openNode', location);
    }
    assert.equal(opened(outsideFile), false, 'a file outside the workspace was opened');
    const inside = uri('models', 'marts', 'customers.yml');
    await vscode.commands.executeCommand('lineagePilot.openNode', { uri: inside.toString(), range: null });
    assert.equal(opened(inside.fsPath), true, 'a file in the workspace was not opened');
    fs.rmSync(outsideDir, { recursive: true, force: true });
  });
});

(expectTrusted ? suite.skip : suite)('Restricted Mode', () => {
  test('the workspace is not trusted, and the lineage still works', async () => {
    assert.equal(vscode.workspace.isTrusted, false);
    const server = await readyServer();
    assert.equal(server.status.trusted, false);
    // The workspace tried to shrink the artifact limit to 1 KB; in Restricted Mode VS Code
    // ignores it (restrictedConfigurations), so the project was indexed.
    assert.equal(server.status.state, 'ready');
  });

  test('dbt cannot run', async () => {
    const outcome = await vscode.commands.executeCommand<RefreshOutcome>('lineagePilot.refreshArtifacts', 'parse', root());
    assert.deepEqual(outcome, { status: 'refused', reason: 'restricted' });
    assert.equal(ran('user-dbt-ran'), undefined, "the user's dbt ran");
    assert.equal(ran('planted-dbt-ran'), undefined, 'the workspace dbt ran');
  });

  test('only built-in rules run, and the rule pack file says so', async () => {
    const server = await readyServer();
    assert.deepEqual([server.status.rules.restricted, server.status.rules.packLoaded], [true, false]);
    const customers = uri('models', 'marts', 'customers.yml');
    await until('DBT_009, which the unloaded pack would turn off', 60_000, () =>
      vscode.languages.getDiagnostics(customers).some((d) => codeOf(d) === 'DBT_009'));
    const pack = uri('.lineagepilot', 'rules.yml');
    const [note] = await until('the note on the pack', 30_000, () => {
      const found = vscode.languages.getDiagnostics(pack);
      return found.length ? found : undefined;
    });
    assert.equal(codeOf(note), 'restricted-mode');
  });
});
