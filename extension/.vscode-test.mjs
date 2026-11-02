// Integration tests in a real VS Code on a copy of the jaffle-shop fixture (task 2.4, Phase 2
// exit). Run with `npx vscode-test --label <label>`:
//
//   trusted           a trusted workspace; the interpreter comes from the user setting
//   restricted        an untrusted workspace (Restricted Mode)
//   path              no setting: Python is found on PATH
//   python-extension  no setting: the Python extension's selected interpreter
//   coverage          trusted, plus the unit tests, against the source build (--coverage)
//
// LP_TEST_PYTHON must name a Python 3.10+ interpreter with nothing installed. LP_EXTENSION_DIR
// tests an unpacked VSIX instead of this folder. LP_VSCODE_PATH uses an existing VS Code.
//
// Every workspace is hostile: its .vscode/settings.json points the interpreter and dbt at
// planted scripts inside it, which leave a marker file if anything ever runs them.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { defineConfig } from '@vscode/test-cli';
import { downloadAndUnzipVSCode } from '@vscode/test-electron';

const root = import.meta.dirname;
const repo = path.resolve(root, '..');
const python = process.env.LP_TEST_PYTHON;
const extensionDir = process.env.LP_EXTENSION_DIR ? path.resolve(process.env.LP_EXTENSION_DIR) : root;
const windows = process.platform === 'win32';

// Run from inside VS Code (an integrated terminal or an agent), these variables leak into the
// test instance and break it (spike S1, finding 12).
for (const name of Object.keys(process.env)) {
  if (name === 'ELECTRON_RUN_AS_NODE' || name.startsWith('VSCODE_')) {
    delete process.env[name];
  }
}

// Short paths: VS Code's IPC socket lives in the user-data dir and macOS caps socket paths at
// 103 characters (S1).
const work = fs.mkdtempSync(path.join(os.tmpdir(), 'lp-'));
const markers = path.join(work, 'markers');
fs.mkdirSync(markers);

function script(file, body) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, body, { mode: 0o755 });
  return file;
}

// An executable that records that it ran, and with which arguments, and then does `then`
function recorder(file, name, then = '') {
  return windows
    ? script(`${file}.cmd`, `@echo %* > "${path.join(markers, name)}"\r\n${then}`)
    : script(file, `#!/bin/sh\necho "$@" > "${path.join(markers, name)}"\n${then}\n`);
}

function workspace(label, hostile = {}) {
  const dir = path.join(work, label, 'jaffle-shop');
  fs.cpSync(path.join(repo, 'fixtures', 'jaffle-shop'), dir, { recursive: true });
  // Source files older than the manifest, so nothing is stale until a test makes it so
  const past = new Date(Date.now() - 3_600_000);
  for (const entry of fs.readdirSync(dir, { recursive: true, withFileTypes: true })) {
    const full = path.join(entry.parentPath, entry.name);
    if (entry.isFile() && !full.includes(`${path.sep}target${path.sep}`)) {
      fs.utimesSync(full, past, past);
    }
  }
  const planted = recorder(path.join(dir, '.venv', windows ? 'Scripts' : 'bin', 'python'), 'planted-python-ran');
  const plantedDbt = recorder(path.join(dir, '.venv', windows ? 'Scripts' : 'bin', 'dbt'), 'planted-dbt-ran');
  fs.mkdirSync(path.join(dir, '.vscode'));
  fs.writeFileSync(path.join(dir, '.vscode', 'settings.json'), JSON.stringify({
    'lineagePilot.python.interpreterPath': planted,
    'lineagePilot.dbt.executablePath': plantedDbt,
    ...hostile,
  }, null, 2));
  // A rule pack that turns off a built-in rule: loaded only when the workspace is trusted
  fs.mkdirSync(path.join(dir, '.lineagepilot'));
  fs.writeFileSync(path.join(dir, '.lineagepilot', 'rules.yml'), 'version: 1\ndisable: [DBT_009]\n');
  return dir;
}

// The dbt that the user configured: records its arguments and touches the manifest
const userDbt = windows
  ? undefined  // Node runs .cmd files only through a shell; the extension refuses them
  : recorder(path.join(work, 'bin', 'dbt'), 'user-dbt-ran', 'touch target/manifest.json');

function userData(label, settings) {
  const dir = path.join(work, label, 'ud');
  fs.mkdirSync(path.join(dir, 'User'), { recursive: true });
  fs.writeFileSync(path.join(dir, 'User', 'settings.json'), JSON.stringify({
    'telemetry.telemetryLevel': 'off',
    'workbench.startupEditor': 'none',
    'extensions.autoUpdate': false,
    ...settings,
  }, null, 2));
  return dir;
}

const pythonVersion = python
  ? execFileSync(python, ['-I', '-c', 'import sys; print("%d.%d" % sys.version_info[:2])'], { encoding: 'utf8' }).trim()
  : undefined;
const version = process.env.LP_VSCODE_VERSION ?? '1.144.0';
const vscode = process.env.LP_VSCODE_PATH
  ? { useInstallation: { fromPath: process.env.LP_VSCODE_PATH } }
  : { version };

// @vscode/test-electron always passes --disable-workspace-trust, which makes every workspace
// trusted. Restricted Mode is tested by starting VS Code through this wrapper, which drops
// that one argument (a .cmd shim on Windows, which test-electron runs through a shell).
async function withWorkspaceTrust() {
  const real = process.env.LP_VSCODE_PATH ?? await downloadAndUnzipVSCode(version);
  const script = path.join(work, 'keep-trust.mjs');
  fs.writeFileSync(script, [
    '#!/usr/bin/env node',
    "import { spawn } from 'node:child_process';",
    `const child = spawn(${JSON.stringify(real)}, process.argv.slice(2).filter((a) => a !== '--disable-workspace-trust'),`,
    "  { stdio: 'inherit' });",
    'child.on(\'exit\', (code, signal) => process.exit(code ?? (signal ? 1 : 0)));',
    '',
  ].join('\n'), { mode: 0o755 });
  if (!windows) {
    return { useInstallation: { fromPath: script } };
  }
  const shim = path.join(work, 'keep-trust.cmd');
  fs.writeFileSync(shim, `@node "${script}" %*\r\n`);
  return { useInstallation: { fromPath: shim } };
}

function config(label, { trusted, settings = {}, env = {}, files, extensions, install = vscode }) {
  // A restricted configuration: VS Code must ignore the workspace's value in Restricted Mode,
  // or nothing would be indexed. (A trusted workspace may set it, so only this one does.)
  const ws = workspace(label, trusted ? {} : { 'lineagePilot.index.maxArtifactMB': 0.001 });
  const ud = userData(label, {
    'security.workspace.trust.enabled': !trusted,
    'security.workspace.trust.startupPrompt': 'never',
    'security.workspace.trust.banner': 'never',
    'lineagePilot.dbt.executablePath': userDbt ?? '',
    ...settings,
  });
  return {
    label,
    ...install,
    extensionDevelopmentPath: extensionDir,
    workspaceFolder: ws,
    files: files ?? ['out/test/integration/*.test.js'],
    launchArgs: [
      '--user-data-dir', ud, '--skip-welcome', '--skip-release-notes', '--disable-gpu',
      // test-cli installs `installExtensions` into test-electron's default extensions folder,
      // so a configuration that needs one keeps that folder; the others get an empty one
      ...(extensions ? [] : ['--extensions-dir', path.join(work, label, 'ext'), '--disable-extensions']),
      ...(process.env.LP_TRACE ? ['--log', 'trace'] : []),
    ],
    installExtensions: extensions,
    mocha: { ui: 'tdd', timeout: 120_000, reporter: path.join(root, 'out', 'test', 'reporter.js') },
    env: {
      LP_LABEL: label,
      LP_MARKERS: markers,
      LP_EXPECT_TRUSTED: trusted ? '1' : '0',
      LP_EXPECT_PYTHON_VERSION: pythonVersion,
      LP_EXTENSION_DIR: extensionDir,
      LP_USER_DBT: userDbt ?? '',
      LP_TEST_LOG: process.env.LP_TEST_LOG ?? path.join(work, `${label}-results.log`),
      ...env,
    },
  };
}

const withSetting = { 'lineagePilot.python.interpreterPath': python };

export default defineConfig({
  tests: [
    config('trusted', { trusted: true, settings: withSetting, env: { LP_EXPECT_SOURCE: 'setting' } }),
    config('restricted', {
      trusted: false, settings: withSetting, env: { LP_EXPECT_SOURCE: 'setting' }, install: await withWorkspaceTrust(),
    }),
    config('path', {
      trusted: true,
      // Which python3 wins is not ours to choose: on macOS VS Code rebuilds PATH from the login
      // shell (S1, finding 8). The test checks the choice matches a lookup in the extension host.
      env: { LP_EXPECT_SOURCE: 'path', LP_EXPECT_PYTHON_VERSION: undefined, LP_EXPECT_PATH_LOOKUP: '1',
        PATH: python ? `${path.dirname(python)}${path.delimiter}${process.env.PATH}` : process.env.PATH },
    }),
    config('python-extension', {
      trusted: true,
      settings: { 'python.defaultInterpreterPath': python },
      env: { LP_EXPECT_SOURCE: 'python-extension' },
      extensions: ['ms-python.python'],
    }),
    config('coverage', {
      trusted: true,
      settings: withSetting,
      env: { LP_EXPECT_SOURCE: 'setting' },
      files: ['out/test/unit/*.test.js', 'out/test/integration/*.test.js'],
    }),
  ],
  // For the coverage label, dist/extension.js loads out/ (scripts/coverage_shim.mjs). test-cli
  // matches these against the absolute paths of the scripts that ran, before source maps take
  // them back to src/*.ts. protocol.js is generated.
  coverage: {
    include: ['**/out/**/*.js'],
    exclude: ['**/out/test/**', '**/out/protocol.js', '**/node_modules/**'],
    reporter: ['text-summary', 'json-summary', 'html'],
  },
});
