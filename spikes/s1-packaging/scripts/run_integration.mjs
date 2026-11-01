// Runs the integration suite in a real VS Code against the fixture workspace.
//
//   node scripts/run_integration.mjs --python <exe> [--source setting|path|python-extension]
//                                    [--vsix dist/lineagepilot-spike.vsix] [--vscode stable]
//
// --source picks how the extension should find the interpreter:
//   setting           lineagePilot.python.interpreterPath in user settings; other extensions disabled
//   path              no setting; the interpreter's directory is put first on PATH; other extensions disabled
//   python-extension  ms-python.python is installed with python.defaultInterpreterPath set
// --vsix tests the packaged extension (unzipped) instead of the source folder.
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { downloadAndUnzipVSCode, resolveCliArgsFromVSCodeExecutablePath, runTests } from '@vscode/test-electron';

const root = path.resolve(import.meta.dirname, '..');
const { values: opts } = parseArgs({
  options: {
    python: { type: 'string' },
    source: { type: 'string', default: 'setting' },
    vsix: { type: 'string' },
    vscode: { type: 'string', default: 'stable' },
  },
});
if (!opts.python || !['setting', 'path', 'python-extension'].includes(opts.source)) {
  console.error('usage: run_integration.mjs --python <exe> [--source setting|path|python-extension] [--vsix file]');
  process.exit(2);
}

// Run from inside VS Code (an integrated terminal or an agent), these variables leak into the test
// instance: ELECTRON_RUN_AS_NODE turns it into plain Node, and VSCODE_IPC_HOOK and friends make it
// try to join the parent window. Clear them so the test VS Code starts clean.
for (const name of Object.keys(process.env)) {
  if (name === 'ELECTRON_RUN_AS_NODE' || name.startsWith('VSCODE_')) {
    delete process.env[name];
  }
}

const python = opts.python;
const version = execFileSync(python, ['-I', '-c', 'import sys; print("%d.%d" % sys.version_info[:2])'], { encoding: 'utf8' }).trim();
// Short paths: VS Code's IPC socket lives in the user-data dir and macOS caps socket paths at 103 chars.
const work = fs.mkdtempSync(path.join(os.tmpdir(), 'lp-'));
const userDataDir = path.join(work, 'ud');
const extensionsDir = path.join(work, 'ext');
fs.mkdirSync(path.join(userDataDir, 'User'), { recursive: true });

let extensionDevelopmentPath = root;
if (opts.vsix) {
  const unpacked = path.join(work, 'vsix');
  // Python is a prerequisite anyway, and its zipfile works the same on every OS.
  execFileSync(python, ['-I', '-c', 'import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])',
    path.resolve(opts.vsix), unpacked]);
  extensionDevelopmentPath = path.join(unpacked, 'extension');
}

const vscodeExecutablePath = await downloadAndUnzipVSCode(opts.vscode);
const settings = { 'security.workspace.trust.enabled': false, 'telemetry.telemetryLevel': 'off' };
const launchArgs = [path.join(root, 'fixture-workspace'), '--user-data-dir', userDataDir,
  '--extensions-dir', extensionsDir, '--skip-welcome', '--skip-release-notes', '--disable-gpu'];
const extensionTestsEnv = { LP_EXPECT_SOURCE: opts.source, LP_EXPECT_PYTHON_VERSION: version };

if (opts.source === 'setting') {
  settings['lineagePilot.python.interpreterPath'] = python;
  launchArgs.push('--disable-extensions');
} else if (opts.source === 'path') {
  launchArgs.push('--disable-extensions');
  extensionTestsEnv.PATH = `${path.dirname(python)}${path.delimiter}${process.env.PATH}`;
  // Which python3 wins is not ours to choose: on macOS VS Code rebuilds PATH from the login shell,
  // and on Windows the py launcher picks its own default. The test instead checks the extension
  // chose what a PATH lookup inside the extension host finds.
  delete extensionTestsEnv.LP_EXPECT_PYTHON_VERSION;
  extensionTestsEnv.LP_EXPECT_PATH_LOOKUP = '1';
} else {
  settings['python.defaultInterpreterPath'] = python;
  const [cli, ...cliArgs] = resolveCliArgsFromVSCodeExecutablePath(vscodeExecutablePath);
  execFileSync(cli, [...cliArgs, '--extensions-dir', extensionsDir, '--install-extension', 'ms-python.python'],
    { stdio: 'inherit', shell: process.platform === 'win32' });
}
fs.writeFileSync(path.join(userDataDir, 'User', 'settings.json'), JSON.stringify(settings, null, 2));

console.log(`[S1] VS Code ${opts.vscode}, source=${opts.source}, Python ${version} (${python})`);
console.log(`[S1] extension under test: ${extensionDevelopmentPath}`);
try {
  await runTests({
    vscodeExecutablePath,
    extensionDevelopmentPath,
    extensionTestsPath: path.join(root, 'out', 'test', 'suite', 'index.js'),
    launchArgs,
    extensionTestsEnv,
  });
} catch (error) {
  console.error(`[S1] integration tests failed: ${error}`);
  process.exit(1);
}
