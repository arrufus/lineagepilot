// Interpreter resolution (task 2.4, design sections 4.4 and 10), with every dependency faked.
import { strict as assert } from 'assert';

import {
  Candidate, isInside, meetsMinimum, parseProbe, pathCandidates, Probe, resolveInterpreter, ResolverDeps,
} from '../../interpreter';

const PY313: Probe = { executable: '/usr/bin/python3.13', version: [3, 13, 3] };

function deps(overrides: Partial<ResolverDeps> & { probes?: Record<string, Probe | undefined> } = {}): ResolverDeps & { probed: string[] } {
  const probed: string[] = [];
  const probes = overrides.probes ?? {};
  return {
    settingPath: () => undefined,
    pythonExtensionPath: async () => undefined,
    which: (command) => ({ python3: '/usr/bin/python3', python: '/usr/bin/python', py: 'C:\\Windows\\py.exe' } as Record<string, string>)[command],
    probe: async (candidate: Candidate) => {
      probed.push([candidate.command, ...candidate.args].join(' '));
      return probes[candidate.command];
    },
    uvFind: async () => undefined,
    uvInstall: async () => undefined,
    platform: 'linux',
    isTrusted: true,
    workspaceFolders: ['/work/repo'],
    ...overrides,
    probed,
  };
}

suite('interpreter resolution', () => {
  test('an explicit setting wins and is probed', async () => {
    const d = deps({ settingPath: () => '/opt/py/bin/python', probes: { '/opt/py/bin/python': PY313 } });
    const result = await resolveInterpreter(d);
    assert.equal(result.interpreter?.source, 'setting');
    assert.deepEqual(d.probed, ['/opt/py/bin/python']);
  });

  test('an unusable setting is an error, never a silent fallback', async () => {
    const missing = await resolveInterpreter(deps({ settingPath: () => '/nope/python' }));
    assert.match(missing.error!, /did not run as Python/);
    const old = await resolveInterpreter(deps({
      settingPath: () => '/old/python', probes: { '/old/python': { executable: '/old/python', version: [3, 9, 18] } },
    }));
    assert.match(old.error!, /is Python 3\.9\.18; LineagePilot needs 3\.10 or later/);
    const unknown = await resolveInterpreter(deps({ settingPath: () => 'pythonX' }));
    assert.match(unknown.error!, /was not found/);
  });

  test('a setting naming a command is resolved on PATH first', async () => {
    const d = deps({ settingPath: () => 'python3', probes: { '/usr/bin/python3': PY313 } });
    assert.equal((await resolveInterpreter(d)).interpreter?.source, 'setting');
    assert.deepEqual(d.probed, ['/usr/bin/python3']);
  });

  test('in an untrusted workspace a setting inside it is refused without running', async () => {
    const d = deps({ isTrusted: false, settingPath: () => '/work/repo/.venv/bin/python' });
    const result = await resolveInterpreter(d);
    assert.match(result.error!, /inside the workspace, which is not trusted/);
    assert.deepEqual(d.probed, []);
    const resolvesInside = deps({
      isTrusted: false, settingPath: () => '/usr/local/bin/python3',
      probes: { '/usr/local/bin/python3': { executable: '/work/repo/.venv/bin/python', version: [3, 12, 0] } },
    });
    assert.match((await resolveInterpreter(resolvesInside)).error!, /inside the workspace/);
  });

  test('then the Python extension, then PATH', async () => {
    const d = deps({ pythonExtensionPath: async () => '/envs/a/bin/python', probes: { '/usr/bin/python3': PY313 } });
    const result = await resolveInterpreter(d);
    assert.equal(result.interpreter?.source, 'path');
    assert.deepEqual(d.probed, ['/envs/a/bin/python', '/usr/bin/python3']);
    assert.match(result.attempts[0], /did not run as Python/);
  });

  test('PATH candidates are resolved before they run, so a workspace one never runs untrusted', async () => {
    const d = deps({
      isTrusted: false,
      which: (command) => (command === 'python3' ? '/work/repo/bin/python3' : undefined),
    });
    const result = await resolveInterpreter(d);
    assert.equal(result.interpreter, undefined);
    assert.deepEqual(d.probed, []);
    assert.match(result.attempts.join('\n'), /skipped, \/work\/repo\/bin\/python3 is inside the workspace/);
    assert.match(result.attempts.join('\n'), /python: not found/);
  });

  test('the Python extension may select a workspace .venv; untrusted, it is skipped unprobed', async () => {
    const d = deps({
      isTrusted: false,
      pythonExtensionPath: async () => '/work/repo/.venv/bin/python',
      probes: { '/usr/bin/python3': PY313 },
    });
    const result = await resolveInterpreter(d);
    assert.equal(result.interpreter?.executable, PY313.executable);
    assert.ok(!d.probed.includes('/work/repo/.venv/bin/python'));
  });

  test('a trusted workspace may use its own .venv', async () => {
    const venv = '/work/repo/.venv/bin/python';
    const d = deps({ pythonExtensionPath: async () => venv, probes: { [venv]: { executable: venv, version: [3, 11, 1] } } });
    assert.equal((await resolveInterpreter(d)).interpreter?.executable, venv);
  });

  test('a candidate resolving into an untrusted workspace is dropped after the probe', async () => {
    const d = deps({ isTrusted: false, probes: { '/usr/bin/python3': { executable: '/work/repo/x/python', version: [3, 12, 0] } } });
    const result = await resolveInterpreter(d);
    assert.match(result.attempts.join('\n'), /resolves to \/work\/repo\/x\/python inside the untrusted workspace/);
  });

  test('old Pythons are passed over', async () => {
    const d = deps({ probes: {
      '/usr/bin/python3': { executable: '/usr/bin/python3', version: [3, 9, 2] },
      '/usr/bin/python': PY313,
    } });
    assert.equal((await resolveInterpreter(d)).interpreter?.executable, PY313.executable);
  });

  test('uv: an installed Python first, then a download with consent', async () => {
    const managed = '/home/u/.local/share/uv/python/cpython-3.13/bin/python3.13';
    const installed = deps({ which: () => undefined, uvFind: async () => managed, probes: { [managed]: { executable: managed, version: [3, 13, 3] } } });
    assert.equal((await resolveInterpreter(installed)).interpreter?.source, 'uv');

    let asked = false;
    const downloaded = deps({
      which: () => undefined,
      uvInstall: async () => { asked = true; return managed; },
      probes: { [managed]: { executable: managed, version: [3, 13, 3] } },
    });
    const result = await resolveInterpreter(downloaded);
    assert.ok(asked);
    assert.equal(result.interpreter?.source, 'uv');
    assert.match(result.attempts.join('\n'), /uv: installed: none/);
  });

  test('nothing found at all', async () => {
    const result = await resolveInterpreter(deps({ which: () => undefined }));
    assert.equal(result.interpreter, undefined);
    assert.equal(result.error, undefined);
    assert.equal(result.attempts.length, 4);
  });
});

suite('interpreter helpers', () => {
  test('PATH candidates per platform', () => {
    assert.deepEqual(pathCandidates('win32').map((c) => [c.command, ...c.args].join(' ')), ['py -3', 'python']);
    assert.deepEqual(pathCandidates('darwin').map((c) => c.command), ['python3', 'python']);
  });

  test('probe output', () => {
    assert.deepEqual(parseProbe('noise\n{"executable": "/p", "version": [3, 12, 1]}\n'), { executable: '/p', version: [3, 12, 1] });
    for (const bad of ['', 'not json', '{"executable": 1, "version": [3, 1, 2]}', '{"executable": "/p", "version": [3, "x", 1]}',
      '{"executable": "/p", "version": [3, 1]}']) {
      assert.equal(parseProbe(bad), undefined, bad);
    }
  });

  test('minimum version', () => {
    assert.ok(meetsMinimum([3, 10, 0]) && meetsMinimum([4, 0, 0]) && !meetsMinimum([3, 9, 18]) && !meetsMinimum([2, 7, 18]));
  });

  test('inside a folder, with the platform case rules', () => {
    assert.ok(isInside('/w/repo/.venv/bin/python', '/w/repo', 'linux'));
    assert.ok(isInside('/w/repo', '/w/repo', 'linux'));
    assert.ok(!isInside('/w/repository/x', '/w/repo', 'linux'));
    assert.ok(!isInside('/W/REPO/x', '/w/repo', 'linux'));
    assert.ok(isInside('/W/REPO/x', '/w/repo', 'darwin'));
    assert.ok(isInside('C:\\Users\\A\\Repo\\.venv\\Scripts\\python.exe', 'c:\\users\\a\\repo', 'win32'));
  });
});
