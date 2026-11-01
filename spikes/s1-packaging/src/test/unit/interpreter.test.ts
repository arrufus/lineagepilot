import { strict as assert } from 'node:assert';
import { describe, it } from 'node:test';

import {
  Candidate, isInside, meetsMinimum, parseProbe, pathCandidates, Probe, resolveInterpreter, ResolverDeps,
} from '../../interpreter';

type ProbeTable = Record<string, Probe | undefined>;

function deps(overrides: Partial<ResolverDeps> & { probes?: ProbeTable } = {}): ResolverDeps & { probed: string[] } {
  const probed: string[] = [];
  const probes = overrides.probes ?? {};
  return {
    settingPath: () => undefined,
    pythonExtensionPath: async () => undefined,
    probe: async (c: Candidate) => {
      probed.push(c.command);
      return probes[c.command];
    },
    platform: 'linux',
    isTrusted: true,
    workspaceFolders: ['/work/repo'],
    ...overrides,
    probed,
  };
}

const py313: Probe = { executable: '/usr/bin/python3.13', version: [3, 13, 3] };
const py310: Probe = { executable: '/opt/py/bin/python3.10', version: [3, 10, 17] };
const py39: Probe = { executable: '/usr/bin/python3.9', version: [3, 9, 6] };

describe('resolveInterpreter', () => {
  it('uses the setting first', async () => {
    const d = deps({ settingPath: () => '/opt/py/bin/python3.10', probes: { '/opt/py/bin/python3.10': py310 } });
    const r = await resolveInterpreter(d);
    assert.equal(r.interpreter?.source, 'setting');
    assert.equal(r.interpreter?.executable, py310.executable);
    assert.deepEqual(d.probed, ['/opt/py/bin/python3.10']);
  });

  it('reports a broken setting instead of falling back', async () => {
    const d = deps({ settingPath: () => '/missing', probes: { python3: py313 } });
    const r = await resolveInterpreter(d);
    assert.equal(r.interpreter, undefined);
    assert.match(r.error ?? '', /did not run as Python/);
    assert.deepEqual(d.probed, ['/missing']);
  });

  it('reports a setting that is too old', async () => {
    const r = await resolveInterpreter(deps({ settingPath: () => '/usr/bin/python3.9', probes: { '/usr/bin/python3.9': py39 } }));
    assert.match(r.error ?? '', /Python 3\.9\.6; LineagePilot needs 3\.10 or later/);
  });

  it('treats a blank setting as unset', async () => {
    const r = await resolveInterpreter(deps({ settingPath: () => '  ', probes: { python3: py313 } }));
    assert.equal(r.interpreter?.source, 'path');
  });

  it('prefers the Python extension over PATH', async () => {
    const d = deps({ pythonExtensionPath: async () => '/envs/a/bin/python', probes: { '/envs/a/bin/python': py310, python3: py313 } });
    const r = await resolveInterpreter(d);
    assert.equal(r.interpreter?.source, 'python-extension');
    assert.deepEqual(d.probed, ['/envs/a/bin/python']);
  });

  it('falls back to PATH and skips old or missing interpreters', async () => {
    const d = deps({ pythonExtensionPath: async () => '/envs/old/bin/python', probes: { '/envs/old/bin/python': py39, python: py313 } });
    const r = await resolveInterpreter(d);
    assert.equal(r.interpreter?.source, 'path');
    assert.equal(r.interpreter?.executable, py313.executable);
    assert.equal(r.attempts.length, 2);
    assert.match(r.attempts[0], /older than 3\.10/);
    assert.match(r.attempts[1], /python3: not found/);
  });

  it('returns no interpreter when nothing works', async () => {
    const r = await resolveInterpreter(deps());
    assert.equal(r.interpreter, undefined);
    assert.equal(r.error, undefined);
    assert.equal(r.attempts.length, 2);
  });

  it('never runs a workspace interpreter in an untrusted workspace', async () => {
    const d = deps({
      isTrusted: false,
      pythonExtensionPath: async () => '/work/repo/.venv/bin/python',
      probes: { '/work/repo/.venv/bin/python': py313, python3: py310 },
    });
    const r = await resolveInterpreter(d);
    assert.equal(r.interpreter?.source, 'path');
    assert.deepEqual(d.probed, ['python3']);
    assert.match(r.attempts[0], /not trusted/);
  });

  it('rejects a PATH interpreter that resolves into an untrusted workspace', async () => {
    const d = deps({
      isTrusted: false,
      probes: { python3: { executable: '/work/repo/bin/python3', version: [3, 12, 0] }, python: py313 },
    });
    const r = await resolveInterpreter(d);
    assert.equal(r.interpreter?.executable, py313.executable);
    assert.match(r.attempts[0], /inside the untrusted workspace/);
  });

  it('allows a workspace interpreter once the workspace is trusted', async () => {
    const r = await resolveInterpreter(deps({
      pythonExtensionPath: async () => '/work/repo/.venv/bin/python',
      probes: { '/work/repo/.venv/bin/python': { executable: '/work/repo/.venv/bin/python', version: [3, 12, 1] } },
    }));
    assert.equal(r.interpreter?.source, 'python-extension');
  });
});

describe('helpers', () => {
  it('lists py -3 first on Windows and python3 first elsewhere', () => {
    assert.deepEqual(pathCandidates('win32').map((c) => [c.command, ...c.args].join(' ')), ['py -3', 'python']);
    assert.deepEqual(pathCandidates('darwin').map((c) => c.command), ['python3', 'python']);
  });

  it('parses the last line of probe output', () => {
    assert.deepEqual(parseProbe('noise\n{"executable":"/p","version":[3,13,3]}\n'), { executable: '/p', version: [3, 13, 3] });
  });

  it('rejects malformed probe output', () => {
    for (const bad of ['', 'not json', '{"executable":1,"version":[3,13,3]}', '{"executable":"/p","version":[3,13]}',
      '{"executable":"/p","version":["3","13","3"]}']) {
      assert.equal(parseProbe(bad), undefined, bad);
    }
  });

  it('compares against the minimum version', () => {
    assert.ok(meetsMinimum([3, 10, 0]));
    assert.ok(meetsMinimum([4, 0, 0]));
    assert.ok(!meetsMinimum([3, 9, 18]));
    assert.ok(!meetsMinimum([2, 7, 18]));
  });

  it('detects paths inside a folder without prefix false positives', () => {
    assert.ok(isInside('/work/repo/.venv/bin/python', '/work/repo', 'linux'));
    assert.ok(isInside('/work/repo', '/work/repo', 'linux'));
    assert.ok(!isInside('/work/repo-other/python', '/work/repo', 'linux'));
    assert.ok(!isInside('/work/Repo/python', '/work/repo', 'linux'));
    assert.ok(isInside('/Work/Repo/python', '/work/repo', 'darwin'));
    assert.ok(isInside('C:\\Work\\Repo\\.venv\\Scripts\\python.exe', 'c:\\work\\repo', 'win32'));
    assert.ok(!isInside('D:\\repo\\python.exe', 'C:\\repo', 'win32'));
  });
});
