// The extension's pure modules: PATH lookup, restart backoff, the status bar, project discovery,
// and the protocol validators against the shared contract examples (tasks 2.3, 2.4 and 2.6).
import { strict as assert } from 'assert';
import * as fs from 'fs';
import * as path from 'path';

import { RestartPolicy } from '../../backoff';
import { errorCode, isNotReady, parseNotification, ProtocolError, request } from '../../lineage';
import { projectOf, projectsFrom } from '../../projects';
import * as protocol from '../../protocol';
import { age, describe as describeProject, statusBar } from '../../status';
import { which } from '../../which';

const EXAMPLES = JSON.parse(fs.readFileSync(
  path.resolve(__dirname, '..', '..', '..', '..', 'protocol', 'examples.json'), 'utf8'));

suite('which', () => {
  const files = new Set(['/usr/bin/python3', '/opt/bin/dbt', 'C:\\Py\\python.EXE', 'C:\\Tools\\uv.exe']);
  const isExecutable = (f: string) => files.has(f);

  test('POSIX PATH, in order, absolute directories only', () => {
    const env = { PATH: 'relative:/usr/local/bin:/usr/bin:/opt/bin' };
    assert.equal(which('python3', { env, platform: 'linux', isExecutable }), '/usr/bin/python3');
    assert.equal(which('dbt', { env, platform: 'linux', isExecutable }), '/opt/bin/dbt');
    assert.equal(which('nope', { env, platform: 'linux', isExecutable }), undefined);
    assert.equal(which('/usr/bin/python3', { env: {}, platform: 'linux', isExecutable }), '/usr/bin/python3');
    assert.equal(which('./python3', { env, platform: 'linux', isExecutable }), undefined);
    assert.equal(which('python3', { env: {}, platform: 'linux', isExecutable }), undefined);
  });

  test('Windows Path and PATHEXT', () => {
    const env = { Path: 'C:\\Py;C:\\Tools', PATHEXT: '.COM;.EXE' };
    assert.equal(which('python', { env, platform: 'win32', isExecutable: (f) => f.toLowerCase() === 'c:\\py\\python.exe' }),
      'C:\\Py\\python.EXE'.replace('.EXE', '.exe'));
    assert.equal(which('uv.exe', { env, platform: 'win32', isExecutable }), 'C:\\Tools\\uv.exe');
    assert.equal(which('C:\\Tools\\uv.exe', { env: {}, platform: 'win32', isExecutable }), 'C:\\Tools\\uv.exe');
  });

  test('the real file system', () => {
    assert.equal(which(process.execPath, { env: {}, platform: process.platform }), process.execPath);
    assert.equal(which(path.join(__dirname, 'no-such-file'), { env: {}, platform: process.platform }), undefined);
  });
});

suite('restart backoff', () => {
  test('doubles from 1 s, caps at 30 s, gives up after 5 crashes in 3 minutes', () => {
    let now = 0;
    const policy = new RestartPolicy({ maxRestarts: 5, windowMs: 180_000, baseDelayMs: 1000, maxDelayMs: 8000 }, () => now);
    assert.deepEqual([1, 2, 3, 4, 5].map(() => policy.next()), [1000, 2000, 4000, 8000, 8000]);
    assert.equal(policy.next(), undefined);
    now = 200_000;  // the old crashes are outside the window
    assert.equal(policy.next(), 1000);
    policy.reset();
    assert.equal(policy.recentCrashes, 0);
  });
});

function status(overrides: Partial<protocol.Status> = {}): protocol.Status {
  const base = EXAMPLE_STATUS;
  return { ...base, ...overrides };
}

const EXAMPLE_STATUS: protocol.Status = {
  protocolVersion: 1,
  server: { version: '0.1.0', python: '3.13.3', executable: '/py', pid: 1, isolated: true, modules: {} },
  project: { root: '/w/shop', name: 'shop', problems: [] },
  trusted: true,
  state: 'ready',
  indexing: false,
  generation: 1,
  columnLineage: 'done',
  message: null,
  errors: [],
  errorCount: 0,
  artifacts: {
    manifest: { path: '/w/shop/target/manifest.json', exists: true, generatedAt: '2026-11-02T10:00:00Z',
      dbtVersion: '1.12.6', schemaVersion: 12, modifiedAt: null },
    catalog: null,
  },
  counts: { nodes: 1204, columns: 5000, edges: 3000, unresolved: 0, nodeKinds: { model: 1000 } },
  columnStates: { resolved: 1, partial: 0, unknown: 0 },
  stale: { count: 0, examples: [] },
  fromCache: false,
  indexedAt: null,
  rules: { pack: null, packLoaded: false, restricted: false, problems: [], findings: 0, score: null, coverage: null },
};

suite('status bar', () => {
  const now = Date.parse('2026-11-02T10:03:20Z');

  test('the example status matches the schema', () => {
    protocol.Status.parse(EXAMPLE_STATUS);
  });

  test('ages', () => {
    assert.equal(age('2026-11-02T10:03:00Z', now), 'just now');
    assert.equal(age('2026-11-02T10:00:00Z', now), '3 min old');
    assert.equal(age('2026-11-02T07:00:00Z', now), '3 h old');
    assert.equal(age('2026-10-29T10:00:00Z', now), '4 days old');
    assert.equal(age('not a date', now), undefined);
    assert.equal(age(null, now), undefined);
  });

  test('one project in each state', () => {
    const one = (view: Parameters<typeof describeProject>[0]) => statusBar([view], now);
    assert.equal(one({ root: '/w', status: status() }).text, '$(type-hierarchy) Lineage: 1,204 nodes · manifest 3 min old');
    const stale = one({ root: '/w', status: status({ stale: { count: 3, examples: ['models/a.sql'] } }) });
    assert.equal(stale.text, '$(warning) Lineage: 1,204 nodes · 3 files newer than the manifest');
    assert.equal(stale.level, 'warning');
    assert.equal(one({ root: '/w' }).text, '$(sync~spin) Lineage: indexing…');
    assert.equal(one({ root: '/w', status: status({ indexing: true }) }).level, 'busy');
    assert.match(one({ root: '/w', status: status({ state: 'unknown', message: 'No manifest', counts: null }) }).text,
      /\$\(warning\) Lineage: lineage unknown: No manifest/);
    assert.match(one({ root: '/w', status: status({ state: 'failed', message: null, counts: null }) }).text, /index failed: see the output/);
    assert.match(one({ root: '/w', problem: 'no Python 3.10+ found' }).text, /\$\(error\) Lineage: no Python/);
    const undated = status({ artifacts: { manifest: null, catalog: null } });
    assert.equal(one({ root: '/w', status: undated }).text, '$(type-hierarchy) Lineage: 1,204 nodes');
  });

  test('several projects add up, and the worst state shows', () => {
    const bar = statusBar([
      { root: '/w/a', status: status() },
      { root: '/w/b', status: status({ state: 'unknown', message: 'x', counts: null }) },
    ], now);
    assert.equal(bar.text, '$(warning) Lineage: 2 projects · 1,204 nodes · 1 need attention');
    assert.match(bar.tooltip, /shop: 1,204 nodes/);
    assert.equal(statusBar([], now).text, '$(warning) Lineage: no dbt project');
  });
});

suite('projects', () => {
  test('dbt_project.yml files become projects; packages and build output do not', () => {
    const projects = projectsFrom([
      '/w/repo/dbt_project.yml',
      '/w/repo/dbt_packages/dbt_utils/dbt_project.yml',
      '/w/repo/target/x/dbt_project.yml',
      '/w/repo/sub/analytics/dbt_project.yml',
      '/elsewhere/dbt_project.yml',
      '/w/repo/models/schema.yml',
    ], ['/w/repo'], 'linux');
    assert.deepEqual(projects, [
      { root: '/w/repo', folder: '/w/repo' },
      { root: '/w/repo/sub/analytics', folder: '/w/repo' },
    ]);
    assert.equal(projectOf('/w/repo/sub/analytics/models/a.sql', projects, 'linux')?.root, '/w/repo/sub/analytics');
    assert.equal(projectOf('/w/repo/models/a.sql', projects, 'linux')?.root, '/w/repo');
    assert.equal(projectOf('/other/a.sql', projects, 'linux'), undefined);
  });

  test('Windows paths', () => {
    const projects = projectsFrom(['C:\\w\\repo\\dbt_project.yml'], ['c:\\w\\repo'], 'win32');
    assert.deepEqual(projects, [{ root: 'C:\\w\\repo', folder: 'c:\\w\\repo' }]);
  });
});

suite('protocol', () => {
  for (const example of EXAMPLES.valid) {
    test(`accepts a valid ${example.type}`, () => {
      const schema = (protocol as unknown as Record<string, { parse(v: unknown): unknown }>)[example.type];
      schema.parse(example.value);
    });
  }
  for (const example of EXAMPLES.invalid) {
    test(`rejects ${example.type}: ${example.why}`, () => {
      const schema = (protocol as unknown as Record<string, { safeParse(v: unknown): { success: boolean } }>)[example.type];
      assert.equal(schema.safeParse(example.value).success, false);
    });
  }

  test('every method names its params and result', () => {
    assert.deepEqual(Object.keys(protocol.requests).sort(), [
      'lineage/export', 'lineage/graph', 'lineage/impact', 'lineage/node', 'lineage/nodesForFile', 'lineage/refresh', 'lineage/reviewChanges', 'lineage/status',
    ]);
  });
});

suite('typed requests', () => {
  test('params are checked before sending, results when they arrive', async () => {
    const sent: unknown[] = [];
    const sender = { sendRequest: async (method: string, params: unknown) => { sent.push([method, params]); return EXAMPLE_STATUS; } };
    const result = await request(sender, 'lineage/status', {});
    assert.equal(result.counts?.nodes, 1204);
    assert.deepEqual(sent, [['lineage/status', {}]]);
    await assert.rejects(request(sender, 'lineage/graph', { root: '', direction: 'up', depth: 0, granularity: 'model' } as never));
    assert.equal(sent.length, 1);
  });

  test('a malformed result is a ProtocolError naming the problem', async () => {
    const sender = { sendRequest: async () => ({ ...EXAMPLE_STATUS, state: 'sideways' }) };
    await assert.rejects(request(sender, 'lineage/status', {}), (error: unknown) =>
      error instanceof ProtocolError && /state/.test(error.message));
  });

  test('notifications and error codes', () => {
    assert.equal((parseNotification('lineage/indexUpdated', EXAMPLE_STATUS) as protocol.Status).generation, 1);
    assert.ok(parseNotification('lineage/indexUpdated', { nope: 1 }) instanceof ProtocolError);
    assert.equal(errorCode({ code: -32001 }), -32001);
    assert.equal(errorCode(new Error('x')), undefined);
    assert.ok(isNotReady({ code: protocol.ErrorCode.NotReady }));
    assert.ok(!isNotReady(undefined));
  });
});
