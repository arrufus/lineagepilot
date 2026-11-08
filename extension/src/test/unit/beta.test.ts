// The beta's usage counts and the sample project's files (tasks 6.1 and 6.3), without VS Code.
import { strict as assert } from 'assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  applySampleChange, copySample, filesBelow, freeFolder, isSample, SAMPLE_CHANGE, SAMPLE_FOLDER, SAMPLE_MARKER,
} from '../../sample';
import { isoWeek, localDay, USAGE_KEY, UsageCounts, validState, weekStart, WEEKS_KEPT } from '../../usage';

class MemoryStore {
  readonly values = new Map<string, unknown>();
  writes = 0;
  get<T>(key: string): T | undefined {
    return this.values.get(key) as T | undefined;
  }
  update(key: string, value: unknown): Promise<void> {
    this.writes += 1;
    if (value === undefined) {
      this.values.delete(key);
    } else {
      this.values.set(key, JSON.parse(JSON.stringify(value)));  // as Memento stores it
    }
    return Promise.resolve();
  }
}

suite('usage counts: weeks', () => {
  test('ISO 8601 weeks, at the turn of the year too', () => {
    assert.equal(isoWeek(new Date(2026, 10, 8)), '2026-W45');  // a Sunday: the last day of its week
    assert.equal(isoWeek(new Date(2026, 10, 2)), '2026-W45');  // the Monday
    assert.equal(isoWeek(new Date(2026, 10, 9)), '2026-W46');
    assert.equal(isoWeek(new Date(2021, 0, 3)), '2020-W53');   // 2021 starts on a Friday
    assert.equal(isoWeek(new Date(2021, 0, 4)), '2021-W01');
    assert.equal(isoWeek(new Date(2024, 11, 30)), '2025-W01'); // the week holding 1 January 2025
    assert.equal(isoWeek(new Date(2026, 11, 31)), '2026-W53');
    assert.equal(isoWeek(new Date(2027, 0, 3, 23, 59)), '2026-W53');
  });

  test('a week starts on its Monday', () => {
    assert.equal(weekStart('2026-W45'), '2026-11-02');
    assert.equal(weekStart('2020-W53'), '2020-12-28');
    assert.equal(weekStart('2025-W01'), '2024-12-30');
    assert.equal(weekStart('2026-W01'), '2025-12-29');
    assert.equal(weekStart('not a week'), 'not a week');
    for (let day = 0; day < 400; day += 3) {
      const date = new Date(2025, 0, 1 + day);
      const monday = weekStart(isoWeek(date));
      const [y, m, d] = monday.split('-').map(Number);
      const start = new Date(y, m - 1, d);
      assert.equal(start.getDay(), 1, monday);
      const days = Math.round((Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()) - Date.UTC(y, m - 1, d)) / 86_400_000);
      assert.ok(days >= 0 && days < 7, `${localDay(date)} is not in the week from ${monday}`);
    }
  });

  test('local days', () => {
    assert.equal(localDay(new Date(2026, 0, 5, 23, 59)), '2026-01-05');
  });
});

suite('usage counts: recording', () => {
  let now: Date;
  let enabled: boolean;
  let store: MemoryStore;
  let usage: UsageCounts;

  setup(() => {
    now = new Date(2026, 10, 3, 10, 0);
    enabled = true;
    store = new MemoryStore();
    usage = new UsageCounts(store, () => enabled, () => now);
  });

  test('nothing is recorded while counting is off', async () => {
    enabled = false;
    await usage.record('day');
    await usage.record('opened');
    await usage.record('copied');
    assert.equal(store.writes, 0);
    assert.deepEqual(usage.state(), { version: 1, weeks: {} });
  });

  test('days, reviews opened and reviews copied, by week', async () => {
    await usage.record('day');
    await usage.record('day');
    assert.equal(store.writes, 1, 'a day already counted is not written again');
    await usage.record('opened');
    await usage.record('opened');
    await usage.record('copied');
    now = new Date(2026, 10, 4, 9, 0);
    await usage.record('opened');  // using the review counts the day as well
    now = new Date(2026, 10, 10, 9, 0);
    await usage.record('day');
    assert.deepEqual(usage.state(), {
      version: 1,
      weeks: {
        '2026-W45': { days: ['2026-11-03', '2026-11-04'], opened: 3, copied: 1 },
        '2026-W46': { days: ['2026-11-10'], opened: 0, copied: 0 },
      },
    });
  });

  test(`only the newest ${WEEKS_KEPT} weeks are kept`, async () => {
    for (let week = 0; week < WEEKS_KEPT + 3; week++) {
      now = new Date(2026, 0, 5 + week * 7);
      await usage.record('opened');
    }
    const weeks = Object.keys(usage.state().weeks);
    assert.equal(weeks.length, WEEKS_KEPT);
    assert.equal(weeks.sort()[0], isoWeek(new Date(2026, 0, 5 + 3 * 7)));
  });

  test('clearing forgets everything', async () => {
    await usage.record('opened');
    await usage.clear();
    assert.equal(store.values.has(USAGE_KEY), false);
    const writes = store.writes;
    await usage.clear();
    assert.equal(store.writes, writes, 'nothing to clear, nothing written');
  });

  test('the summary is the table the user pastes, and nothing else', async () => {
    assert.equal(usage.summary('0.1.0'),
      'LineagePilot 0.1.0 usage counts, counted on this machine and copied on 2026-11-03. '
      + 'They hold nothing about any project.\n\nNothing counted yet.\n');
    await usage.record('opened');
    await usage.record('opened');
    await usage.record('copied');
    now = new Date(2026, 10, 11);
    await usage.record('day');
    assert.equal(usage.summary('0.1.0'), [
      'LineagePilot 0.1.0 usage counts, counted on this machine and copied on 2026-11-11. They hold nothing about any project.',
      '',
      '| Week | Days with a dbt project open | Reviews opened | Reviews copied as Markdown |',
      '|---|---:|---:|---:|',
      '| 2026-W45 (from 2026-11-02) | 1 | 2 | 1 |',
      '| 2026-W46 (from 2026-11-09) | 1 | 0 | 0 |',
      '',
    ].join('\n'));
  });

  test('what was stored is checked: anything else starts again', async () => {
    const empty = { version: 1, weeks: {} };
    for (const stored of [undefined, null, 'x', 42, [], { version: 2, weeks: {} }, { version: 1 }, { version: 1, weeks: null }]) {
      assert.deepEqual(validState(stored), empty, JSON.stringify(stored));
    }
    assert.deepEqual(validState({
      version: 1,
      weeks: {
        '2026-W45': { days: ['2026-11-03', '2026-11-03', '../etc', 7, '2026-11-02'], opened: -1, copied: 2.5 },
        'C:\\project': { days: [], opened: 1, copied: 1 },
        '2026-W46': null,
        '2026-W47': { days: 'x', opened: 3, copied: 1 },
      },
    }), {
      version: 1,
      weeks: {
        '2026-W45': { days: ['2026-11-02', '2026-11-03'], opened: 0, copied: 0 },
        '2026-W47': { days: [], opened: 3, copied: 1 },
      },
    });
    store.values.set(USAGE_KEY, { version: 1, weeks: { '2026-W45': { days: Array.from({ length: 9 }, (_, i) => `2026-11-0${i + 1}`), opened: 1, copied: 0 } } });
    assert.equal(usage.state().weeks['2026-W45'].days.length, 7);
  });
});

suite('sample project', () => {
  let work: string;

  // A small stand-in for the bundled sample: the real one is tested in VS Code (walkthrough.test.ts)
  function bundled(): string {
    const dir = path.join(work, 'bundled');
    const write = (relative: string, text: string) => {
      fs.mkdirSync(path.dirname(path.join(dir, relative)), { recursive: true });
      fs.writeFileSync(path.join(dir, relative), text);
    };
    write(SAMPLE_MARKER, '{"sample": "impact-shop", "version": 1}\n');
    write('dbt_project.yml', 'name: impact_shop\n');
    write('models/marts/orders.sql', 'select amount as order_total\n');
    write('models/marts/orders.yml', 'columns: [order_total]\n');
    write('target/manifest.json', '{"before": true}\n');
    write(path.join(SAMPLE_CHANGE, 'models/marts/orders.sql'), 'select amount as total_usd\n');
    write(path.join(SAMPLE_CHANGE, 'models/marts/orders.yml'), 'columns: [total_usd]\n');
    write(path.join(SAMPLE_CHANGE, 'target/manifest.json'), '{"after": true}\n');
    return dir;
  }

  setup(() => {
    work = fs.mkdtempSync(path.join(os.tmpdir(), 'lp-unit-sample-'));
  });

  teardown(() => {
    fs.rmSync(work, { recursive: true, force: true });
  });

  test('a copy never overwrites a folder that exists', () => {
    const taken = new Set([path.join('/p', SAMPLE_FOLDER), path.join('/p', `${SAMPLE_FOLDER}-2`)]);
    assert.equal(freeFolder('/p', (c) => taken.has(c)), path.join('/p', `${SAMPLE_FOLDER}-3`));
    assert.equal(freeFolder('/q', () => false), path.join('/q', SAMPLE_FOLDER));
    assert.throws(() => freeFolder('/r', () => true), /Too many copies/);
  });

  test('copying: every file, the artifacts newer than the sources, beside any copy already there', async () => {
    const source = bundled();
    const parent = path.join(work, 'parent');
    fs.mkdirSync(parent);
    const now = Date.now();
    const first = await copySample(source, parent, now);
    assert.equal(first, path.join(parent, SAMPLE_FOLDER));
    assert.deepEqual(await filesBelow(first), await filesBelow(source));
    assert.ok(isSample(first));
    const sql = fs.statSync(path.join(first, 'models', 'marts', 'orders.sql')).mtimeMs;
    const manifest = fs.statSync(path.join(first, 'target', 'manifest.json')).mtimeMs;
    assert.ok(manifest - sql >= 3_000_000, `manifest ${manifest}, model ${sql}`);
    const second = await copySample(source, parent, now);
    assert.equal(second, path.join(parent, `${SAMPLE_FOLDER}-2`));
  });

  test('copying refuses a build without the sample', async () => {
    fs.mkdirSync(path.join(work, 'empty'));
    await assert.rejects(copySample(path.join(work, 'empty'), work), /sample project is missing/);
  });

  test('applying the change: the files, then the manifest, and only once', async () => {
    const source = bundled();
    const copy = await copySample(source, work);
    const outcome = await applySampleChange(copy, source);
    assert.deepEqual(outcome, {
      status: 'applied',
      files: [path.join('models', 'marts', 'orders.sql'), path.join('models', 'marts', 'orders.yml')],
    });
    assert.equal(fs.readFileSync(path.join(copy, 'models', 'marts', 'orders.sql'), 'utf8'), 'select amount as total_usd\n');
    assert.equal(fs.readFileSync(path.join(copy, 'target', 'manifest.json'), 'utf8'), '{"after": true}\n');
    // As after an edit and dbt compile: the manifest is not older than the model, so nothing is stale
    const sql = fs.statSync(path.join(copy, 'models', 'marts', 'orders.sql')).mtimeMs;
    const manifest = fs.statSync(path.join(copy, 'target', 'manifest.json')).mtimeMs;
    assert.ok(manifest >= sql && manifest - sql < 2000, `manifest ${manifest}, model ${sql}`);
    assert.deepEqual(await applySampleChange(copy, source), { status: 'already-applied' });
  });

  test('the change is made only in a copy of the sample', async () => {
    const source = bundled();
    const copy = await copySample(source, work);
    fs.rmSync(path.join(copy, SAMPLE_MARKER));
    assert.deepEqual(await applySampleChange(copy, source), { status: 'not-sample' });
    assert.equal(fs.readFileSync(path.join(copy, 'models', 'marts', 'orders.sql'), 'utf8'), 'select amount as order_total\n');
    assert.equal(isSample(path.join(work, 'nowhere')), false);
  });

  test("what is written comes from the extension, never from the folder's own changes/", async () => {
    const source = bundled();
    const copy = await copySample(source, work);
    fs.writeFileSync(path.join(copy, SAMPLE_CHANGE, 'models', 'marts', 'orders.sql'), 'select 1 as hostile\n');
    fs.writeFileSync(path.join(copy, SAMPLE_CHANGE, 'planted.txt'), 'planted\n');
    assert.equal((await applySampleChange(copy, source)).status, 'applied');
    assert.equal(fs.readFileSync(path.join(copy, 'models', 'marts', 'orders.sql'), 'utf8'), 'select amount as total_usd\n');
    assert.equal(fs.existsSync(path.join(copy, 'planted.txt')), false);
  });

  test('a file changed since the copy, or missing, is left alone, and so is everything else', async () => {
    const source = bundled();
    const copy = await copySample(source, work);
    fs.writeFileSync(path.join(copy, 'models', 'marts', 'orders.yml'), 'columns: [mine]\n');
    fs.rmSync(path.join(copy, 'target', 'manifest.json'));
    assert.deepEqual(await applySampleChange(copy, source), {
      status: 'modified',
      files: [path.join('models', 'marts', 'orders.yml'), path.join('target', 'manifest.json')],
    });
    assert.equal(fs.readFileSync(path.join(copy, 'models', 'marts', 'orders.sql'), 'utf8'), 'select amount as order_total\n');
  });

  suite('symbolic links', () => {
    setup(function () {
      if (process.platform === 'win32') {
        this.skip();  // creating one needs developer mode or elevation
      }
    });

    test('are not followed or copied when the sample is copied', async () => {
      const source = bundled();
      fs.symlinkSync('/etc', path.join(source, 'models', 'linked'));
      const copy = await copySample(source, work);
      assert.equal(fs.existsSync(path.join(copy, 'models', 'linked')), false);
    });

    test('a folder on the way that is a link is refused, and nothing outside is written', async () => {
      const source = bundled();
      const copy = await copySample(source, work);
      const outside = path.join(work, 'outside');
      fs.cpSync(path.join(copy, 'models', 'marts'), outside, { recursive: true });
      fs.rmSync(path.join(copy, 'models', 'marts'), { recursive: true });
      fs.symlinkSync(outside, path.join(copy, 'models', 'marts'));
      assert.deepEqual(await applySampleChange(copy, source), { status: 'unsafe', file: path.join('models', 'marts', 'orders.sql') });
      assert.equal(fs.readFileSync(path.join(outside, 'orders.sql'), 'utf8'), 'select amount as order_total\n');
    });

    test('a file that is a link is refused, even to a copy of the sample', async () => {
      const source = bundled();
      const copy = await copySample(source, work);
      const outside = path.join(work, 'manifest.json');
      fs.copyFileSync(path.join(copy, 'target', 'manifest.json'), outside);
      fs.rmSync(path.join(copy, 'target', 'manifest.json'));
      fs.symlinkSync(outside, path.join(copy, 'target', 'manifest.json'));
      assert.deepEqual(await applySampleChange(copy, source), { status: 'unsafe', file: path.join('target', 'manifest.json') });
      assert.equal(fs.readFileSync(outside, 'utf8'), '{"before": true}\n');
    });

    test('a cloned repository with the marker and a link out of it writes nothing', async () => {
      // The review's proof of concept: a marker, changes/…/up/.zshrc, and up -> ../..
      const source = bundled();
      const repo = path.join(work, 'clone', 'repo');
      const home = path.join(work, 'home');
      fs.mkdirSync(path.join(repo, '.lineagepilot'), { recursive: true });
      fs.mkdirSync(path.join(repo, SAMPLE_CHANGE, 'up'), { recursive: true });
      fs.mkdirSync(home);
      fs.writeFileSync(path.join(repo, SAMPLE_MARKER), '{}');
      fs.writeFileSync(path.join(repo, SAMPLE_CHANGE, 'up', '.zshrc'), 'planted\n');
      fs.symlinkSync(home, path.join(repo, 'up'));
      assert.equal(isSample(repo), true);
      assert.equal((await applySampleChange(repo, source)).status, 'modified');
      assert.deepEqual(fs.readdirSync(home), []);
    });

    test('a marker that is a link does not make a sample', async () => {
      const copy = await copySample(bundled(), work);
      const elsewhere = path.join(work, 'marker.json');
      fs.renameSync(path.join(copy, SAMPLE_MARKER), elsewhere);
      fs.symlinkSync(elsewhere, path.join(copy, SAMPLE_MARKER));
      assert.equal(isSample(copy), false);
    });
  });
});
