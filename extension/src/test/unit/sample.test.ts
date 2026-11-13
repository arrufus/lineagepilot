// The sample project's files (task 6.1), and forgetting what 0.1.x counted, without VS Code.
import { strict as assert } from 'assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { forgetOldCounts, OLD_COUNTS_KEY } from '../../legacy';
import {
  applySampleChange, copySample, filesBelow, freeFolder, isSample, SAMPLE_CHANGE, SAMPLE_FOLDER, SAMPLE_MARKER,
} from '../../sample';

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

suite('what 0.1.x counted', () => {
  test('is deleted once, and nothing else is touched', async () => {
    const store = new MemoryStore();
    store.values.set(OLD_COUNTS_KEY, { version: 1, weeks: { '2026-W45': { days: ['2026-11-03'], opened: 2, copied: 1 } } });
    store.values.set('lineagePilot.other', 'kept');
    assert.equal(await forgetOldCounts(store), true);
    assert.equal(store.get(OLD_COUNTS_KEY), undefined);
    assert.equal(store.get('lineagePilot.other'), 'kept');
    assert.equal(store.writes, 1);
    // Nothing left: no write at all
    assert.equal(await forgetOldCounts(store), false);
    assert.equal(store.writes, 1);
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
