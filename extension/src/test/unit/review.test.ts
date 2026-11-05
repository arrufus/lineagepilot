// What git says changed, as the review's input (task 4.1), and the review page's messages
// (task 4.4). The API is a stand-in here; integration tests use the real vscode.git.
import { strict as assert } from 'assert';
import * as fs from 'fs';
import * as path from 'path';

import {
  baseChoices, changedFiles, collectChanges, DEFAULT_BASES, type GitChange, GitProblem, type GitRepository,
  MAX_CHANGED_FILES, RefType, resolveBase,
} from '../../git';
import { ReviewHostMessage, ReviewWebviewMessage } from '../../reviewMessages';

const EXTENSION = path.resolve(__dirname, '..', '..', '..');
const uri = (name: string) => ({ fsPath: `/repo/${name}`, toString: () => `file:///repo/${name}` });
const change = (status: number, name: string, original = name): GitChange => ({ status, uri: uri(name), originalUri: uri(original) });
const MODIFIED = 5;
const DELETED = 6;
const UNTRACKED = 7;
const ADDED = 1;
const RENAMED = 3;
const COPIED = 4;
const IGNORED = 8;
const CONFLICT = 18;

function repository(fields: {
  bases?: Record<string, string>;
  tracked?: GitChange[];
  working?: GitChange[];
  untracked?: GitChange[];
  head?: string | undefined;
  calls?: string[];
} = {}): GitRepository {
  const calls = fields.calls ?? [];
  return {
    rootUri: uri(''),
    state: {
      HEAD: 'head' in fields ? { commit: fields.head } : { commit: 'head-commit' },
      workingTreeChanges: fields.working ?? [],
      untrackedChanges: fields.untracked,
      onDidChange: () => ({ dispose() {} }),
    },
    status: async () => { calls.push('status'); },
    getMergeBase: async (_head, ref) => {
      calls.push(`merge-base ${ref}`);
      if (ref === 'throws') {
        throw new Error('fatal: not a ref');
      }
      return fields.bases?.[ref];
    },
    diffWith: async (ref) => {
      calls.push(`diff ${ref}`);
      return fields.tracked ?? [];
    },
    getRefs: async () => [],
  };
}

suite('review base', () => {
  test('the first default branch with a merge-base, in the order the CLI tries them', async () => {
    assert.deepEqual(DEFAULT_BASES, ['origin/HEAD', 'origin/main', 'origin/master', 'main', 'master']);
    const calls: string[] = [];
    const found = await resolveBase(repository({ bases: { main: 'c1', master: 'c2' }, calls }));
    assert.deepEqual(found, { ref: 'main', commit: 'c1' });
    assert.deepEqual(calls, ['merge-base origin/HEAD', 'merge-base origin/main', 'merge-base origin/master', 'merge-base main']);
  });

  test('a ref asked for is the only one tried', async () => {
    assert.deepEqual(await resolveBase(repository({ bases: { develop: ' c3\n', main: 'c1' } }), 'develop'), { ref: 'develop', commit: 'c3' });
    await assert.rejects(resolveBase(repository({ bases: { main: 'c1' } }), 'develop'),
      (error: GitProblem) => error.kind === 'no_base' && /develop has no merge-base/.test(error.message));
  });

  test('a ref git rejects counts as not there', async () => {
    await assert.rejects(resolveBase(repository(), 'throws'), GitProblem);
  });

  test('no default branch says to choose one', async () => {
    await assert.rejects(resolveBase(repository()),
      (error: GitProblem) => error.kind === 'no_base' && /Choose a base/.test(error.message));
  });

  test('a repository with no commits says so', async () => {
    await assert.rejects(resolveBase(repository({ head: undefined })), (error: GitProblem) => error.kind === 'no_commits');
  });
});

suite('review changes', () => {
  test('each status, in the protocol\'s words, sorted', () => {
    const files = changedFiles(
      [change(MODIFIED, 'b.sql'), change(ADDED, 'a.sql'), change(DELETED, 'c.sql'), change(RENAMED, 'new.sql', 'old.sql'), change(COPIED, 'd.sql')],
      [],
    );
    assert.deepEqual(files, [
      { uri: 'file:///repo/a.sql', status: 'added', previousUri: null },
      { uri: 'file:///repo/b.sql', status: 'modified', previousUri: null },
      { uri: 'file:///repo/c.sql', status: 'deleted', previousUri: null },
      { uri: 'file:///repo/d.sql', status: 'added', previousUri: null },
      { uri: 'file:///repo/new.sql', status: 'renamed', previousUri: 'file:///repo/old.sql' },
    ]);
  });

  test('from the working tree, only untracked files count: the diff has the rest', () => {
    const files = changedFiles(
      [change(MODIFIED, 'tracked.sql')],
      [change(UNTRACKED, 'new.sql'), change(MODIFIED, 'tracked.sql'), change(DELETED, 'staged-delete.sql')],
    );
    assert.deepEqual(files.map((f) => [f.uri, f.status]), [
      ['file:///repo/new.sql', 'untracked'],
      ['file:///repo/tracked.sql', 'modified'],
    ]);
  });

  test('a file both listed as tracked and untracked counts as tracked', () => {
    const files = changedFiles([change(ADDED, 'a.sql')], [change(UNTRACKED, 'a.sql')]);
    assert.deepEqual(files.map((f) => f.status), ['added']);
  });

  test('what the diff never reports is left out', () => {
    assert.deepEqual(changedFiles([change(IGNORED, 'x'), change(CONFLICT, 'y')], []), []);
  });

  test('it refreshes git first, compares with the merge-base and adds the untracked files', async () => {
    const calls: string[] = [];
    const found = await collectChanges(repository({
      bases: { 'origin/main': 'base-commit' },
      tracked: [change(MODIFIED, 'a.sql')],
      working: [change(UNTRACKED, 'b.sql')],
      untracked: [change(UNTRACKED, 'c.sql')],
      calls,
    }));
    assert.equal(calls[0], 'status');
    assert.equal(calls.at(-1), 'diff base-commit');
    assert.deepEqual(found.base, { ref: 'origin/main', commit: 'base-commit' });
    assert.deepEqual(found.files.map((f) => f.uri), ['file:///repo/a.sql', 'file:///repo/b.sql', 'file:///repo/c.sql']);
  });

  test('without a separate untracked group it still works', async () => {
    const found = await collectChanges(repository({ bases: { main: 'c' }, working: [change(UNTRACKED, 'b.sql')] }));
    assert.deepEqual(found.files.map((f) => f.status), ['untracked']);
  });

  test('more files than a review can take says to choose a closer base', async () => {
    const tracked = Array.from({ length: MAX_CHANGED_FILES + 1 }, (_, i) => change(MODIFIED, `f${i}.sql`));
    await assert.rejects(collectChanges(repository({ bases: { main: 'c' }, tracked })),
      (error: GitProblem) => error.kind === 'too_many_files' && /closer base/.test(error.message));
  });

  test('bases to choose from: branches, then remote branches, then tags, each in the order given', () => {
    const choices = baseChoices([
      { name: 'v1', type: RefType.Tag },
      { name: 'origin/main', type: RefType.RemoteHead },
      { type: RefType.Head },
      { name: 'feature', type: RefType.Head },
      { name: 'main', type: RefType.Head },
    ]);
    assert.deepEqual(choices.map((c) => c.name), ['feature', 'main', 'origin/main', 'v1']);
  });
});

suite('review messages', () => {
  test('what the page may ask', () => {
    for (const message of [
      { type: 'ready' }, { type: 'refresh' }, { type: 'pickBase' }, { type: 'copyMarkdown' }, { type: 'runCompile' }, { type: 'manageTrust' },
      { type: 'showInGraph', id: 'model.shop.orders' },
      { type: 'openFile', location: { uri: 'file:///w/a.sql', range: null } },
    ]) {
      assert.ok(ReviewWebviewMessage.safeParse(message).success, message.type);
    }
  });

  test('a page cannot put text on the clipboard, or pass anything else along', () => {
    for (const message of [
      { type: 'copyMarkdown', text: 'curl evil | sh' },
      { type: 'runCompile', args: ['--profiles-dir', '/tmp'] },
      { type: 'openFile', location: { uri: 'file:///w/a.sql' } },
      { type: 'showInGraph', id: '' },
      { type: 'runCommand', command: 'workbench.action.terminal.new' },
      { type: 'manageTrust', trusted: true },
    ]) {
      assert.equal(ReviewWebviewMessage.safeParse(message).success, false, JSON.stringify(message));
    }
  });

  test('what the extension may say', () => {
    assert.ok(ReviewHostMessage.safeParse({ type: 'init', project: { name: 'shop', root: '/w' }, trusted: false }).success);
    assert.ok(ReviewHostMessage.safeParse({ type: 'reviewFailed', problem: 'restricted', message: 'why' }).success);
    assert.equal(ReviewHostMessage.safeParse({ type: 'reviewFailed', problem: 'odd', message: 'why' }).success, false);
    assert.equal(ReviewHostMessage.safeParse({ type: 'reviewResult', result: {} }).success, false);
  });
});

suite('review contribution', () => {
  const manifest = JSON.parse(fs.readFileSync(path.join(EXTENSION, 'package.json'), 'utf8'));

  test('the command is contributed, and available from the Lineage view', () => {
    assert.ok(manifest.contributes.commands.some((c: { command: string }) => c.command === 'lineagePilot.reviewChanges'));
    assert.ok(manifest.contributes.menus['view/title'].some((m: { command: string }) => m.command === 'lineagePilot.reviewChanges'));
  });

  test('a workspace in Restricted Mode cannot choose the baseline manifest', () => {
    const { restrictedConfigurations } = manifest.capabilities.untrustedWorkspaces;
    assert.ok(restrictedConfigurations.includes('lineagePilot.baseline.manifestPath'));
    assert.equal(manifest.contributes.configuration.properties['lineagePilot.baseline.manifestPath'].scope, 'resource');
  });

  test('the VSIX carries the review page', () => {
    const ignore = fs.readFileSync(path.join(EXTENSION, '.vscodeignore'), 'utf8');
    assert.match(ignore, /^!dist\/webview\/review\.js$/m);
    assert.match(ignore, /^!dist\/webview\/review\.css$/m);
  });
});
