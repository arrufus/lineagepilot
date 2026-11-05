// What changed since the base, through the vscode.git extension's API (task 4.1, design section
// 8). lineagepilot review asks git the same questions (packages/core/.../gitdiff.py): the same
// candidates for the base, the merge-base with HEAD, the same comparison with the working tree,
// and the untracked files, so the panel and the command line are given the same input. Kept free
// of the vscode module: the parts of the API it uses are typed here.

import type { ChangedFile, FileStatus } from './protocol';

export interface GitUri {
  readonly fsPath: string;
  toString(): string;
}

export interface GitChange {
  readonly uri: GitUri;
  readonly originalUri: GitUri;
  readonly status: number;
}

export interface GitRef {
  readonly name?: string;
  readonly type: number;
  readonly commit?: string;
}

export interface GitDisposable {
  dispose(): void;
}

export interface GitRepository {
  readonly rootUri: GitUri;
  readonly state: {
    readonly HEAD?: { readonly name?: string; readonly commit?: string };
    readonly workingTreeChanges: readonly GitChange[];
    readonly untrackedChanges?: readonly GitChange[];
    onDidChange(listener: () => void): GitDisposable;
  };
  status(): Promise<void>;
  getMergeBase(ref1: string, ref2: string): Promise<string | undefined>;
  diffWith(ref: string): Promise<GitChange[]>;
  getRefs(query: { sort?: 'alphabetically' | 'committerdate'; count?: number }): Promise<GitRef[]>;
}

// vscode.git's Status and RefType, which are const enums it does not export at run time
const Status = { INDEX_MODIFIED: 0, INDEX_ADDED: 1, INDEX_DELETED: 2, INDEX_RENAMED: 3, INDEX_COPIED: 4, MODIFIED: 5, DELETED: 6, UNTRACKED: 7 } as const;
export const RefType = { Head: 0, RemoteHead: 1, Tag: 2 } as const;

export const DEFAULT_BASES = ['origin/HEAD', 'origin/main', 'origin/master', 'main', 'master'];
// The protocol's limit: a review of more than this is not a review of a pull request
export const MAX_CHANGED_FILES = 20_000;

export type GitProblemKind = 'no_commits' | 'no_base' | 'too_many_files';

/** Why git cannot say what changed, in words for the panel. */
export class GitProblem extends Error {
  constructor(readonly kind: GitProblemKind, message: string) {
    super(message);
  }
}

export interface ReviewBase {
  ref: string;
  commit: string;
}

async function mergeBase(repository: GitRepository, ref: string): Promise<string | undefined> {
  try {
    return (await repository.getMergeBase('HEAD', ref))?.trim() || undefined;
  } catch {
    return undefined;  // no such ref, or no history in common
  }
}

/** The merge-base with the ref asked for, or with the first default branch that exists. */
export async function resolveBase(repository: GitRepository, requested?: string | null): Promise<ReviewBase> {
  if (!repository.state.HEAD?.commit) {
    throw new GitProblem('no_commits', 'This repository has no commits yet, so there is nothing to compare with.');
  }
  for (const ref of requested ? [requested] : DEFAULT_BASES) {
    const commit = await mergeBase(repository, ref);
    if (commit) {
      return { ref, commit };
    }
  }
  throw new GitProblem('no_base', requested
    ? `${requested} has no merge-base with HEAD. Check that it is fetched, and not a shallow clone.`
    : `None of ${DEFAULT_BASES.join(', ')} has a merge-base with HEAD. Choose a base to compare with.`);
}

function fileStatus(status: number): FileStatus | undefined {
  switch (status) {
    case Status.INDEX_MODIFIED:
    case Status.MODIFIED:
      return 'modified';
    case Status.INDEX_ADDED:
    case Status.INDEX_COPIED:
      return 'added';
    case Status.INDEX_DELETED:
    case Status.DELETED:
      return 'deleted';
    case Status.INDEX_RENAMED:
      return 'renamed';
    case Status.UNTRACKED:
      return 'untracked';
    default:
      return undefined;  // ignored files, and conflicts, which diffWith does not report
  }
}

/** The protocol's changed files: what differs from the base, then what git does not track yet. */
export function changedFiles(tracked: readonly GitChange[], working: readonly GitChange[]): ChangedFile[] {
  const files = new Map<string, ChangedFile>();
  for (const change of tracked) {
    const status = fileStatus(change.status);
    if (status) {
      const uri = change.uri.toString();
      files.set(uri, { uri, status, previousUri: status === 'renamed' ? change.originalUri.toString() : null });
    }
  }
  for (const change of working) {
    const uri = change.uri.toString();
    if (change.status === Status.UNTRACKED && !files.has(uri)) {
      files.set(uri, { uri, status: 'untracked', previousUri: null });
    }
  }
  return [...files.values()].sort((a, b) => (a.uri < b.uri ? -1 : a.uri > b.uri ? 1 : 0));
}

export interface ReviewChanges {
  base: ReviewBase;
  files: ChangedFile[];
}

/** Everything that differs from the base, in the working tree as it is now. */
export async function collectChanges(repository: GitRepository, requested?: string | null): Promise<ReviewChanges> {
  await repository.status();
  const base = await resolveBase(repository, requested);
  const tracked = await repository.diffWith(base.commit);
  // git.untrackedChanges decides which group untracked files are in, and "hidden" lists them nowhere
  const state = repository.state;
  const files = changedFiles(tracked, [...state.workingTreeChanges, ...(state.untrackedChanges ?? [])]);
  if (files.length > MAX_CHANGED_FILES) {
    throw new GitProblem('too_many_files',
      `${files.length.toLocaleString('en')} files differ from ${base.ref}, more than a review can take `
      + `(${MAX_CHANGED_FILES.toLocaleString('en')}). Choose a closer base.`);
  }
  return { base, files };
}

/** Branches and tags to offer as a base: local ones first, then remote, then tags. */
export function baseChoices(refs: readonly GitRef[]): { name: string; type: number }[] {
  const named = refs.filter((ref): ref is GitRef & { name: string } => !!ref.name);
  const order = [RefType.Head, RefType.RemoteHead, RefType.Tag] as number[];
  return named
    .map((ref, index) => ({ name: ref.name, type: ref.type, index }))
    .sort((a, b) => order.indexOf(a.type) - order.indexOf(b.type) || a.index - b.index)
    .map(({ name, type }) => ({ name, type }));
}
