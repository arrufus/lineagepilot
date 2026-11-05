// Finding the repository through the vscode.git extension (task 4.1). VS Code turns that extension
// off in Restricted Mode, and a user can turn it off with git.enabled, so each way of not having
// git has its own words: a review without git would have to claim that nothing changed.

import * as vscode from 'vscode';

import type { GitRepository } from './git';
import type { ReviewProblem } from './reviewMessages';

interface GitApi {
  readonly state: 'uninitialized' | 'initialized';
  onDidChangeState(listener: (state: 'uninitialized' | 'initialized') => void): vscode.Disposable;
  getRepository(uri: vscode.Uri): GitRepository | null;
  openRepository(uri: vscode.Uri): Promise<GitRepository | null>;
}

interface GitExports {
  readonly enabled: boolean;
  getAPI(version: 1): GitApi;
}

const STARTUP_MS = 15_000;

export class NoRepository extends Error {
  constructor(readonly kind: ReviewProblem, message: string) {
    super(message);
  }
}

function initialized(api: GitApi): Promise<void> {
  if (api.state === 'initialized') {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    const timer = setTimeout(() => { subscription.dispose(); resolve(); }, STARTUP_MS);
    const subscription = api.onDidChangeState((state) => {
      if (state === 'initialized') {
        clearTimeout(timer);
        subscription.dispose();
        resolve();
      }
    });
  });
}

/** The repository holding a folder. Throws NoRepository, saying why, when there is none to use. */
export async function repositoryFor(folder: string): Promise<GitRepository> {
  if (!vscode.workspace.isTrusted) {
    throw new NoRepository('restricted', 'VS Code turns Git off in Restricted Mode, so LineagePilot cannot tell what '
      + 'changed. Trust this workspace to review changes. Lineage from the existing artifacts keeps working.');
  }
  const extension = vscode.extensions.getExtension<GitExports>('vscode.git');
  if (!extension) {
    throw new NoRepository('no_git', 'The built-in Git extension is not available, so LineagePilot cannot tell what changed.');
  }
  const git = extension.isActive ? extension.exports : await extension.activate();
  if (!git.enabled) {
    throw new NoRepository('no_git', 'Git is turned off in VS Code (git.enabled), so LineagePilot cannot tell what changed.');
  }
  const api = git.getAPI(1);
  await initialized(api);
  const uri = vscode.Uri.file(folder);
  const repository = api.getRepository(uri) ?? await api.openRepository(uri);
  if (!repository) {
    throw new NoRepository('no_repository', 'This dbt project is not in a Git repository, so there is nothing to compare it with.');
  }
  return repository;
}
