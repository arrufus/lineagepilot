// "Refresh dbt Artifacts" (task 2.5, design sections 6.1 and 10): the only place LineagePilot
// runs dbt. It is refused in Restricted Mode, and the executable comes from the user's own
// machine-scoped setting or PATH, never from workspace settings. dbt parse takes 40-54 s at
// 5,000 models (spike S2), so it runs in the background with progress and can be cancelled.

import { spawn } from 'child_process';
import * as vscode from 'vscode';

import { onPath, userSetting } from './python';

export const SUBCOMMANDS = ['parse', 'compile', 'docs generate'] as const;
export type Subcommand = (typeof SUBCOMMANDS)[number];

export type RefreshOutcome =
  | { status: 'refused'; reason: string }
  | { status: 'no-dbt' | 'cancelled' | 'no-project' }
  | { status: 'failed' | 'done'; executable: string; exitCode: number | null };

export const RESTRICTED_MESSAGE =
  'Refresh dbt Artifacts runs dbt, which LineagePilot never does in Restricted Mode. '
  + 'Trust the workspace to use it; lineage from the existing artifacts keeps working.';

const DESCRIPTIONS: Record<Subcommand, string> = {
  parse: 'manifest.json only; fast, no warehouse connection',
  compile: 'adds compiled SQL for column lineage; needs a warehouse connection',
  'docs generate': 'adds catalog.json for column lineage; needs a warehouse connection',
};

/** The dbt to run: the user's setting, else dbt on PATH. Workspace settings are never read. */
export function dbtExecutable(): string | undefined {
  const configured = userSetting('dbt.executablePath')?.trim();
  return configured || onPath('dbt');
}

export async function pickSubcommand(): Promise<Subcommand | undefined> {
  const picked = await vscode.window.showQuickPick(
    SUBCOMMANDS.map((label) => ({ label: `dbt ${label}`, description: DESCRIPTIONS[label], subcommand: label })),
    { title: 'Refresh dbt artifacts', placeHolder: 'Which dbt command should write the artifacts?' },
  );
  return picked?.subcommand;
}

export async function refreshArtifacts(
  projectRoot: string, subcommand: Subcommand, output: vscode.LogOutputChannel,
): Promise<RefreshOutcome> {
  // Checked here as well as by the command's enablement, which only covers the UI
  if (!vscode.workspace.isTrusted) {
    output.warn('Refresh dbt Artifacts refused: the workspace is not trusted');
    void vscode.window.showWarningMessage(RESTRICTED_MESSAGE);
    return { status: 'refused', reason: 'restricted' };
  }
  const executable = dbtExecutable();
  if (!executable) {
    void vscode.window.showErrorMessage(
      'dbt was not found on PATH. Set lineagePilot.dbt.executablePath in your user settings.');
    return { status: 'no-dbt' };
  }
  if (process.platform === 'win32' && /\.(cmd|bat)$/i.test(executable)) {
    // Node runs batch files only through a shell, which would interpret the arguments
    void vscode.window.showErrorMessage(
      `${executable} is a batch file; set lineagePilot.dbt.executablePath to dbt.exe instead.`);
    return { status: 'refused', reason: 'batch file' };
  }
  const args = [...subcommand.split(' '), '--project-dir', projectRoot];
  output.info(`Running ${executable} ${args.join(' ')} in ${projectRoot}`);
  return vscode.window.withProgress(
    { location: vscode.ProgressLocation.Notification, title: `dbt ${subcommand}`, cancellable: true },
    (_progress, token) => new Promise<RefreshOutcome>((resolve) => {
      const child = spawn(executable, args, { cwd: projectRoot, windowsHide: true, shell: false });
      const log = (chunk: Buffer) => chunk.toString().split(/\r?\n/).filter(Boolean)
        .forEach((line) => output.info(`dbt: ${line}`));
      child.stdout.on('data', log);
      child.stderr.on('data', log);
      let cancelled = false;
      token.onCancellationRequested(() => {
        cancelled = true;
        child.kill();
      });
      child.on('error', (error) => {
        output.error(`dbt could not run: ${error.message}`);
        resolve({ status: 'failed', executable, exitCode: null });
      });
      child.on('close', (exitCode) => {
        if (cancelled) {
          resolve({ status: 'cancelled' });
        } else if (exitCode === 0) {
          resolve({ status: 'done', executable, exitCode });
        } else {
          void vscode.window.showErrorMessage(`dbt ${subcommand} failed (exit code ${exitCode}); see the output.`);
          resolve({ status: 'failed', executable, exitCode });
        }
      });
    }),
  );
}
