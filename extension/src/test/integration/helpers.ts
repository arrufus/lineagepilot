// Shared by the integration tests, which run inside a real VS Code (see .vscode-test.mjs).
import * as fs from 'fs';
import * as path from 'path';
import * as vscode from 'vscode';

import type { LineagePilotApi } from '../../extension';
import type { Status } from '../../protocol';
import type { ProjectServer } from '../../server';

export const EXTENSION_ID = 'lineagepilot.lineagepilot';
export const J = 'model.pilot_shop';
export const label = process.env.LP_LABEL ?? 'trusted';
export const expectTrusted = process.env.LP_EXPECT_TRUSTED !== '0';
const markers = process.env.LP_MARKERS ?? '';

export function root(): string {
  return vscode.workspace.workspaceFolders![0].uri.fsPath;
}

export function uri(...parts: string[]): vscode.Uri {
  return vscode.Uri.file(path.join(root(), ...parts));
}

/** Whether a planted or fake executable recorded that it ran. */
export function ran(name: string): string | undefined {
  const file = path.join(markers, name);
  return fs.existsSync(file) ? fs.readFileSync(file, 'utf8').trim() : undefined;
}

export async function until<T>(what: string, timeoutMs: number, fn: () => Promise<T | undefined> | T | undefined): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: unknown;
  while (Date.now() < deadline) {
    try {
      const value = await fn();
      if (value !== undefined && value !== false) {
        return value;
      }
    } catch (error) {
      last = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`timed out after ${timeoutMs} ms waiting for ${what}${last ? `: ${last}` : ''}`);
}

export async function api(): Promise<LineagePilotApi> {
  const extension = vscode.extensions.getExtension<LineagePilotApi>(EXTENSION_ID);
  if (!extension) {
    throw new Error(`${EXTENSION_ID} is not installed`);
  }
  return extension.isActive ? extension.exports : extension.activate();
}

/** The project's server, once its index is complete. */
export async function readyServer(): Promise<ProjectServer & { status: Status }> {
  const { manager } = await api();
  return until('the index to be ready', 120_000, () => {
    const server = manager.list()[0];
    const status = server?.status;
    return status?.state === 'ready' && !status.indexing && status.columnLineage === 'done'
      ? (server as ProjectServer & { status: Status }) : undefined;
  });
}

export function position(document: vscode.TextDocument, text: string, offset = 1): vscode.Position {
  const index = document.getText().indexOf(text);
  if (index < 0) {
    throw new Error(`${text} is not in ${document.uri.fsPath}`);
  }
  return document.positionAt(index + offset);
}

/** A diagnostic's code: a plain value, or the value of a code with a documentation link. */
export function codeOf(diagnostic: vscode.Diagnostic): string {
  const code = diagnostic.code;
  return String(typeof code === 'object' && code !== null ? code.value : code);
}
