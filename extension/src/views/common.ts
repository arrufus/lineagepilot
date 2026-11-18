// What the Pipelines and Consumers views share (task 7.6).

import * as vscode from 'vscode';

import type { NodeKind } from '../protocol';

/** dbt's own kinds: what a file defines that a pipeline runs or a consumer reads. */
export const DBT_KINDS: ReadonlySet<NodeKind> = new Set(['model', 'seed', 'snapshot', 'source', 'exposure', 'relation']);

/** A line of text in a view, at the top or under a group. */
export class ViewMessage<P = undefined> {
  readonly type = 'message';
  constructor(readonly text: string, readonly parent?: P) {}

  treeItem(): vscode.TreeItem {
    const tree = new vscode.TreeItem(this.text, vscode.TreeItemCollapsibleState.None);
    tree.iconPath = new vscode.ThemeIcon('info');
    return tree;
  }
}

export function escapeMarkdown(text: string): string {
  return text.replace(/([\\`*_{}[\]()#+\-.!|<>~&])/g, '\\$1');
}

export function messageOf(error: unknown): string {
  const text = error instanceof Error ? error.message : String(error);
  return text.length > 200 ? `${text.slice(0, 197)}...` : text;
}
