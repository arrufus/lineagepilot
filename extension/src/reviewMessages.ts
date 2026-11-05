// Messages between the extension and the Change Impact Review webview (task 4.4, design section
// 9). Both sides validate every message against these schemas. The webview only asks: the
// extension reads git, asks the language server, copies to the clipboard and opens files. It never
// takes text to copy from the page, so a page cannot put anything on the clipboard but the review.
// Kept free of the vscode module, so the webview bundle can import it.

import * as z from 'zod';

import { Count, Id, Location, Name, ReviewResult, Text } from './protocol';

export const ReviewProblem = z.enum([
  'restricted', 'no_git', 'no_repository', 'no_commits', 'no_base', 'too_many_files', 'not_ready', 'failed',
]);
export type ReviewProblem = z.infer<typeof ReviewProblem>;

/** From the webview to the extension. */
export const ReviewWebviewMessage = z.discriminatedUnion('type', [
  z.strictObject({ type: z.literal('ready') }),
  z.strictObject({ type: z.literal('refresh') }),
  z.strictObject({ type: z.literal('pickBase') }),
  z.strictObject({ type: z.literal('copyMarkdown') }),
  z.strictObject({ type: z.literal('openFile'), location: Location }),
  z.strictObject({ type: z.literal('showInGraph'), id: Id }),
  z.strictObject({ type: z.literal('runCompile') }),
  z.strictObject({ type: z.literal('manageTrust') }),
]);
export type ReviewWebviewMessage = z.infer<typeof ReviewWebviewMessage>;

/** From the extension to the webview. */
export const ReviewHostMessage = z.discriminatedUnion('type', [
  z.strictObject({ type: z.literal('init'), project: z.strictObject({ name: Name, root: Text }), trusted: z.boolean() }),
  z.strictObject({ type: z.literal('reviewStarted') }),
  z.strictObject({ type: z.literal('reviewResult'), result: ReviewResult }),
  z.strictObject({ type: z.literal('reviewFailed'), problem: ReviewProblem, message: Text }),
  z.strictObject({ type: z.literal('copied'), characters: Count }),
]);
export type ReviewHostMessage = z.infer<typeof ReviewHostMessage>;
