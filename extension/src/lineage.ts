// Typed lineage/* requests (task 2.3, design section 9). Params are checked before they are
// sent and every result is validated against the shared schema when it arrives, so nothing
// malformed from the language server reaches the views. Kept free of the vscode module.

import { z } from 'zod';

import { ErrorCode, notifications, requests, type NotificationMethod, type RequestMethod } from './protocol';

export interface Sender {
  sendRequest(method: string, params: unknown): Promise<unknown>;
}

export type Params<M extends RequestMethod> = z.input<(typeof requests)[M]['params']>;
export type Result<M extends RequestMethod> = z.output<(typeof requests)[M]['result']>;

export class ProtocolError extends Error {
  constructor(readonly method: string, readonly issues: string) {
    super(`The language server's ${method} response does not match the protocol: ${issues}`);
  }
}

function summarise(error: z.ZodError): string {
  return error.issues
    .slice(0, 3)
    .map((issue) => `${issue.path.join('.') || '(root)'}: ${issue.message}`)
    .join('; ');
}

export async function request<M extends RequestMethod>(
  sender: Sender, method: M, params: Params<M>,
): Promise<Result<M>> {
  // Always a params object: pygls cannot decode a request without one (spike S1, finding 4)
  const checked = requests[method].params.parse(params);
  const raw = await sender.sendRequest(method, checked);
  const parsed = requests[method].result.safeParse(raw);
  if (!parsed.success) {
    throw new ProtocolError(method, summarise(parsed.error));
  }
  return parsed.data as Result<M>;
}

export function parseNotification<M extends NotificationMethod>(
  method: M, raw: unknown,
): z.output<(typeof notifications)[M]> | ProtocolError {
  const parsed = notifications[method].safeParse(raw);
  return parsed.success ? parsed.data : new ProtocolError(method, summarise(parsed.error));
}

/** The JSON-RPC error code of a failed request, if it has one. */
export function errorCode(error: unknown): number | undefined {
  const code = (error as { code?: unknown } | undefined)?.code;
  return typeof code === 'number' ? code : undefined;
}

export function isNotReady(error: unknown): boolean {
  return errorCode(error) === ErrorCode.NotReady;
}
