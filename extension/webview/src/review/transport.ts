// The review page's only way out: messages to the extension. Every message received is validated
// against the shared schema; anything else is dropped.

import { ReviewHostMessage, type ReviewWebviewMessage } from '../../../src/reviewMessages';
import type { VsCodeApi } from '../transport';

export class ReviewTransport {
  private readonly listeners = new Set<(message: ReviewHostMessage) => void>();

  constructor(readonly api: VsCodeApi, target: Pick<Window, 'addEventListener'> = window) {
    target.addEventListener('message', (event) => this.receive((event as MessageEvent).data));
  }

  receive(raw: unknown): void {
    const parsed = ReviewHostMessage.safeParse(raw);
    if (!parsed.success) {
      console.warn('LineagePilot: ignoring a malformed message', parsed.error.issues[0]?.message);
      return;
    }
    this.listeners.forEach((listener) => listener(parsed.data));
  }

  onMessage(listener: (message: ReviewHostMessage) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  send(message: ReviewWebviewMessage): void {
    this.api.postMessage(message);
  }
}
