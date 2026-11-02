// The webview's only way out (task 3.1): typed requests to the extension over postMessage,
// replacing the legacy dashboard's @/lib/api hooks. Every message received is validated
// against the shared schema; anything else is dropped.

import type { GraphParams, GraphResult, ImpactParams, ImpactResult, Location, NodeParams, NodeResult } from '../../src/protocol';
import { HostMessage, type ViewSettings, ViewState, type WebviewMessage } from '../../src/webviewMessages';

/** The object acquireVsCodeApi() returns inside a VS Code webview. */
export interface VsCodeApi {
  postMessage(message: unknown): void;
  getState(): unknown;
  setState(state: unknown): void;
}

export class RequestError extends Error {
  constructor(message: string, readonly code: number | null) {
    super(message);
  }
}

type Notice = Extract<HostMessage, { type: 'init' | 'focus' | 'indexUpdated' }>;
type Pending = { resolve: (value: unknown) => void; reject: (error: Error) => void; expects: string };

export class Transport {
  private nextId = 1;
  private readonly pending = new Map<number, Pending>();
  private readonly listeners = new Set<(notice: Notice) => void>();

  constructor(readonly api: VsCodeApi, target: Pick<Window, 'addEventListener'> = window) {
    target.addEventListener('message', (event) => this.receive((event as MessageEvent).data));
  }

  /** A message from the extension. */
  receive(raw: unknown): void {
    const parsed = HostMessage.safeParse(raw);
    if (!parsed.success) {
      console.warn('LineagePilot: ignoring a malformed message', parsed.error.issues[0]?.message);
      return;
    }
    const message = parsed.data;
    if (message.type === 'init' || message.type === 'focus' || message.type === 'indexUpdated') {
      this.listeners.forEach((listener) => listener(message));
      return;
    }
    const waiting = this.pending.get(message.id);
    if (!waiting) {
      return;
    }
    this.pending.delete(message.id);
    if (message.type === 'error') {
      waiting.reject(new RequestError(message.message, message.code));
    } else if (message.type !== waiting.expects) {
      waiting.reject(new RequestError(`expected ${waiting.expects}, received ${message.type}`, null));
    } else {
      waiting.resolve(message.result);
    }
  }

  onNotice(listener: (notice: Notice) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private send(message: WebviewMessage): void {
    this.api.postMessage(message);
  }

  private request<T>(type: 'graph' | 'impact' | 'node', params: unknown): Promise<T> {
    const id = this.nextId++;
    return new Promise<T>((resolve, reject) => {
      this.pending.set(id, { resolve: resolve as (value: unknown) => void, reject, expects: `${type}Result` });
      this.send({ type, id, params } as WebviewMessage);
    });
  }

  graph(params: GraphParams): Promise<GraphResult> {
    return this.request('graph', params);
  }

  impact(params: ImpactParams): Promise<ImpactResult> {
    return this.request('impact', params);
  }

  node(params: NodeParams): Promise<NodeResult> {
    return this.request('node', params);
  }

  ready(): void {
    this.send({ type: 'ready' });
  }

  /** Open a file, or with ``preserveFocus`` preview it and keep the keyboard in the graph. */
  openFile(location: Location, preserveFocus = false): void {
    this.send({ type: 'openFile', location, preserveFocus });
  }

  saveSettings(settings: ViewSettings): void {
    this.send({ type: 'saveSettings', settings });
  }

  /** What this view showed before VS Code last hid it, if anything valid was kept. */
  restoreState(): ViewState | null {
    const parsed = ViewState.safeParse(this.api.getState());
    return parsed.success ? parsed.data : null;
  }

  saveState(state: ViewState): void {
    this.api.setState(state);
  }
}
