// The browser the webview tests run in: jsdom, plus what React Flow needs that jsdom lacks.
// Required first (mocha --require), before React and Testing Library load: Testing Library's
// screen binds to the document when it loads.

// As in the webview: zod builds its schemas without eval
import '../src/jitless';

import { JSDOM } from 'jsdom';
// React touches no DOM when it loads (react-dom does), so it may come first
import { act } from 'react';

const dom = new JSDOM('<!doctype html><html><body></body></html>', { pretendToBeVisual: true, url: 'http://localhost/' });
const win = dom.window as unknown as Window & typeof globalThis & Record<string, unknown>;

// jsdom lays nothing out. React Flow hides a node until it has measured it, and needs a sized
// container, so elements report a size: a node's from its CSS, anything else a 1200x800 pane.
const NODE_SIZE = { width: 240, height: 64 };
for (const [property, axis] of [['offsetWidth', 'width'], ['offsetHeight', 'height']] as const) {
  Object.defineProperty(win.HTMLElement.prototype, property, {
    configurable: true,
    get(this: HTMLElement) {
      return this.classList.contains('react-flow__node') ? NODE_SIZE[axis] : axis === 'width' ? 1200 : 800;
    },
  });
}

// Browser callbacks that update React run with React applying the updates (in act, so tests
// see them). Testing Library turns the act environment off while a findBy query waits; then
// they run as they come, as Testing Library expects.
function inAct(callback: () => void): void {
  if ((globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT) {
    act(callback);
  } else {
    callback();
  }
}

// Reports each observed element once, a little later, as a browser does when observation starts
class ResizeObserverStub {
  constructor(private readonly callback: ResizeObserverCallback) {}
  observe(target: Element): void {
    const { offsetWidth: width, offsetHeight: height } = target as HTMLElement;
    const entry = { target, contentRect: { width, height }, borderBoxSize: [{ inlineSize: width, blockSize: height }] };
    queueMicrotask(() => inAct(() => this.callback([entry as unknown as ResizeObserverEntry], this as unknown as ResizeObserver)));
  }
  unobserve(): void {}
  disconnect(): void {}
}

// React Flow reads the zoom level from the pane's transform
class DOMMatrixReadOnlyStub {
  readonly m22: number;
  constructor(transform?: string) {
    this.m22 = Number(/scale\(([^)]+)\)/.exec(transform ?? '')?.[1] ?? 1);
  }
}

// Edge labels measure their text to size the background
(win.SVGElement.prototype as unknown as { getBBox(this: SVGElement): DOMRect }).getBBox = function getBBox() {
  return { x: 0, y: 0, width: 8 * (this.textContent?.length ?? 0), height: 14 } as DOMRect;
};

win.ResizeObserver = ResizeObserverStub as unknown as typeof ResizeObserver;

win.DOMMatrixReadOnly = DOMMatrixReadOnlyStub as unknown as typeof DOMMatrixReadOnly;

// Expose the window's globals, as a page has them. Node's own (Event, EventTarget, ...) stay,
// except navigator, which React and user-event read.
const globals = globalThis as Record<string, unknown>;
for (const key of Object.getOwnPropertyNames(win)) {
  if (!key.startsWith('_') && !(key in globals)) {
    Object.defineProperty(globals, key, { configurable: true, get: () => win[key] });
  }
}
Object.defineProperty(globals, 'navigator', { configurable: true, get: () => win.navigator });
Object.defineProperty(globals, 'window', { configurable: true, value: win });
globals.IS_REACT_ACT_ENVIRONMENT = true;
