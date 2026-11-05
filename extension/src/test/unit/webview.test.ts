// The lineage view's page (task 3.1): a strict CSP with a fresh nonce, only the extension's own
// script and stylesheet, and nothing from the caller unescaped.
import { strict as assert } from 'assert';
import * as vm from 'vm';

import { build } from 'esbuild';
import { JSDOM, VirtualConsole } from 'jsdom';

import { contentSecurityPolicy, nonce, webviewHtml } from '../../webviewHtml';

const PAGE = {
  cspSource: 'https://file+.vscode-resource.vscode-cdn.net',
  scriptUri: 'https://file+.vscode-resource.vscode-cdn.net/ext/dist/webview/webview.js',
  styleUri: 'https://file+.vscode-resource.vscode-cdn.net/ext/dist/webview/webview.css',
  title: 'Lineage: jaffle_shop',
};

suite('webview page', () => {
  test('the CSP denies everything but the nonce script and the extension\'s own styles and images', () => {
    const csp = contentSecurityPolicy(PAGE.cspSource, 'abc123');
    assert.equal(csp, [
      "default-src 'none'",
      "script-src 'nonce-abc123'",
      `style-src ${PAGE.cspSource} 'unsafe-inline'`,
      `img-src ${PAGE.cspSource} data:`,
    ].join('; '));
    assert.doesNotMatch(csp, /unsafe-eval|connect-src|frame-src|\*/);
  });

  test('the page carries the CSP, one script with its nonce, and the stylesheet', () => {
    const html = webviewHtml({ ...PAGE, nonce: 'n0nce' });
    assert.match(html, /<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'nonce-n0nce';/);
    assert.equal(html.match(/<script\b/g)?.length, 1);
    assert.ok(html.includes(`<script nonce="n0nce" src="${PAGE.scriptUri}"></script>`));
    assert.ok(html.includes(`<link rel="stylesheet" href="${PAGE.styleUri}">`));
    assert.ok(html.includes('<div id="root"></div>'));
  });

  test('a fresh nonce for every page', () => {
    const nonces = new Set(Array.from({ length: 50 }, () => nonce()));
    assert.equal(nonces.size, 50);
    for (const value of nonces) {
      assert.match(value, /^[A-Za-z0-9+/]{24}$/);
    }
    const first = /nonce-([^']+)'/.exec(webviewHtml(PAGE))?.[1];
    const second = /nonce-([^']+)'/.exec(webviewHtml(PAGE))?.[1];
    assert.ok(first && second && first !== second);
  });

  test('a project name cannot break out of the page', () => {
    const html = webviewHtml({ ...PAGE, title: 'Lineage: "x"><script>alert(1)</script>', nonce: 'n' });
    assert.equal(html.match(/<script\b/g)?.length, 1);
    assert.ok(html.includes('<title>Lineage: &quot;x&quot;&gt;&lt;script&gt;alert(1)&lt;/script&gt;</title>'));
  });

  // zod probes for eval with `new Function` when it builds an object schema. The CSP blocks that and
  // zod catches the error, but the browser still reports a CSP violation (Phase 3 exit, LP-08j).
  for (const entry of ['webview/src/main.tsx', 'webview/src/review/main.tsx']) test(`${entry} never asks for eval, which the CSP forbids`, async function () {
    this.timeout(20000);
    // As esbuild.mjs builds it for the VSIX
    const bundle = await build({
      entryPoints: [entry], bundle: true, write: false, format: 'iife', platform: 'browser',
      target: 'chrome120', jsx: 'automatic', define: { 'process.env.NODE_ENV': '"production"' }, logLevel: 'silent',
    });
    const dom = new JSDOM('<!doctype html><div id="root"></div>', { runScripts: 'outside-only', virtualConsole: new VirtualConsole() });
    const asked: string[] = [];
    // What the CSP does to eval and the Function constructor, recorded
    const forbid = <T extends object>(name: string, real: T): T => {
      const refuse = (): never => {
        asked.push(name);
        throw new dom.window.EvalError(`${name} is not allowed by the CSP`);
      };
      return new Proxy(real, { apply: refuse, construct: refuse });
    };
    Object.assign(dom.window, {
      Function: forbid('Function', dom.window.Function),
      eval: forbid('eval', dom.window.eval),
      acquireVsCodeApi: () => ({ postMessage() {}, getState: () => undefined, setState() {} }),
    });
    try {
      new vm.Script(bundle.outputFiles[0].text).runInContext(dom.getInternalVMContext());
    } finally {
      dom.window.close();
    }
    assert.deepEqual(asked, []);
  });
});
