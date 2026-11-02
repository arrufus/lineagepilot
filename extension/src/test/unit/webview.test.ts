// The lineage view's page (task 3.1): a strict CSP with a fresh nonce, only the extension's own
// script and stylesheet, and nothing from the caller unescaped.
import { strict as assert } from 'assert';

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
});
