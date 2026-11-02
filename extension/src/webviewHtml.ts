// The lineage webview's HTML (task 3.1, design section 10): a strict Content Security Policy
// with a per-load nonce, only the extension's own bundle as script and style, no remote
// resources, and nothing from the workspace in the page itself. Kept free of the vscode module.

import { randomBytes } from 'crypto';

export interface WebviewPage {
  // webview.cspSource: the origin VS Code serves localResourceRoots from
  cspSource: string;
  scriptUri: string;
  styleUri: string;
  title: string;
  nonce?: string;
}

export function nonce(): string {
  return randomBytes(18).toString('base64');
}

function escapeAttribute(value: string): string {
  return value.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

export function contentSecurityPolicy(cspSource: string, scriptNonce: string): string {
  return [
    "default-src 'none'",
    `script-src 'nonce-${scriptNonce}'`,
    // React Flow positions nodes with inline styles
    `style-src ${cspSource} 'unsafe-inline'`,
    `img-src ${cspSource} data:`,
  ].join('; ');
}

export function webviewHtml(page: WebviewPage): string {
  const scriptNonce = page.nonce ?? nonce();
  const csp = contentSecurityPolicy(page.cspSource, scriptNonce);
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="Content-Security-Policy" content="${escapeAttribute(csp)}">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="stylesheet" href="${escapeAttribute(page.styleUri)}">
<title>${escapeAttribute(page.title)}</title>
</head>
<body>
<div id="root"></div>
<script nonce="${escapeAttribute(scriptNonce)}" src="${escapeAttribute(page.scriptUri)}"></script>
</body>
</html>
`;
}
