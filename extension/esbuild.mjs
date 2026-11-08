// Bundles the extension and its npm dependencies into one file, so the VSIX ships no node_modules,
// and each webview into one script and one stylesheet: the lineage graph (Phase 3), and the Change
// Impact Review (Phase 4), which has no graph to pay for.
import fs from 'node:fs';
import { build } from 'esbuild';

const production = process.argv.includes('--production');

// What each bundle took from node_modules: scripts/third_party_notices.py lists their licences
const inputs = new Set();
const record = (result) => Object.keys(result.metafile.inputs).forEach((input) => inputs.add(input));

record(await build({
  entryPoints: ['src/extension.ts'],
  bundle: true,
  outfile: 'dist/extension.js',
  external: ['vscode'],
  format: 'cjs',
  platform: 'node',
  target: 'node20',
  sourcemap: true,
  // The VSIX build is minified; development builds keep names for debugging and coverage
  minify: production,
  metafile: true,
  logLevel: 'info',
}));

// A development build's source map must not outlive it
fs.rmSync('dist/webview', { recursive: true, force: true });

// The webview runs in VS Code's Chromium, under a CSP that allows only this script (by nonce)
// and stylesheets from the extension: no eval, no inline scripts, nothing fetched at run time.
record(await build({
  entryPoints: { webview: 'webview/src/main.tsx', review: 'webview/src/review/main.tsx' },
  bundle: true,
  outdir: 'dist/webview',
  format: 'iife',
  platform: 'browser',
  target: 'chrome120',
  jsx: 'automatic',
  define: { 'process.env.NODE_ENV': JSON.stringify(production ? 'production' : 'development') },
  sourcemap: !production,
  minify: production,
  metafile: true,
  logLevel: 'info',
}));
record(await build({
  entryPoints: { webview: 'webview/src/styles.css', review: 'webview/src/review/review.css' },
  bundle: true,
  outdir: 'dist/webview',
  minify: production,
  metafile: true,
  logLevel: 'info',
}));
fs.writeFileSync('dist/bundled-inputs.json', `${JSON.stringify([...inputs].sort(), null, 1)}\n`);
