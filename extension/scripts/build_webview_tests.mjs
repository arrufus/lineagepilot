// Compiles the webview, its tests and the message contract (Phase 3) to CommonJS in
// out/webview-test, one file per source with a source map, for mocha and c8:
//
//   node scripts/build_webview_tests.mjs
//   mocha --require out/webview-test/webview/test/setup.js --require .../cleanup.js \
//     "out/webview-test/webview/test/*.test.js"
//
// esbuild, not tsc: the webview resolves modules as a bundler does (dagre is ESM; see
// webview/tsconfig.json), which tsc will not emit as CommonJS. Not bundled, so c8 filters
// coverage by compiled path, as source maps take it back to webview/src: with a bundle it
// would have to filter after remapping, and c8 12 then drops branch and function counts.
import fs from 'node:fs';
import path from 'node:path';
import { build } from 'esbuild';

const sources = (dir) => fs.readdirSync(dir, { recursive: true })
  .filter((file) => /\.tsx?$/.test(file) && !file.endsWith('.d.ts'))
  .map((file) => path.join(dir, file));

const entryPoints = [...sources('webview/src'), ...sources('webview/test'), 'src/protocol.ts', 'src/webviewMessages.ts', 'src/reviewMessages.ts'];
fs.rmSync('out/webview-test', { recursive: true, force: true });
await build({
  entryPoints,
  outdir: 'out/webview-test',
  outbase: '.',
  format: 'cjs',
  platform: 'node',
  target: 'node20',
  jsx: 'automatic',
  sourcemap: true,
  logLevel: 'warning',
});
console.log(`compiled ${entryPoints.length} webview files`);
