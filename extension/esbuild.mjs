// Bundles the extension and its npm dependencies into one file, so the VSIX ships no node_modules.
import { build } from 'esbuild';

await build({
  entryPoints: ['src/extension.ts'],
  bundle: true,
  outfile: 'dist/extension.js',
  external: ['vscode'],
  format: 'cjs',
  platform: 'node',
  target: 'node20',
  sourcemap: true,
  // The VSIX build is minified; development builds keep names for debugging and coverage
  minify: process.argv.includes('--production'),
  logLevel: 'info',
});
