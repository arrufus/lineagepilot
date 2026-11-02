// For the coverage run only: make dist/extension.js load the tsc output in out/ instead of the
// esbuild bundle. c8 filters scripts before source maps are applied, so in the bundle our code
// and its dependencies are one file; loaded from out/, dependencies come from node_modules (and
// are excluded), our modules map back to src/*.ts, and the tests share the extension's modules.
import fs from 'node:fs';

fs.mkdirSync('dist', { recursive: true });
fs.writeFileSync('dist/extension.js', "module.exports = require('../out/extension.js');\n");
console.log('dist/extension.js now loads out/extension.js (coverage build)');
