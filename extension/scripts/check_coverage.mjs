// Fails unless the extension's line coverage meets the gate (Phase 2 exit: at least 75%).
//   node scripts/check_coverage.mjs coverage/coverage-summary.json 75
import fs from 'node:fs';

const [file, threshold] = process.argv.slice(2);
const summary = JSON.parse(fs.readFileSync(file, 'utf8'));
const { lines, branches, functions } = summary.total;
console.log(`coverage: lines ${lines.pct}%, branches ${branches.pct}%, functions ${functions.pct}%`);
for (const [name, entry] of Object.entries(summary)) {
  if (name !== 'total') {
    console.log(`  ${entry.lines.pct.toFixed(1).padStart(5)}%  ${name}`);
  }
}
// An empty report gives "Unknown", which must fail, not compare as false
if (typeof lines.pct !== 'number' || lines.pct < Number(threshold)) {
  console.error(`line coverage ${lines.pct}% is below the ${threshold}% gate`);
  process.exit(1);
}
