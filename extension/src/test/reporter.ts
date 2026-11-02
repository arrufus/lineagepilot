// Mocha's spec reporter, plus each result appended to $LP_TEST_LOG as it happens, so a run that
// ends early (the window closes, the host crashes) still leaves its failures behind.
import * as fs from 'fs';
import Mocha = require('mocha');

class FileSpecReporter extends Mocha.reporters.Spec {
  constructor(runner: Mocha.Runner, options?: Mocha.MochaOptions) {
    super(runner, options);
    const file = process.env.LP_TEST_LOG;
    if (!file) {
      return;
    }
    const write = (line: string) => fs.appendFileSync(file, `${line}\n`);
    runner.on('pass', (test) => write(`PASS ${test.fullTitle()} (${test.duration} ms)`));
    runner.on('pending', (test) => write(`SKIP ${test.fullTitle()}`));
    runner.on('fail', (test, error: Error) => write(`FAIL ${test.fullTitle()}\n${error.stack ?? error.message}\n`));
    runner.on('end', () => write(`END ${runner.stats?.passes ?? 0} passed, ${runner.stats?.failures ?? 0} failed`));
  }
}

export = FileSpecReporter;
