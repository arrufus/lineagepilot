// Entry point that VS Code loads inside the extension host to run the integration suite.
import * as path from 'path';

export async function run(): Promise<void> {
  // Mocha 12 is ESM-only; this file compiles to CommonJS, so load it with a dynamic import.
  const { default: Mocha } = await import('mocha');
  const mocha = new Mocha({ ui: 'tdd', color: true, timeout: 120_000 });
  mocha.addFile(path.resolve(__dirname, 'extension.test.js'));
  await mocha.loadFilesAsync();
  await new Promise<void>((resolve, reject) => {
    mocha.run((failures) => (failures ? reject(new Error(`${failures} integration tests failed`)) : resolve()));
  });
}
