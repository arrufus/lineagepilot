// The readers' settings, as the extension contributes them (tasks 5.1 and 8.2)
import { strict as assert } from 'assert';
import * as fs from 'fs';
import * as path from 'path';

const EXTENSION = path.resolve(__dirname, '..', '..', '..');
const manifest = JSON.parse(fs.readFileSync(path.join(EXTENSION, 'package.json'), 'utf8'));
const properties = manifest.contributes.configuration.properties;

suite('reader settings', () => {
  test('the Dagster paths are the whole workspace by default, per folder, and bounded', () => {
    const paths = properties['lineagePilot.dagster.paths'];
    assert.deepEqual(paths.default, ['.']);
    assert.equal(paths.scope, 'resource');
    assert.equal(paths.items.type, 'string');
    // As the server cleans them: a pattern of at most 200 characters, and at most 50 of them
    assert.equal(paths.items.maxLength, 200);
    assert.equal(paths.maxItems, 50);
  });

  test('each reader setting reaches the server under the option its reader declares', () => {
    const server = fs.readFileSync(path.join(EXTENSION, 'src', 'server.ts'), 'utf8');
    assert.match(server, /dagFolders: config\.get<string\[\]>\('airflow\.dagFolders', \['\*\*\/dags'\]\)/);
    assert.match(server, /dagsterPaths: config\.get<string\[\]>\('dagster\.paths', \['\.'\]\)/);
  });
});
