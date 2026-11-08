// The sample project (task 6.1): a small dbt project of our own with real dbt 1.12 artifacts, three
// Airflow DAG files and a prepared change, built into the VSIX from fixtures/impact-shop by
// scripts/build_sample.py. LineagePilot: Open Sample Project copies it to a folder the user picks;
// LineagePilot: Apply the Sample Change then makes the change and puts the manifest `dbt compile`
// would write in target/, so the review can be tried with neither dbt nor Airflow installed.
//
// Nothing here runs anything. It copies files that ship with the extension: into a new folder, or
// over a copy of the sample's own files, never through a symbolic link.

import * as fs from 'fs';
import * as path from 'path';

export const SAMPLE_FOLDER = 'lineagepilot-sample';
/** Marks a folder as a copy of the sample, so the change is never applied to anything else. */
export const SAMPLE_MARKER = path.join('.lineagepilot', 'sample.json');
/** The prepared change: files at their paths in the project, target/manifest.json included. */
export const SAMPLE_CHANGE = path.join('changes', 'rename-order-total');
const ARTIFACTS = 'target';
// Source files are dated this far before the artifacts, so nothing starts out stale
const SOURCE_AGE_MS = 3_600_000;

/** The first of `lineagepilot-sample`, `lineagepilot-sample-2`, … that does not exist in `parent`. */
export function freeFolder(parent: string, exists: (candidate: string) => boolean = fs.existsSync): string {
  for (let n = 1; n < 1000; n++) {
    const candidate = path.join(parent, n === 1 ? SAMPLE_FOLDER : `${SAMPLE_FOLDER}-${n}`);
    if (!exists(candidate)) {
      return candidate;
    }
  }
  throw new Error(`Too many copies of the sample in ${parent}`);
}

/** Every regular file below `root`, relative to it, in a stable order. Symlinks are not followed. */
export async function filesBelow(root: string): Promise<string[]> {
  const found: string[] = [];
  const walk = async (relative: string) => {
    const entries = await fs.promises.readdir(path.join(root, relative), { withFileTypes: true });
    for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
      const child = path.join(relative, entry.name);
      if (entry.isDirectory()) {
        await walk(child);
      } else if (entry.isFile()) {
        found.push(child);
      }
    }
  };
  await walk('');
  return found;
}

function isArtifact(relative: string): boolean {
  return relative.split(path.sep)[0] === ARTIFACTS;
}

/** Date the source files before the artifacts, as `dbt compile` would leave them. A copy's time
 * depends on the platform: on macOS it can keep the original's, elsewhere it is the time the copy
 * was written, so the order of the writes would decide what looks stale. */
async function settle(root: string, relatives: string[], now: number): Promise<void> {
  const source = new Date(now - SOURCE_AGE_MS);
  const artifact = new Date(now);
  for (const relative of relatives) {
    const when = isArtifact(relative) ? artifact : source;
    await fs.promises.utimes(path.join(root, relative), when, when);
  }
}

/** Copy the sample from `bundled` into a new folder in `parent`; returns the new folder. */
export async function copySample(bundled: string, parent: string, now = Date.now()): Promise<string> {
  if (!fs.existsSync(path.join(bundled, SAMPLE_MARKER))) {
    throw new Error(`The sample project is missing from this build of LineagePilot (${bundled}).`);
  }
  const destination = freeFolder(parent);
  await fs.promises.mkdir(destination, { recursive: false });
  const files = await filesBelow(bundled);
  for (const relative of files) {
    const target = path.join(destination, relative);
    await fs.promises.mkdir(path.dirname(target), { recursive: true });
    await fs.promises.copyFile(path.join(bundled, relative), target, fs.constants.COPYFILE_EXCL);
  }
  await settle(destination, files, now);
  return destination;
}

/** Whether `root` holds the sample's marker, as a regular file. A cloned repository can hold one
 * too, so this only says where to offer the change: applySampleChange checks much more. */
export function isSample(root: string): boolean {
  try {
    return fs.lstatSync(path.join(root, SAMPLE_MARKER)).isFile();
  } catch {
    return false;
  }
}

export type ApplyOutcome =
  | { status: 'applied'; files: string[] }
  | { status: 'already-applied' }
  | { status: 'not-sample' }
  /** A file the change replaces is no longer the sample's own, so it is left alone. */
  | { status: 'modified'; files: string[] }
  /** A path the change would write through is a symbolic link or not a regular file. */
  | { status: 'unsafe'; file: string };

/** Why writing `relative` below `root` could leave `root`, or undefined if it cannot: every folder on
 * the way must be a real folder and the file a regular file, none of them a symbolic link. */
async function unsafePath(root: string, relative: string): Promise<string | undefined> {
  const parts = relative.split(path.sep);
  let current = root;
  for (const [index, part] of parts.entries()) {
    current = path.join(current, part);
    let stat: fs.Stats;
    try {
      stat = await fs.promises.lstat(current);
    } catch {
      return 'missing';
    }
    const last = index === parts.length - 1;
    if (stat.isSymbolicLink() || (last ? !stat.isFile() : !stat.isDirectory())) {
      return last ? 'not a regular file' : 'not a real folder';
    }
  }
  return undefined;
}

/** Make the prepared change in a copy of the sample: the edited files first, then the manifest, so
 * the manifest is the newer, as after `dbt compile`.
 *
 * Everything written comes from `bundled`, the sample inside the extension, never from the folder:
 * a cloned repository can hold the marker and a `changes/` folder of its own. A file is replaced
 * only if it is still byte for byte the sample's, through no symbolic link, so the most a hostile
 * folder can get is the sample's own change made to its own copies of the sample's files. */
export async function applySampleChange(root: string, bundled: string, now = Date.now()): Promise<ApplyOutcome> {
  if (!isSample(root)) {
    return { status: 'not-sample' };
  }
  const real = await fs.promises.realpath(root);
  const change = path.join(bundled, SAMPLE_CHANGE);
  const files = await filesBelow(change);
  const sources = files.filter((f) => !isArtifact(f));
  const modified: string[] = [];
  let applied = 0;
  for (const relative of files) {
    const problem = await unsafePath(real, relative);
    if (problem === 'missing') {
      modified.push(relative);
      continue;
    }
    if (problem) {
      return { status: 'unsafe', file: relative };
    }
    const [current, original, changed] = await Promise.all([
      fs.promises.readFile(path.join(real, relative)),
      fs.promises.readFile(path.join(bundled, relative)),
      fs.promises.readFile(path.join(change, relative)),
    ]);
    if (current.equals(changed)) {
      applied += 1;
    } else if (!current.equals(original)) {
      modified.push(relative);
    }
  }
  if (modified.length) {
    return { status: 'modified', files: modified };
  }
  if (applied === files.length) {
    return { status: 'already-applied' };
  }
  // The edited files are new and the manifest newer still, as after an edit and `dbt compile`
  for (const relative of [...sources, ...files.filter(isArtifact)]) {
    const when = new Date(isArtifact(relative) ? now : now - 1000);
    // O_NOFOLLOW where there is one: the file was checked above, and must still not be a link
    const flags = fs.constants.O_WRONLY | fs.constants.O_TRUNC | (fs.constants.O_NOFOLLOW ?? 0);
    const handle = await fs.promises.open(path.join(real, relative), flags);
    try {
      await handle.writeFile(await fs.promises.readFile(path.join(change, relative)));
      await handle.utimes(when, when);
    } finally {
      await handle.close();
    }
  }
  return { status: 'applied', files: sources };
}
