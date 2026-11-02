// Which folders are dbt projects: one language server runs for each (design section 4). Kept
// free of the vscode module so it can be unit-tested.

import * as path from 'path';

import { isInside } from './interpreter';

export const PROJECT_FILE = 'dbt_project.yml';

// Folders whose dbt_project.yml files belong to installed packages or build output, not to
// projects the user works on.
export const EXCLUDED_DIRECTORIES = [
  'dbt_packages', 'dbt_modules', 'node_modules', 'target', '.venv', 'venv', 'site-packages', '.git',
];

export const PROJECT_GLOB = `**/${PROJECT_FILE}`;
export const EXCLUDE_GLOB = `{${EXCLUDED_DIRECTORIES.map((d) => `**/${d}/**`).join(',')}}`;

export interface Project {
  root: string;
  // The workspace folder that contains it
  folder: string;
}

/** Project roots from found dbt_project.yml paths, sorted, with excluded locations dropped. */
export function projectsFrom(files: string[], folders: string[], platform: NodeJS.Platform): Project[] {
  const p = platform === 'win32' ? path.win32 : path.posix;
  const found = new Map<string, Project>();
  for (const file of files) {
    if (p.basename(file) !== PROJECT_FILE) {
      continue;
    }
    const root = p.dirname(file);
    const folder = folders
      .filter((f) => isInside(root, f, platform))
      .sort((a, b) => b.length - a.length)[0];
    if (!folder) {
      continue;
    }
    const parts = p.relative(folder, root).split(p.sep);
    if (parts.some((part) => EXCLUDED_DIRECTORIES.includes(part))) {
      continue;
    }
    found.set(root, { root, folder });
  }
  return [...found.values()].sort((a, b) => a.root.localeCompare(b.root));
}

/** The project a file belongs to: the innermost project root that contains it. */
export function projectOf<T extends { root: string }>(
  file: string, projects: T[], platform: NodeJS.Platform,
): T | undefined {
  return projects
    .filter((project) => isInside(file, project.root, platform))
    .sort((a, b) => b.root.length - a.root.length)[0];
}
