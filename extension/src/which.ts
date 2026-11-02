// Find a command on PATH without running it, so the interpreter resolver can check where a
// candidate lives before it ever executes it (design section 10). Kept free of the vscode
// module so it can be unit-tested.

import * as fs from 'fs';
import * as path from 'path';

export interface WhichOptions {
  env: NodeJS.ProcessEnv;
  platform: NodeJS.Platform;
  isExecutable?: (file: string) => boolean;
}

function defaultIsExecutable(file: string): boolean {
  try {
    const stat = fs.statSync(file);
    if (!stat.isFile()) {
      return false;
    }
    if (process.platform === 'win32') {
      return true;
    }
    fs.accessSync(file, fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

/** The absolute path PATH lookup would run for `command`, or undefined. */
export function which(command: string, options: WhichOptions): string | undefined {
  const { env, platform } = options;
  const isExecutable = options.isExecutable ?? defaultIsExecutable;
  const p = platform === 'win32' ? path.win32 : path.posix;
  if (command.includes('/') || (platform === 'win32' && command.includes('\\'))) {
    return p.isAbsolute(command) && isExecutable(command) ? command : undefined;
  }
  const pathValue = (platform === 'win32' ? env.Path ?? env.PATH : env.PATH) ?? '';
  const directories = pathValue.split(platform === 'win32' ? ';' : ':').filter((d) => d && p.isAbsolute(d));
  const extensions = platform === 'win32'
    ? (env.PATHEXT ?? '.COM;.EXE;.BAT;.CMD').split(';').filter(Boolean).map((e) => e.toLowerCase())
    : [''];
  const hasExtension = platform === 'win32' && extensions.includes(p.extname(command).toLowerCase());
  for (const directory of directories) {
    for (const extension of hasExtension ? [''] : extensions) {
      const candidate = p.join(directory, command + extension);
      if (isExecutable(candidate)) {
        return candidate;
      }
    }
  }
  return undefined;
}
