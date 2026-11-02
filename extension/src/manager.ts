// Every dbt project in the workspace and its language server (tasks 2.4 and 2.5). Python is
// resolved once for all of them, and again when the interpreter setting, the Python
// extension's environment or Workspace Trust changes.

import * as fs from 'fs';
import * as vscode from 'vscode';

import type { Interpreter, Resolution } from './interpreter';
import { EXCLUDE_GLOB, PROJECT_GLOB, projectOf, projectsFrom } from './projects';
import { resolvePython } from './python';
import { ProjectServer } from './server';

export class ServerManager implements vscode.Disposable {
  readonly servers = new Map<string, ProjectServer>();
  resolution: Resolution | undefined;
  private readonly changed = new vscode.EventEmitter<void>();
  readonly onDidChange = this.changed.event;
  private updating: Promise<void> = Promise.resolve();

  constructor(
    private readonly context: vscode.ExtensionContext,
    readonly output: vscode.LogOutputChannel,
  ) {}

  get interpreter(): Interpreter | undefined {
    return this.resolution?.interpreter;
  }

  list(): ProjectServer[] {
    return [...this.servers.values()];
  }

  /** The server for a project root, however its path is spelled (symlinks resolved). */
  serverForRoot(root: string): ProjectServer | undefined {
    const exact = this.servers.get(root);
    if (exact) {
      return exact;
    }
    const real = realpath(root);
    return this.list().find((server) => realpath(server.project.root) === real);
  }

  serverFor(file: string): ProjectServer | undefined {
    return projectOf(file, this.list().map((s) => ({ root: s.project.root, server: s })), process.platform)?.server;
  }

  /** Resolve Python, then start a server for every project. */
  start(offerDownload: boolean): Promise<void> {
    return this.queue(async () => {
      await this.resolve(offerDownload);
      await this.sync();
    });
  }

  /** Start servers for new projects and stop those whose project is gone. */
  rediscover(): Promise<void> {
    return this.queue(() => this.sync());
  }

  /** Resolve Python again and restart every server, e.g. after a setting or trust change. */
  restartAll(manual = false): Promise<void> {
    return this.queue(async () => {
      await this.resolve(false);
      for (const server of this.list()) {
        await server.restart(manual);
      }
      this.changed.fire();
    });
  }

  private queue(work: () => Promise<void>): Promise<void> {
    this.updating = this.updating.then(work, work);
    return this.updating;
  }

  private async resolve(offerDownload: boolean): Promise<void> {
    const folder = vscode.workspace.workspaceFolders?.[0]?.uri;
    this.resolution = await resolvePython({
      output: this.output, extensionPath: this.context.extensionPath, folder, offerDownload,
    });
    this.resolution.attempts.forEach((attempt) => this.output.info(`interpreter ${attempt}`));
    const interpreter = this.resolution.interpreter;
    if (interpreter) {
      this.output.info(`Using Python ${interpreter.version.join('.')} at ${interpreter.executable} `
        + `(${interpreter.source})`);
    } else {
      const message = this.resolution.error
        ?? 'LineagePilot needs Python 3.10 or later. Install it, or set lineagePilot.python.interpreterPath.';
      this.output.error(message);
      void vscode.window.showErrorMessage(message, 'Open Settings').then((choice) => {
        if (choice) {
          void vscode.commands.executeCommand('workbench.action.openSettings', 'lineagePilot.python.interpreterPath');
        }
      });
    }
  }

  private async sync(): Promise<void> {
    const folders = (vscode.workspace.workspaceFolders ?? []).map((f) => f.uri.fsPath);
    const files = await vscode.workspace.findFiles(PROJECT_GLOB, EXCLUDE_GLOB);
    const projects = projectsFrom(files.map((f) => f.fsPath), folders, process.platform);
    const wanted = new Set(projects.map((p) => p.root));
    for (const [root, server] of this.servers) {
      if (!wanted.has(root)) {
        this.servers.delete(root);
        await server.stop();
        server.dispose();
      }
    }
    for (const project of projects) {
      if (this.servers.has(project.root)) {
        continue;
      }
      const server = new ProjectServer(project, {
        extensionPath: this.context.extensionPath,
        storagePath: this.context.storageUri?.fsPath,
        output: this.output,
        interpreter: () => this.interpreter,
      });
      server.onDidChange(() => this.changed.fire());
      this.servers.set(project.root, server);
      await server.start();
    }
    this.changed.fire();
  }

  dispose(): void {
    for (const server of this.servers.values()) {
      server.dispose();
    }
    this.changed.dispose();
  }
}

function realpath(file: string): string {
  try {
    return fs.realpathSync.native(file);
  } catch {
    return file;
  }
}
