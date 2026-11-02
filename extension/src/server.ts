// One language server per dbt project (design section 4, task 2.4): start, watch, restart
// with backoff after a crash, and keep the latest lineage/status.

import * as path from 'path';
import * as vscode from 'vscode';
import {
  CloseAction, CloseHandlerResult, ErrorAction, ErrorHandlerResult, LanguageClient, LanguageClientOptions,
  ServerOptions, State, TransportKind,
} from 'vscode-languageclient/node';

import { RestartPolicy } from './backoff';
import type { Interpreter } from './interpreter';
import { parseNotification, request, type Params, type Result } from './lineage';
import type { Project } from './projects';
import { PROTOCOL_VERSION, type RequestMethod, type Status } from './protocol';

export const SERVER_SCRIPT = path.join('bundled', 'tool', 'lsp_server.py');

export interface ServerEnvironment {
  extensionPath: string;
  storagePath?: string;
  output: vscode.LogOutputChannel;
  interpreter(): Interpreter | undefined;
}

export function serverSettings(folder: vscode.Uri) {
  const config = vscode.workspace.getConfiguration('lineagePilot', folder);
  return {
    columnLineage: config.get<boolean>('index.columnLineage', true),
    maxArtifactMB: config.get<number>('index.maxArtifactMB', 512),
    dagFolders: config.get<string[]>('airflow.dagFolders', ['dags']),
  };
}

export class ProjectServer implements vscode.Disposable {
  client: LanguageClient | undefined;
  status: Status | undefined;
  // Why the server is not running, when it is not
  problem: string | undefined;
  readonly policy = new RestartPolicy();
  private restartTimer: NodeJS.Timeout | undefined;
  private stopping = false;
  private readonly changed = new vscode.EventEmitter<void>();
  readonly onDidChange = this.changed.event;

  constructor(readonly project: Project, private readonly env: ServerEnvironment) {}

  get name(): string {
    return this.status?.project.name ?? path.basename(this.project.root);
  }

  get running(): boolean {
    return this.client?.state === State.Running;
  }

  async start(): Promise<void> {
    this.stopping = false;
    const interpreter = this.env.interpreter();
    if (!interpreter) {
      this.setProblem('no Python 3.10+ found');
      return;
    }
    const output = this.env.output;
    const script = path.join(this.env.extensionPath, SERVER_SCRIPT);
    const serverOptions: ServerOptions = {
      command: interpreter.executable,
      // -I: nothing from the workspace, the user site-packages or PYTHON* variables can shadow
      // the bundled libraries. The working directory is the extension's, never the workspace.
      args: ['-I', script],
      transport: TransportKind.stdio,
      options: { cwd: path.dirname(script) },
    };
    const folder = vscode.Uri.file(this.project.folder);
    const clientOptions: LanguageClientOptions = {
      documentSelector: [
        { scheme: 'file', pattern: { baseUri: vscode.Uri.file(this.project.root).toString(), pattern: '**/*.{sql,yml,yaml}' } },
      ],
      outputChannel: output,
      initializationOptions: {
        protocolVersions: [PROTOCOL_VERSION],
        projectRoot: this.project.root,
        trusted: vscode.workspace.isTrusted,
        storagePath: this.env.storagePath,
        debounceMs: 1000,
        ...serverSettings(folder),
      },
      workspaceFolder: { uri: folder, name: path.basename(this.project.folder), index: 0 },
      errorHandler: {
        error: (): ErrorHandlerResult => ({ action: ErrorAction.Continue }),
        closed: (): CloseHandlerResult => {
          if (!this.stopping) {
            this.crashed();
          }
          return { action: CloseAction.DoNotRestart, handled: true };
        },
      },
      initializationFailedHandler: () => false,
      connectionOptions: { maxRestartCount: 0 },
    };
    const client = new LanguageClient('lineagePilot', `LineagePilot (${path.basename(this.project.root)})`,
      serverOptions, clientOptions);
    this.client = client;
    client.onNotification('lineage/indexUpdated', (raw: unknown) => {
      const parsed = parseNotification('lineage/indexUpdated', raw);
      if (parsed instanceof Error) {
        output.error(parsed.message);
        return;
      }
      this.status = parsed;
      this.changed.fire();
    });
    try {
      await client.start();
    } catch (error) {
      this.client = undefined;
      this.setProblem(`the language server did not start: ${error instanceof Error ? error.message : error}`);
      return;
    }
    const negotiated = client.initializeResult?.capabilities.experimental?.lineagePilot?.protocolVersion;
    if (negotiated !== PROTOCOL_VERSION) {
      await this.stop();
      this.setProblem(`the language server speaks lineage protocol ${negotiated ?? 'none'}, `
        + `the extension ${PROTOCOL_VERSION}; reinstall LineagePilot`);
      return;
    }
    this.problem = undefined;
    output.info(`Language server for ${this.project.root} started with Python `
      + `${interpreter.version.join('.')} (${interpreter.source})`);
    try {
      this.status = await this.request('lineage/status', {});
    } catch (error) {
      output.warn(`lineage/status failed: ${error}`);
    }
    this.changed.fire();
  }

  async stop(): Promise<void> {
    this.stopping = true;
    clearTimeout(this.restartTimer);
    const client = this.client;
    this.client = undefined;
    if (client && client.state !== State.Stopped) {
      try {
        await client.stop(10_000);
      } catch (error) {
        this.env.output.warn(`Stopping the language server: ${error}`);
      }
    }
  }

  async restart(manual = false): Promise<void> {
    if (manual) {
      this.policy.reset();
    }
    await this.stop();
    this.status = undefined;
    await this.start();
  }

  request<M extends RequestMethod>(method: M, params: Params<M>): Promise<Result<M>> {
    const client = this.client;
    if (!client || !this.running) {
      return Promise.reject(new Error(`the language server for ${this.name} is not running`));
    }
    return request(client, method, params);
  }

  private crashed(): void {
    this.client = undefined;
    const delay = this.policy.next();
    if (delay === undefined) {
      this.setProblem('the language server keeps stopping; see the output');
      void vscode.window.showErrorMessage(
        `The LineagePilot language server for ${this.name} stopped ${this.policy.recentCrashes} times `
        + 'in 3 minutes, so it will not be restarted automatically.', 'Restart', 'Show Output',
      ).then((choice) => {
        if (choice === 'Restart') {
          void this.restart(true);
        } else if (choice === 'Show Output') {
          this.env.output.show();
        }
      });
      return;
    }
    this.setProblem(`the language server stopped; restarting in ${Math.round(delay / 1000)} s`);
    this.restartTimer = setTimeout(() => void this.start(), delay);
  }

  private setProblem(problem: string): void {
    this.problem = problem;
    this.env.output.error(`${this.project.root}: ${problem}`);
    this.changed.fire();
  }

  dispose(): void {
    void this.stop();
    this.changed.dispose();
  }
}
