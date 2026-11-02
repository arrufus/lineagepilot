// When to restart a language server that stopped unexpectedly (task 2.4). Each crash waits
// twice as long as the one before, from 1 s up to 30 s; after 5 crashes within 3 minutes the
// extension stops trying and offers a manual restart. Kept free of the vscode module.

export interface BackoffOptions {
  maxRestarts: number;
  windowMs: number;
  baseDelayMs: number;
  maxDelayMs: number;
}

export const DEFAULT_BACKOFF: BackoffOptions = {
  maxRestarts: 5,
  windowMs: 3 * 60_000,
  baseDelayMs: 1_000,
  maxDelayMs: 30_000,
};

export class RestartPolicy {
  private crashes: number[] = [];

  constructor(
    private readonly options: BackoffOptions = DEFAULT_BACKOFF,
    private readonly now: () => number = Date.now,
  ) {}

  /** Record a crash. Returns how long to wait before restarting, or undefined to give up. */
  next(): number | undefined {
    const time = this.now();
    this.crashes = this.crashes.filter((t) => time - t < this.options.windowMs);
    this.crashes.push(time);
    if (this.crashes.length > this.options.maxRestarts) {
      return undefined;
    }
    const delay = this.options.baseDelayMs * 2 ** (this.crashes.length - 1);
    return Math.min(delay, this.options.maxDelayMs);
  }

  /** Forget past crashes, after the user restarts the server by hand. */
  reset(): void {
    this.crashes = [];
  }

  get recentCrashes(): number {
    return this.crashes.length;
  }
}
