// What earlier versions kept that this one no longer uses. Versions 0.1.x kept opt-in usage counts
// for the closed beta's weekly check-in in the extension's global state, under this key: three
// numbers a week, and nothing about any project. They were removed at the first public release
// (0.2.0, design decision D-7), so whatever a user had counted is deleted on activation.

export const OLD_COUNTS_KEY = 'lineagePilot.usageCounts';

/** The part of vscode.Memento this needs, so tests can pass a Map. */
export interface StateStore {
  get<T>(key: string): T | undefined;
  update(key: string, value: unknown): Thenable<void>;
}

/** Delete what 0.1.x counted, if anything is there. True if something was deleted. */
export async function forgetOldCounts(store: StateStore): Promise<boolean> {
  if (store.get(OLD_COUNTS_KEY) === undefined) {
    return false;
  }
  await store.update(OLD_COUNTS_KEY, undefined);
  return true;
}
