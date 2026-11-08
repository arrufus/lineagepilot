// Usage counts for the closed beta (task 6.3, design decision D-7). Off unless the user turns on
// lineagePilot.usageCounts.enabled, an application-scoped setting that a workspace cannot set.
// The counts stay in this machine's extension storage and are never sent anywhere: the user copies
// them with LineagePilot: Copy Usage Counts and pastes them into the beta's weekly check-in. They
// hold three numbers a week and nothing about any project: no names, paths or contents.

export const USAGE_SETTING = 'lineagePilot.usageCounts.enabled';
export const USAGE_KEY = 'lineagePilot.usageCounts';
/** Weeks kept: the four of the beta (Gate G1), with room for a late start. */
export const WEEKS_KEPT = 12;

export interface WeekCounts {
  /** Local days, YYYY-MM-DD, on which a dbt project was open with LineagePilot running. */
  days: string[];
  /** Times the Change Impact Review was opened or brought to the front. */
  opened: number;
  /** Times the review was copied as Markdown. */
  copied: number;
}

export interface UsageState {
  version: 1;
  /** By ISO week, YYYY-Www. */
  weeks: Record<string, WeekCounts>;
}

/** The part of vscode.Memento this needs, so tests can pass a Map. */
export interface UsageStore {
  get<T>(key: string): T | undefined;
  update(key: string, value: unknown): Thenable<void>;
}

function pad(value: number, width = 2): string {
  return String(value).padStart(width, '0');
}

/** The local calendar day, YYYY-MM-DD. */
export function localDay(date: Date): string {
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

/** The ISO 8601 week of the local calendar day, YYYY-Www: weeks start on Monday, and week 1 holds
 * the year's first Thursday. */
export function isoWeek(date: Date): string {
  // Work in UTC on the local calendar date, so daylight saving cannot move a day
  const day = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const weekday = day.getUTCDay() || 7;
  day.setUTCDate(day.getUTCDate() + 4 - weekday);  // the Thursday of this week decides the year
  const yearStart = Date.UTC(day.getUTCFullYear(), 0, 1);
  const week = Math.ceil(((day.getTime() - yearStart) / 86_400_000 + 1) / 7);
  return `${day.getUTCFullYear()}-W${pad(week)}`;
}

/** The Monday that starts an ISO week, YYYY-MM-DD. */
export function weekStart(week: string): string {
  const match = /^(\d{4})-W(\d{2})$/.exec(week);
  if (!match) {
    return week;
  }
  const [year, number] = [Number(match[1]), Number(match[2])];
  // 4 January is always in week 1
  const january4 = new Date(Date.UTC(year, 0, 4));
  const monday = new Date(january4);
  monday.setUTCDate(january4.getUTCDate() - ((january4.getUTCDay() || 7) - 1) + (number - 1) * 7);
  return `${monday.getUTCFullYear()}-${pad(monday.getUTCMonth() + 1)}-${pad(monday.getUTCDate())}`;
}

const WEEK = /^\d{4}-W\d{2}$/;
const DAY = /^\d{4}-\d{2}-\d{2}$/;

function count(value: unknown): number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : 0;
}

/** What was stored, checked: anything else, from another version or edited by hand, starts again. */
export function validState(value: unknown): UsageState {
  const empty: UsageState = { version: 1, weeks: {} };
  if (typeof value !== 'object' || value === null || (value as UsageState).version !== 1) {
    return empty;
  }
  const weeks = (value as UsageState).weeks;
  if (typeof weeks !== 'object' || weeks === null) {
    return empty;
  }
  const state: UsageState = { version: 1, weeks: {} };
  for (const [week, counts] of Object.entries(weeks)) {
    if (!WEEK.test(week) || typeof counts !== 'object' || counts === null) {
      continue;
    }
    const days = Array.isArray(counts.days) ? counts.days.filter((d): d is string => typeof d === 'string' && DAY.test(d)) : [];
    state.weeks[week] = { days: [...new Set(days)].sort().slice(0, 7), opened: count(counts.opened), copied: count(counts.copied) };
  }
  return state;
}

export type UsageEvent = 'day' | 'opened' | 'copied';

export class UsageCounts {
  constructor(
    private readonly store: UsageStore,
    private readonly enabled: () => boolean,
    private readonly now: () => Date = () => new Date(),
  ) {}

  get isEnabled(): boolean {
    return this.enabled();
  }

  state(): UsageState {
    return validState(this.store.get(USAGE_KEY));
  }

  /** Count one event, if counting is on. Nothing else is recorded. */
  async record(event: UsageEvent): Promise<void> {
    if (!this.enabled()) {
      return;
    }
    const now = this.now();
    const state = this.state();
    const week = isoWeek(now);
    const counts = state.weeks[week] ??= { days: [], opened: 0, copied: 0 };
    const day = localDay(now);
    if (event === 'day') {
      if (counts.days.includes(day)) {
        return;  // already counted: no write
      }
      counts.days = [...counts.days, day].sort();
    } else {
      // Using the review is using LineagePilot that day
      if (!counts.days.includes(day)) {
        counts.days = [...counts.days, day].sort();
      }
      counts[event] += 1;
    }
    const kept = Object.keys(state.weeks).sort().slice(-WEEKS_KEPT);
    await this.store.update(USAGE_KEY, { version: 1, weeks: Object.fromEntries(kept.map((w) => [w, state.weeks[w]])) });
  }

  /** Forget every count: when counting is turned off. */
  async clear(): Promise<void> {
    if (this.store.get(USAGE_KEY) !== undefined) {
      await this.store.update(USAGE_KEY, undefined);
    }
  }

  /** The counts as Markdown, for the weekly check-in: exactly what the user pastes. */
  summary(version: string): string {
    const weeks = Object.entries(this.state().weeks).sort(([a], [b]) => a.localeCompare(b));
    const lines = [
      `LineagePilot ${version} usage counts, counted on this machine and copied on ${localDay(this.now())}. `
        + 'They hold nothing about any project.',
      '',
    ];
    if (!weeks.length) {
      return [...lines, 'Nothing counted yet.', ''].join('\n');
    }
    lines.push(
      '| Week | Days with a dbt project open | Reviews opened | Reviews copied as Markdown |',
      '|---|---:|---:|---:|',
      ...weeks.map(([week, c]) => `| ${week} (from ${weekStart(week)}) | ${c.days.length} | ${c.opened} | ${c.copied} |`),
      '',
    );
    return lines.join('\n');
  }
}
