// The one-line view of a session list, for a collapsed `list`/`list-cwd` row.
import type { SessionInfo } from "./types.ts";

/** Counts over the same "other sessions" a list result shows, so the summary matches what expanding reveals. */
export interface RosterSummary {
  readonly others: number;
  /** Sessions on this computer. */
  readonly local: number;
  /** Sessions on linked computers, and how many computers they're on. */
  readonly linked: number;
  readonly computers: number;
  /** Sessions doing something (anything but idle). */
  readonly working: number;
  /** The directory a `list-cwd` filtered by. */
  readonly cwd?: string;
}

export function summarizeRoster(others: readonly SessionInfo[], cwd?: string): RosterSummary {
  const linked = others.filter((session) => session.federation);
  return {
    others: others.length,
    local: others.length - linked.length,
    linked: linked.length,
    computers: new Set(linked.map((session) => session.federation!.originId)).size,
    working: others.filter((session) => session.status && session.status !== "idle").length,
    ...(cwd !== undefined ? { cwd } : {}),
  };
}

export function formatRosterSummary(summary: RosterSummary): string {
  const where = summary.cwd !== undefined ? ` in ${summary.cwd}` : "";
  if (summary.others === 0) return `No other sessions${where}`;
  const parts = [`${summary.others} other session${summary.others === 1 ? "" : "s"}${where}`];
  if (summary.linked > 0) {
    parts.push(`${summary.local} on this computer, ${summary.linked} on ${summary.computers} linked computer${summary.computers === 1 ? "" : "s"}`);
  }
  if (summary.working > 0) parts.push(`${summary.working} working`);
  return parts.join(" · ");
}

/** A tool result's roster summary, when it is a list. */
export function rosterSummaryOf(details: unknown): RosterSummary | undefined {
  const roster = (details as { roster?: unknown } | undefined)?.roster;
  return typeof roster === "object" && roster !== null && typeof (roster as RosterSummary).others === "number"
    ? roster as RosterSummary
    : undefined;
}
