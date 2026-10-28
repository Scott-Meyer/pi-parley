// Scopes that join the ordinary (unscoped) roster on this broker.
//
// A routing scope (PI_PARLEY_SCOPE_ID) walls a set of sessions off from the
// rest of the computer. Applications used to put their sessions in one by
// default (FlightDeck's "flightdeck"); now everyone on a computer shares one
// roster and the app a session lives in is metadata. A merged scope lets that
// change roll out without anyone losing contact: sessions that still name it
// (old shells keep their environment) join the ordinary roster, and so do
// peer-link bindings for it, so this broker exports everyone over a link that
// still binds the old scope and imports what arrives on it into one roster.
import { readFileSync } from "node:fs";
import { join } from "node:path";

/** Merged by default: FlightDeck's former scope. */
export const DEFAULT_MERGED_SCOPES = ["flightdeck"] as const;

/** From the broker's config.json ("mergedScopes": [...]), else the default. */
export function loadMergedScopes(parleyDir: string): ReadonlySet<string> {
  try {
    const parsed = JSON.parse(readFileSync(join(parleyDir, "config.json"), "utf8")) as { mergedScopes?: unknown };
    if (Array.isArray(parsed.mergedScopes) && parsed.mergedScopes.every((scope) => typeof scope === "string")) {
      return new Set(parsed.mergedScopes.map((scope) => scope.trim()).filter(Boolean));
    }
  } catch { /* no config: the default */ }
  return new Set(DEFAULT_MERGED_SCOPES);
}

/** The scope a session or binding actually lives in on this broker. */
export function effectiveScope<T extends string | null | undefined>(scopeId: T, merged: ReadonlySet<string>): T | undefined {
  return typeof scopeId === "string" && merged.has(scopeId) ? undefined : scopeId;
}
