// What a broadcast's recipients read about who else it went to.
import { AUDIENCE_GROUP_MAX_LENGTH, type MessageAudience } from "./types.ts";

export interface BroadcastScope {
  /** The reference pattern the sender gave, e.g. `flightdeck:*`. */
  readonly to?: string;
  /** The resolved directory the sender filtered by. */
  readonly cwd?: string;
  readonly app?: string;
}

/**
 * The audience as the sender named it. Receivers refuse a label over the cap, so it is
 * shortened here: the directory is usually the long part, so its tail is kept first, and
 * only then is the whole label cut.
 */
export function broadcastAudience(scope: BroadcastScope): MessageAudience {
  const to = scope.to?.trim();
  const app = scope.app?.trim();
  const cwd = scope.cwd?.trim();
  const label = (directory: string | undefined) =>
    [to, directory && `sessions in ${directory}`, app && `sessions in ${app}`].filter(Boolean).join(", ");
  let group = label(cwd);
  if (group.length > AUDIENCE_GROUP_MAX_LENGTH && cwd) {
    const room = AUDIENCE_GROUP_MAX_LENGTH - label("…").length;
    if (room > 0) group = label(`…${cwd.slice(-room)}`);
  }
  if (group.length > AUDIENCE_GROUP_MAX_LENGTH) group = `${group.slice(0, AUDIENCE_GROUP_MAX_LENGTH - 1)}…`;
  return group ? { kind: "broadcast", group } : { kind: "broadcast" };
}
