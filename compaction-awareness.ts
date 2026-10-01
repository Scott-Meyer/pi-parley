import type { PeerCompactionNotice } from "./types.ts";

/** A small note on a send: the colleague compacted since you last talked. Nothing about how much
 * context anyone has; just that their memory of earlier details may be vaguer.
 * Session identities are written canonically; model-facing presentation turns them into references. */
export function formatPeerCompactionNotice(
  peerDisplay: string,
  notice: PeerCompactionNotice,
  expectedPeerSessionId?: string,
  /** Typed session naming; without it identities are written canonically for programmatic callers. */
  sessionRef?: (id: string, name?: string) => string,
): string {
  const compactionCount = notice.generation - notice.previousGeneration;
  const countText = compactionCount === 1 ? "" : ` (${compactionCount} times)`;
  const shortPeerId = notice.peerSessionId.slice(0, 8);
  const requested = peerDisplay.trim();
  const peerName = notice.peerName?.trim();
  // Broker-authored rebound metadata is authoritative. A caller may pass a
  // routing name when no live roster ID was available, so a value matching the
  // actual peer name must not be mistaken for a different stable identity.
  const expectedPeerId = notice.requestedPeerSessionId ?? expectedPeerSessionId;
  const expectedMatchesName = notice.requestedPeerSessionId === undefined
    && peerName !== undefined
    && expectedPeerId?.toLocaleLowerCase() === peerName.toLocaleLowerCase();
  const rebound = expectedPeerId !== undefined
    && expectedPeerId !== notice.peerSessionId
    && !expectedMatchesName;
  const requestedMatchesActual = requested === notice.peerSessionId
    || requested.includes(shortPeerId)
    || (peerName !== undefined && requested.toLocaleLowerCase() === peerName.toLocaleLowerCase());
  const actual = sessionRef?.(notice.peerSessionId, peerName);
  const identifiedPeer = sessionRef
    ? rebound
      ? `${actual} (message was requested for ${sessionRef(expectedPeerId!)})`
      : requestedMatchesActual || actual === requested
        ? requested
        : `${actual} (requested as ${requested})`
    : rebound
      ? `${peerName || "peer"} [session ${notice.peerSessionId}] (message was requested for ${requested} [session ${expectedPeerId!}])`
      : requestedMatchesActual
        ? requested
        : peerName
          ? `${peerName} (requested as ${requested})`
          : `${requested} [peer session ${notice.peerSessionId}]`;
  return `Note: ${identifiedPeer} has compacted since you last talked${countText}. It probably still knows what's going on, but if this depends on something specific from before, its memory of that may be vaguer.`;
}
