import net from "node:net";
import { randomUUID } from "node:crypto";
import { createMessageReader, writeMessage } from "./framing.ts";
import {
  FEDERATION_PROTOCOL_NAME,
  FEDERATION_PROTOCOL_VERSION,
  FEDERATION_PING_FEATURE,
  FEDERATION_REQUIRED_FEATURES,
  FEDERATION_SCOPED_LINKS_FEATURE,
  FEDERATION_SUPPORTED_FEATURES,
  type BrokerAcceptPeerRequest,
  type BrokerDialPeerRequest,
  type FederationFailureCode,
  type FederationOrigin,
  type FederationScopeBinding,
  type FederationScopeMapping,
  type PeerHello,
  type PeerHelloAck,
} from "./federation-types.ts";
import {
  bindingsMatchPeerMappings,
  isBrokerAcceptPeerRequest,
  isBrokerDialPeerRequest,
  isBrokerStartPeerRequest,
  isPeerHello,
  isPeerHelloAck,
  scopeMappingsFromBindings,
} from "./federation-protocol.ts";

const PEER_HANDSHAKE_TIMEOUT_MS = 5_000;
const MAX_PEER_LINKS = 16;
/** Two links that would carry the same local scope to the same remote broker
 * conflict. Within this long of each other they are a simultaneous attach,
 * settled by preferred direction. After it, the existing link is defended:
 * the newcomer is refused, and the refusal asks the existing link whether
 * anyone is still behind it. A link that stays silent is closed, so a link
 * left lingering by a restarted peer or controller clears for the retry. */
const SIMULTANEOUS_ATTACH_MS = 10_000;
const LIVENESS_PROBE_MS = 3_000;
function isSettled(link: FederationPeerLink): boolean {
  return Date.now() - link.connectedAt >= SIMULTANEOUS_ATTACH_MS;
}
function sharesLocalScope(left: readonly FederationScopeBinding[], right: readonly FederationScopeBinding[]): boolean {
  return left.some((binding) => right.some((other) => other.localScopeId === binding.localScopeId));
}
const PEER_FRAME_RATE_CAPACITY = 240;
const PEER_FRAME_REFILL_PER_SECOND = 120;

export interface FederationPeerLink {
  linkId: string;
  direction: "inbound" | "outbound";
  socket: net.Socket;
  localOrigin: FederationOrigin;
  remoteOrigin: FederationOrigin;
  /** Local authority binding; raw scope IDs are never sent to the peer. */
  scopeBindings: FederationScopeBinding[];
  features: string[];
  connectedAt: number;
}

export interface PreparedInboundPeer {
  linkId: string;
  localOrigin: FederationOrigin;
  remoteOrigin: FederationOrigin;
  scopeBindings: FederationScopeBinding[];
}

/** Broker-owned outbound authority, shared by dial and supplied streams. */
export interface PreparedOutboundPeer extends PreparedInboundPeer {}

export class FederationPeerError extends Error {
  constructor(readonly code: FederationFailureCode, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "FederationPeerError";
  }
}

export interface PeerLinkManagerOptions {
  onSocketOpened?: (socket: net.Socket) => void;
  onSocketClosed?: (socket: net.Socket) => void;
  onLinkUp?: (link: FederationPeerLink) => void;
  onLinkDown?: (link: FederationPeerLink) => void;
  onPeerMessage?: (link: FederationPeerLink, value: unknown) => void;
  /** Make the peer send something back, for links that predate peer_ping. */
  requestPeerResponse?: (link: FederationPeerLink) => void;
  /** Optional features this broker offers beyond FEDERATION_SUPPORTED_FEATURES. */
  extraFeatures?: readonly string[];
}

function isValidNegotiatedFeatures(negotiated: readonly string[], offered: readonly string[]): boolean {
  return FEDERATION_REQUIRED_FEATURES.every((feature) => negotiated.includes(feature))
    && negotiated.every((feature) => offered.includes(feature));
}

function sameMappings(left: FederationScopeMapping[], right: FederationScopeMapping[]): boolean {
  return left.length === right.length && left.every((mapping, index) => {
    const other = right[index];
    return other?.localScopeAlias === mapping.localScopeAlias
      && other.remoteScopeAlias === mapping.remoteScopeAlias;
  });
}

function rejection(hello: PeerHello, code: FederationFailureCode, error: string): PeerHelloAck {
  return {
    type: "peer_hello_ack",
    protocol: FEDERATION_PROTOCOL_NAME,
    version: FEDERATION_PROTOCOL_VERSION,
    linkId: hello.linkId,
    accepted: false,
    code,
    error,
  };
}

/** Owns peer authority preparation, handshake, deterministic link choice, and lifecycle. */
export class PeerLinkManager {
  private readonly linksById = new Map<string, FederationPeerLink>();
  private readonly linkIdsByRemoteOrigin = new Map<string, Set<string>>();
  private readonly lastHeardAt = new Map<string, number>();
  private readonly probing = new Map<string, NodeJS.Timeout>();
  private readonly activatedLinkIds = new Set<string>();
  private readonly outboundFrameLimits = new Map<string, { tokens: number; lastRefillAt: number }>();
  private localOrigin: FederationOrigin | undefined;

  private readonly offeredFeatures: readonly string[];

  constructor(private readonly options: PeerLinkManagerOptions = {}) {
    this.offeredFeatures = [...FEDERATION_SUPPORTED_FEATURES, ...(options.extraFeatures ?? [])];
  }

  get size(): number {
    return this.linksById.size;
  }

  listLinks(): FederationPeerLink[] {
    return [...this.linksById.values()];
  }

  prepareInbound(value: unknown): PreparedInboundPeer {
    if (!isBrokerAcceptPeerRequest(value)) {
      throw new FederationPeerError("E_INVALID_REQUEST", "Invalid broker accept peer request");
    }
    if (this.localOrigin && this.localOrigin.id !== value.localOrigin.id) {
      throw new FederationPeerError("E_ORIGIN_MISMATCH", "Prepared destination origin does not match this broker");
    }
    if (this.linksById.has(value.linkId)) {
      throw new FederationPeerError("E_ALREADY_CONNECTED", "Peer link ID is already connected");
    }
    return {
      linkId: value.linkId,
      localOrigin: value.localOrigin,
      remoteOrigin: value.remoteOrigin,
      scopeBindings: value.scopeBindings,
    };
  }

  acceptInbound(
    socket: net.Socket,
    value: unknown,
    prepared: PreparedInboundPeer,
  ): { ack: PeerHelloAck; link?: FederationPeerLink } {
    if (!isPeerHello(value)) throw new FederationPeerError("E_INVALID_REQUEST", "Invalid peer hello");
    const hello = value;
    if (hello.linkId !== prepared.linkId) return { ack: rejection(hello, "E_NOT_PREPARED", "Peer link was not prepared") };
    if (hello.origin.id !== prepared.remoteOrigin.id || hello.expectedPeerOrigin.id !== prepared.localOrigin.id) {
      return { ack: rejection(hello, "E_ORIGIN_MISMATCH", "Peer origins do not match destination authority") };
    }
    if (!bindingsMatchPeerMappings(prepared.scopeBindings, hello.scopeMappings)) {
      return { ack: rejection(hello, "E_SCOPE_MISMATCH", "Peer scope aliases do not match destination authority") };
    }
    if (this.linksById.has(hello.linkId)) {
      return { ack: rejection(hello, "E_ALREADY_CONNECTED", "Peer link ID is already connected") };
    }
    const refusal = this.admission("inbound", prepared, hello.features);
    if (refusal) return { ack: rejection(hello, "E_ALREADY_CONNECTED", refusal) };

    const negotiatedFeatures = this.offeredFeatures.filter((feature) => hello.features.includes(feature));
    const link: FederationPeerLink = {
      linkId: hello.linkId,
      direction: "inbound",
      socket,
      localOrigin: prepared.localOrigin,
      remoteOrigin: prepared.remoteOrigin,
      scopeBindings: prepared.scopeBindings,
      features: negotiatedFeatures,
      connectedAt: Date.now(),
    };
    try {
      this.registerLink(link);
    } catch (error) {
      const failure = error instanceof FederationPeerError
        ? error
        : new FederationPeerError("E_HANDSHAKE_FAILED", "Failed to register peer link", { cause: error });
      return { ack: rejection(hello, failure.code, failure.message) };
    }
    return {
      link,
      ack: {
        type: "peer_hello_ack",
        protocol: FEDERATION_PROTOCOL_NAME,
        version: FEDERATION_PROTOCOL_VERSION,
        linkId: hello.linkId,
        accepted: true,
        origin: prepared.localOrigin,
        acceptedPeerOriginId: prepared.remoteOrigin.id,
        scopeMappings: hello.scopeMappings,
        features: negotiatedFeatures,
      },
    };
  }

  prepareOutbound(value: unknown): PreparedOutboundPeer {
    if (!isBrokerStartPeerRequest(value)) {
      throw new FederationPeerError("E_INVALID_REQUEST", "Invalid broker start peer request");
    }
    if (this.localOrigin && this.localOrigin.id !== value.localOrigin.id) {
      throw new FederationPeerError("E_ORIGIN_MISMATCH", "Local federation origin is already fixed for this broker");
    }
    if (this.linksById.has(value.linkId)) {
      throw new FederationPeerError("E_ALREADY_CONNECTED", "Peer link ID is already connected");
    }
    const refusal = this.admission("outbound", value);
    if (refusal) throw new FederationPeerError("E_ALREADY_CONNECTED", refusal);
    return {
      linkId: value.linkId, localOrigin: this.localOrigin ?? value.localOrigin,
      remoteOrigin: value.remoteOrigin, scopeBindings: value.scopeBindings,
    };
  }

  outboundHello(prepared: PreparedOutboundPeer): PeerHello {
    return {
      type: "peer_hello", protocol: FEDERATION_PROTOCOL_NAME, version: FEDERATION_PROTOCOL_VERSION,
      linkId: prepared.linkId, origin: prepared.localOrigin, expectedPeerOrigin: prepared.remoteOrigin,
      scopeMappings: scopeMappingsFromBindings(prepared.scopeBindings), features: [...this.offeredFeatures],
    };
  }

  /** Validates the real peer ack and registers, but does not activate. The caller
   * must first queue any trusted-local readiness result on a supplied stream. */
  acceptOutbound(socket: net.Socket, value: unknown, prepared: PreparedOutboundPeer): FederationPeerLink {
    if (!isPeerHelloAck(value)) {
      throw new FederationPeerError("E_HANDSHAKE_FAILED", "Invalid peer hello acknowledgement");
    }
    if (value.linkId !== prepared.linkId) {
      throw new FederationPeerError("E_ORIGIN_MISMATCH", "Peer acknowledgement did not match the requested link");
    }
    if (!value.accepted) throw new FederationPeerError(value.code, value.error);
    if (value.origin.id !== prepared.remoteOrigin.id || value.acceptedPeerOriginId !== prepared.localOrigin.id) {
      throw new FederationPeerError("E_ORIGIN_MISMATCH", "Peer acknowledgement did not match the requested origins");
    }
    if (!sameMappings(value.scopeMappings, scopeMappingsFromBindings(prepared.scopeBindings))) {
      throw new FederationPeerError("E_SCOPE_MISMATCH", "Peer acknowledgement did not match the requested scopes");
    }
    if (!isValidNegotiatedFeatures(value.features, this.offeredFeatures)) {
      throw new FederationPeerError("E_FEATURE_UNSUPPORTED", "Peer acknowledged invalid or unoffered features");
    }
    const link: FederationPeerLink = {
      ...prepared, direction: "outbound", socket, features: value.features, connectedAt: Date.now(),
    };
    this.registerLink(link);
    return link;
  }

  async dial(request: BrokerDialPeerRequest, signal?: AbortSignal): Promise<FederationPeerLink> {
    if (!isBrokerDialPeerRequest(request)) {
      throw new FederationPeerError("E_INVALID_REQUEST", "Invalid broker dial peer request");
    }
    if (signal?.aborted) throw new FederationPeerError("E_DIAL_FAILED", "Peer dial control was abandoned");
    const endpoint = request.endpoint;
    const requestedLocalOrigin = request.localOrigin;
    const remoteOrigin = request.remoteOrigin;
    const scopeBindings = request.scopeBindings;
    let capability = request.capability;
    // Ensure the long-lived socket reader's function environment cannot retain
    // the authority-bearing request object after the attachment write.
    request = { ...request, capability: "" };

    const prepared = this.prepareOutbound({
      type: "broker_start_peer", requestId: request.requestId, linkId: randomUUID(),
      localOrigin: requestedLocalOrigin, remoteOrigin, scopeBindings,
    });
    const { linkId } = prepared;
    const socket = net.connect({ host: endpoint.host, port: endpoint.port });
    this.options.onSocketOpened?.(socket);

    return await new Promise<FederationPeerLink>((resolve, reject) => {
      let settled = false;
      let active = false;
      const finishFailure = (error: FederationPeerError): void => {
        capability = "";
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        signal?.removeEventListener("abort", onAbort);
        socket.off("data", reader);
        socket.destroy();
        reject(error);
      };
      const timeout = setTimeout(() => {
        finishFailure(new FederationPeerError("E_HANDSHAKE_FAILED", "Peer handshake timed out"));
      }, PEER_HANDSHAKE_TIMEOUT_MS);
      timeout.unref?.();

      const reader = createMessageReader((value) => {
        if (active) {
          const link = this.linksById.get(linkId);
          if (!link) {
            socket.destroy(new Error("Federation peer link is no longer active"));
            return;
          }
          if (!this.options.onPeerMessage) {
            socket.destroy(new Error("Federation peer frames are not supported by this broker"));
            return;
          }
          if (link.direction === "outbound" && !this.consumeOutboundFrameToken(link.linkId)) {
            socket.destroy(new Error("Federation peer frame rate limit exceeded"));
            return;
          }
          try {
            if (this.heard(link, value)) return;
            this.options.onPeerMessage(link, value);
          } catch (error) {
            socket.destroy(error instanceof Error ? error : new Error(String(error)));
          }
          return;
        }
        let link: FederationPeerLink;
        try {
          link = this.acceptOutbound(socket, value, prepared);
        } catch (error) {
          finishFailure(error instanceof FederationPeerError
            ? error
            : new FederationPeerError("E_HANDSHAKE_FAILED", "Failed to register peer link", { cause: error }));
          return;
        }
        active = true;
        try {
          this.activateLink(link.linkId);
        } catch (error) {
          active = false;
          finishFailure(new FederationPeerError("E_HANDSHAKE_FAILED", "Failed to activate peer link", { cause: error }));
          return;
        }
        settled = true;
        clearTimeout(timeout);
        signal?.removeEventListener("abort", onAbort);
        resolve(link);
      }, (error) => {
        if (active) socket.destroy(error);
        else finishFailure(new FederationPeerError("E_HANDSHAKE_FAILED", "Failed to read peer handshake", { cause: error }));
      });

      const onAbort = () => finishFailure(new FederationPeerError("E_DIAL_FAILED", "Peer dial control was abandoned"));
      signal?.addEventListener("abort", onAbort, { once: true });
      socket.on("data", reader);
      socket.once("connect", () => {
        try {
          writeMessage(socket, {
            type: "bridge_attach",
            protocol: FEDERATION_PROTOCOL_NAME,
            version: FEDERATION_PROTOCOL_VERSION,
            linkId,
            capability,
          });
          capability = "";
          writeMessage(socket, this.outboundHello(prepared));
        } catch (error) {
          capability = "";
          finishFailure(new FederationPeerError("E_DIAL_FAILED", "Failed to write peer handshake", { cause: error }));
        }
      });
      socket.once("error", (error) => {
        if (!active) finishFailure(new FederationPeerError("E_DIAL_FAILED", "Failed to connect to peer attachment endpoint", { cause: error }));
      });
      socket.once("close", () => {
        clearTimeout(timeout);
        capability = "";
        signal?.removeEventListener("abort", onAbort);
        this.options.onSocketClosed?.(socket);
        const link = this.removeLinkBySocket(socket);
        if (link) this.options.onLinkDown?.(link);
        if (!settled) {
          settled = true;
          reject(new FederationPeerError("E_DIAL_FAILED", "Peer connection closed before handshake completed"));
        }
      });
    });
  }

  getLink(linkId: string): FederationPeerLink | undefined {
    return this.linksById.get(linkId);
  }

  activateLink(linkId: string): void {
    const link = this.linksById.get(linkId);
    if (!link || this.activatedLinkIds.has(linkId)) return;
    this.activatedLinkIds.add(linkId);
    try {
      this.options.onLinkUp?.(link);
    } catch (error) {
      this.activatedLinkIds.delete(linkId);
      throw error;
    }
  }

  handlePostHandshakeMessage(linkId: string, value: unknown): void {
    const link = this.linksById.get(linkId);
    if (!link) throw new FederationPeerError("E_INVALID_REQUEST", "Unknown peer link");
    if (this.heard(link, value)) return;
    if (!this.options.onPeerMessage) {
      throw new FederationPeerError("E_FEATURE_UNSUPPORTED", "Federation peer frames are not supported by this broker");
    }
    this.options.onPeerMessage(link, value);
  }

  closeLink(linkId: string): boolean {
    const link = this.linksById.get(linkId);
    if (!link) return false;
    this.unregisterLink(link);
    link.socket.destroy();
    this.options.onLinkDown?.(link);
    return true;
  }

  removeInbound(linkId: string, socket: net.Socket): FederationPeerLink | undefined {
    const link = this.linksById.get(linkId);
    if (!link || link.socket !== socket) return undefined;
    this.unregisterLink(link);
    this.options.onLinkDown?.(link);
    return link;
  }

  close(): void {
    const links = [...this.linksById.values()];
    for (const link of links) {
      this.unregisterLink(link);
      link.socket.end();
      link.socket.destroy();
      this.options.onLinkDown?.(link);
    }
  }

  private linksToOrigin(remoteOriginId: string): FederationPeerLink[] {
    return [...this.linkIdsByRemoteOrigin.get(remoteOriginId) ?? []]
      .map((linkId) => this.linksById.get(linkId))
      .filter((link): link is FederationPeerLink => link !== undefined);
  }

  /** Why a new link may not join, or undefined when it may. Conflicting links
   * it would win (a simultaneous attach in its preferred direction) are left
   * for registerLink to replace. peerFeatures is what the other broker offered,
   * when known: one that holds a single link per broker can't take a second. */
  private admission(
    direction: FederationPeerLink["direction"],
    candidate: { localOrigin: FederationOrigin; remoteOrigin: FederationOrigin; scopeBindings: FederationScopeBinding[] },
    peerFeatures?: readonly string[],
  ): string | undefined {
    const existing = this.linksToOrigin(candidate.remoteOrigin.id);
    const conflicting = existing.filter((link) => sharesLocalScope(link.scopeBindings, candidate.scopeBindings));
    for (const link of conflicting) {
      if (isSettled(link)) {
        this.probe(link);
        return "This computer is already linked with that one for these sessions; the existing link is being checked";
      }
      if (link.direction === direction || this.isPreferredDirection(link.direction, link.localOrigin.id, link.remoteOrigin.id)
        || !this.isPreferredDirection(direction, candidate.localOrigin.id, candidate.remoteOrigin.id)) {
        return "The existing peer link has the deterministic preferred direction";
      }
    }
    const alongside = existing.filter((link) => !conflicting.includes(link));
    if (alongside.length > 0 && peerFeatures && !peerFeatures.includes(FEDERATION_SCOPED_LINKS_FEATURE)) {
      return "That computer's Parley holds one link per computer and is already linked with this one; update Parley there";
    }
    if (conflicting.length === 0 && this.linksById.size >= MAX_PEER_LINKS) return "Peer link limit reached";
    return undefined;
  }

  /** Ask a settled link whether anyone is behind it; close it if nobody answers. */
  private probe(link: FederationPeerLink): void {
    if (this.probing.has(link.linkId)) return;
    const askedAt = Date.now();
    if (link.features.includes(FEDERATION_PING_FEATURE)) {
      try { writeMessage(link.socket, { type: "peer_ping", protocol: FEDERATION_PROTOCOL_NAME, version: FEDERATION_PROTOCOL_VERSION, nonce: randomUUID() }); } catch { /* silence decides */ }
    } else {
      // Older peers answer a roster resynchronization request instead.
      try { this.options.requestPeerResponse?.(link); } catch { /* silence decides */ }
    }
    const timer = setTimeout(() => {
      this.probing.delete(link.linkId);
      if (!this.linksById.has(link.linkId)) return;
      if ((this.lastHeardAt.get(link.linkId) ?? 0) >= askedAt) return;
      this.closeLink(link.linkId);
    }, LIVENESS_PROBE_MS);
    timer.unref?.();
    this.probing.set(link.linkId, timer);
  }

  /** Every frame from a peer is evidence someone is behind the link. Pings are answered here. */
  private heard(link: FederationPeerLink, value: unknown): boolean {
    this.lastHeardAt.set(link.linkId, Date.now());
    if (!link.features.includes(FEDERATION_PING_FEATURE) || typeof value !== "object" || value === null) return false;
    const type = (value as { type?: unknown }).type;
    if (type === "peer_pong") return true;
    if (type !== "peer_ping") return false;
    const nonce = (value as { nonce?: unknown }).nonce;
    if (typeof nonce !== "string" || nonce.length > 64) return false;
    writeMessage(link.socket, { type: "peer_pong", protocol: FEDERATION_PROTOCOL_NAME, version: FEDERATION_PROTOCOL_VERSION, nonce });
    return true;
  }

  private isPreferredDirection(
    direction: FederationPeerLink["direction"],
    localOriginId: string,
    remoteOriginId: string,
  ): boolean {
    return direction === (localOriginId < remoteOriginId ? "outbound" : "inbound");
  }

  private registerLink(link: FederationPeerLink): void {
    if (this.linksById.has(link.linkId)) {
      throw new FederationPeerError("E_ALREADY_CONNECTED", "Peer link ID is already connected");
    }
    if (this.localOrigin && this.localOrigin.id !== link.localOrigin.id) {
      throw new FederationPeerError("E_ORIGIN_MISMATCH", "Local federation origin is already fixed for this broker");
    }
    const refusal = this.admission(link.direction, link, link.features);
    if (refusal) throw new FederationPeerError("E_ALREADY_CONNECTED", refusal);
    // What remains in conflict is a simultaneous attach this link wins.
    for (const existing of this.linksToOrigin(link.remoteOrigin.id)) {
      if (!sharesLocalScope(existing.scopeBindings, link.scopeBindings)) continue;
      this.unregisterLink(existing);
      existing.socket.destroy();
      this.options.onLinkDown?.(existing);
    }
    this.localOrigin ??= link.localOrigin;
    this.linksById.set(link.linkId, link);
    const ids = this.linkIdsByRemoteOrigin.get(link.remoteOrigin.id) ?? new Set<string>();
    ids.add(link.linkId);
    this.linkIdsByRemoteOrigin.set(link.remoteOrigin.id, ids);
    this.lastHeardAt.set(link.linkId, Date.now());
    if (link.direction === "outbound") {
      this.outboundFrameLimits.set(link.linkId, { tokens: PEER_FRAME_RATE_CAPACITY, lastRefillAt: Date.now() });
    }
  }

  private removeLinkBySocket(socket: net.Socket): FederationPeerLink | undefined {
    for (const link of this.linksById.values()) {
      if (link.socket !== socket) continue;
      this.unregisterLink(link);
      return link;
    }
    return undefined;
  }

  private consumeOutboundFrameToken(linkId: string, now = Date.now()): boolean {
    const state = this.outboundFrameLimits.get(linkId);
    if (!state) return false;
    const elapsedMs = Math.max(0, now - state.lastRefillAt);
    state.tokens = Math.min(
      PEER_FRAME_RATE_CAPACITY,
      state.tokens + elapsedMs * PEER_FRAME_REFILL_PER_SECOND / 1000,
    );
    state.lastRefillAt = now;
    if (state.tokens < 1) return false;
    state.tokens -= 1;
    return true;
  }

  private unregisterLink(link: FederationPeerLink): void {
    this.activatedLinkIds.delete(link.linkId);
    this.outboundFrameLimits.delete(link.linkId);
    this.linksById.delete(link.linkId);
    this.lastHeardAt.delete(link.linkId);
    clearTimeout(this.probing.get(link.linkId));
    this.probing.delete(link.linkId);
    const ids = this.linkIdsByRemoteOrigin.get(link.remoteOrigin.id);
    ids?.delete(link.linkId);
    if (ids?.size === 0) this.linkIdsByRemoteOrigin.delete(link.remoteOrigin.id);
  }
}
