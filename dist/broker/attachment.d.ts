import { Duplex } from "node:stream";
import type { BrokerConnectTarget } from "./paths.js";
import type { FederationFailureCode, FederationOrigin, FederationScopeBinding } from "./federation-types.js";
/** Maximum copied bytes in each admitted opaque write; at most one per direction. */
export declare const PEER_STREAM_COPY_BYTES: number;
export interface AttachPeerStreamOptions {
    /** Explicit local endpoint; TCP targets must use numeric loopback. */
    localBroker: BrokerConnectTarget;
    /** Already authenticated to the exact destination broker. Ownership transfers
     * immediately, including on validation/abort/startup failure. Must be a binary
     * Node Duplex with normal write-callback and destruction semantics. No retry,
     * reopening, discovery or replay is performed. Duplex.from({readable,writable})
     * can adapt a provider's stdio without exposing that provider to this module. */
    stream: Duplex;
    localOrigin: FederationOrigin;
    remoteOrigin: FederationOrigin;
    localScopeBindings: FederationScopeBinding[];
    /** Independently authorized destination bindings, not inferred local authority. */
    remoteScopeBindings: FederationScopeBinding[];
    /** Destination broker TCP authentication, if required. Never inferred. */
    remoteStateId?: string;
    /** Cancels startup or closes the active attachment. */
    signal?: AbortSignal;
    /** Entire broker-handshake deadline, not a forwarding-setup deadline. Default 10s. */
    handshakeTimeoutMs?: number;
}
/** An already authenticated connection to one exact broker. No endpoint
 * discovery, acquisition, dialing or provider knowledge is needed here. */
export interface PeerStreamEndpoint {
    /** Binary Node Duplex with normal write-callback/destruction semantics.
     * Ownership transfers immediately when attachPeerStreams is called. */
    stream: Duplex;
    origin: FederationOrigin;
    /** Independently authorized local scope authority for this broker. */
    scopeBindings: FederationScopeBinding[];
    /** Required when this broker uses opt-in loopback TCP authentication. */
    stateId?: string;
}
export interface AttachPeerStreamsOptions {
    local: PeerStreamEndpoint;
    remote: PeerStreamEndpoint;
    /** Cancels startup or closes the active attachment. */
    signal?: AbortSignal;
    /** Entire handshake deadline; default 10s. */
    handshakeTimeoutMs?: number;
}
export type PeerStreamAttachmentErrorCode = FederationFailureCode | "E_INVALID_OPTIONS" | "E_ABORTED" | "E_TIMEOUT" | "E_STREAM_FAILED" | "E_CLOSED";
/** Diagnostics deliberately contain no capabilities, endpoint credentials or raw frames. */
export declare class PeerStreamAttachmentError extends Error {
    readonly code: PeerStreamAttachmentErrorCode;
    constructor(code: PeerStreamAttachmentErrorCode);
}
export type PeerStreamCompletion = {
    status: "end";
} | {
    status: "closed";
    reason: "close" | "aborted" | "stream";
} | {
    status: "failure";
    error: PeerStreamAttachmentError;
};
export interface PeerStreamAttachment {
    /** Ready means the brokers accepted their peer handshake, not merely setup. */
    readonly linkId: string;
    readonly localOrigin: Readonly<FederationOrigin>;
    readonly remoteOrigin: Readonly<FederationOrigin>;
    /** Never rejects. Resolves after admitted write callbacks and owned cleanup join. */
    readonly completion: Promise<PeerStreamCompletion>;
    /** Stops admissions, destroys owned resources and joins completion. Idempotent. */
    close(): Promise<PeerStreamCompletion>;
}
/** Attach the local broker to one exact authenticated destination broker stream.
 * Resolves only after both brokers accept the peer handshake. Startup rejection
 * also joins destruction of the supplied stream. Caller supplies all authority;
 * peer traffic after preparation is copied opaquely, byte-for-byte, in order. */
export declare function attachPeerStream(options: AttachPeerStreamOptions): Promise<PeerStreamAttachment>;
/** Join two exact broker streams as a direct, single-hop peer link. Both
 * streams transfer immediately, even on invalid arguments, cancellation or
 * startup rejection. No stream is reopened, retried or replayed. The remote
 * broker is prepared first; the local broker emits its actual outbound hello
 * and validates the actual remote ack. Readiness requires its start success.
 * Thereafter bytes are copied opaquely, in order, with bounded backpressure.
 * Rejection joins both streams' destruction and admitted write callbacks. */
export declare function attachPeerStreams(options: AttachPeerStreamsOptions): Promise<PeerStreamAttachment>;
export type PeerStreamAttachOptions = Omit<AttachPeerStreamOptions, "stream">;
/** Acquisition is provider-owned: binding is opaque to Parley. Factories must
 * eventually settle after cancellation; close joins rather than abandons them. */
export type PeerStreamProviderFactory<TBinding> = (binding: TBinding, signal: AbortSignal) => Promise<Duplex>;
export interface PeerStreamProviderRegistration<TBinding> {
    /** Registration itself never acquires. Each call makes one acquisition and
     * one attachment attempt, without reconnect/retry/replay. */
    attach(binding: TBinding, options: PeerStreamAttachOptions): Promise<PeerStreamAttachment>;
    /** Revokes admissions, aborts and joins acquisitions/attachments. Late streams
     * are destroyed and joined, never handed to a broker. Idempotent. */
    close(): Promise<void>;
}
/** Caller-created, provider-neutral lifetime for explicit attachment attempts.
 * No globals, discovery, default endpoints, host interpretation or auto-enable.
 * Registration alone never invokes a factory. Close permanently revokes all
 * admissions and joins started acquisitions, late-stream disposal and links. */
export declare class PeerStreamController {
    private closed;
    private readonly direct;
    private readonly providers;
    private closing?;
    registerProvider<TBinding>(factory: PeerStreamProviderFactory<TBinding>): PeerStreamProviderRegistration<TBinding>;
    attachOwnedStream(options: AttachPeerStreamOptions): Promise<PeerStreamAttachment>;
    close(): Promise<void>;
}
