import { Duplex } from "node:stream";
export interface ScopeBinding {
    /** Exact scope local to this broker, or null for its unscoped namespace. */
    readonly localScopeId: string | null;
    /** Public alias this broker uses for its local scope on this link. */
    readonly localScopeAlias: string;
    /** Public alias this broker expects for the peer scope on this link. */
    readonly remoteScopeAlias: string;
}
export interface BrokerOrigin {
    readonly id: string;
    /** Presentation only; never used for routing or trust. */
    readonly label?: string;
}
export interface BrokerInspectionScope {
    readonly scopeId: string | null;
    readonly liveSessions: number;
}
export interface BrokerInspection {
    /** Canonical identity persisted by this Parley broker installation. */
    readonly origin: Readonly<{
        readonly id: string;
    }>;
    /** Inspection-time snapshot. It may change immediately and is not durable
     * scope authorization. */
    readonly scopes: readonly Readonly<BrokerInspectionScope>[];
}
/** Opaque inspected broker handle retained for naming compatibility. */
export type BrokerHandle = BrokerInspection;
export type BrokerAttachmentErrorCode = "E_INVALID_REQUEST" | "E_DIAL_FAILED" | "E_HANDSHAKE_FAILED" | "E_VERSION_UNSUPPORTED" | "E_FEATURE_UNSUPPORTED" | "E_ORIGIN_MISMATCH" | "E_SCOPE_MISMATCH" | "E_ALREADY_CONNECTED" | "E_NOT_PREPARED" | "E_INVALID_OPTIONS" | "E_ABORTED" | "E_TIMEOUT" | "E_STREAM_FAILED" | "E_CLOSED";
/** Sanitized attachment/acquisition failure. */
export declare class BrokerAttachmentError extends Error {
    readonly code: BrokerAttachmentErrorCode;
    constructor(code: BrokerAttachmentErrorCode);
}
export type BrokerAttachmentCompletion = {
    readonly status: "end";
} | {
    readonly status: "closed";
    readonly reason: "close" | "aborted" | "stream";
} | {
    readonly status: "failure";
    readonly error: BrokerAttachmentError;
};
export interface BrokerAttachment {
    readonly linkId: string;
    readonly initiatorOrigin: Readonly<BrokerOrigin>;
    readonly acceptorOrigin: Readonly<BrokerOrigin>;
    /** Never rejects. Resolves after admitted writes and owned cleanup join. */
    readonly completion: Promise<BrokerAttachmentCompletion>;
    /** Stops admissions and joins cleanup. Idempotent and returns completion. */
    close(): Promise<BrokerAttachmentCompletion>;
}
/** A broker endpoint on one already-authorized host. TCP is always loopback;
 * filesystem paths are relative to that host's Pi agent directory. */
export type BrokerLocalEndpoint = {
    readonly transport: "tcp";
    readonly port: number;
} | {
    readonly transport: "unix";
    readonly path: readonly string[];
} | {
    readonly transport: "pipe";
    readonly name: string;
};
/** Host access is deliberately rooted at one Pi agent directory and one host's
 * local IPC namespace. Implementations should reject parent traversal,
 * non-loopback TCP, and endpoints outside that authority. */
export interface BrokerHostAccess {
    readonly platform: "darwin" | "linux" | "win32";
    /** Absolute Pi agent directory. It identifies the Windows pipe namespace but
     * is never passed back to readAgentFile as an unrestricted path. */
    readonly agentDir: string;
    /** Return undefined when the relative file is known absent. Providers whose
     * sanitized boundary cannot distinguish absence may reject instead. Reads are
     * bounded by maxBytes and should eventually settle after cancellation. */
    readAgentFile(relativePath: readonly string[], options: {
        readonly maxBytes: number;
        readonly signal: AbortSignal;
    }): Promise<Uint8Array | undefined>;
    /** Resolve only after a binary Node Duplex is connected. Ownership transfers
     * to Parley when the promise fulfills, including after cancellation. */
    openLocal(endpoint: BrokerLocalEndpoint, options: {
        readonly signal: AbortSignal;
    }): Promise<Duplex>;
}
export type BrokerInspectionErrorCode = "E_INVALID_OPTIONS" | "E_ABORTED" | "E_TIMEOUT" | "E_DISCOVERY_FAILED" | "E_BROKER_UNAVAILABLE" | "E_BROKER_PROTOCOL";
/** Sanitized inspection failure: paths, endpoint credentials, host diagnostics,
 * and raw broker frames are deliberately omitted. */
export declare class BrokerInspectionError extends Error {
    readonly code: BrokerInspectionErrorCode;
    constructor(code: BrokerInspectionErrorCode);
}
export interface InspectBrokerOptions {
    signal?: AbortSignal;
    /** Entire publication discovery, connection, and control exchange deadline. */
    timeoutMs?: number;
}
export interface AttachBrokerSide {
    broker: BrokerHandle;
    /** Presentation only; never used for trust or routing. */
    originLabel?: string;
    /** Independently approved authority for this broker. No mapping is inferred
     * from the inspection snapshot or from the opposite side. */
    scopeBindings: readonly ScopeBinding[];
}
export interface AttachBrokersOptions {
    /** Broker that emits and validates the peer hello. */
    initiator: AttachBrokerSide;
    /** Broker that validates the peer hello and emits the acknowledgement. */
    acceptor: AttachBrokerSide;
    /** Cancels acquisition or closes the active attachment. */
    signal?: AbortSignal;
    /** One absolute deadline covering both concurrent opens and broker handshake. */
    timeoutMs?: number;
}
/** Inspect one live Parley broker without exposing its endpoint credential or
 * control protocol. The first successful inspection may mint and persist the
 * broker installation's canonical federation origin. The returned handle owns
 * no stream; TCP handles become stale when that broker restarts. */
export declare function inspectBroker(host: BrokerHostAccess, options?: InspectBrokerOptions): Promise<BrokerHandle>;
/** Connect two inspected brokers through their caller-owned host capabilities.
 * Both exact streams are opened concurrently and become Parley-owned when their
 * promises fulfill. A stale handle is never rediscovered or replayed; callers
 * reinspect and decide whether to reconnect. */
export declare function attachBrokers(options: AttachBrokersOptions): Promise<BrokerAttachment>;
export interface LocalBrokerAccessOptions {
    /** Defaults to PI_CODING_AGENT_DIR, resolved against cwd, or ~/.pi/agent. */
    agentDir?: string;
    env?: NodeJS.ProcessEnv;
    homeDir?: string;
    cwd?: string;
}
/** Built-in rooted host access for standalone callers on this machine. */
export declare function createLocalBrokerAccess(options?: LocalBrokerAccessOptions): BrokerHostAccess;
