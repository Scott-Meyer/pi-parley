/**
 * Broker-federation v1 wire contracts.
 *
 * A trusted local controller owns authenticated transport. It prepares the
 * destination with broker_accept_peer and starts outbound authority through
 * broker_dial_peer or broker_start_peer, then copies the peer stream opaquely. Brokers own identity, scope authorization,
 * peer roles, and all later federation state.
 */
export declare const FEDERATION_PROTOCOL_NAME: "pi-parley-peer";
export declare const FEDERATION_PROTOCOL_VERSION: 1;
export declare const FEDERATION_IDENTITY_FEATURE: "peer-identity-v1";
export declare const FEDERATION_SINGLE_HOP_FEATURE: "peer-single-hop-v1";
export declare const FEDERATION_ROSTER_FEATURE: "peer-roster-v1";
export declare const FEDERATION_SEND_FEATURE: "peer-send-v1";
/** Optional extension: endpoint-pinned sends and structured rebound failures. */
export declare const FEDERATION_EXACT_SEND_FEATURE: "peer-send-exact-v1";
/** Optional single-hop text asks/replies with authenticated retained identity. */
export declare const FEDERATION_CONVERSATION_FEATURE: "peer-conversation-text-v1";
export declare const FEDERATION_CAPABILITY_MIN_LENGTH = 32;
export declare const FEDERATION_CAPABILITY_MAX_LENGTH = 128;
export declare const FEDERATION_CORRELATION_ID_MAX_LENGTH = 128;
export declare const FEDERATION_DISPLAY_LABEL_MAX_LENGTH = 80;
export declare const FEDERATION_MAX_FEATURES = 16;
export declare const FEDERATION_MAX_SCOPE_MAPPINGS = 16;
export declare const FEDERATION_ORIGIN_ID_MAX_LENGTH = 96;
export declare const FEDERATION_SCOPE_ALIAS_MAX_LENGTH = 64;
export declare const FEDERATION_SESSION_ID_MAX_LENGTH = 512;
export declare const FEDERATION_LOCAL_SCOPE_ID_MAX_LENGTH = 256;
export declare const FEDERATION_REQUIRED_FEATURES: readonly ["peer-identity-v1", "peer-single-hop-v1"];
export declare const FEDERATION_SUPPORTED_FEATURES: readonly ["peer-identity-v1", "peer-single-hop-v1", "peer-roster-v1", "peer-send-v1", "peer-send-exact-v1", "peer-conversation-text-v1"];
export type FederationRequiredFeature = typeof FEDERATION_REQUIRED_FEATURES[number];
export interface FederationOrigin {
    /** Stable installation identity, for example `host:penguin`. */
    id: string;
    /** Optional bounded presentation label; never used for routing or trust. */
    label?: string;
}
/** Public alias mapping sent broker-to-broker; it contains no local scope ID. */
export interface FederationScopeMapping {
    localScopeAlias: string;
    remoteScopeAlias: string;
}
/** Local authority binding supplied independently to each broker. */
export interface FederationScopeBinding extends FederationScopeMapping {
    /** Exact local PI_PARLEY_SCOPE_ID, or null for the unscoped namespace. */
    localScopeId: string | null;
}
export interface FederationLoopbackEndpoint {
    transport: "tcp";
    host: "127.0.0.1" | "::1";
    port: number;
}
/** Trusted-local request from an authenticated transport controller to the dialing broker. */
export interface BrokerDialPeerRequest {
    type: "broker_dial_peer";
    requestId: string;
    endpoint: FederationLoopbackEndpoint;
    /** Single-use, high-entropy authority consumed by the attachment controller. */
    capability: string;
    localOrigin: FederationOrigin;
    remoteOrigin: FederationOrigin;
    scopeBindings: FederationScopeBinding[];
    /** Required only when the broker itself uses opt-in localhost TCP. */
    stateId?: string;
}
/**
 * Trusted-local destination preparation written by the controller before it
 * starts opaque cross-piping. The prepared broker connection itself must
 * become the destination half of that pipe; preparation is not transferable
 * to a second socket. This is authority, not a peer assertion.
 */
export interface BrokerAcceptPeerRequest {
    type: "broker_accept_peer";
    requestId: string;
    linkId: string;
    localOrigin: FederationOrigin;
    remoteOrigin: FederationOrigin;
    scopeBindings: FederationScopeBinding[];
    stateId?: string;
}
/** Trusted-local outbound preparation on this exact supplied connection.
 * The broker writes its hello here, validates the returned peer acknowledgement,
 * then writes broker_start_peer_result before any activated peer traffic.
 * No dialing, bridge capability or additional endpoint is involved. */
export interface BrokerStartPeerRequest extends Omit<BrokerAcceptPeerRequest, "type"> {
    type: "broker_start_peer";
}
export type FederationFailureCode = "E_INVALID_REQUEST" | "E_DIAL_FAILED" | "E_HANDSHAKE_FAILED" | "E_VERSION_UNSUPPORTED" | "E_FEATURE_UNSUPPORTED" | "E_ORIGIN_MISMATCH" | "E_SCOPE_MISMATCH" | "E_ALREADY_CONNECTED" | "E_NOT_PREPARED";
export type BrokerDialPeerResult = {
    type: "broker_dial_peer_result";
    requestId: string;
    ok: true;
    linkId: string;
} | {
    type: "broker_dial_peer_result";
    requestId: string;
    ok: false;
    code: FederationFailureCode;
    error: string;
};
export type BrokerStartPeerResult = {
    type: "broker_start_peer_result";
    requestId: string;
    ok: true;
    linkId: string;
} | {
    type: "broker_start_peer_result";
    requestId: string;
    ok: false;
    code: FederationFailureCode;
    error: string;
};
/** First frame on a dialed broker -> controller attachment socket. */
export interface FederationBridgeAttach {
    type: "bridge_attach";
    protocol: typeof FEDERATION_PROTOCOL_NAME;
    version: typeof FEDERATION_PROTOCOL_VERSION;
    linkId: string;
    capability: string;
}
/** First opaque broker-to-broker frame after destination preparation. */
export interface PeerHello {
    type: "peer_hello";
    protocol: typeof FEDERATION_PROTOCOL_NAME;
    version: typeof FEDERATION_PROTOCOL_VERSION;
    linkId: string;
    origin: FederationOrigin;
    expectedPeerOrigin: FederationOrigin;
    scopeMappings: FederationScopeMapping[];
    features: string[];
}
export type PeerHelloAck = {
    type: "peer_hello_ack";
    protocol: typeof FEDERATION_PROTOCOL_NAME;
    version: typeof FEDERATION_PROTOCOL_VERSION;
    linkId: string;
    accepted: true;
    origin: FederationOrigin;
    acceptedPeerOriginId: string;
    scopeMappings: FederationScopeMapping[];
    features: string[];
} | {
    type: "peer_hello_ack";
    protocol: typeof FEDERATION_PROTOCOL_NAME;
    version: typeof FEDERATION_PROTOCOL_VERSION;
    linkId: string;
    accepted: false;
    code: FederationFailureCode;
    error: string;
};
/** Canonical tuple; the encoded string is display/routing serialization only. */
export interface OriginQualifiedSessionIdentity {
    originId: string;
    remoteScopeAlias: string;
    remoteStableSessionId: string;
}
export type BrokerAcceptPeerResult = {
    type: "broker_accept_peer_result";
    requestId: string;
    ok: true;
    linkId: string;
} | {
    type: "broker_accept_peer_result";
    requestId: string;
    ok: false;
    code: FederationFailureCode;
    error: string;
};
/** Trusted-local enumeration of the canonical federation origin and known
 * local scopes, for controller pickers. Raw scope ids never leave the
 * machine through a peer link. */
export interface BrokerListScopesRequest {
    type: "broker_list_scopes";
    requestId: string;
    stateId?: string;
}
export interface BrokerScopeSummary {
    /** Exact local PI_PARLEY_SCOPE_ID, or null for the unscoped namespace. */
    scopeId: string | null;
    liveSessions: number;
}
export type BrokerListScopesResult = {
    type: "broker_list_scopes_result";
    requestId: string;
    ok: true;
    localOrigin: {
        id: string;
    };
    scopes: BrokerScopeSummary[];
} | {
    type: "broker_list_scopes_result";
    requestId: string;
    ok: false;
    code: FederationFailureCode;
    error: string;
};
export type FederationControlMessage = BrokerDialPeerRequest | BrokerAcceptPeerRequest | BrokerListScopesRequest;
export type FederationPeerMessage = PeerHello | PeerHelloAck;
