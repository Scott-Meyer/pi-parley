/**
 * Broker-federation v1 wire contracts.
 *
 * A trusted local controller owns authenticated transport. It prepares the
 * destination with broker_accept_peer and starts outbound authority through
 * broker_dial_peer or broker_start_peer, then copies the peer stream opaquely. Brokers own identity, scope authorization,
 * peer roles, and all later federation state.
 */
export const FEDERATION_PROTOCOL_NAME = "pi-parley-peer";
export const FEDERATION_PROTOCOL_VERSION = 1;
export const FEDERATION_IDENTITY_FEATURE = "peer-identity-v1";
export const FEDERATION_SINGLE_HOP_FEATURE = "peer-single-hop-v1";
export const FEDERATION_ROSTER_FEATURE = "peer-roster-v1";
export const FEDERATION_SEND_FEATURE = "peer-send-v1";
/** Optional extension: endpoint-pinned sends and structured rebound failures. */
export const FEDERATION_EXACT_SEND_FEATURE = "peer-send-exact-v1";
/** Optional single-hop text asks/replies with authenticated retained identity. */
export const FEDERATION_CONVERSATION_FEATURE = "peer-conversation-text-v1";
export const FEDERATION_CAPABILITY_MIN_LENGTH = 32;
export const FEDERATION_CAPABILITY_MAX_LENGTH = 128;
export const FEDERATION_CORRELATION_ID_MAX_LENGTH = 128;
export const FEDERATION_DISPLAY_LABEL_MAX_LENGTH = 80;
export const FEDERATION_MAX_FEATURES = 16;
export const FEDERATION_MAX_SCOPE_MAPPINGS = 16;
export const FEDERATION_ORIGIN_ID_MAX_LENGTH = 96;
export const FEDERATION_SCOPE_ALIAS_MAX_LENGTH = 64;
export const FEDERATION_SESSION_ID_MAX_LENGTH = 512;
export const FEDERATION_LOCAL_SCOPE_ID_MAX_LENGTH = 256;
export const FEDERATION_REQUIRED_FEATURES = [
    FEDERATION_IDENTITY_FEATURE,
    FEDERATION_SINGLE_HOP_FEATURE,
];
export const FEDERATION_SUPPORTED_FEATURES = [
    ...FEDERATION_REQUIRED_FEATURES,
    FEDERATION_ROSTER_FEATURE,
    FEDERATION_SEND_FEATURE,
    FEDERATION_EXACT_SEND_FEATURE,
    FEDERATION_CONVERSATION_FEATURE,
];
