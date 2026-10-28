import { createRequire as __parleyCreateRequire } from "node:module"; const require = __parleyCreateRequire(import.meta.url);

// broker/broker.ts
import net3 from "net";
import { chmodSync as chmodSync3, mkdirSync as mkdirSync6, readdirSync as readdirSync2, readFileSync as readFileSync8, writeFileSync as writeFileSync5, unlinkSync } from "fs";
import { join as join9 } from "path";
import { createHash as createHash6, randomUUID as randomUUID8 } from "crypto";

// broker/framing.ts
var MAX_FRAME_BYTES = 1024 * 1024;
function writeMessage(socket, msg) {
  const json = JSON.stringify(msg);
  const payloadLength = Buffer.byteLength(json, "utf-8");
  const frame = Buffer.allocUnsafe(4 + payloadLength);
  frame.writeUInt32BE(payloadLength, 0);
  frame.write(json, 4, payloadLength, "utf-8");
  socket.write(frame);
}
function createMessageReader(onMessage, onError, maxFrameBytes = MAX_FRAME_BYTES) {
  const header = Buffer.allocUnsafe(4);
  let headerBytes = 0;
  let payload = null;
  let payloadBytes = 0;
  let payloadLength = 0;
  function reportMessage(framePayload) {
    let msg;
    try {
      msg = JSON.parse(framePayload.toString("utf-8"));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      onError(new Error(`Failed to parse parley message: ${message}`, { cause: error }));
      return false;
    }
    try {
      onMessage(msg);
      return true;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      onError(new Error(`Failed to handle parley message: ${message}`, { cause: error }));
      return false;
    }
  }
  return (data) => {
    let offset = 0;
    while (offset < data.length) {
      if (headerBytes < 4) {
        const bytes2 = Math.min(4 - headerBytes, data.length - offset);
        data.copy(header, headerBytes, offset, offset + bytes2);
        headerBytes += bytes2;
        offset += bytes2;
        if (headerBytes < 4) {
          return;
        }
        payloadLength = header.readUInt32BE(0);
        if (payloadLength > maxFrameBytes) {
          headerBytes = 0;
          onError(new Error(`Parley frame length ${payloadLength} exceeds maximum ${maxFrameBytes} bytes`));
          return;
        }
      }
      if (payloadBytes === 0 && data.length - offset >= payloadLength) {
        const framePayload2 = data.subarray(offset, offset + payloadLength);
        offset += payloadLength;
        headerBytes = 0;
        payload = null;
        payloadLength = 0;
        if (!reportMessage(framePayload2)) {
          return;
        }
        continue;
      }
      if (payload === null || payload.length !== payloadLength) {
        payload = Buffer.allocUnsafe(payloadLength);
      }
      const bytes = Math.min(payloadLength - payloadBytes, data.length - offset);
      data.copy(payload, payloadBytes, offset, offset + bytes);
      payloadBytes += bytes;
      offset += bytes;
      if (payloadBytes < payloadLength) {
        return;
      }
      const framePayload = payload;
      headerBytes = 0;
      payload = null;
      payloadBytes = 0;
      payloadLength = 0;
      if (!reportMessage(framePayload)) {
        return;
      }
    }
  };
}

// session-profile.ts
var SESSION_DESCRIPTION_MIN_WORDS = 5;
var SESSION_DESCRIPTION_MAX_WORDS = 9;
var SESSION_DESCRIPTION_MAX_LENGTH = 96;
var UNSAFE_CONTROL_CHARACTERS = /[\p{Cc}\p{Cf}]/u;
function normalizeInlineText(value) {
  return value.replace(/\s+/g, " ").trim();
}
function containsUnsafeControlCharacters(value) {
  return UNSAFE_CONTROL_CHARACTERS.test(value);
}
var RESERVED_SESSION_NAME_PREFIX = "oqs1.";
function isValidSessionName(value) {
  return typeof value === "string" && !containsUnsafeControlCharacters(value) && !value.startsWith(RESERVED_SESSION_NAME_PREFIX);
}
function normalizeSessionDescription(value) {
  return normalizeInlineText(value);
}
function sessionDescriptionWordCount(value) {
  const normalized = normalizeSessionDescription(value);
  return normalized ? normalized.split(" ").length : 0;
}
function isValidSessionDescription(value) {
  if (typeof value !== "string") return false;
  const normalized = normalizeSessionDescription(value);
  const words = sessionDescriptionWordCount(normalized);
  return normalized === value && !containsUnsafeControlCharacters(normalized) && normalized.length <= SESSION_DESCRIPTION_MAX_LENGTH && words >= SESSION_DESCRIPTION_MIN_WORDS && words <= SESSION_DESCRIPTION_MAX_WORDS;
}

// broker/federation-types.ts
var FEDERATION_PROTOCOL_NAME = "pi-parley-peer";
var FEDERATION_PROTOCOL_VERSION = 1;
var FEDERATION_IDENTITY_FEATURE = "peer-identity-v1";
var FEDERATION_SINGLE_HOP_FEATURE = "peer-single-hop-v1";
var FEDERATION_ROSTER_FEATURE = "peer-roster-v1";
var FEDERATION_SEND_FEATURE = "peer-send-v1";
var FEDERATION_EXACT_SEND_FEATURE = "peer-send-exact-v1";
var FEDERATION_CONVERSATION_FEATURE = "peer-conversation-text-v1";
var FEDERATION_SCOPED_LINKS_FEATURE = "peer-links-by-scope-v1";
var FEDERATION_PING_FEATURE = "peer-ping-v1";
var FEDERATION_APP_FEATURE = "peer-roster-app-v1";
var FEDERATION_CAPABILITY_MIN_LENGTH = 32;
var FEDERATION_CAPABILITY_MAX_LENGTH = 128;
var FEDERATION_CORRELATION_ID_MAX_LENGTH = 128;
var FEDERATION_DISPLAY_LABEL_MAX_LENGTH = 80;
var FEDERATION_MAX_FEATURES = 16;
var FEDERATION_MAX_SCOPE_MAPPINGS = 16;
var FEDERATION_ORIGIN_ID_MAX_LENGTH = 96;
var FEDERATION_SCOPE_ALIAS_MAX_LENGTH = 64;
var FEDERATION_SESSION_ID_MAX_LENGTH = 512;
var FEDERATION_LOCAL_SCOPE_ID_MAX_LENGTH = 256;
var FEDERATION_REQUIRED_FEATURES = [
  FEDERATION_IDENTITY_FEATURE,
  FEDERATION_SINGLE_HOP_FEATURE
];
var FEDERATION_SUPPORTED_FEATURES = [
  ...FEDERATION_REQUIRED_FEATURES,
  FEDERATION_ROSTER_FEATURE,
  FEDERATION_SEND_FEATURE,
  FEDERATION_EXACT_SEND_FEATURE,
  FEDERATION_CONVERSATION_FEATURE,
  FEDERATION_SCOPED_LINKS_FEATURE,
  FEDERATION_PING_FEATURE,
  FEDERATION_APP_FEATURE
];

// broker/federation-protocol.ts
var CONTROL_OR_FORMAT_CHARACTERS = /[\p{Cc}\p{Cf}]/u;
var ORIGIN_ID = /^[a-z][a-z0-9]*(?::[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$/;
var SCOPE_ALIAS = /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/;
var CORRELATION_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{7,127}$/;
var CAPABILITY = /^[A-Za-z0-9_-]+$/;
var FEATURE = /^[a-z0-9][a-z0-9.-]*$/;
var QUALIFIED_ID_PREFIX = "oqs1.";
function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function hasOnlyKeys(value, required, optional = []) {
  const keys = Object.keys(value);
  const allowed = /* @__PURE__ */ new Set([...required, ...optional]);
  return required.every((key) => Object.prototype.hasOwnProperty.call(value, key)) && keys.every((key) => allowed.has(key));
}
function isSafeText(value, maxLength, requireTrimmed = true) {
  return typeof value === "string" && value.length > 0 && value.length <= maxLength && (!requireTrimmed || value.trim() === value) && value.trim().length > 0 && !CONTROL_OR_FORMAT_CHARACTERS.test(value);
}
function isCanonicalFederationOriginId(value) {
  return typeof value === "string" && value.length <= FEDERATION_ORIGIN_ID_MAX_LENGTH && ORIGIN_ID.test(value);
}
function isCanonicalFederationScopeAlias(value) {
  return typeof value === "string" && value.length <= FEDERATION_SCOPE_ALIAS_MAX_LENGTH && SCOPE_ALIAS.test(value);
}
function isFederationCorrelationId(value) {
  return typeof value === "string" && value.length <= FEDERATION_CORRELATION_ID_MAX_LENGTH && CORRELATION_ID.test(value);
}
function isFederationCapability(value) {
  return typeof value === "string" && value.length >= FEDERATION_CAPABILITY_MIN_LENGTH && value.length <= FEDERATION_CAPABILITY_MAX_LENGTH && CAPABILITY.test(value);
}
function isFederationOrigin(value) {
  if (!isRecord(value) || !hasOnlyKeys(value, ["id"], ["label"])) return false;
  return isCanonicalFederationOriginId(value.id) && (value.label === void 0 || isSafeText(value.label, FEDERATION_DISPLAY_LABEL_MAX_LENGTH));
}
function isFederationLoopbackEndpoint(value) {
  return isRecord(value) && hasOnlyKeys(value, ["transport", "host", "port"]) && value.transport === "tcp" && (value.host === "127.0.0.1" || value.host === "::1") && Number.isInteger(value.port) && value.port >= 1 && value.port <= 65535;
}
function isFederationScopeMapping(value) {
  return isRecord(value) && hasOnlyKeys(value, ["localScopeAlias", "remoteScopeAlias"]) && isCanonicalFederationScopeAlias(value.localScopeAlias) && isCanonicalFederationScopeAlias(value.remoteScopeAlias);
}
function isFederationScopeBinding(value) {
  return isRecord(value) && hasOnlyKeys(value, ["localScopeId", "localScopeAlias", "remoteScopeAlias"]) && (value.localScopeId === null || isSafeText(value.localScopeId, FEDERATION_LOCAL_SCOPE_ID_MAX_LENGTH)) && isCanonicalFederationScopeAlias(value.localScopeAlias) && isCanonicalFederationScopeAlias(value.remoteScopeAlias);
}
function hasUniqueMappingAliases(mappings) {
  return new Set(mappings.map((mapping) => mapping.localScopeAlias)).size === mappings.length && new Set(mappings.map((mapping) => mapping.remoteScopeAlias)).size === mappings.length;
}
function isScopeMappings(value) {
  return Array.isArray(value) && value.length >= 1 && value.length <= FEDERATION_MAX_SCOPE_MAPPINGS && value.every(isFederationScopeMapping) && hasUniqueMappingAliases(value);
}
function isScopeBindings(value) {
  if (!Array.isArray(value) || value.length < 1 || value.length > FEDERATION_MAX_SCOPE_MAPPINGS || !value.every(isFederationScopeBinding) || !hasUniqueMappingAliases(value)) return false;
  return new Set(value.map((binding) => binding.localScopeId)).size === value.length;
}
function isFeatures(value) {
  if (!Array.isArray(value) || value.length < 1 || value.length > FEDERATION_MAX_FEATURES) return false;
  if (!value.every((feature) => typeof feature === "string" && feature.length <= 64 && FEATURE.test(feature))) return false;
  if (new Set(value).size !== value.length) return false;
  return value.includes(FEDERATION_IDENTITY_FEATURE) && value.includes(FEDERATION_SINGLE_HOP_FEATURE);
}
function scopeMappingsFromBindings(bindings) {
  return bindings.map(({ localScopeAlias, remoteScopeAlias }) => ({ localScopeAlias, remoteScopeAlias }));
}
function bindingsMatchPeerMappings(bindings, peerMappings) {
  const expected = scopeMappingsFromBindings(bindings);
  return expected.length === peerMappings.length && expected.every((mapping, index) => {
    const peer = peerMappings[index];
    return peer?.localScopeAlias === mapping.remoteScopeAlias && peer.remoteScopeAlias === mapping.localScopeAlias;
  });
}
function isBrokerDialPeerRequest(value) {
  if (!isRecord(value) || !hasOnlyKeys(
    value,
    ["type", "requestId", "endpoint", "capability", "localOrigin", "remoteOrigin", "scopeBindings"],
    ["stateId"]
  )) return false;
  return value.type === "broker_dial_peer" && isFederationCorrelationId(value.requestId) && isFederationLoopbackEndpoint(value.endpoint) && isFederationCapability(value.capability) && isFederationOrigin(value.localOrigin) && isFederationOrigin(value.remoteOrigin) && value.localOrigin.id !== value.remoteOrigin.id && isScopeBindings(value.scopeBindings) && (value.stateId === void 0 || isFederationCorrelationId(value.stateId));
}
function isSuppliedPeerRequest(value, type) {
  if (!isRecord(value) || !hasOnlyKeys(
    value,
    ["type", "requestId", "linkId", "localOrigin", "remoteOrigin", "scopeBindings"],
    ["stateId"]
  )) return false;
  return value.type === type && isFederationCorrelationId(value.requestId) && isFederationCorrelationId(value.linkId) && isFederationOrigin(value.localOrigin) && isFederationOrigin(value.remoteOrigin) && value.localOrigin.id !== value.remoteOrigin.id && isScopeBindings(value.scopeBindings) && (value.stateId === void 0 || isFederationCorrelationId(value.stateId));
}
function isBrokerAcceptPeerRequest(value) {
  return isSuppliedPeerRequest(value, "broker_accept_peer");
}
function isBrokerStartPeerRequest(value) {
  return isSuppliedPeerRequest(value, "broker_start_peer");
}
function isBrokerListScopesRequest(value) {
  if (!isRecord(value) || !hasOnlyKeys(value, ["type", "requestId"], ["stateId", "include"])) return false;
  return value.type === "broker_list_scopes" && isFederationCorrelationId(value.requestId) && (value.stateId === void 0 || isFederationCorrelationId(value.stateId)) && (value.include === void 0 || Array.isArray(value.include) && value.include.length <= 4 && value.include.every((item) => item === "mergedScopes"));
}
function isPeerHello(value) {
  if (!isRecord(value) || !hasOnlyKeys(
    value,
    ["type", "protocol", "version", "linkId", "origin", "expectedPeerOrigin", "scopeMappings", "features"]
  )) return false;
  return value.type === "peer_hello" && value.protocol === FEDERATION_PROTOCOL_NAME && value.version === FEDERATION_PROTOCOL_VERSION && isFederationCorrelationId(value.linkId) && isFederationOrigin(value.origin) && isFederationOrigin(value.expectedPeerOrigin) && value.origin.id !== value.expectedPeerOrigin.id && isScopeMappings(value.scopeMappings) && isFeatures(value.features);
}
var FAILURE_CODES = /* @__PURE__ */ new Set([
  "E_INVALID_REQUEST",
  "E_DIAL_FAILED",
  "E_HANDSHAKE_FAILED",
  "E_VERSION_UNSUPPORTED",
  "E_FEATURE_UNSUPPORTED",
  "E_ORIGIN_MISMATCH",
  "E_SCOPE_MISMATCH",
  "E_ALREADY_CONNECTED",
  "E_NOT_PREPARED"
]);
function isFailureCode(value) {
  return typeof value === "string" && FAILURE_CODES.has(value);
}
function isPeerHelloAck(value) {
  if (!isRecord(value) || value.type !== "peer_hello_ack" || value.protocol !== FEDERATION_PROTOCOL_NAME || value.version !== FEDERATION_PROTOCOL_VERSION || !isFederationCorrelationId(value.linkId) || typeof value.accepted !== "boolean") return false;
  if (value.accepted) {
    return hasOnlyKeys(value, ["type", "protocol", "version", "linkId", "accepted", "origin", "acceptedPeerOriginId", "scopeMappings", "features"]) && isFederationOrigin(value.origin) && isCanonicalFederationOriginId(value.acceptedPeerOriginId) && value.origin.id !== value.acceptedPeerOriginId && isScopeMappings(value.scopeMappings) && isFeatures(value.features);
  }
  return hasOnlyKeys(value, ["type", "protocol", "version", "linkId", "accepted", "code", "error"]) && isFailureCode(value.code) && isSafeText(value.error, 256);
}
function isStableSessionId(value) {
  return isSafeText(value, FEDERATION_SESSION_ID_MAX_LENGTH, false);
}
function isOriginQualifiedSessionIdentity(value) {
  if (!isRecord(value) || !hasOnlyKeys(value, ["originId", "remoteScopeAlias", "remoteStableSessionId"])) return false;
  return isCanonicalFederationOriginId(value.originId) && isCanonicalFederationScopeAlias(value.remoteScopeAlias) && isStableSessionId(value.remoteStableSessionId);
}
function encodeOriginQualifiedSessionIdentity(identity) {
  if (!isOriginQualifiedSessionIdentity(identity)) throw new Error("Invalid origin-qualified session identity");
  const tuple = [identity.originId, identity.remoteScopeAlias, identity.remoteStableSessionId];
  return `${QUALIFIED_ID_PREFIX}${Buffer.from(JSON.stringify(tuple), "utf8").toString("base64url")}`;
}

// broker/paths.ts
import { createHash } from "crypto";
import { chmodSync, mkdirSync, readFileSync } from "fs";
import { isAbsolute, join, resolve, win32 } from "path";
import { homedir } from "os";
var PARLEY_DIR_MODE = 448;
var PARLEY_RUNTIME_FILE_MODE = 384;
var PARLEY_TCP_HOST = "127.0.0.1";
var PARLEY_PROTOCOL_NAME = "pi-parley";
var PARLEY_PROTOCOL_VERSION = 1;
function getBrokerPipeName(agentDir) {
  let normalized = win32.normalize(agentDir).toLowerCase();
  const rootLength = win32.parse(normalized).root.length;
  while (normalized.length > rootLength && normalized.endsWith("\\")) {
    normalized = normalized.slice(0, -1);
  }
  const digest = createHash("sha256").update("pi-parley-agent-dir\0", "utf8").update(normalized, "utf8").digest("hex");
  return `pi-parley-${digest}`;
}
function getAgentDirPath(env = process.env, homeDir = homedir(), cwd = process.cwd()) {
  const configured = env.PI_CODING_AGENT_DIR?.trim();
  if (!configured) {
    return join(homeDir, ".pi/agent");
  }
  return isAbsolute(configured) ? configured : resolve(cwd, configured);
}
function getParleyDirPath(agentDir = getAgentDirPath()) {
  return join(agentDir, "parley");
}
function shouldUseTcpTransport(env = process.env, platform = process.platform) {
  const transport = env.PI_PARLEY_TRANSPORT?.trim().toLowerCase();
  if (transport === "tcp") return true;
  if (transport === "socket") return false;
  const tcpOptIn = env.PI_PARLEY_TCP?.trim().toLowerCase();
  if (tcpOptIn === "1" || tcpOptIn === "true") return true;
  if (tcpOptIn === "0" || tcpOptIn === "false") return false;
  return platform === "win32";
}
function getBrokerPortFilePath(parleyDir = getParleyDirPath()) {
  return join(parleyDir, "broker.port.json");
}
function getBrokerSocketPath(platform = process.platform, agentDir = getAgentDirPath()) {
  if (platform === "win32") {
    return `\\\\.\\pipe\\${getBrokerPipeName(agentDir)}`;
  }
  return join(getParleyDirPath(agentDir), "broker.sock");
}
function readBrokerTcpEndpoint(parleyDir = getParleyDirPath()) {
  const endpointFile = getBrokerPortFilePath(parleyDir);
  const parsed = JSON.parse(readFileSync(endpointFile, "utf-8"));
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`Invalid parley TCP endpoint at ${endpointFile}: expected a JSON object`);
  }
  const endpoint = parsed;
  if (endpoint.transport !== "tcp" || endpoint.host !== PARLEY_TCP_HOST || typeof endpoint.port !== "number" || !Number.isSafeInteger(endpoint.port) || endpoint.port <= 0 || endpoint.port > 65535 || typeof endpoint.stateId !== "string" || endpoint.stateId.length === 0) {
    throw new Error(`Invalid parley TCP endpoint at ${endpointFile}`);
  }
  return Object.freeze({ transport: "tcp", host: endpoint.host, port: endpoint.port, stateId: endpoint.stateId });
}
function getBrokerListenTarget(platform = process.platform, env = process.env) {
  if (shouldUseTcpTransport(env, platform)) {
    return { transport: "tcp", host: PARLEY_TCP_HOST, port: 0 };
  }
  return getBrokerSocketPath(platform, getAgentDirPath(env));
}
function ensureParleyRuntimeDir(parleyDir = getParleyDirPath(), platform = process.platform) {
  mkdirSync(parleyDir, { recursive: true, mode: PARLEY_DIR_MODE });
  if (platform !== "win32") {
    chmodSync(parleyDir, PARLEY_DIR_MODE);
  }
}
function restrictParleyRuntimeFile(filePath, platform = process.platform) {
  if (platform !== "win32") {
    chmodSync(filePath, PARLEY_RUNTIME_FILE_MODE);
  }
}

// broker/protocol.ts
function isBrokerHealthOkMessage(message, requestId) {
  if (!isRecord2(message)) return false;
  return message.type === "health_ok" && message.requestId === requestId && message.protocol === PARLEY_PROTOCOL_NAME && message.version === PARLEY_PROTOCOL_VERSION;
}
function isRecord2(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function isMessageReceiptStatus(value) {
  return value === "receiver_received" || value === "queued" || value === "injected" || value === "acknowledged" || value === "expired" || value === "cancelled" || value === "superseded" || value === "cancellation_requested";
}
function isMessageReceipt(value) {
  if (!isRecord2(value)) {
    return false;
  }
  if (typeof value.messageId !== "string" || !isMessageReceiptStatus(value.status) || typeof value.timestamp !== "number") {
    return false;
  }
  return value.detail === void 0 || typeof value.detail === "string";
}
function isAttachment(value) {
  if (!isRecord2(value)) {
    return false;
  }
  if (value.type !== "file" && value.type !== "snippet" && value.type !== "context") {
    return false;
  }
  if (typeof value.name !== "string" || typeof value.content !== "string") {
    return false;
  }
  return value.language === void 0 || typeof value.language === "string";
}
function isMessageProvenance(value) {
  if (!isRecord2(value)) {
    return false;
  }
  if (value.type === "session_person") return Object.keys(value).length === 1;
  return value.type === "extension_outbox" && typeof value.extensionId === "string" && typeof value.extensionName === "string" && typeof value.requestId === "string";
}
function isPeerCompactionNotice(value) {
  if (!isRecord2(value)) return false;
  if (typeof value.peerSessionId !== "string" || value.peerName !== void 0 && typeof value.peerName !== "string" || value.requestedPeerSessionId !== void 0 && typeof value.requestedPeerSessionId !== "string" || !Number.isSafeInteger(value.generation) || value.generation < 1 || !Number.isSafeInteger(value.previousGeneration) || value.previousGeneration < 0 || value.previousGeneration >= value.generation || !Number.isSafeInteger(value.compactedAt) || value.compactedAt < 0) {
    return false;
  }
  return value.contextPct === void 0 || typeof value.contextPct === "number";
}
function isMessage(value) {
  if (!isRecord2(value)) {
    return false;
  }
  if (typeof value.id !== "string" || typeof value.timestamp !== "number") {
    return false;
  }
  for (const key of ["senderSequence", "brokerReceivedAt", "brokerDeliveredAt", "receiverReceivedAt", "injectedAt"]) {
    if (value[key] !== void 0 && typeof value[key] !== "number") {
      return false;
    }
  }
  if (value.supersedes !== void 0 && typeof value.supersedes !== "string") {
    return false;
  }
  if (value.retryOf !== void 0 && typeof value.retryOf !== "string") {
    return false;
  }
  if (value.replyTo !== void 0 && typeof value.replyTo !== "string") {
    return false;
  }
  if (value.expectsReply !== void 0 && typeof value.expectsReply !== "boolean") {
    return false;
  }
  if (value.completesAsk !== void 0 && typeof value.completesAsk !== "boolean") return false;
  if (value.completesAsk === true && !value.replyTo) return false;
  if (value.senderWaitMode !== void 0 && value.senderWaitMode !== "blocking" && value.senderWaitMode !== "nonblocking") return false;
  if (value.replyDeadline !== void 0 && (!Number.isSafeInteger(value.replyDeadline) || value.replyDeadline < 0)) return false;
  if (value.provenance !== void 0 && !isMessageProvenance(value.provenance)) {
    return false;
  }
  if (value.peerCompaction !== void 0 && !isPeerCompactionNotice(value.peerCompaction)) {
    return false;
  }
  if (value.contactToken !== void 0 && typeof value.contactToken !== "string") {
    return false;
  }
  if (value.contactBaseline !== void 0 && typeof value.contactBaseline !== "boolean") {
    return false;
  }
  if (value.contactBaseline === true && typeof value.contactToken !== "string") {
    return false;
  }
  if (!isRecord2(value.content) || typeof value.content.text !== "string") {
    return false;
  }
  return value.content.attachments === void 0 || Array.isArray(value.content.attachments) && value.content.attachments.every(isAttachment);
}
function isAppLabel(value) {
  return typeof value === "string" && value.trim().length > 0 && value.length <= 64 && !/[\p{Cc}\p{Cf}]/u.test(value);
}
function isSessionInfo(value) {
  if (!isRecord2(value)) {
    return false;
  }
  if (typeof value.id !== "string" || typeof value.cwd !== "string" || typeof value.model !== "string" || typeof value.pid !== "number" || typeof value.startedAt !== "number" || typeof value.lastActivity !== "number") {
    return false;
  }
  if (value.endpointEpoch !== void 0 && typeof value.endpointEpoch !== "string") {
    return false;
  }
  if (value.name !== void 0 && !isValidSessionName(value.name)) {
    return false;
  }
  if (value.description !== void 0 && !isValidSessionDescription(value.description)) {
    return false;
  }
  if (value.runtimeFallbackAlias !== void 0 && typeof value.runtimeFallbackAlias !== "boolean") {
    return false;
  }
  if (value.status !== void 0 && typeof value.status !== "string") {
    return false;
  }
  if (value.peerUid !== void 0 && typeof value.peerUid !== "number") {
    return false;
  }
  for (const key of ["contextPct", "contextTokens", "contextWindow"]) {
    if (value[key] !== void 0 && typeof value[key] !== "number") {
      return false;
    }
  }
  if (value.tmuxPane !== void 0 && typeof value.tmuxPane !== "string") {
    return false;
  }
  if (value.app !== void 0 && !isAppLabel(value.app)) {
    return false;
  }
  if (value.isSubagent !== void 0 && typeof value.isSubagent !== "boolean") {
    return false;
  }
  if (value.supervisorSessionId !== void 0 && typeof value.supervisorSessionId !== "string") {
    return false;
  }
  if (value.supervisorName !== void 0 && typeof value.supervisorName !== "string") {
    return false;
  }
  if (value.advertised !== void 0 && typeof value.advertised !== "boolean") {
    return false;
  }
  if (value.extensions !== void 0) {
    if (!Array.isArray(value.extensions) || value.extensions.length > 32) return false;
    for (const extension of value.extensions) {
      if (!isRecord2(extension) || Object.keys(extension).some((key) => key !== "namespace" && key !== "ownerEligible")) return false;
      if (typeof extension.namespace !== "string" || extension.namespace.length === 0 || extension.namespace.length > 64 || !/^[a-z0-9][a-z0-9._/-]*$/.test(extension.namespace) || typeof extension.ownerEligible !== "boolean") return false;
    }
  }
  if (value.federation !== void 0) {
    if (!isRecord2(value.federation)) return false;
    const keys = Object.keys(value.federation);
    if (!keys.every((key) => ["originId", "originLabel", "remoteScopeAlias", "remoteStableSessionId", "conversation", "originEpoch"].includes(key))) return false;
    if (value.federation.conversation !== void 0 && typeof value.federation.conversation !== "boolean") return false;
    if (value.federation.originEpoch !== void 0 && !isFederationCorrelationId(value.federation.originEpoch)) return false;
    if (!isCanonicalFederationOriginId(value.federation.originId) || !isCanonicalFederationScopeAlias(value.federation.remoteScopeAlias) || typeof value.federation.remoteStableSessionId !== "string" || value.federation.remoteStableSessionId.length < 1 || value.federation.remoteStableSessionId.length > 512 || /[\p{Cc}\p{Cf}]/u.test(value.federation.remoteStableSessionId) || value.federation.originLabel !== void 0 && (typeof value.federation.originLabel !== "string" || value.federation.originLabel.length < 1 || value.federation.originLabel.length > 80 || /[\p{Cc}\p{Cf}]/u.test(value.federation.originLabel))) return false;
    if (value.trustedLocal !== false || value.peerUid !== void 0 || value.isSubagent !== void 0 || value.supervisorSessionId !== void 0 || value.supervisorName !== void 0 || value.advertised !== void 0 || value.extensions !== void 0) return false;
    try {
      if (value.id !== encodeOriginQualifiedSessionIdentity({
        originId: value.federation.originId,
        remoteScopeAlias: value.federation.remoteScopeAlias,
        remoteStableSessionId: value.federation.remoteStableSessionId
      })) return false;
    } catch {
      return false;
    }
  }
  return value.trustedLocal === void 0 || typeof value.trustedLocal === "boolean";
}
function isAuthoredMessage(value) {
  return isMessage(value) && value.peerCompaction === void 0 && value.contactToken === void 0 && value.contactBaseline === void 0 && value.replyDeadline === void 0;
}
function isSessionId(value) {
  return typeof value === "string" && value.trim().length > 0 && value.length <= 512 && !/[\p{Cc}\p{Cf}]/u.test(value) && !value.startsWith("oqs1.");
}
function isSessionRegistration(value) {
  if (!isRecord2(value)) {
    return false;
  }
  if (typeof value.cwd !== "string" || typeof value.model !== "string" || typeof value.pid !== "number" || typeof value.startedAt !== "number" || typeof value.lastActivity !== "number") {
    return false;
  }
  if (value.name !== void 0 && !isValidSessionName(value.name)) {
    return false;
  }
  if (value.description !== void 0 && !isValidSessionDescription(value.description)) {
    return false;
  }
  if (value.runtimeFallbackAlias !== void 0 && typeof value.runtimeFallbackAlias !== "boolean") {
    return false;
  }
  if (value.extensions !== void 0 && !Array.isArray(value.extensions)) {
    return false;
  }
  if (value.tmuxPane !== void 0 && typeof value.tmuxPane !== "string") {
    return false;
  }
  if (value.app !== void 0 && !isAppLabel(value.app)) {
    return false;
  }
  if (value.isSubagent !== void 0 && typeof value.isSubagent !== "boolean") {
    return false;
  }
  if (value.supervisorSessionId !== void 0 && typeof value.supervisorSessionId !== "string") {
    return false;
  }
  if (value.supervisorName !== void 0 && typeof value.supervisorName !== "string") {
    return false;
  }
  return value.status === void 0 || typeof value.status === "string";
}

// config.ts
var DEFAULT_ASK_TIMEOUT_MS = 10 * 60 * 1e3;
function getAskTimeoutMs() {
  const raw = process.env.PI_PARLEY_ASK_TIMEOUT_MS;
  if (raw === void 0 || raw.trim() === "") {
    return DEFAULT_ASK_TIMEOUT_MS;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error("PI_PARLEY_ASK_TIMEOUT_MS must be a positive integer number of milliseconds");
  }
  return value;
}

// broker/merged-scopes.ts
import { readFileSync as readFileSync2 } from "node:fs";
import { join as join2 } from "node:path";
var DEFAULT_MERGED_SCOPES = ["flightdeck"];
function loadMergedScopes(parleyDir) {
  try {
    const parsed = JSON.parse(readFileSync2(join2(parleyDir, "config.json"), "utf8"));
    if (Array.isArray(parsed.mergedScopes) && parsed.mergedScopes.every((scope) => typeof scope === "string")) {
      return new Set(parsed.mergedScopes.map((scope) => scope.trim()).filter(Boolean));
    }
  } catch {
  }
  return new Set(DEFAULT_MERGED_SCOPES);
}
function effectiveScope(scopeId, merged) {
  return typeof scopeId === "string" && merged.has(scopeId) ? void 0 : scopeId;
}

// cwd.ts
import { resolve as resolve2 } from "node:path";
import { realpathSync } from "node:fs";
var normalizeCache = /* @__PURE__ */ new Map();
function normalizeCwd(cwd) {
  const cached = normalizeCache.get(cwd);
  if (cached !== void 0) {
    return cached;
  }
  const resolved = resolve2(cwd);
  let normalized;
  try {
    normalized = realpathSync(resolved);
  } catch {
    normalized = resolved;
  }
  normalizeCache.set(cwd, normalized);
  return normalized;
}
function sameCwd(a, b) {
  return normalizeCwd(a) === normalizeCwd(b);
}

// types.ts
var EXTENSION_BUS_FEATURE = "extension-bus-v1";
var EXACT_SEND_FEATURE = "exact-send-v1";
var EXACT_IDENTITY_SEND_FEATURE = "exact-identity-send-v1";
var COMPACTION_AWARENESS_FEATURE = "compaction-awareness-v1";
var SESSION_PROFILE_FEATURE = "session-profile-v1";
var FEDERATED_CONVERSATION_FEATURE = "federated-conversation-text-v1";
var CONVERSATION_CONTRACT_FEATURE = "conversation-contract-v1";
var PERSON_PROVENANCE_FEATURE = "person-provenance-v1";

// broker/extension-state.ts
import { createHash as createHash2, randomUUID } from "node:crypto";
import {
  closeSync,
  copyFileSync,
  existsSync,
  fsyncSync,
  mkdirSync as mkdirSync2,
  openSync,
  readFileSync as readFileSync3,
  renameSync,
  rmSync,
  writeFileSync
} from "node:fs";
import { dirname, join as join3 } from "node:path";
var MAX_STATE_BYTES = 64 * 1024;
function serializePayload(payload) {
  try {
    const json = JSON.stringify(payload);
    if (json === void 0 || Buffer.byteLength(json, "utf8") > MAX_STATE_BYTES) {
      return null;
    }
    return json;
  } catch {
    return null;
  }
}
function payloadHash(payloadJson) {
  return createHash2("sha256").update(payloadJson).digest("hex");
}
var ExtensionStateManager = class {
  states = /* @__PURE__ */ new Map();
  stateDir;
  constructor(runtimeDir) {
    this.stateDir = join3(runtimeDir, "extension-state");
    mkdirSync2(this.stateDir, { recursive: true, mode: 448 });
  }
  statePath(namespace) {
    const hash2 = createHash2("sha256").update(namespace).digest("hex");
    return join3(this.stateDir, `${hash2}.json`);
  }
  backupPath(namespace) {
    return `${this.statePath(namespace)}.bak`;
  }
  readEnvelope(filePath, namespace) {
    if (!existsSync(filePath)) return null;
    try {
      const content = readFileSync3(filePath, "utf8");
      const value = JSON.parse(content);
      if (!value || typeof value !== "object" || Array.isArray(value)) return null;
      const envelope = value;
      const revision = envelope.revision;
      const updatedAt = envelope.updatedAt;
      const payloadSha256 = envelope.payloadSha256;
      if (envelope.formatVersion !== 1 || envelope.namespace !== namespace || typeof revision !== "number" || !Number.isSafeInteger(revision) || revision < 0 || typeof updatedAt !== "number" || typeof payloadSha256 !== "string") {
        return null;
      }
      const payloadJson = serializePayload(envelope.payload);
      if (payloadJson === null || payloadHash(payloadJson) !== payloadSha256) {
        return null;
      }
      return {
        formatVersion: 1,
        namespace,
        revision,
        updatedAt,
        payloadSha256,
        payload: envelope.payload
      };
    } catch {
      return null;
    }
  }
  loadState(namespace) {
    const cached = this.states.get(namespace);
    if (cached) return cached;
    const envelope = this.readEnvelope(this.statePath(namespace), namespace) ?? this.readEnvelope(this.backupPath(namespace), namespace);
    if (!envelope) return null;
    const state = { revision: envelope.revision, payload: envelope.payload };
    this.states.set(namespace, state);
    return state;
  }
  commitState(namespace, expectedRevision, payload) {
    const payloadJson = serializePayload(payload);
    const current = this.loadState(namespace);
    const currentRevision = current?.revision ?? 0;
    if (payloadJson === null) {
      return {
        committed: false,
        revision: currentRevision,
        reason: "Invalid extension state or payload exceeds 64 KiB limit"
      };
    }
    if (!Number.isSafeInteger(expectedRevision) || expectedRevision < 0) {
      return { committed: false, revision: currentRevision, reason: "Invalid expected revision" };
    }
    if (expectedRevision !== currentRevision) {
      return {
        committed: false,
        revision: currentRevision,
        reason: "Revision mismatch",
        ...current ? { payload: current.payload } : {}
      };
    }
    const envelope = {
      formatVersion: 1,
      namespace,
      revision: currentRevision + 1,
      updatedAt: Date.now(),
      payloadSha256: payloadHash(payloadJson),
      payload
    };
    const statePath = this.statePath(namespace);
    const backupPath = this.backupPath(namespace);
    const tempPath = `${statePath}.tmp.${process.pid}.${randomUUID()}`;
    try {
      writeFileSync(tempPath, JSON.stringify(envelope), { mode: 384 });
      const file = openSync(tempPath, "r");
      try {
        fsyncSync(file);
      } finally {
        closeSync(file);
      }
      if (this.readEnvelope(statePath, namespace)) {
        copyFileSync(statePath, backupPath);
      }
      renameSync(tempPath, statePath);
      try {
        const directory = openSync(dirname(statePath), "r");
        try {
          fsyncSync(directory);
        } finally {
          closeSync(directory);
        }
      } catch {
      }
      const state = { revision: envelope.revision, payload };
      this.states.set(namespace, state);
      return { committed: true, revision: envelope.revision };
    } catch {
      return { committed: false, revision: currentRevision, reason: "Failed to persist extension state" };
    } finally {
      rmSync(tempPath, { force: true });
    }
  }
  getCurrentRevision(namespace) {
    return this.loadState(namespace)?.revision ?? 0;
  }
};

// broker/runtime-claim.ts
import { lstatSync } from "node:fs";
import net from "node:net";
import { join as join4 } from "node:path";
import { randomUUID as randomUUID3 } from "node:crypto";

// broker/process-lock.ts
import { randomUUID as randomUUID2 } from "node:crypto";
import {
  closeSync as closeSync2,
  constants,
  fstatSync,
  mkdirSync as mkdirSync3,
  openSync as openSync2,
  readSync,
  writeFileSync as writeFileSync2
} from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
var loadedNativeLocks;
function nativeLocks() {
  loadedNativeLocks ??= createRequire(import.meta.url)("fs-native-extensions");
  return loadedNativeLocks;
}
function tryAcquireProcessLock(directory) {
  mkdirSync3(directory, { recursive: true, mode: 448 });
  const fd = openSync2(path.join(directory, ".process.lock"), constants.O_RDWR | constants.O_CREAT, 384);
  let retained = false;
  try {
    if (!fstatSync(fd).isFile()) throw new Error("Process lock must be a regular file");
    if (!nativeLocks().tryLock(fd)) {
      return Object.freeze({ status: "occupied", owner: readDiagnosticOwner(directory) });
    }
    const owner = Object.freeze({
      pid: process.pid,
      claimId: randomUUID2(),
      acquiredAt: (/* @__PURE__ */ new Date()).toISOString()
    });
    writeFileSync2(path.join(directory, ".process.owner.json"), `${JSON.stringify(owner)}
`, { mode: 384 });
    let descriptor = fd;
    const lease = Object.freeze({
      owner,
      release() {
        if (descriptor === null) return;
        const ownedDescriptor = descriptor;
        descriptor = null;
        try {
          nativeLocks().unlock(ownedDescriptor);
        } finally {
          closeSync2(ownedDescriptor);
        }
      }
    });
    retained = true;
    return Object.freeze({ status: "acquired", lease });
  } finally {
    if (!retained) closeSync2(fd);
  }
}
function readDiagnosticOwner(directory) {
  let fd = null;
  try {
    fd = openSync2(path.join(directory, ".process.owner.json"), "r");
    const bytes = Buffer.alloc(4096);
    const count = readSync(fd, bytes, 0, bytes.length, 0);
    if (count === 0 || count === bytes.length) return null;
    const value = JSON.parse(bytes.subarray(0, count).toString("utf8"));
    if (typeof value !== "object" || value === null) return null;
    const owner = value;
    if (typeof owner.pid !== "number" || !Number.isSafeInteger(owner.pid) || owner.pid <= 0 || typeof owner.claimId !== "string" || !/^[0-9a-f-]{36}$/.test(owner.claimId) || typeof owner.acquiredAt !== "string" || !Number.isFinite(Date.parse(owner.acquiredAt))) return null;
    return Object.freeze({ pid: owner.pid, claimId: owner.claimId, acquiredAt: owner.acquiredAt });
  } catch {
    return null;
  } finally {
    if (fd !== null) {
      try {
        closeSync2(fd);
      } catch {
      }
    }
  }
}

// broker/runtime-claim.ts
var BROKER_RUNTIME_OCCUPIED_EXIT_CODE = 75;
var BrokerRuntimeOccupiedError = class extends Error {
  constructor(runtimeDir, reportedOwner) {
    const owner = reportedOwner ? ` (reported pid ${reportedOwner.pid})` : "";
    super(`Parley runtime is already owned${owner}: ${runtimeDir}`);
    this.reportedOwner = reportedOwner;
    this.name = "BrokerRuntimeOccupiedError";
  }
  reportedOwner;
  code = "E_RUNTIME_OWNED";
};
async function claimBrokerRuntime(runtimeDir, listenTarget) {
  ensureParleyRuntimeDir(runtimeDir);
  const claim = tryAcquireProcessLock(join4(runtimeDir, "broker.ownership"));
  if (claim.status === "occupied") {
    throw new BrokerRuntimeOccupiedError(runtimeDir, claim.owner);
  }
  try {
    if (typeof listenTarget === "string") {
      await assertEndpointInactive(listenTarget);
    }
    const publishedTcp = readPublishedTcpEndpoint(runtimeDir);
    if (publishedTcp && await servesParley(publishedTcp)) {
      throw new Error(`Parley TCP endpoint is already serving: ${publishedTcp.host}:${publishedTcp.port}`);
    }
    return claim.lease;
  } catch (error) {
    claim.lease.release();
    throw error;
  }
}
function assertEndpointInactive(target) {
  if (process.platform !== "win32") {
    try {
      if (!lstatSync(target).isSocket()) {
        return Promise.reject(new Error(`Refusing to replace a non-socket Parley IPC path: ${target}`));
      }
    } catch (error) {
      if (error.code === "ENOENT") return Promise.resolve();
      return Promise.reject(error);
    }
  }
  return new Promise((resolve3, reject) => {
    const socket = net.connect(target);
    let settled = false;
    const finish = (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      socket.destroy();
      if (error) reject(error);
      else resolve3();
    };
    socket.once("connect", () => finish(new Error(`Parley IPC endpoint is already serving: ${target}`)));
    socket.once("error", (error) => {
      if (error.code === "ENOENT" || error.code === "ECONNREFUSED") finish();
      else finish(error);
    });
    const timeout = setTimeout(() => finish(new Error(`Could not establish whether Parley IPC endpoint is inactive: ${target}`)), 1e3);
  });
}
function readPublishedTcpEndpoint(runtimeDir) {
  try {
    return readBrokerTcpEndpoint(runtimeDir);
  } catch (error) {
    const code = error.code;
    if (code === "ENOENT" || error instanceof SyntaxError || error instanceof Error && error.message.startsWith("Invalid parley TCP endpoint at ")) return void 0;
    throw error;
  }
}
function servesParley(target) {
  return new Promise((resolve3) => {
    const requestId = randomUUID3();
    const socket = net.connect({ host: target.host, port: target.port });
    let settled = false;
    const finish = (serving) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      socket.destroy();
      resolve3(serving);
    };
    socket.once("connect", () => {
      try {
        writeMessage(socket, { type: "health", requestId, stateId: target.stateId });
      } catch {
        finish(false);
      }
    });
    socket.once("error", () => finish(false));
    socket.once("close", () => finish(false));
    socket.on("data", createMessageReader(
      (message) => finish(isBrokerHealthOkMessage(message, requestId)),
      () => finish(false)
    ));
    const timeout = setTimeout(() => finish(false), 1e3);
  });
}

// broker/build.ts
import { createHash as createHash3 } from "node:crypto";
import { readFileSync as readFileSync4 } from "node:fs";
import { dirname as dirname2, join as join5 } from "node:path";
import { fileURLToPath } from "node:url";

// broker/bundle.ts
var bundledBuild = true ? JSON.parse('{"packageVersion":"1.6.0","sourceId":"2a7065408bfd38e34b57cfd4944b46f6c05f275657423bf9f8e1f2a822fd90cb"}') : void 0;

// broker/build.ts
function getBrokerBuildIdentity(extensionDir = join5(dirname2(fileURLToPath(import.meta.url)), "..")) {
  if (bundledBuild) return bundledBuild;
  const packageRaw = readFileSync4(join5(extensionDir, "package.json"));
  const manifest = JSON.parse(packageRaw.toString("utf8"));
  if (!manifest || typeof manifest !== "object" || Array.isArray(manifest)) {
    throw new Error("Invalid Parley package manifest");
  }
  const { version, files } = manifest;
  if (typeof version !== "string" || !version.trim() || !Array.isArray(files)) {
    throw new Error("Parley package manifest requires a version and source file list");
  }
  const inputs = [...new Set(files.filter(
    (file) => typeof file === "string" && file.endsWith(".ts") && !file.includes("*") && !file.startsWith("/") && !file.includes("\\") && !file.split("/").includes("..") && (!file.includes("/") || file.startsWith("broker/"))
  ))].sort();
  if (!inputs.includes("broker/broker.ts")) {
    throw new Error("Parley package manifest must include the broker entrypoint");
  }
  const hash2 = createHash3("sha256");
  const addInput = (name, bytes) => {
    hash2.update(name).update("\0").update(String(bytes.length)).update("\0").update(bytes);
  };
  addInput("package.json", packageRaw);
  for (const input of inputs) addInput(input, readFileSync4(join5(extensionDir, input)));
  return Object.freeze({ packageVersion: version, sourceId: hash2.digest("hex") });
}

// broker/collaboration-state.ts
import { createHash as createHash4, randomUUID as randomUUID4 } from "node:crypto";
import {
  chmodSync as chmodSync2,
  closeSync as closeSync3,
  copyFileSync as copyFileSync2,
  existsSync as existsSync2,
  fsyncSync as fsyncSync2,
  mkdirSync as mkdirSync4,
  openSync as openSync3,
  readFileSync as readFileSync5,
  readdirSync,
  renameSync as renameSync2,
  rmSync as rmSync2,
  statSync,
  utimesSync,
  writeFileSync as writeFileSync3
} from "node:fs";
import { dirname as dirname3, join as join6 } from "node:path";
var STATE_FORMAT_VERSION = 1;
var HASH_PATTERN = /^[a-f0-9]{64}$/;
var SCOPE_FILE_PATTERN = /^([a-f0-9]{64})\.json(?:\.bak)?$/;
var DEFAULT_MAX_IDENTITIES = 2048;
var DEFAULT_MAX_CONTACT_EDGES = 2048;
var DEFAULT_MAX_SERIALIZED_BYTES = 256 * 1024;
var DEFAULT_CONTACT_FLUSH_DELAY_MS = 250;
var MAX_RECENT_COMPACTION_EVENTS = 64;
var MAX_LOADED_SCOPE_STATES = 512;
var MAX_HISTORICAL_SCOPE_STATES = 512;
function isRecord3(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function isNonNegativeSafeInteger(value) {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}
function hash(value) {
  return createHash4("sha256").update(value).digest("hex");
}
function payloadHash2(payloadJson) {
  return hash(payloadJson);
}
function cloneState(state) {
  return {
    identities: new Map(Array.from(state.identities, ([key, value]) => [key, {
      ...value,
      recentEvents: value.recentEvents.map((event) => ({ ...event }))
    }])),
    contacts: new Map(Array.from(state.contacts, ([key, value]) => [key, { ...value }])),
    stagedBaselines: new Map(Array.from(state.stagedBaselines, ([key, value]) => [key, { ...value }])),
    acceptedBaselineTokens: new Map(Array.from(state.acceptedBaselineTokens, ([key, value]) => [key, { ...value }]))
  };
}
function payloadFromState(state) {
  return {
    identities: Object.fromEntries(state.identities),
    contacts: Object.fromEntries(state.contacts),
    ...state.stagedBaselines.size > 0 ? { stagedBaselines: Object.fromEntries(state.stagedBaselines) } : {},
    ...state.acceptedBaselineTokens.size > 0 ? { acceptedBaselineTokens: Object.fromEntries(state.acceptedBaselineTokens) } : {}
  };
}
function serializeEnvelope(state, revision, updatedAt) {
  const payload = payloadFromState(state);
  const payloadJson = JSON.stringify(payload);
  const envelope = {
    formatVersion: STATE_FORMAT_VERSION,
    revision,
    updatedAt,
    payloadSha256: payloadHash2(payloadJson),
    payload
  };
  return JSON.stringify(envelope);
}
function oldestKeys(records) {
  return Array.from(records).sort(([leftKey, left], [rightKey, right]) => left.touchedAt - right.touchedAt || leftKey.localeCompare(rightKey)).map(([key]) => key);
}
function removeIdentity(state, identityKey) {
  state.identities.delete(identityKey);
  for (const [edgeKey, edge] of state.contacts) {
    if (edge.observerKey === identityKey || edge.peerKey === identityKey) {
      state.contacts.delete(edgeKey);
    }
  }
  for (const [tokenHash, staged] of state.stagedBaselines) {
    if (staged.observerKey === identityKey || staged.peerKey === identityKey) {
      state.stagedBaselines.delete(tokenHash);
    }
  }
  for (const [tokenHash, accepted] of state.acceptedBaselineTokens) {
    if (accepted.observerKey === identityKey) state.acceptedBaselineTokens.delete(tokenHash);
  }
}
var CollaborationStateStore = class {
  stateDir;
  contactFlushDelayMs;
  maxIdentities;
  maxContactEdges;
  maxSerializedBytes;
  scopes = /* @__PURE__ */ new Map();
  closed = false;
  constructor(runtimeDir, options = {}) {
    this.contactFlushDelayMs = this.validateLimit(options.contactFlushDelayMs ?? DEFAULT_CONTACT_FLUSH_DELAY_MS, "contactFlushDelayMs", true);
    this.maxIdentities = this.validateLimit(options.maxIdentities ?? DEFAULT_MAX_IDENTITIES, "maxIdentities");
    this.maxContactEdges = this.validateLimit(options.maxContactEdges ?? DEFAULT_MAX_CONTACT_EDGES, "maxContactEdges");
    this.maxSerializedBytes = this.validateLimit(options.maxSerializedBytes ?? DEFAULT_MAX_SERIALIZED_BYTES, "maxSerializedBytes");
    if (this.maxSerializedBytes < 512) {
      throw new RangeError("maxSerializedBytes must be at least 512");
    }
    this.stateDir = join6(runtimeDir, "collaboration-state");
    mkdirSync4(this.stateDir, { recursive: true, mode: 448 });
    if (process.platform !== "win32") chmodSync2(this.stateDir, 448);
    this.pruneHistoricalScopeFiles(/* @__PURE__ */ new Set());
  }
  /**
   * Synchronously and durably commits a successful compaction before returning.
   * Retrying the same opaque event ID for the same stable identity returns the
   * original result. Throws without advancing the in-memory generation if persistence fails.
   */
  recordSuccessfulCompaction(scopeId, sessionId, eventId, compactedAt = Date.now()) {
    this.assertOpen();
    this.validateIdentity(scopeId, sessionId);
    if (typeof eventId !== "string" || eventId.length === 0) {
      throw new TypeError("eventId must be a non-empty opaque event ID");
    }
    if (!isNonNegativeSafeInteger(compactedAt)) throw new RangeError("compactedAt must be a non-negative safe integer");
    const scope = this.getScope(scopeId);
    const identityKey = this.identityKey(scopeId, sessionId);
    const eventHash = this.compactionEventHash(scopeId, sessionId, eventId);
    const current = scope.state.identities.get(identityKey);
    const recordedEvent = current?.recentEvents.find((event) => event.eventHash === eventHash);
    if (recordedEvent) {
      return { generation: recordedEvent.generation, compactedAt: recordedEvent.compactedAt };
    }
    const generation = (current?.generation ?? 0) + 1;
    if (!Number.isSafeInteger(generation)) throw new RangeError("Compaction generation exhausted");
    const candidate = cloneState(scope.state);
    const recentEvents = [
      ...current?.recentEvents ?? [],
      { eventHash, generation, compactedAt }
    ].slice(-MAX_RECENT_COMPACTION_EVENTS);
    candidate.identities.set(identityKey, {
      generation,
      compactedAt,
      touchedAt: this.nextTouchedAt(scope),
      recentEvents
    });
    this.persist(scope, candidate);
    return { generation, compactedAt };
  }
  /**
   * Reads a peer generation and the observer→peer watermark without changing
   * identity/edge recency or scheduling persistence. A missing watermark is
   * first-ever contact.
   */
  readContact(scopeId, observerSessionId, peerSessionId) {
    this.validateIdentity(scopeId, observerSessionId);
    this.validateIdentity(scopeId, peerSessionId);
    const scope = this.getScope(scopeId);
    const observerKey = this.identityKey(scopeId, observerSessionId);
    const peerKey = this.identityKey(scopeId, peerSessionId);
    const peer = scope.state.identities.get(peerKey);
    const contact = scope.state.contacts.get(this.edgeKey(observerKey, peerKey));
    const peerGeneration = peer?.generation ?? 0;
    const lastContactGeneration = contact?.generation;
    return {
      peerGeneration,
      ...peer ? { peerCompactedAt: peer.compactedAt } : {},
      ...lastContactGeneration === void 0 ? {} : { lastContactGeneration },
      compactedSinceLastContact: lastContactGeneration !== void 0 && peerGeneration > lastContactGeneration
    };
  }
  /**
   * Records a directional observer→peer watermark after contact was accepted.
   * Pass the generation from the pre-delivery `readContact` snapshot. Failed
   * deliveries and broadcasts should not call this method. First-contact
   * baselines use `durable=true` so a crash cannot suppress the first later notice.
   */
  recordAcceptedContact(scopeId, observerSessionId, peerSessionId, observedPeerGeneration, durable = false) {
    this.assertOpen();
    this.validateIdentity(scopeId, observerSessionId);
    this.validateIdentity(scopeId, peerSessionId);
    if (!isNonNegativeSafeInteger(observedPeerGeneration)) {
      throw new RangeError("observedPeerGeneration must be a non-negative safe integer");
    }
    const scope = this.getScope(scopeId);
    const observerKey = this.identityKey(scopeId, observerSessionId);
    const peerKey = this.identityKey(scopeId, peerSessionId);
    const edgeKey = this.edgeKey(observerKey, peerKey);
    const previous = scope.state.contacts.get(edgeKey);
    const candidate = cloneState(scope.state);
    candidate.contacts.set(edgeKey, {
      observerKey,
      peerKey,
      generation: Math.max(previous?.generation ?? 0, observedPeerGeneration),
      touchedAt: this.nextTouchedAt(scope)
    });
    const nextState = this.prune(candidate, scope.revision + 1, Date.now());
    if (durable) {
      this.persist(scope, nextState);
    } else {
      scope.state = nextState;
      scope.contactsPending = true;
      this.scheduleContactFlush(scope);
    }
  }
  /**
   * Durably stages a receiver's first-contact baseline before delivery. The
   * stage is ignored by readContact until the capable receiver acknowledges its
   * opaque token, and survives a broker restart in the meantime.
   */
  stageFirstContactBaseline(scopeId, observerSessionId, peerSessionId, observedPeerGeneration, token) {
    this.assertOpen();
    this.validateIdentity(scopeId, observerSessionId);
    this.validateIdentity(scopeId, peerSessionId);
    if (!isNonNegativeSafeInteger(observedPeerGeneration)) {
      throw new RangeError("observedPeerGeneration must be a non-negative safe integer");
    }
    if (!token) throw new TypeError("baseline token must be non-empty");
    const scope = this.getScope(scopeId);
    const observerKey = this.identityKey(scopeId, observerSessionId);
    const peerKey = this.identityKey(scopeId, peerSessionId);
    const tokenHash = this.baselineTokenHash(scopeId, token);
    const candidate = cloneState(scope.state);
    candidate.stagedBaselines.set(tokenHash, {
      tokenHash,
      observerKey,
      peerKey,
      generation: observedPeerGeneration,
      touchedAt: this.nextTouchedAt(scope)
    });
    this.persist(scope, candidate);
    if (!scope.state.stagedBaselines.has(tokenHash)) {
      throw new Error("Collaboration state bounds could not retain staged baseline");
    }
  }
  /** Promotes a previously staged first-contact token to a durable watermark. */
  acceptStagedFirstContactBaseline(scopeId, observerSessionId, token) {
    this.assertOpen();
    this.validateIdentity(scopeId, observerSessionId);
    if (!token) return false;
    const scope = this.getScope(scopeId);
    const tokenHash = this.baselineTokenHash(scopeId, token);
    const staged = scope.state.stagedBaselines.get(tokenHash);
    const observerKey = this.identityKey(scopeId, observerSessionId);
    const alreadyAccepted = scope.state.acceptedBaselineTokens.get(tokenHash);
    if (alreadyAccepted) return alreadyAccepted.observerKey === observerKey;
    if (!staged || staged.observerKey !== observerKey) return false;
    const edgeKey = this.edgeKey(staged.observerKey, staged.peerKey);
    const previous = scope.state.contacts.get(edgeKey);
    const candidate = cloneState(scope.state);
    candidate.stagedBaselines.delete(tokenHash);
    candidate.acceptedBaselineTokens.set(tokenHash, {
      observerKey: staged.observerKey,
      touchedAt: this.nextTouchedAt(scope)
    });
    candidate.contacts.set(edgeKey, {
      observerKey: staged.observerKey,
      peerKey: staged.peerKey,
      generation: Math.max(previous?.generation ?? 0, staged.generation),
      touchedAt: this.nextTouchedAt(scope)
    });
    this.persist(scope, candidate);
    return true;
  }
  /** Synchronously persists any debounced accepted-contact updates in all loaded scopes. */
  flush() {
    this.assertOpen();
    for (const scope of this.scopes.values()) {
      if (scope.contactsPending) this.flushScope(scope);
    }
  }
  /** Flushes pending contacts in all loaded scopes and prevents further mutations. Idempotent. */
  close() {
    if (this.closed) return;
    for (const scope of this.scopes.values()) this.clearContactFlushTimer(scope);
    for (const scope of this.scopes.values()) {
      if (scope.contactsPending) this.persist(scope, scope.state);
    }
    this.closed = true;
    this.pruneHistoricalScopeFiles(/* @__PURE__ */ new Set());
  }
  validateLimit(value, name, allowZero = false) {
    if (!Number.isSafeInteger(value) || value < (allowZero ? 0 : 1)) {
      throw new RangeError(`${name} must be ${allowZero ? "a non-negative" : "a positive"} safe integer`);
    }
    return value;
  }
  validateIdentity(scopeId, sessionId) {
    if (scopeId !== void 0 && (typeof scopeId !== "string" || scopeId.length === 0)) {
      throw new TypeError("scopeId must be undefined or a non-empty string");
    }
    if (typeof sessionId !== "string" || sessionId.length === 0) {
      throw new TypeError("sessionId must be a non-empty stable session ID");
    }
  }
  assertOpen() {
    if (this.closed) throw new Error("CollaborationStateStore is closed");
  }
  scopeKey(scopeId) {
    return hash(JSON.stringify(["scope", scopeId ?? null]));
  }
  identityKey(scopeId, sessionId) {
    return hash(JSON.stringify(["scope", scopeId ?? null, "session", sessionId]));
  }
  compactionEventHash(scopeId, sessionId, eventId) {
    return hash(JSON.stringify(["scope", scopeId ?? null, "session", sessionId, "compaction-event", eventId]));
  }
  edgeKey(observerKey, peerKey) {
    return hash(JSON.stringify(["observer", observerKey, "peer", peerKey]));
  }
  baselineTokenHash(scopeId, token) {
    return hash(JSON.stringify(["scope", scopeId ?? null, "baseline-token", token]));
  }
  getScope(scopeId) {
    const scopeKey = this.scopeKey(scopeId);
    const existing = this.scopes.get(scopeKey);
    if (existing) {
      this.scopes.delete(scopeKey);
      this.scopes.set(scopeKey, existing);
      return existing;
    }
    while (this.scopes.size >= MAX_LOADED_SCOPE_STATES) {
      const oldestKey = this.scopes.keys().next().value;
      if (oldestKey === void 0) break;
      const oldest = this.scopes.get(oldestKey);
      this.clearContactFlushTimer(oldest);
      if (oldest.contactsPending) this.flushScope(oldest);
      this.scopes.delete(oldestKey);
    }
    const statePath = join6(this.stateDir, `${scopeKey}.json`);
    const backupPath = `${statePath}.bak`;
    const primary = this.readStateFile(statePath);
    const backup = this.readStateFile(backupPath);
    const loaded = primary ?? backup;
    if (primary && backup?.revision !== primary.revision) {
      this.mirrorStateFile(statePath, backupPath);
    } else if (!primary && backup) {
      this.mirrorStateFile(backupPath, statePath);
    }
    const revision = loaded?.revision ?? 0;
    const state = this.prune(loaded?.state ?? { identities: /* @__PURE__ */ new Map(), contacts: /* @__PURE__ */ new Map(), stagedBaselines: /* @__PURE__ */ new Map(), acceptedBaselineTokens: /* @__PURE__ */ new Map() }, revision, Date.now());
    let lastTouchedAt = 0;
    for (const record of state.identities.values()) lastTouchedAt = Math.max(lastTouchedAt, record.touchedAt);
    for (const record of state.contacts.values()) lastTouchedAt = Math.max(lastTouchedAt, record.touchedAt);
    for (const record of state.stagedBaselines.values()) lastTouchedAt = Math.max(lastTouchedAt, record.touchedAt);
    for (const record of state.acceptedBaselineTokens.values()) lastTouchedAt = Math.max(lastTouchedAt, record.touchedAt);
    const scope = {
      scopeKey,
      statePath,
      backupPath,
      revision,
      state,
      lastTouchedAt,
      contactFlushTimer: void 0,
      contactsPending: false,
      hasCommittedPrimary: loaded !== null
    };
    this.scopes.set(scopeKey, scope);
    this.touchScopeFiles(scope);
    return scope;
  }
  nextTouchedAt(scope) {
    const now = Date.now();
    scope.lastTouchedAt = Math.max(now, scope.lastTouchedAt + 1);
    if (!Number.isSafeInteger(scope.lastTouchedAt)) scope.lastTouchedAt = now;
    return scope.lastTouchedAt;
  }
  scheduleContactFlush(scope) {
    if (scope.contactFlushTimer || this.contactFlushDelayMs === 0) {
      if (this.contactFlushDelayMs === 0) this.flushScope(scope);
      return;
    }
    scope.contactFlushTimer = setTimeout(() => {
      scope.contactFlushTimer = void 0;
      try {
        this.flushScope(scope);
      } catch {
      }
    }, this.contactFlushDelayMs);
    scope.contactFlushTimer.unref?.();
  }
  clearContactFlushTimer(scope) {
    if (!scope.contactFlushTimer) return;
    clearTimeout(scope.contactFlushTimer);
    scope.contactFlushTimer = void 0;
  }
  flushScope(scope) {
    if (!scope.contactsPending) return;
    this.clearContactFlushTimer(scope);
    this.persist(scope, scope.state);
  }
  prune(source, revision, updatedAt) {
    const state = cloneState(source);
    const oldestAcceptedTokens = oldestKeys(state.acceptedBaselineTokens);
    while (state.acceptedBaselineTokens.size > this.maxContactEdges) {
      state.acceptedBaselineTokens.delete(oldestAcceptedTokens.shift());
    }
    const oldestStaged = oldestKeys(state.stagedBaselines);
    while (state.stagedBaselines.size > this.maxContactEdges) {
      state.stagedBaselines.delete(oldestStaged.shift());
    }
    const oldestContacts = oldestKeys(state.contacts);
    while (state.contacts.size > this.maxContactEdges) {
      state.contacts.delete(oldestContacts.shift());
    }
    const oldestIdentities = oldestKeys(state.identities);
    while (state.identities.size > this.maxIdentities) {
      removeIdentity(state, oldestIdentities.shift());
    }
    let serialized = serializeEnvelope(state, revision, updatedAt);
    for (const key of oldestKeys(state.acceptedBaselineTokens)) {
      if (Buffer.byteLength(serialized, "utf8") <= this.maxSerializedBytes) break;
      state.acceptedBaselineTokens.delete(key);
      serialized = serializeEnvelope(state, revision, updatedAt);
    }
    for (const key of oldestKeys(state.stagedBaselines)) {
      if (Buffer.byteLength(serialized, "utf8") <= this.maxSerializedBytes) break;
      state.stagedBaselines.delete(key);
      serialized = serializeEnvelope(state, revision, updatedAt);
    }
    for (const key of oldestKeys(state.contacts)) {
      if (Buffer.byteLength(serialized, "utf8") <= this.maxSerializedBytes) break;
      state.contacts.delete(key);
      serialized = serializeEnvelope(state, revision, updatedAt);
    }
    for (const key of oldestKeys(state.identities)) {
      const identity = state.identities.get(key);
      while (identity.recentEvents.length > 1 && Buffer.byteLength(serialized, "utf8") > this.maxSerializedBytes) {
        identity.recentEvents.shift();
        serialized = serializeEnvelope(state, revision, updatedAt);
      }
    }
    for (const key of oldestKeys(state.identities)) {
      if (Buffer.byteLength(serialized, "utf8") <= this.maxSerializedBytes) break;
      removeIdentity(state, key);
      serialized = serializeEnvelope(state, revision, updatedAt);
    }
    if (Buffer.byteLength(serialized, "utf8") > this.maxSerializedBytes) {
      throw new Error("Collaboration state cannot fit within maxSerializedBytes");
    }
    return state;
  }
  persist(scope, source) {
    const createsScopeFile = !scope.hasCommittedPrimary;
    if (!scope.hasCommittedPrimary) {
      const initialUpdatedAt = Date.now();
      const initial = serializeEnvelope(scope.state, scope.revision, initialUpdatedAt);
      this.writeStateFileAtomically(scope.backupPath, initial);
      this.writeStateFileAtomically(scope.statePath, initial);
      scope.hasCommittedPrimary = true;
    }
    const updatedAt = Date.now();
    const nextRevision = scope.revision + 1;
    if (!Number.isSafeInteger(nextRevision)) throw new RangeError("Collaboration state revision exhausted");
    const candidate = this.prune(source, nextRevision, updatedAt);
    const serialized = serializeEnvelope(candidate, nextRevision, updatedAt);
    this.writeStateFileAtomically(scope.backupPath, serialized);
    this.writeStateFileAtomically(scope.statePath, serialized);
    scope.state = candidate;
    scope.revision = nextRevision;
    scope.contactsPending = false;
    this.clearContactFlushTimer(scope);
    if (createsScopeFile) this.pruneHistoricalScopeFiles(new Set(this.scopes.keys()));
  }
  writeStateFileAtomically(destinationPath, serialized) {
    const tempPath = `${destinationPath}.tmp.${process.pid}.${randomUUID4()}`;
    try {
      writeFileSync3(tempPath, serialized, { mode: 384 });
      if (process.platform !== "win32") chmodSync2(tempPath, 384);
      this.fsyncFile(tempPath);
      renameSync2(tempPath, destinationPath);
      this.fsyncDirectory(dirname3(destinationPath));
    } catch (error) {
      throw new Error("Failed to persist collaboration state", { cause: error });
    } finally {
      rmSync2(tempPath, { force: true });
    }
  }
  mirrorStateFile(sourcePath, destinationPath) {
    const tempPath = `${destinationPath}.tmp.${process.pid}.${randomUUID4()}`;
    try {
      copyFileSync2(sourcePath, tempPath);
      if (process.platform !== "win32") chmodSync2(tempPath, 384);
      this.fsyncFile(tempPath);
      renameSync2(tempPath, destinationPath);
      this.fsyncDirectory(dirname3(destinationPath));
    } catch (error) {
      throw new Error("Failed to mirror collaboration state", { cause: error });
    } finally {
      rmSync2(tempPath, { force: true });
    }
  }
  touchScopeFiles(scope) {
    const now = /* @__PURE__ */ new Date();
    for (const filePath of [scope.statePath, scope.backupPath]) {
      try {
        if (existsSync2(filePath)) utimesSync(filePath, now, now);
      } catch {
      }
    }
  }
  pruneHistoricalScopeFiles(protectedScopeKeys) {
    try {
      const lastUsedByScope = /* @__PURE__ */ new Map();
      for (const entry of readdirSync(this.stateDir, { withFileTypes: true })) {
        if (!entry.isFile()) continue;
        const match = SCOPE_FILE_PATTERN.exec(entry.name);
        if (!match) continue;
        const scopeKey = match[1];
        let modifiedAt;
        try {
          modifiedAt = statSync(join6(this.stateDir, entry.name)).mtimeMs;
        } catch {
          continue;
        }
        lastUsedByScope.set(scopeKey, Math.max(lastUsedByScope.get(scopeKey) ?? 0, modifiedAt));
      }
      const removable = Array.from(lastUsedByScope).filter(([scopeKey]) => !protectedScopeKeys.has(scopeKey)).sort(([leftKey, leftTime], [rightKey, rightTime]) => leftTime - rightTime || leftKey.localeCompare(rightKey));
      let excess = lastUsedByScope.size - MAX_HISTORICAL_SCOPE_STATES;
      for (const [scopeKey] of removable) {
        if (excess <= 0) break;
        rmSync2(join6(this.stateDir, `${scopeKey}.json`), { force: true });
        rmSync2(join6(this.stateDir, `${scopeKey}.json.bak`), { force: true });
        excess -= 1;
      }
    } catch {
    }
  }
  fsyncFile(filePath) {
    const file = openSync3(filePath, "r+");
    try {
      fsyncSync2(file);
    } finally {
      closeSync3(file);
    }
  }
  fsyncDirectory(directoryPath) {
    try {
      const directory = openSync3(directoryPath, "r");
      try {
        fsyncSync2(directory);
      } finally {
        closeSync3(directory);
      }
    } catch {
    }
  }
  readStateFile(filePath) {
    if (!existsSync2(filePath)) return null;
    try {
      if (statSync(filePath).size > this.maxSerializedBytes) return null;
      const value = JSON.parse(readFileSync5(filePath, "utf8"));
      if (!isRecord3(value) || value.formatVersion !== STATE_FORMAT_VERSION) return null;
      if (!isNonNegativeSafeInteger(value.revision) || !isNonNegativeSafeInteger(value.updatedAt)) return null;
      if (typeof value.payloadSha256 !== "string" || !isRecord3(value.payload)) return null;
      const payloadJson = JSON.stringify(value.payload);
      if (payloadHash2(payloadJson) !== value.payloadSha256) return null;
      const rawIdentities = value.payload.identities;
      const rawContacts = value.payload.contacts;
      const rawStagedBaselines = value.payload.stagedBaselines ?? {};
      const rawAcceptedBaselineTokens = value.payload.acceptedBaselineTokens ?? {};
      if (!isRecord3(rawIdentities) || !isRecord3(rawContacts) || !isRecord3(rawStagedBaselines) || !isRecord3(rawAcceptedBaselineTokens)) return null;
      const identities = /* @__PURE__ */ new Map();
      const contacts = /* @__PURE__ */ new Map();
      const stagedBaselines = /* @__PURE__ */ new Map();
      const acceptedBaselineTokens = /* @__PURE__ */ new Map();
      for (const [key, raw] of Object.entries(rawIdentities)) {
        if (!HASH_PATTERN.test(key) || !isRecord3(raw)) continue;
        if (!isNonNegativeSafeInteger(raw.generation) || raw.generation < 1 || !isNonNegativeSafeInteger(raw.compactedAt) || !isNonNegativeSafeInteger(raw.touchedAt) || raw.recentEvents !== void 0 && !Array.isArray(raw.recentEvents)) continue;
        const recentEvents = [];
        for (const rawEvent of raw.recentEvents ?? []) {
          if (!isRecord3(rawEvent) || typeof rawEvent.eventHash !== "string" || !HASH_PATTERN.test(rawEvent.eventHash) || !isNonNegativeSafeInteger(rawEvent.generation) || rawEvent.generation < 1 || rawEvent.generation > raw.generation || !isNonNegativeSafeInteger(rawEvent.compactedAt)) continue;
          const duplicateIndex = recentEvents.findIndex((event) => event.eventHash === rawEvent.eventHash);
          if (duplicateIndex >= 0) recentEvents.splice(duplicateIndex, 1);
          recentEvents.push({
            eventHash: rawEvent.eventHash,
            generation: rawEvent.generation,
            compactedAt: rawEvent.compactedAt
          });
        }
        identities.set(key, {
          generation: raw.generation,
          compactedAt: raw.compactedAt,
          touchedAt: raw.touchedAt,
          recentEvents: recentEvents.slice(-MAX_RECENT_COMPACTION_EVENTS)
        });
      }
      for (const [key, raw] of Object.entries(rawContacts)) {
        if (!HASH_PATTERN.test(key) || !isRecord3(raw)) continue;
        if (typeof raw.observerKey !== "string" || !HASH_PATTERN.test(raw.observerKey) || typeof raw.peerKey !== "string" || !HASH_PATTERN.test(raw.peerKey) || !isNonNegativeSafeInteger(raw.generation) || !isNonNegativeSafeInteger(raw.touchedAt) || this.edgeKey(raw.observerKey, raw.peerKey) !== key) continue;
        contacts.set(key, {
          observerKey: raw.observerKey,
          peerKey: raw.peerKey,
          generation: raw.generation,
          touchedAt: raw.touchedAt
        });
      }
      for (const [key, raw] of Object.entries(rawStagedBaselines)) {
        if (!HASH_PATTERN.test(key) || !isRecord3(raw)) continue;
        if (typeof raw.tokenHash !== "string" || raw.tokenHash !== key || typeof raw.observerKey !== "string" || !HASH_PATTERN.test(raw.observerKey) || typeof raw.peerKey !== "string" || !HASH_PATTERN.test(raw.peerKey) || !isNonNegativeSafeInteger(raw.generation) || !isNonNegativeSafeInteger(raw.touchedAt)) continue;
        stagedBaselines.set(key, {
          tokenHash: key,
          observerKey: raw.observerKey,
          peerKey: raw.peerKey,
          generation: raw.generation,
          touchedAt: raw.touchedAt
        });
      }
      for (const [key, raw] of Object.entries(rawAcceptedBaselineTokens)) {
        if (!HASH_PATTERN.test(key) || !isRecord3(raw)) continue;
        if (typeof raw.observerKey !== "string" || !HASH_PATTERN.test(raw.observerKey) || !isNonNegativeSafeInteger(raw.touchedAt)) continue;
        acceptedBaselineTokens.set(key, {
          observerKey: raw.observerKey,
          touchedAt: raw.touchedAt
        });
      }
      return {
        revision: value.revision,
        state: { identities, contacts, stagedBaselines, acceptedBaselineTokens }
      };
    } catch {
      return null;
    }
  }
};

// broker/peer-link.ts
import net2 from "node:net";
import { randomUUID as randomUUID5 } from "node:crypto";
var PEER_HANDSHAKE_TIMEOUT_MS = 5e3;
var MAX_PEER_LINKS = 16;
var SIMULTANEOUS_ATTACH_MS = 1e4;
var LIVENESS_PROBE_MS = 3e3;
function isSettled(link) {
  return Date.now() - link.connectedAt >= SIMULTANEOUS_ATTACH_MS;
}
function sharesLocalScope(left, right) {
  return left.some((binding) => right.some((other) => other.localScopeId === binding.localScopeId));
}
var PEER_FRAME_RATE_CAPACITY = 240;
var PEER_FRAME_REFILL_PER_SECOND = 120;
var FederationPeerError = class extends Error {
  constructor(code, message, options) {
    super(message, options);
    this.code = code;
    this.name = "FederationPeerError";
  }
  code;
};
function isValidNegotiatedFeatures(negotiated, offered) {
  return FEDERATION_REQUIRED_FEATURES.every((feature) => negotiated.includes(feature)) && negotiated.every((feature) => offered.includes(feature));
}
function sameMappings(left, right) {
  return left.length === right.length && left.every((mapping, index) => {
    const other = right[index];
    return other?.localScopeAlias === mapping.localScopeAlias && other.remoteScopeAlias === mapping.remoteScopeAlias;
  });
}
function rejection(hello, code, error) {
  return {
    type: "peer_hello_ack",
    protocol: FEDERATION_PROTOCOL_NAME,
    version: FEDERATION_PROTOCOL_VERSION,
    linkId: hello.linkId,
    accepted: false,
    code,
    error
  };
}
var PeerLinkManager = class {
  constructor(options = {}) {
    this.options = options;
  }
  options;
  linksById = /* @__PURE__ */ new Map();
  linkIdsByRemoteOrigin = /* @__PURE__ */ new Map();
  lastHeardAt = /* @__PURE__ */ new Map();
  probing = /* @__PURE__ */ new Map();
  activatedLinkIds = /* @__PURE__ */ new Set();
  outboundFrameLimits = /* @__PURE__ */ new Map();
  localOrigin;
  get size() {
    return this.linksById.size;
  }
  listLinks() {
    return [...this.linksById.values()];
  }
  prepareInbound(value) {
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
      scopeBindings: value.scopeBindings
    };
  }
  acceptInbound(socket, value, prepared) {
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
    const negotiatedFeatures = FEDERATION_SUPPORTED_FEATURES.filter((feature) => hello.features.includes(feature));
    const link = {
      linkId: hello.linkId,
      direction: "inbound",
      socket,
      localOrigin: prepared.localOrigin,
      remoteOrigin: prepared.remoteOrigin,
      scopeBindings: prepared.scopeBindings,
      features: negotiatedFeatures,
      connectedAt: Date.now()
    };
    try {
      this.registerLink(link);
    } catch (error) {
      const failure = error instanceof FederationPeerError ? error : new FederationPeerError("E_HANDSHAKE_FAILED", "Failed to register peer link", { cause: error });
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
        features: negotiatedFeatures
      }
    };
  }
  prepareOutbound(value) {
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
      linkId: value.linkId,
      localOrigin: this.localOrigin ?? value.localOrigin,
      remoteOrigin: value.remoteOrigin,
      scopeBindings: value.scopeBindings
    };
  }
  outboundHello(prepared) {
    return {
      type: "peer_hello",
      protocol: FEDERATION_PROTOCOL_NAME,
      version: FEDERATION_PROTOCOL_VERSION,
      linkId: prepared.linkId,
      origin: prepared.localOrigin,
      expectedPeerOrigin: prepared.remoteOrigin,
      scopeMappings: scopeMappingsFromBindings(prepared.scopeBindings),
      features: [...FEDERATION_SUPPORTED_FEATURES]
    };
  }
  /** Validates the real peer ack and registers, but does not activate. The caller
   * must first queue any trusted-local readiness result on a supplied stream. */
  acceptOutbound(socket, value, prepared) {
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
    if (!isValidNegotiatedFeatures(value.features, FEDERATION_SUPPORTED_FEATURES)) {
      throw new FederationPeerError("E_FEATURE_UNSUPPORTED", "Peer acknowledged invalid or unoffered features");
    }
    const link = {
      ...prepared,
      direction: "outbound",
      socket,
      features: value.features,
      connectedAt: Date.now()
    };
    this.registerLink(link);
    return link;
  }
  async dial(request, signal) {
    if (!isBrokerDialPeerRequest(request)) {
      throw new FederationPeerError("E_INVALID_REQUEST", "Invalid broker dial peer request");
    }
    if (signal?.aborted) throw new FederationPeerError("E_DIAL_FAILED", "Peer dial control was abandoned");
    const endpoint = request.endpoint;
    const requestedLocalOrigin = request.localOrigin;
    const remoteOrigin = request.remoteOrigin;
    const scopeBindings = request.scopeBindings;
    let capability = request.capability;
    request = { ...request, capability: "" };
    const prepared = this.prepareOutbound({
      type: "broker_start_peer",
      requestId: request.requestId,
      linkId: randomUUID5(),
      localOrigin: requestedLocalOrigin,
      remoteOrigin,
      scopeBindings
    });
    const { linkId } = prepared;
    const socket = net2.connect({ host: endpoint.host, port: endpoint.port });
    this.options.onSocketOpened?.(socket);
    return await new Promise((resolve3, reject) => {
      let settled = false;
      let active = false;
      const finishFailure = (error) => {
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
          const link2 = this.linksById.get(linkId);
          if (!link2) {
            socket.destroy(new Error("Federation peer link is no longer active"));
            return;
          }
          if (!this.options.onPeerMessage) {
            socket.destroy(new Error("Federation peer frames are not supported by this broker"));
            return;
          }
          if (link2.direction === "outbound" && !this.consumeOutboundFrameToken(link2.linkId)) {
            socket.destroy(new Error("Federation peer frame rate limit exceeded"));
            return;
          }
          try {
            if (this.heard(link2, value)) return;
            this.options.onPeerMessage(link2, value);
          } catch (error) {
            socket.destroy(error instanceof Error ? error : new Error(String(error)));
          }
          return;
        }
        let link;
        try {
          link = this.acceptOutbound(socket, value, prepared);
        } catch (error) {
          finishFailure(error instanceof FederationPeerError ? error : new FederationPeerError("E_HANDSHAKE_FAILED", "Failed to register peer link", { cause: error }));
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
        resolve3(link);
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
            capability
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
  getLink(linkId) {
    return this.linksById.get(linkId);
  }
  activateLink(linkId) {
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
  handlePostHandshakeMessage(linkId, value) {
    const link = this.linksById.get(linkId);
    if (!link) throw new FederationPeerError("E_INVALID_REQUEST", "Unknown peer link");
    if (this.heard(link, value)) return;
    if (!this.options.onPeerMessage) {
      throw new FederationPeerError("E_FEATURE_UNSUPPORTED", "Federation peer frames are not supported by this broker");
    }
    this.options.onPeerMessage(link, value);
  }
  closeLink(linkId) {
    const link = this.linksById.get(linkId);
    if (!link) return false;
    this.unregisterLink(link);
    link.socket.destroy();
    this.options.onLinkDown?.(link);
    return true;
  }
  removeInbound(linkId, socket) {
    const link = this.linksById.get(linkId);
    if (!link || link.socket !== socket) return void 0;
    this.unregisterLink(link);
    this.options.onLinkDown?.(link);
    return link;
  }
  close() {
    const links = [...this.linksById.values()];
    for (const link of links) {
      this.unregisterLink(link);
      link.socket.end();
      link.socket.destroy();
      this.options.onLinkDown?.(link);
    }
  }
  linksToOrigin(remoteOriginId) {
    return [...this.linkIdsByRemoteOrigin.get(remoteOriginId) ?? []].map((linkId) => this.linksById.get(linkId)).filter((link) => link !== void 0);
  }
  /** Why a new link may not join, or undefined when it may. Conflicting links
   * it would win (a simultaneous attach in its preferred direction) are left
   * for registerLink to replace. peerFeatures is what the other broker offered,
   * when known: one that holds a single link per broker can't take a second. */
  admission(direction, candidate, peerFeatures) {
    const existing = this.linksToOrigin(candidate.remoteOrigin.id);
    const conflicting = existing.filter((link) => sharesLocalScope(link.scopeBindings, candidate.scopeBindings));
    for (const link of conflicting) {
      if (isSettled(link)) {
        this.probe(link);
        return "This computer is already linked with that one for these sessions; the existing link is being checked";
      }
      if (link.direction === direction || this.isPreferredDirection(link.direction, link.localOrigin.id, link.remoteOrigin.id) || !this.isPreferredDirection(direction, candidate.localOrigin.id, candidate.remoteOrigin.id)) {
        return "The existing peer link has the deterministic preferred direction";
      }
    }
    const alongside = existing.filter((link) => !conflicting.includes(link));
    if (alongside.length > 0 && peerFeatures && !peerFeatures.includes(FEDERATION_SCOPED_LINKS_FEATURE)) {
      return "That computer's Parley holds one link per computer and is already linked with this one; update Parley there";
    }
    if (conflicting.length === 0 && this.linksById.size >= MAX_PEER_LINKS) return "Peer link limit reached";
    return void 0;
  }
  /** Ask a settled link whether anyone is behind it; close it if nobody answers. */
  probe(link) {
    if (this.probing.has(link.linkId)) return;
    const askedAt = Date.now();
    if (link.features.includes(FEDERATION_PING_FEATURE)) {
      try {
        writeMessage(link.socket, { type: "peer_ping", protocol: FEDERATION_PROTOCOL_NAME, version: FEDERATION_PROTOCOL_VERSION, nonce: randomUUID5() });
      } catch {
      }
    } else {
      try {
        this.options.requestPeerResponse?.(link);
      } catch {
      }
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
  heard(link, value) {
    this.lastHeardAt.set(link.linkId, Date.now());
    if (!link.features.includes(FEDERATION_PING_FEATURE) || typeof value !== "object" || value === null) return false;
    const type = value.type;
    if (type === "peer_pong") return true;
    if (type !== "peer_ping") return false;
    const nonce = value.nonce;
    if (typeof nonce !== "string" || nonce.length > 64) return false;
    writeMessage(link.socket, { type: "peer_pong", protocol: FEDERATION_PROTOCOL_NAME, version: FEDERATION_PROTOCOL_VERSION, nonce });
    return true;
  }
  isPreferredDirection(direction, localOriginId, remoteOriginId) {
    return direction === (localOriginId < remoteOriginId ? "outbound" : "inbound");
  }
  registerLink(link) {
    if (this.linksById.has(link.linkId)) {
      throw new FederationPeerError("E_ALREADY_CONNECTED", "Peer link ID is already connected");
    }
    if (this.localOrigin && this.localOrigin.id !== link.localOrigin.id) {
      throw new FederationPeerError("E_ORIGIN_MISMATCH", "Local federation origin is already fixed for this broker");
    }
    const refusal = this.admission(link.direction, link, link.features);
    if (refusal) throw new FederationPeerError("E_ALREADY_CONNECTED", refusal);
    for (const existing of this.linksToOrigin(link.remoteOrigin.id)) {
      if (!sharesLocalScope(existing.scopeBindings, link.scopeBindings)) continue;
      this.unregisterLink(existing);
      existing.socket.destroy();
      this.options.onLinkDown?.(existing);
    }
    this.localOrigin ??= link.localOrigin;
    this.linksById.set(link.linkId, link);
    const ids = this.linkIdsByRemoteOrigin.get(link.remoteOrigin.id) ?? /* @__PURE__ */ new Set();
    ids.add(link.linkId);
    this.linkIdsByRemoteOrigin.set(link.remoteOrigin.id, ids);
    this.lastHeardAt.set(link.linkId, Date.now());
    if (link.direction === "outbound") {
      this.outboundFrameLimits.set(link.linkId, { tokens: PEER_FRAME_RATE_CAPACITY, lastRefillAt: Date.now() });
    }
  }
  removeLinkBySocket(socket) {
    for (const link of this.linksById.values()) {
      if (link.socket !== socket) continue;
      this.unregisterLink(link);
      return link;
    }
    return void 0;
  }
  consumeOutboundFrameToken(linkId, now = Date.now()) {
    const state = this.outboundFrameLimits.get(linkId);
    if (!state) return false;
    const elapsedMs = Math.max(0, now - state.lastRefillAt);
    state.tokens = Math.min(
      PEER_FRAME_RATE_CAPACITY,
      state.tokens + elapsedMs * PEER_FRAME_REFILL_PER_SECOND / 1e3
    );
    state.lastRefillAt = now;
    if (state.tokens < 1) return false;
    state.tokens -= 1;
    return true;
  }
  unregisterLink(link) {
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
};

// broker/federation-roster.ts
var MAX_SNAPSHOT_SESSIONS = 128;
var MAX_DELTA_CHANGES = 256;
var MAX_IMPORT_EPOCHS_PER_LINK = 16;
var MAX_ROSTER_FRAME_BYTES = 900 * 1024;
var MAX_VISIBLE_ROSTER_BYTES_PER_LOCAL_SCOPE = 800 * 1024;
var MAX_SESSION_PROJECTION_BYTES = 1400;
var CONTROL_OR_FORMAT_CHARACTERS2 = /[\p{Cc}\p{Cf}]/u;
var FederationRosterError = class extends Error {
  constructor(message) {
    super(message);
    this.name = "FederationRosterError";
  }
};
function isRecord4(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function hasOnlyKeys2(value, required, optional = []) {
  const allowed = /* @__PURE__ */ new Set([...required, ...optional]);
  return required.every((key) => Object.prototype.hasOwnProperty.call(value, key)) && Object.keys(value).every((key) => allowed.has(key));
}
function serializedSize(value) {
  try {
    return Buffer.byteLength(JSON.stringify(value), "utf8");
  } catch {
    return Number.POSITIVE_INFINITY;
  }
}
function isSafeBoundedText(value, maxLength, allowEmpty = false) {
  return typeof value === "string" && value.length <= maxLength && (allowEmpty || value.length > 0) && !CONTROL_OR_FORMAT_CHARACTERS2.test(value);
}
function isStableSessionId2(value) {
  return isSafeBoundedText(value, FEDERATION_SESSION_ID_MAX_LENGTH) && value.trim().length > 0;
}
function isSequence(value) {
  return Number.isSafeInteger(value) && value >= 1;
}
function isProjection(value) {
  if (!isRecord4(value) || !hasOnlyKeys2(value, ["cwd", "model", "pid", "startedAt", "lastActivity"], [
    "conversation",
    "endpointEpoch",
    "name",
    "description",
    "runtimeFallbackAlias",
    "status",
    "contextPct",
    "contextTokens",
    "contextWindow",
    "tmuxPane",
    "app"
  ])) return false;
  if (value.app !== void 0 && (typeof value.app !== "string" || value.app.trim().length === 0 || value.app.length > 64 || /[\p{Cc}\p{Cf}]/u.test(value.app))) return false;
  if (!isSafeBoundedText(value.cwd, 4096, true) || !isSafeBoundedText(value.model, 256, true)) return false;
  if (!Number.isSafeInteger(value.pid) || value.pid < 0) return false;
  if (!Number.isSafeInteger(value.startedAt) || value.startedAt < 0) return false;
  if (!Number.isSafeInteger(value.lastActivity) || value.lastActivity < 0) return false;
  if (value.conversation !== void 0 && typeof value.conversation !== "boolean") return false;
  if (value.endpointEpoch !== void 0 && !isFederationCorrelationId(value.endpointEpoch)) return false;
  if (value.status !== void 0 && !isSafeBoundedText(value.status, 256, true)) return false;
  if (value.tmuxPane !== void 0 && !isSafeBoundedText(value.tmuxPane, 128)) return false;
  if (value.runtimeFallbackAlias !== void 0 && typeof value.runtimeFallbackAlias !== "boolean") return false;
  if (value.contextPct !== void 0 && (typeof value.contextPct !== "number" || !Number.isFinite(value.contextPct) || value.contextPct < 0 || value.contextPct > 100)) return false;
  for (const field of ["contextTokens", "contextWindow"]) {
    if (value[field] !== void 0 && (!Number.isSafeInteger(value[field]) || value[field] < 0)) return false;
  }
  const candidate = {
    id: "projection",
    cwd: value.cwd,
    model: value.model,
    pid: value.pid,
    startedAt: value.startedAt,
    lastActivity: value.lastActivity,
    ...value.endpointEpoch !== void 0 ? { endpointEpoch: value.endpointEpoch } : {},
    ...value.name !== void 0 ? { name: value.name } : {},
    ...value.description !== void 0 ? { description: value.description } : {},
    ...value.runtimeFallbackAlias !== void 0 ? { runtimeFallbackAlias: value.runtimeFallbackAlias } : {},
    ...value.status !== void 0 ? { status: value.status } : {},
    // Context usage from older peers is accepted on the wire but never kept.
    ...value.tmuxPane !== void 0 ? { tmuxPane: value.tmuxPane } : {},
    ...value.app !== void 0 ? { app: value.app } : {}
  };
  return isSessionInfo(candidate) && serializedSize(value) <= MAX_SESSION_PROJECTION_BYTES;
}
function isRosterEntry(value) {
  return isRecord4(value) && hasOnlyKeys2(value, ["scopeAlias", "stableSessionId", "session"]) && isCanonicalFederationScopeAlias(value.scopeAlias) && isStableSessionId2(value.stableSessionId) && isProjection(value.session);
}
function isRosterRemoval(value) {
  return isRecord4(value) && hasOnlyKeys2(value, ["scopeAlias", "stableSessionId"]) && isCanonicalFederationScopeAlias(value.scopeAlias) && isStableSessionId2(value.stableSessionId);
}
function rosterKey(scopeAlias, stableSessionId) {
  return JSON.stringify([scopeAlias, stableSessionId]);
}
function noDuplicateEntries(entries) {
  return new Set(entries.map((entry) => rosterKey(entry.scopeAlias, entry.stableSessionId))).size === entries.length;
}
function isPeerRosterSnapshot(value) {
  return isRecord4(value) && hasOnlyKeys2(value, ["type", "protocol", "version", "originId", "originEpoch", "sequence", "sessions"]) && value.type === "peer_roster_snapshot" && value.protocol === FEDERATION_PROTOCOL_NAME && value.version === FEDERATION_PROTOCOL_VERSION && isCanonicalFederationOriginId(value.originId) && isFederationCorrelationId(value.originEpoch) && isSequence(value.sequence) && Array.isArray(value.sessions) && value.sessions.length <= MAX_SNAPSHOT_SESSIONS && value.sessions.every(isRosterEntry) && noDuplicateEntries(value.sessions) && serializedSize(value) <= MAX_ROSTER_FRAME_BYTES;
}
function isPeerRosterDelta(value) {
  if (!isRecord4(value) || !hasOnlyKeys2(value, ["type", "protocol", "version", "originId", "originEpoch", "sequence", "upserts", "removals"]) || value.type !== "peer_roster_delta" || value.protocol !== FEDERATION_PROTOCOL_NAME || value.version !== FEDERATION_PROTOCOL_VERSION || !isCanonicalFederationOriginId(value.originId) || !isFederationCorrelationId(value.originEpoch) || !isSequence(value.sequence) || !Array.isArray(value.upserts) || !Array.isArray(value.removals) || value.upserts.length + value.removals.length < 1 || value.upserts.length + value.removals.length > MAX_DELTA_CHANGES || !value.upserts.every(isRosterEntry) || !value.removals.every(isRosterRemoval) || !noDuplicateEntries(value.upserts) || serializedSize(value) > MAX_ROSTER_FRAME_BYTES) return false;
  const changedKeys = [
    ...value.upserts.map((entry) => rosterKey(entry.scopeAlias, entry.stableSessionId)),
    ...value.removals.map((entry) => rosterKey(entry.scopeAlias, entry.stableSessionId))
  ];
  return new Set(changedKeys).size === changedKeys.length;
}
function isPeerRosterResyncRequest(value) {
  return isRecord4(value) && hasOnlyKeys2(value, ["type", "protocol", "version", "originId", "expectedSequence"], ["expectedOriginEpoch"]) && value.type === "peer_roster_resync_request" && value.protocol === FEDERATION_PROTOCOL_NAME && value.version === FEDERATION_PROTOCOL_VERSION && isCanonicalFederationOriginId(value.originId) && isSequence(value.expectedSequence) && (value.expectedOriginEpoch === void 0 || isFederationCorrelationId(value.expectedOriginEpoch)) && serializedSize(value) <= MAX_ROSTER_FRAME_BYTES;
}
function isFederationRosterFrame(value) {
  return isPeerRosterSnapshot(value) || isPeerRosterDelta(value) || isPeerRosterResyncRequest(value);
}
function projectionFromSession(info, conversation, shareApp = false) {
  const safeInteger = (value) => Number.isSafeInteger(value) && value >= 0 ? value : 0;
  const cwd = isSafeBoundedText(info.cwd, 4096, true) ? info.cwd : "";
  const model = isSafeBoundedText(info.model, 256, true) ? info.model : "unknown";
  const projection = {
    ...conversation !== void 0 ? { conversation } : {},
    ...info.endpointEpoch !== void 0 && isFederationCorrelationId(info.endpointEpoch) ? { endpointEpoch: info.endpointEpoch } : {},
    ...info.name !== void 0 ? { name: info.name } : {},
    ...info.description !== void 0 ? { description: info.description } : {},
    ...info.runtimeFallbackAlias !== void 0 ? { runtimeFallbackAlias: info.runtimeFallbackAlias } : {},
    cwd,
    model,
    pid: safeInteger(info.pid),
    startedAt: safeInteger(info.startedAt),
    lastActivity: safeInteger(info.lastActivity),
    ...info.status !== void 0 && isSafeBoundedText(info.status, 256, true) ? { status: info.status } : {},
    // Context usage is never shared with peer brokers.
    ...info.tmuxPane !== void 0 && isSafeBoundedText(info.tmuxPane, 128) ? { tmuxPane: info.tmuxPane } : {},
    // Only peers that negotiated it accept the field; older ones would reject the entry.
    ...shareApp && info.app !== void 0 ? { app: info.app } : {}
  };
  if (isProjection(projection)) return projection;
  const compact = {
    ...conversation !== void 0 ? { conversation } : {},
    ...info.endpointEpoch !== void 0 && isFederationCorrelationId(info.endpointEpoch) ? { endpointEpoch: info.endpointEpoch } : {},
    cwd: cwd.slice(0, 256),
    model: model.slice(0, 128),
    pid: safeInteger(info.pid),
    startedAt: safeInteger(info.startedAt),
    lastActivity: safeInteger(info.lastActivity),
    ...info.name !== void 0 && info.name.length <= 128 ? { name: info.name } : {},
    ...info.description !== void 0 ? { description: info.description } : {}
  };
  if (!isProjection(compact)) throw new FederationRosterError("Local session metadata cannot be represented safely in federation");
  return compact;
}
function sameEntry(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}
function sameImported(left, right) {
  return left.localScopeId === right.localScopeId && JSON.stringify(left.info) === JSON.stringify(right.info);
}
var FederationRosterState = class {
  constructor(options) {
    this.options = options;
    if (!isFederationCorrelationId(options.originEpoch)) {
      throw new FederationRosterError("Invalid local federation origin epoch");
    }
  }
  options;
  links = /* @__PURE__ */ new Map();
  linkUp(link) {
    this.linkDown(link.linkId);
    if (!link.features.includes(FEDERATION_ROSTER_FEATURE)) return;
    const desired = this.buildExportedRoster(link);
    const state = {
      link,
      exportSequence: 1,
      exported: desired,
      importSequence: 0,
      awaitingSnapshot: true,
      resyncRequested: false,
      seenImportEpochs: /* @__PURE__ */ new Set(),
      imported: /* @__PURE__ */ new Map()
    };
    this.links.set(link.linkId, state);
    this.sendSnapshot(state);
  }
  linkDown(linkId) {
    const state = this.links.get(linkId);
    if (!state) return;
    this.links.delete(linkId);
    if (state.imported.size > 0) {
      this.notify({ joined: [], updated: [], left: [...state.imported.values()] });
    }
  }
  reconcileLocalRoster() {
    this.pruneImportsExceedingVisibleBudget();
    for (const state of this.links.values()) {
      try {
        const desired = this.buildExportedRoster(state.link);
        const upserts = [];
        const removals = [];
        for (const [key, entry] of desired) {
          const previous = state.exported.get(key);
          if (!previous || !sameEntry(previous, entry)) upserts.push(entry);
        }
        for (const [key, previous] of state.exported) {
          if (!desired.has(key)) removals.push({ scopeAlias: previous.scopeAlias, stableSessionId: previous.stableSessionId });
        }
        state.exported = desired;
        this.sendDeltaChunks(state, upserts, removals);
      } catch (error) {
        const failure = error instanceof FederationRosterError ? error : new FederationRosterError("Failed to reconcile the local federation roster");
        if (this.options.onLinkError) this.options.onLinkError(state.link, failure);
        else throw failure;
      }
    }
  }
  handlePeerFrame(linkId, value) {
    const state = this.links.get(linkId);
    if (!state) throw new FederationRosterError("Roster frame arrived for an unknown peer link");
    if (!isFederationRosterFrame(value)) return false;
    if (value.originId !== state.link.remoteOrigin.id) {
      throw new FederationRosterError("Roster frame origin does not match the peer link");
    }
    if (isPeerRosterResyncRequest(value)) {
      if (state.lastResyncResponseSequence !== state.exportSequence) {
        this.sendSnapshot(state, true);
        state.lastResyncResponseSequence = state.exportSequence;
      }
      return true;
    }
    if (isPeerRosterSnapshot(value)) {
      this.applySnapshot(state, value);
      return true;
    }
    this.applyDelta(state, value);
    return true;
  }
  listImported() {
    return [...this.links.values()].flatMap((state) => [...state.imported.values()]);
  }
  /** Resolves an imported identity only inside the caller-authorized local scope. */
  findImportedByQualifiedId(id, localScopeId) {
    for (const state of this.links.values()) {
      const imported = state.imported.get(id);
      if (imported && imported.localScopeId === localScopeId) return imported;
    }
    return void 0;
  }
  /** Destination-side sender resolution: the peer's own exported tuple on one link. */
  findImportedByRemoteTuple(linkId, remoteScopeAlias, remoteStableSessionId) {
    const state = this.links.get(linkId);
    if (!state) return void 0;
    for (const imported of state.imported.values()) {
      if (imported.info.federation.remoteScopeAlias === remoteScopeAlias && imported.info.federation.remoteStableSessionId === remoteStableSessionId) {
        return imported;
      }
    }
    return void 0;
  }
  /** Origin-side sender identity toward one remote session: the tuple a locally
   * owned session is exported under on the binding that session arrived on. */
  findExportedTuple(linkId, targetRemoteScopeAlias, stableSessionId) {
    const state = this.links.get(linkId);
    if (!state) return void 0;
    const binding = this.bindingForRemoteAlias(state.link.scopeBindings, targetRemoteScopeAlias);
    if (!binding) return void 0;
    const entry = state.exported.get(rosterKey(binding.localScopeAlias, stableSessionId));
    return entry ? { scopeAlias: entry.scopeAlias, stableSessionId: entry.stableSessionId } : void 0;
  }
  sendDeltaChunks(state, upserts, removals) {
    const pending = [
      ...upserts.map((value) => ({ kind: "upsert", value })),
      ...removals.map((value) => ({ kind: "removal", value }))
    ];
    let batchUpserts = [];
    let batchRemovals = [];
    let batchBytes = 0;
    const frame = () => ({
      type: "peer_roster_delta",
      protocol: FEDERATION_PROTOCOL_NAME,
      version: FEDERATION_PROTOCOL_VERSION,
      originId: state.link.localOrigin.id,
      originEpoch: this.options.originEpoch,
      sequence: state.exportSequence + 1,
      upserts: batchUpserts,
      removals: batchRemovals
    });
    const flush = () => {
      if (batchUpserts.length === 0 && batchRemovals.length === 0) return;
      const next = frame();
      if (!isPeerRosterDelta(next)) throw new FederationRosterError("Local roster delta exceeds federation bounds");
      state.exportSequence = next.sequence;
      this.options.send(state.link, next);
      batchUpserts = [];
      batchRemovals = [];
      batchBytes = 0;
    };
    for (const change of pending) {
      const changeBytes = serializedSize(change.value) + 32;
      const count = batchUpserts.length + batchRemovals.length;
      if (count > 0 && (count >= MAX_DELTA_CHANGES || batchBytes + changeBytes > MAX_ROSTER_FRAME_BYTES - 4096)) {
        flush();
      }
      if (change.kind === "upsert") batchUpserts.push(change.value);
      else batchRemovals.push(change.value);
      batchBytes += changeBytes;
    }
    flush();
  }
  visibleBytesByScope(replacingLinkId, replacement) {
    const bytesByScope = /* @__PURE__ */ new Map();
    for (const local of this.options.listLocalSessions()) {
      if (local.ownership !== "local") continue;
      const scopeKey = JSON.stringify(local.localScopeId);
      bytesByScope.set(scopeKey, (bytesByScope.get(scopeKey) ?? 0) + serializedSize(local.info) + 32);
    }
    for (const state of this.links.values()) {
      const imported = state.link.linkId === replacingLinkId && replacement ? replacement : state.imported;
      for (const session of imported.values()) {
        const scopeKey = JSON.stringify(session.localScopeId);
        bytesByScope.set(scopeKey, (bytesByScope.get(scopeKey) ?? 0) + serializedSize(session.info) + 32);
      }
    }
    return bytesByScope;
  }
  assertImportedBudget(replacingLinkId, replacement) {
    if ([...this.visibleBytesByScope(replacingLinkId, replacement).values()].some((bytes) => bytes > MAX_VISIBLE_ROSTER_BYTES_PER_LOCAL_SCOPE)) {
      throw new FederationRosterError("Federated sessions would exceed the local client roster byte budget");
    }
  }
  pruneImportsExceedingVisibleBudget() {
    while (true) {
      const offendingScopes = new Set(
        [...this.visibleBytesByScope().entries()].filter(([, bytes]) => bytes > MAX_VISIBLE_ROSTER_BYTES_PER_LOCAL_SCOPE).map(([scope]) => scope)
      );
      if (offendingScopes.size === 0) return;
      const victim = [...this.links.values()].reverse().find((state) => [...state.imported.values()].some((session) => offendingScopes.has(JSON.stringify(session.localScopeId))));
      if (!victim) return;
      const link = victim.link;
      this.linkDown(link.linkId);
      this.options.onLinkError?.(link, new FederationRosterError(
        "Local roster growth exceeded the client roster byte budget; pruned the newest contributing peer link"
      ));
    }
  }
  buildExportedRoster(link) {
    const result = /* @__PURE__ */ new Map();
    for (const local of this.options.listLocalSessions()) {
      if (local.ownership !== "local" || local.exportEligible !== true) continue;
      for (const binding of link.scopeBindings) {
        if (this.rosterScopeOf(binding.localScopeId) !== local.localScopeId) continue;
        if (!isStableSessionId2(local.info.id)) {
          throw new FederationRosterError("Local stable session identity cannot be represented safely in federation");
        }
        const entry = {
          scopeAlias: binding.localScopeAlias,
          stableSessionId: local.info.id,
          session: projectionFromSession(local.info, link.features.includes(FEDERATION_CONVERSATION_FEATURE) ? local.conversationCapable === true : void 0, link.features.includes(FEDERATION_APP_FEATURE))
        };
        if (!isRosterEntry(entry)) throw new FederationRosterError("Local session cannot be represented in federation");
        result.set(rosterKey(entry.scopeAlias, entry.stableSessionId), entry);
      }
    }
    if (result.size > MAX_SNAPSHOT_SESSIONS) {
      throw new FederationRosterError(`Federation roster exceeds ${MAX_SNAPSHOT_SESSIONS} sessions for one peer`);
    }
    return result;
  }
  sendSnapshot(state, advance = false) {
    if (advance) state.exportSequence += 1;
    const frame = {
      type: "peer_roster_snapshot",
      protocol: FEDERATION_PROTOCOL_NAME,
      version: FEDERATION_PROTOCOL_VERSION,
      originId: state.link.localOrigin.id,
      originEpoch: this.options.originEpoch,
      sequence: state.exportSequence,
      sessions: [...state.exported.values()]
    };
    if (!isPeerRosterSnapshot(frame)) throw new FederationRosterError("Local snapshot exceeds federation bounds");
    this.options.send(state.link, frame);
  }
  applySnapshot(state, frame) {
    if (state.importOriginEpoch === frame.originEpoch) {
      if (frame.sequence <= state.importSequence) return;
    } else if (state.seenImportEpochs.has(frame.originEpoch)) {
      return;
    } else if (state.seenImportEpochs.size >= MAX_IMPORT_EPOCHS_PER_LINK) {
      throw new FederationRosterError("Peer changed roster origin epoch too many times on one link");
    }
    const imported = this.materializeEntries(state.link, frame.originEpoch, frame.sessions);
    this.assertImportedBudget(state.link.linkId, imported);
    const previous = state.imported;
    state.imported = imported;
    state.importOriginEpoch = frame.originEpoch;
    state.importSequence = frame.sequence;
    state.awaitingSnapshot = false;
    state.resyncRequested = false;
    state.seenImportEpochs.add(frame.originEpoch);
    this.notifyDiff(previous, imported);
  }
  applyDelta(state, frame) {
    if (state.importOriginEpoch !== frame.originEpoch && state.seenImportEpochs.has(frame.originEpoch)) return;
    if (state.awaitingSnapshot || state.importOriginEpoch !== frame.originEpoch) {
      if (!state.resyncRequested) this.requestResync(state);
      return;
    }
    if (frame.sequence <= state.importSequence) return;
    if (frame.sequence !== state.importSequence + 1) {
      state.awaitingSnapshot = true;
      this.requestResync(state);
      return;
    }
    const next = new Map(state.imported);
    const upserts = this.materializeEntries(state.link, frame.originEpoch, frame.upserts);
    for (const removal of frame.removals) {
      const qualifiedId = this.qualifiedIdForRemote(state.link, removal.scopeAlias, removal.stableSessionId);
      next.delete(qualifiedId);
    }
    for (const [qualifiedId, imported] of upserts) next.set(qualifiedId, imported);
    this.assertImportedBudget(state.link.linkId, next);
    const previous = state.imported;
    state.imported = next;
    state.importSequence = frame.sequence;
    this.notifyDiff(previous, next);
  }
  /** Ask the peer to send its roster again without changing what this side
   * expects: any answer shows someone is still behind the link. */
  requestPeerResponse(linkId) {
    const state = this.links.get(linkId);
    if (!state) return;
    this.options.send(state.link, {
      type: "peer_roster_resync_request",
      protocol: FEDERATION_PROTOCOL_NAME,
      version: FEDERATION_PROTOCOL_VERSION,
      originId: state.link.localOrigin.id,
      ...state.importOriginEpoch ? { expectedOriginEpoch: state.importOriginEpoch } : {},
      expectedSequence: Math.max(1, state.importSequence + 1)
    });
  }
  requestResync(state) {
    if (!state.awaitingSnapshot) state.awaitingSnapshot = true;
    state.resyncRequested = true;
    this.options.send(state.link, {
      type: "peer_roster_resync_request",
      protocol: FEDERATION_PROTOCOL_NAME,
      version: FEDERATION_PROTOCOL_VERSION,
      originId: state.link.localOrigin.id,
      ...state.importOriginEpoch ? { expectedOriginEpoch: state.importOriginEpoch } : {},
      expectedSequence: Math.max(1, state.importSequence + 1)
    });
  }
  materializeEntries(link, originEpoch, entries) {
    const result = /* @__PURE__ */ new Map();
    for (const entry of entries) {
      const binding = this.bindingForRemoteAlias(link.scopeBindings, entry.scopeAlias);
      if (!binding) throw new FederationRosterError(`Remote scope alias ${entry.scopeAlias} is not authorized on this link`);
      const qualifiedId = this.qualifiedIdForRemote(link, entry.scopeAlias, entry.stableSessionId);
      if (result.has(qualifiedId)) throw new FederationRosterError("Duplicate qualified session identity in roster frame");
      const { conversation: endpointConversation, ...projection } = entry.session;
      if (projection.app !== void 0 && !link.features.includes(FEDERATION_APP_FEATURE)) {
        throw new FederationRosterError("Session app labels were not negotiated on this link");
      }
      if (endpointConversation !== void 0 && !link.features.includes(FEDERATION_CONVERSATION_FEATURE)) {
        throw new FederationRosterError("Endpoint conversation capability was not negotiated on this link");
      }
      const info = {
        id: qualifiedId,
        ...projection,
        trustedLocal: false,
        federation: {
          originId: link.remoteOrigin.id,
          originEpoch,
          conversation: [
            FEDERATION_ROSTER_FEATURE,
            FEDERATION_SEND_FEATURE,
            FEDERATION_EXACT_SEND_FEATURE,
            FEDERATION_CONVERSATION_FEATURE
          ].every((feature) => link.features.includes(feature)) && endpointConversation === true,
          ...link.remoteOrigin.label ? { originLabel: link.remoteOrigin.label } : {},
          remoteScopeAlias: entry.scopeAlias,
          remoteStableSessionId: entry.stableSessionId
        }
      };
      result.set(qualifiedId, { linkId: link.linkId, originEpoch, localScopeId: this.rosterScopeOf(binding.localScopeId), info });
    }
    const merged = (alias) => {
      const binding = this.bindingForRemoteAlias(link.scopeBindings, alias);
      return binding ? this.rosterScopeOf(binding.localScopeId) !== binding.localScopeId : false;
    };
    const kept = /* @__PURE__ */ new Map();
    for (const [qualifiedId, imported] of result) {
      const key = `${JSON.stringify(imported.localScopeId)}|${imported.info.federation.remoteStableSessionId}`;
      const other = kept.get(key);
      if (!other) {
        kept.set(key, qualifiedId);
        continue;
      }
      const keepNew = merged(result.get(other).info.federation.remoteScopeAlias) && !merged(imported.info.federation.remoteScopeAlias);
      result.delete(keepNew ? other : qualifiedId);
      if (keepNew) kept.set(key, qualifiedId);
    }
    return result;
  }
  rosterScopeOf(localScopeId) {
    return this.options.rosterScope ? this.options.rosterScope(localScopeId) : localScopeId;
  }
  bindingForRemoteAlias(bindings, alias) {
    return bindings.find((binding) => binding.remoteScopeAlias === alias);
  }
  qualifiedIdForRemote(link, scopeAlias, stableSessionId) {
    if (!this.bindingForRemoteAlias(link.scopeBindings, scopeAlias)) {
      throw new FederationRosterError(`Remote scope alias ${scopeAlias} is not authorized on this link`);
    }
    return encodeOriginQualifiedSessionIdentity({
      originId: link.remoteOrigin.id,
      remoteScopeAlias: scopeAlias,
      remoteStableSessionId: stableSessionId
    });
  }
  notifyDiff(previous, next) {
    const change = { joined: [], updated: [], left: [] };
    for (const [id, imported] of next) {
      const before = previous.get(id);
      if (!before) change.joined.push(imported);
      else if (!sameImported(before, imported)) change.updated.push(imported);
    }
    for (const [id, imported] of previous) {
      if (!next.has(id)) change.left.push(imported);
    }
    this.notify(change);
  }
  notify(change) {
    if (change.joined.length === 0 && change.updated.length === 0 && change.left.length === 0) return;
    this.options.onImportedChange?.(change);
  }
};

// broker/federation-origin.ts
import { existsSync as existsSync3, readFileSync as readFileSync6, renameSync as renameSync3, writeFileSync as writeFileSync4 } from "fs";
import { join as join7 } from "path";
import { randomUUID as randomUUID6 } from "crypto";
var FEDERATION_ORIGIN_FILE = "federation-origin.json";
function loadPersistedFederationOrigin(parleyDir) {
  const path2 = join7(parleyDir, FEDERATION_ORIGIN_FILE);
  if (!existsSync3(path2)) return void 0;
  try {
    const parsed = JSON.parse(readFileSync6(path2, "utf8"));
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      console.warn(`pi-parley: ${FEDERATION_ORIGIN_FILE} is corrupt; treating the federation origin as absent and adopting or minting a fresh identity.`);
      return void 0;
    }
    const record = parsed;
    if (!isCanonicalFederationOriginId(record.originId) || !Number.isSafeInteger(record.mintedAt) || record.mintedAt <= 0) {
      console.warn(`pi-parley: ${FEDERATION_ORIGIN_FILE} has invalid contents; treating the federation origin as absent and adopting or minting a fresh identity.`);
      return void 0;
    }
    return { originId: record.originId, mintedAt: record.mintedAt };
  } catch {
    console.warn(`pi-parley: ${FEDERATION_ORIGIN_FILE} could not be read; treating the federation origin as absent and adopting or minting a fresh identity.`);
    return void 0;
  }
}
function persistFederationOrigin(parleyDir, origin, platform = process.platform) {
  const path2 = join7(parleyDir, FEDERATION_ORIGIN_FILE);
  const temp = `${path2}.${process.pid}.tmp`;
  writeFileSync4(temp, JSON.stringify(origin), { mode: 384 });
  renameSync3(temp, path2);
  restrictParleyRuntimeFile(path2, platform);
}
function mintFederationOriginId() {
  return `install:${randomUUID6()}`;
}

// broker/federation-conversation.ts
import { createHash as createHash5, randomUUID as randomUUID7 } from "node:crypto";
import { mkdirSync as mkdirSync5, readFileSync as readFileSync7, statSync as statSync2, openSync as openSync4, closeSync as closeSync4, fsyncSync as fsyncSync3, writeSync, renameSync as renameSync4 } from "node:fs";
import { join as join8 } from "node:path";
var PREFIX = "oqm1.";
function validEndpoint(value) {
  return isCanonicalFederationOriginId(value.originId) && isFederationCorrelationId(value.originEpoch) && isCanonicalFederationScopeAlias(value.scopeAlias) && typeof value.stableSessionId === "string" && value.stableSessionId.length > 0 && value.stableSessionId.length <= 512 && value.stableSessionId.trim().length > 0 && !/[\p{Cc}\p{Cf}]/u.test(value.stableSessionId) && isFederationCorrelationId(value.endpointEpoch);
}
function encodeConversationMessageId(identity) {
  if (!validEndpoint(identity) || !isFederationCorrelationId(identity.nonce)) throw new Error("Invalid conversation message identity");
  return PREFIX + Buffer.from(JSON.stringify([
    identity.originId,
    identity.originEpoch,
    identity.scopeAlias,
    identity.stableSessionId,
    identity.endpointEpoch,
    identity.nonce
  ])).toString("base64url");
}
function decodeConversationMessageId(value) {
  if (typeof value !== "string" || !value.startsWith(PREFIX) || value.length > 8192) return;
  const encoded = value.slice(PREFIX.length);
  if (!/^[A-Za-z0-9_-]+$/.test(encoded)) return;
  try {
    const tuple = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8"));
    if (!Array.isArray(tuple) || tuple.length !== 6) return;
    const identity = {
      originId: tuple[0],
      originEpoch: tuple[1],
      scopeAlias: tuple[2],
      stableSessionId: tuple[3],
      endpointEpoch: tuple[4],
      nonce: tuple[5]
    };
    if (encodeConversationMessageId(identity) === value) return identity;
  } catch {
  }
}
function sameConversationEndpoint(left, right) {
  return left.originId === right.originId && left.originEpoch === right.originEpoch && left.scopeAlias === right.scopeAlias && left.stableSessionId === right.stableSessionId && left.endpointEpoch === right.endpointEpoch;
}
var ConversationStoreError = class extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
  code;
};
var FederationConversations = class {
  constructor(barrierDir, barrierCapacity = 8192, journalByteCapacity = 4 * 1024 * 1024) {
    this.barrierDir = barrierDir;
    this.barrierCapacity = barrierCapacity;
    this.journalByteCapacity = journalByteCapacity;
  }
  barrierDir;
  barrierCapacity;
  journalByteCapacity;
  prepared = /* @__PURE__ */ new Map();
  retained = /* @__PURE__ */ new Map();
  dispositions = /* @__PURE__ */ new Map();
  scalarAssociations = /* @__PURE__ */ new Map();
  journalBytes;
  journalFailure;
  admissionFailure;
  journalAbsent = false;
  prepare(author, recipient, requestedId) {
    this.prune();
    const supplied = decodeConversationMessageId(requestedId);
    if (requestedId?.startsWith(PREFIX) && !supplied) throw new Error("E_CONVERSATION_ID");
    if (supplied && !sameConversationEndpoint(supplied, author)) throw new Error("E_CONVERSATION_AUTHOR");
    const id = supplied ? requestedId : encodeConversationMessageId({ ...author, nonce: requestedId ?? randomUUID7() });
    const previous = this.prepared.get(id) ?? this.retained.get(id);
    if (previous && (!sameConversationEndpoint(previous.author, author) || !sameConversationEndpoint(previous.recipient, recipient))) {
      throw new Error("E_CONVERSATION_TARGET");
    }
    this.prepared.set(id, {
      author,
      recipient,
      createdAt: Date.now(),
      ...!supplied && requestedId !== void 0 || previous?.scalarAlias !== void 0 ? { scalarAlias: !supplied && requestedId !== void 0 ? requestedId : previous.scalarAlias } : {}
    });
    return id;
  }
  /** Original caller identity, never a peer routing ID. Its scoped durable
   * barrier is admitted atomically with the authored handle before dispatch. */
  preparedScalarAlias(id) {
    this.prune();
    return this.prepared.get(id)?.scalarAlias;
  }
  isPrepared(id, author, recipient) {
    this.prune();
    const edge = this.prepared.get(id);
    return Boolean(edge && sameConversationEndpoint(edge.author, author) && sameConversationEndpoint(edge.recipient, recipient));
  }
  permitsReply(replyTo, author, recipient) {
    this.prune();
    const edge = this.retained.get(replyTo);
    return Boolean(edge && sameConversationEndpoint(edge.recipient, author) && sameConversationEndpoint(edge.author, recipient));
  }
  retain(id, author, recipient) {
    this.prune();
    this.retained.set(id, { author, recipient, createdAt: Date.now() });
  }
  /** Durable attempt journal: broker-process crash/restart safety.
   * Settled records and older dispatches are pruned/compacted when retention or
   * byte bounds are reached, keeping active associations and in-flight records. */
  /** Passive prior-attempt lookup. Never creates or appends a journal. A torn
   * final append preserves the verified prefix but disables new admission;
   * unreadable or corrupt history is not treated as absence. */
  hasDispatched(id) {
    this.loadJournal(false);
    return this.dispatchKeys(id).some((key) => this.dispositions.get(key) === "unknown");
  }
  /** Persist an immutable identity association before returning a converted
   * handle. Binding is not admission and never asserts a dispatched attempt. */
  bindDispatchAlias(id, aliasId) {
    this.loadJournal(true);
    const keys = this.dispatchKeys(id, aliasId);
    if (keys.length < 2 || this.scalarAssociations.get(keys[0]) === keys[1]) return;
    const additional = keys.filter((key) => !this.dispositions.has(key)).length;
    this.evictToCapacity(additional);
    this.appendDisposition(keys, "bound");
  }
  hasDispatchAlias(id, aliasId) {
    this.loadJournal(false);
    return this.scalarAssociations.get(this.dispatchKey(id)) === this.dispatchKey(aliasId);
  }
  /** Identity equivalence survives edge eviction and disposition changes.
   * A newer admission supersedes earlier partner receipts in either direction. */
  sharesDispatchIdentity(leftId, rightId) {
    this.loadJournal(false);
    const group = (id) => {
      const key = this.dispatchKey(id);
      return this.scalarAssociations.get(key) ?? key;
    };
    return group(leftId) === group(rightId);
  }
  beginDispatch(id, aliasId) {
    this.loadJournal(true);
    const keys = this.dispatchKeys(id, aliasId);
    if (keys.some((key) => this.dispositions.get(key) === "unknown")) return "existing";
    const additional = keys.filter((key) => !this.dispositions.has(key)).length;
    this.evictToCapacity(additional);
    this.appendDisposition(keys, "unknown");
    return "new";
  }
  /** Broker calls ONLY for the exact pending transport correlation and a
   * destination verdict proving no receiver write happened. Never timeout,
   * link loss, duplicate rejection, cancellation, or unconfirmed replacement. */
  settleNotDelivered(id, aliasId) {
    this.loadJournal(true);
    const keys = this.dispatchKeys(id, aliasId);
    if (keys.some((key) => this.dispositions.get(key) !== "unknown")) throw new Error("No pending dispatch evidence exists for this nondelivery verdict");
    this.appendDisposition(keys, "not_delivered");
  }
  dispatchKeys(id, aliasId) {
    const primary = this.dispatchKey(id);
    const retained = this.scalarAssociations.get(primary);
    const alias = aliasId !== void 0 ? this.dispatchKey(aliasId) : retained;
    if (retained !== void 0 && alias !== retained) throw new Error("Dispatch scalar association cannot be changed");
    return [primary, ...alias !== void 0 && alias !== primary ? [alias] : []];
  }
  dispatchKey(id) {
    return createHash5("sha256").update(id).digest("hex");
  }
  journalPath() {
    return join8(this.barrierDir, "dispatch.log");
  }
  loadJournal(create) {
    if (this.journalFailure) throw this.journalFailure;
    if (create && this.admissionFailure) throw this.admissionFailure;
    if (this.journalBytes !== void 0 || !create && this.journalAbsent) return;
    try {
      const path2 = this.journalPath();
      let present = true;
      try {
        statSync2(path2);
      } catch (error) {
        if (error.code === "ENOENT") present = false;
        else throw error;
      }
      if (!present) {
        if (!create) {
          this.journalAbsent = true;
          return;
        }
        mkdirSync5(this.barrierDir, { recursive: true, mode: 448 });
        const fd = openSync4(path2, "wx+", 384);
        try {
          this.writeAll(fd, Buffer.from("parley-dispatch-v1\n"), 0);
          fsyncSync3(fd);
        } finally {
          closeSync4(fd);
        }
        if (process.platform !== "win32") {
          const directory = openSync4(this.barrierDir, "r");
          try {
            fsyncSync3(directory);
          } finally {
            closeSync4(directory);
          }
        }
      }
      if (statSync2(path2).size > this.journalByteCapacity) throw new Error("Dispatch journal exceeds its byte bound");
      const bytes = readFileSync7(path2);
      const text = bytes.toString("utf8");
      const header = "parley-dispatch-v1\n";
      if (!text.startsWith(header)) {
        if (!header.startsWith(text)) throw new Error("Invalid dispatch journal header");
        this.admissionFailure = new ConversationStoreError("E_CONVERSATION_STATE_FAILURE", "Dispatch journal header is incomplete; new federated dispatch is disabled");
        this.journalBytes = bytes.length;
        if (create) throw this.admissionFailure;
        return;
      }
      const records = text.slice(header.length).split("\n");
      const tail = records.pop();
      for (const record of records) {
        const parts = record.split("	");
        if (parts.length !== 2 || this.dispatchKey(parts[0]) !== parts[1]) throw new Error("Corrupt dispatch journal checksum");
        const value = JSON.parse(parts[0]);
        if (!Array.isArray(value) || value.length !== 2 || value[1] !== "bound" && value[1] !== "unknown" && value[1] !== "not_delivered") throw new Error("Invalid dispatch journal record");
        const keys = typeof value[0] === "string" ? [value[0]] : value[0];
        if (!Array.isArray(keys) || keys.length < 1 || keys.length > 2 || value[1] === "bound" && keys.length !== 2 || new Set(keys).size !== keys.length || keys.some((key) => typeof key !== "string" || !/^[0-9a-f]{64}$/.test(key))) throw new Error("Invalid dispatch identities");
        for (const key of keys) {
          const prior = this.dispositions.get(key);
          if (value[1] === "not_delivered" && prior !== "unknown" && prior !== void 0 || value[1] === "unknown" && prior === "unknown") {
            throw new Error("Invalid dispatch journal transition");
          }
        }
        if (keys.length === 2) {
          const retained = this.scalarAssociations.get(keys[0]);
          if (retained !== void 0 && retained !== keys[1]) throw new Error("Invalid dispatch scalar association");
          this.scalarAssociations.set(keys[0], keys[1]);
        }
        for (const key of keys) if (value[1] !== "bound" || !this.dispositions.has(key)) this.dispositions.set(key, value[1]);
        while (this.dispositions.size > this.barrierCapacity) {
          const oldest = this.dispositions.keys().next().value;
          if (!oldest) break;
          this.dispositions.delete(oldest);
        }
      }
      this.journalBytes = bytes.length;
      this.journalAbsent = false;
      if (tail.length > 0) {
        this.admissionFailure = new ConversationStoreError("E_CONVERSATION_STATE_FAILURE", "Dispatch journal has an incomplete tail; new federated dispatch is disabled");
        if (create) throw this.admissionFailure;
      }
    } catch (error) {
      if (error === this.admissionFailure) throw error;
      this.journalFailure = new ConversationStoreError("E_CONVERSATION_STATE_FAILURE", `Dispatch journal is unavailable; federated dispatch is disabled: ${error.message}`);
      throw this.journalFailure;
    }
  }
  evictToCapacity(additional) {
    while (this.dispositions.size + additional > this.barrierCapacity) {
      const candidate = this.findEvictionCandidate();
      if (!candidate) break;
      this.deleteKeyAndAssociations(candidate);
    }
  }
  findEvictionCandidate() {
    for (const [key, disposition] of this.dispositions) {
      if (disposition === "not_delivered") return key;
    }
    for (const [key, disposition] of this.dispositions) {
      if (disposition !== "bound") return key;
    }
    return this.dispositions.keys().next().value;
  }
  deleteKeyAndAssociations(key) {
    this.dispositions.delete(key);
    const alias = this.scalarAssociations.get(key);
    if (alias) {
      this.dispositions.delete(alias);
      this.scalarAssociations.delete(key);
    }
    for (const [k, v] of this.scalarAssociations) {
      if (v === key) this.scalarAssociations.delete(k);
    }
  }
  compactJournal(incomingBytes = 0) {
    const path2 = this.journalPath();
    const tempPath = `${path2}.tmp`;
    const header = Buffer.from("parley-dispatch-v1\n");
    const buildLines = () => {
      const seenPairs = /* @__PURE__ */ new Set();
      const result = [];
      for (const [key, disposition] of this.dispositions) {
        const alias = this.scalarAssociations.get(key);
        if (alias && this.dispositions.has(alias)) {
          const pairKey = [key, alias].sort().join(":");
          if (seenPairs.has(pairKey)) continue;
          seenPairs.add(pairKey);
          const payload = JSON.stringify([[key, alias], disposition]);
          result.push(Buffer.from(payload + "	" + this.dispatchKey(payload) + "\n"));
        } else if (disposition !== "bound") {
          const payload = JSON.stringify([key, disposition]);
          result.push(Buffer.from(payload + "	" + this.dispatchKey(payload) + "\n"));
        }
      }
      return result;
    };
    let lines = buildLines();
    let total = header.length + lines.reduce((acc, line) => acc + line.length, 0);
    while (total + incomingBytes > this.journalByteCapacity && this.dispositions.size > 1) {
      const candidate = this.findEvictionCandidate();
      if (!candidate) break;
      this.deleteKeyAndAssociations(candidate);
      lines = buildLines();
      total = header.length + lines.reduce((acc, line) => acc + line.length, 0);
    }
    const fd = openSync4(tempPath, "w", 384);
    try {
      this.writeAll(fd, header, 0);
      let position = header.length;
      for (const line of lines) {
        this.writeAll(fd, line, position);
        position += line.length;
      }
      fsyncSync3(fd);
    } finally {
      closeSync4(fd);
    }
    renameSync4(tempPath, path2);
    this.journalBytes = statSync2(path2).size;
  }
  appendDisposition(keys, disposition) {
    const payload = JSON.stringify([keys.length === 1 ? keys[0] : keys, disposition]);
    const bytes = Buffer.from(payload + "	" + this.dispatchKey(payload) + "\n");
    if (this.journalBytes + bytes.length > this.journalByteCapacity) {
      this.compactJournal(bytes.length);
    }
    try {
      const fd = openSync4(this.journalPath(), "r+");
      try {
        if (statSync2(this.journalPath()).size !== this.journalBytes) throw new Error("Dispatch journal changed outside this broker");
        this.writeAll(fd, bytes, this.journalBytes);
        fsyncSync3(fd);
      } finally {
        closeSync4(fd);
      }
      this.journalBytes += bytes.length;
      if (keys.length === 2) this.scalarAssociations.set(keys[0], keys[1]);
      for (const key of keys) if (disposition !== "bound" || !this.dispositions.has(key)) this.dispositions.set(key, disposition);
    } catch (error) {
      this.journalFailure = new ConversationStoreError("E_CONVERSATION_STATE_FAILURE", `Dispatch journal write failed: ${error.message}`);
      throw this.journalFailure;
    }
  }
  writeAll(fd, bytes, position) {
    let written = 0;
    while (written < bytes.length) {
      const count = writeSync(fd, bytes, written, bytes.length - written, position + written);
      if (count <= 0) throw new Error("Dispatch journal write made no progress");
      written += count;
    }
  }
  prune() {
    const cutoff = Date.now() - 24 * 60 * 60 * 1e3;
    for (const entries of [this.prepared, this.retained]) {
      for (const [id, edge] of entries) if (edge.createdAt < cutoff) entries.delete(id);
      while (entries.size > 4096) entries.delete(entries.keys().next().value);
    }
  }
};
function conversationCompletesAsk(message) {
  return Boolean(message.replyTo && !message.expectsReply && (message.completesAsk ?? true));
}

// broker/federation-send.ts
var FEDERATION_SEND_TEXT_MAX_LENGTH = 32 * 1024;
var FEDERATION_SEND_ERROR_MAX_LENGTH = 256;
var FEDERATION_SEND_MAX_SEEN_IDS = 4096;
var FEDERATION_SEND_MAX_PENDING = 4096;
var FEDERATION_SEND_TIMEOUT_MS = 8500;
var CONTROL_OR_FORMAT_CHARACTERS3 = /[\p{Cc}\p{Cf}]/u;
function isRecord5(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function hasOnlyKeys3(value, required, optional = []) {
  for (const key of required) {
    if (!(key in value)) return false;
  }
  for (const key of Object.keys(value)) {
    if (!required.includes(key) && !optional.includes(key)) return false;
  }
  return true;
}
function isStableSessionId3(value, conversation = false) {
  return typeof value === "string" && value.length > 0 && value.length <= FEDERATION_SESSION_ID_MAX_LENGTH && !CONTROL_OR_FORMAT_CHARACTERS3.test(value) && (conversation ? value.trim().length > 0 : value.trim() === value);
}
function isTimestamp(value) {
  return Number.isSafeInteger(value) && value >= 0;
}
var PEER_SEND_FAILURE_CODES = [
  "E_SEND_TARGET_NOT_FOUND",
  "E_SEND_TARGET_DISCONNECTED",
  "E_SEND_TARGET_REBOUND",
  "E_SEND_UNAUTHORIZED",
  "E_SEND_INVALID",
  "E_SEND_DUPLICATE",
  "E_SEND_UNSUPPORTED"
];
function isPeerSendRequest(value) {
  if (!isRecord5(value) || !hasOnlyKeys3(value, [
    "type",
    "protocol",
    "version",
    "originId",
    "sendId",
    "senderScopeAlias",
    "senderStableSessionId",
    "targetScopeAlias",
    "targetStableSessionId",
    "message"
  ], ["targetEndpointEpoch", "senderEndpointEpoch", "senderOriginEpoch", "targetOriginEpoch"])) return false;
  if (!isRecord5(value.message) || !hasOnlyKeys3(value.message, ["id", "timestamp", "text"], ["replyTo", "expectsReply", "completesAsk", "senderWaitMode"])) return false;
  const conversation = value.senderOriginEpoch !== void 0;
  if (conversation) {
    if (!isFederationCorrelationId(value.senderOriginEpoch) || !isFederationCorrelationId(value.senderEndpointEpoch) || !isFederationCorrelationId(value.targetOriginEpoch) || !isFederationCorrelationId(value.targetEndpointEpoch) || !decodeConversationMessageId(value.message.id)) return false;
  } else if (value.senderEndpointEpoch !== void 0 || value.targetOriginEpoch !== void 0 || Object.keys(value.message).some((key) => !["id", "timestamp", "text"].includes(key))) return false;
  if (value.message.replyTo !== void 0 && !decodeConversationMessageId(value.message.replyTo)) return false;
  if (value.message.expectsReply !== void 0 && typeof value.message.expectsReply !== "boolean") return false;
  if (value.message.completesAsk !== void 0 && (typeof value.message.completesAsk !== "boolean" || value.message.completesAsk && !value.message.replyTo)) return false;
  if (value.message.senderWaitMode !== void 0 && value.message.senderWaitMode !== "blocking" && value.message.senderWaitMode !== "nonblocking") return false;
  return value.type === "peer_send" && value.protocol === FEDERATION_PROTOCOL_NAME && value.version === FEDERATION_PROTOCOL_VERSION && isCanonicalFederationOriginId(value.originId) && isFederationCorrelationId(value.sendId) && isCanonicalFederationScopeAlias(value.senderScopeAlias) && isStableSessionId3(value.senderStableSessionId, conversation) && isCanonicalFederationScopeAlias(value.targetScopeAlias) && isStableSessionId3(value.targetStableSessionId, conversation) && (value.targetEndpointEpoch === void 0 || isFederationCorrelationId(value.targetEndpointEpoch)) && (conversation || isFederationCorrelationId(value.message.id)) && isTimestamp(value.message.timestamp) && typeof value.message.text === "string" && value.message.text.length <= FEDERATION_SEND_TEXT_MAX_LENGTH;
}
function isPeerSendResult(value) {
  if (!isRecord5(value) || !hasOnlyKeys3(
    value,
    ["type", "protocol", "version", "originId", "sendId", "ok"],
    ["deliveredAt", "code", "error"]
  )) return false;
  if (value.type !== "peer_send_result" || value.protocol !== FEDERATION_PROTOCOL_NAME || value.version !== FEDERATION_PROTOCOL_VERSION || !isCanonicalFederationOriginId(value.originId) || !isFederationCorrelationId(value.sendId)) return false;
  if (value.ok === true) {
    return isTimestamp(value.deliveredAt);
  }
  if (value.ok !== false) return false;
  return PEER_SEND_FAILURE_CODES.includes(value.code) && typeof value.error === "string" && value.error.length > 0 && value.error.length <= FEDERATION_SEND_ERROR_MAX_LENGTH && !CONTROL_OR_FORMAT_CHARACTERS3.test(value.error);
}
var PeerSendDedup = class {
  constructor(capacity = FEDERATION_SEND_MAX_SEEN_IDS) {
    this.capacity = capacity;
  }
  capacity;
  seen = /* @__PURE__ */ new Set();
  /** Returns true when sendId is new; false when it was already observed. */
  observe(sendId) {
    if (this.seen.has(sendId)) return false;
    this.seen.add(sendId);
    if (this.seen.size > this.capacity) {
      const oldest = this.seen.values().next().value;
      this.seen.delete(oldest);
    }
    return true;
  }
  get size() {
    return this.seen.size;
  }
};
var PendingPeerSendTracker = class {
  constructor(capacity = FEDERATION_SEND_MAX_PENDING, timeoutMs = FEDERATION_SEND_TIMEOUT_MS) {
    this.capacity = capacity;
    this.timeoutMs = timeoutMs;
  }
  capacity;
  timeoutMs;
  pending = /* @__PURE__ */ new Map();
  add(sendId, entry, now = Date.now()) {
    if (this.pending.has(sendId) || this.pending.size >= this.capacity) return void 0;
    const pending = {
      ...entry,
      createdAt: now,
      expiresAt: now + this.timeoutMs
    };
    this.pending.set(sendId, pending);
    return pending;
  }
  peek(sendId) {
    return this.pending.get(sendId);
  }
  resolve(sendId) {
    const entry = this.pending.get(sendId);
    if (entry) this.pending.delete(sendId);
    return entry;
  }
  /** Drops and returns every pending send tied to a dead link. */
  dropLink(linkId) {
    const dropped = [];
    for (const [sendId, entry] of this.pending) {
      if (entry.linkId !== linkId) continue;
      this.pending.delete(sendId);
      dropped.push(entry);
    }
    return dropped;
  }
  /** Drops and returns entries whose correlation deadline passed. */
  expire(now = Date.now()) {
    const expired = [];
    for (const [sendId, entry] of this.pending) {
      if (entry.expiresAt > now) continue;
      this.pending.delete(sendId);
      expired.push(entry);
    }
    return expired;
  }
  get size() {
    return this.pending.size;
  }
};

// broker/broker.ts
var PARLEY_DIR = getParleyDirPath();
var MERGED_SCOPES = loadMergedScopes(PARLEY_DIR);
var LISTEN_TARGET = getBrokerListenTarget();
var PID_PATH = join9(PARLEY_DIR, "broker.pid");
var PORT_PATH = getBrokerPortFilePath(PARLEY_DIR);
var PENDING_ASKS_DIR = join9(PARLEY_DIR, "pending-asks");
var BROKER_STATE_ID = randomUUID8();
var BROKER_BUILD = getBrokerBuildIdentity();
var BROKER_STARTED_AT = (/* @__PURE__ */ new Date()).toISOString();
function replyTargetReason(routeKnown) {
  return routeKnown ? "Reply target does not match a previous message from this recipient" : `This broker has no thread for that message. It started at ${BROKER_STARTED_AT}, and threads don't survive a broker restart; send a new message instead of a reply`;
}
var MAX_SESSIONS = 128;
var MAX_UNREGISTERED_CONNECTIONS = 32;
var REGISTRATION_TIMEOUT_MS = 1e3;
var PEER_PREPARED_TIMEOUT_MS = 5e3;
var RATE_LIMIT_CAPACITY = 240;
var RATE_LIMIT_REFILL_PER_SECOND = 120;
var ACK_RATE_LIMIT_CAPACITY = 1024;
var ACK_RATE_LIMIT_REFILL_PER_SECOND = 512;
var PRESENCE_HEARTBEAT_MS = 1e3;
var MAX_EXTENSIONS_PER_SESSION = 32;
var MAX_CLIENT_FEATURES_PER_SESSION = 32;
var MAX_EXTENSION_MESSAGE_BYTES = 16 * 1024;
var MAX_EXTENSION_STATE_BYTES = 64 * 1024;
var MESSAGE_RECEIPT_ROUTE_RETENTION_MS = 60 * 60 * 1e3;
var DISCONNECTED_SESSION_RETENTION_MS = 24 * 60 * 60 * 1e3;
var MAILBOX_MESSAGE_RETENTION_MS = 24 * 60 * 60 * 1e3;
var MAX_MAILBOX_MESSAGES = 256;
var MAILBOX_SWEEP_INTERVAL_MS = 60 * 1e3;
var DELIVERY_RECORD_RETENTION_MS = 60 * 60 * 1e3;
var MAX_DELIVERY_RECORDS = 4096;
var DIRECT_CONTACT_TOKEN_RETENTION_MS = 60 * 60 * 1e3;
var MAX_PENDING_DIRECT_CONTACTS = 4096;
function serializedPayloadSize(payload) {
  try {
    const json = JSON.stringify(payload);
    return json === void 0 ? null : Buffer.byteLength(json, "utf8");
  } catch {
    return null;
  }
}
function normalizeScopeId(value) {
  if (value === void 0) {
    return void 0;
  }
  if (typeof value !== "string") {
    throw new Error("Invalid register scopeId");
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : void 0;
}
function sameScope(a, b) {
  return a === b;
}
function scopedSessionKey(scopeId, sessionId) {
  return JSON.stringify([scopeId ?? null, sessionId]);
}
function scopedExtensionKey(scopeId, namespace) {
  return JSON.stringify([scopeId ?? null, namespace]);
}
function scopedExtensionStateNamespace(scopeId, namespace) {
  if (!scopeId) {
    return namespace;
  }
  return JSON.stringify(["scope", createHash6("sha256").update(scopeId).digest("hex"), namespace]);
}
function scopedPendingAskRecordPath(scopeId, messageId) {
  if (!scopeId) {
    return pendingAskRecordPath(messageId);
  }
  const scopeHash = createHash6("sha256").update(scopeId).digest("hex");
  return join9(PENDING_ASKS_DIR, `${scopeHash}-${encodeURIComponent(messageId)}.json`);
}
function isRecord6(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function matchesSupervisor(child, candidateSupervisor) {
  if (child.supervisorSessionId && child.supervisorSessionId === candidateSupervisor.id) {
    return true;
  }
  if (child.supervisorName && candidateSupervisor.name && candidateSupervisor.name.toLowerCase() === child.supervisorName.toLowerCase()) {
    return true;
  }
  return false;
}
function areSiblings(a, b) {
  if (!a.isSubagent || !b.isSubagent) return false;
  if (a.supervisorSessionId && b.supervisorSessionId && a.supervisorSessionId === b.supervisorSessionId) {
    return true;
  }
  if (a.supervisorName && b.supervisorName && a.supervisorName.toLowerCase() === b.supervisorName.toLowerCase()) {
    return true;
  }
  return false;
}
function canSeeSession(observer, subject) {
  if (observer.id === subject.id) {
    return true;
  }
  if (observer.isSubagent) {
    return matchesSupervisor(observer, subject) || areSiblings(observer, subject);
  }
  if (subject.isSubagent) {
    return matchesSupervisor(subject, observer);
  }
  return true;
}
function isPendingAskRecord(value) {
  if (!isRecord6(value) || !isRecord6(value.asker) || !isRecord6(value.target)) {
    return false;
  }
  return typeof value.askId === "string" && typeof value.messageId === "string" && typeof value.asker.sessionId === "string" && (typeof value.asker.name === "string" || value.asker.name === null) && typeof value.target.sessionId === "string" && (typeof value.target.name === "string" || value.target.name === null) && typeof value.question === "string" && Number.isSafeInteger(value.createdAt) && Number.isSafeInteger(value.expiresAt) && value.expiresAt >= value.createdAt;
}
function pendingAskRecordPath(messageId) {
  return join9(PENDING_ASKS_DIR, `${encodeURIComponent(messageId)}.json`);
}
function ensurePendingAskRecordDir() {
  mkdirSync6(PENDING_ASKS_DIR, { recursive: true, mode: PARLEY_DIR_MODE });
  if (process.platform !== "win32") {
    chmodSync3(PENDING_ASKS_DIR, PARLEY_DIR_MODE);
  }
}
var ParleyBroker = class {
  constructor(runtimeLease2) {
    this.runtimeLease = runtimeLease2;
    this.federationOrigin = loadPersistedFederationOrigin(PARLEY_DIR);
    ensurePendingAskRecordDir();
    this.prunePendingAskRecords();
    this.extensionStateManager = new ExtensionStateManager(PARLEY_DIR);
    this.collaborationState = new CollaborationStateStore(PARLEY_DIR);
    this.peerLinks = new PeerLinkManager({
      onSocketOpened: (socket) => this.connections.add(socket),
      onSocketClosed: (socket) => this.connections.delete(socket),
      onLinkUp: (link) => {
        this.cancelShutdownTimer();
        this.federationRoster.linkUp(link);
      },
      onLinkDown: (link) => {
        this.federationRoster.linkDown(link.linkId);
        this.federationSendDedup.delete(link.linkId);
        for (const pending of this.federationPendingSends.dropLink(link.linkId)) {
          this.failPendingFederatedSend(pending, "Remote federation link disconnected before acknowledgement; delivery may have occurred", "E_DELIVERY_UNKNOWN", false, false);
        }
        this.scheduleShutdownCheck();
      },
      onPeerMessage: (link, value) => this.handleFederationPeerMessage(link, value),
      requestPeerResponse: (link) => this.federationRoster.requestPeerResponse(link.linkId)
    });
    this.federationRoster = new FederationRosterState({
      originEpoch: this.federationOriginEpoch,
      rosterScope: (scopeId) => effectiveScope(scopeId ?? void 0, MERGED_SCOPES) ?? null,
      listLocalSessions: () => this.listLocallyOwnedFederationSessions(),
      send: (link, frame) => this.writeBrokerFrame(link.socket, frame),
      onImportedChange: (change) => this.handleImportedRosterChange(change),
      onLinkError: (link, error) => link.socket.destroy(error)
    });
    if (typeof LISTEN_TARGET === "string" && process.platform !== "win32") {
      try {
        unlinkSync(LISTEN_TARGET);
      } catch {
      }
    }
    this.server = net3.createServer(this.handleConnection.bind(this));
  }
  runtimeLease;
  sessions = /* @__PURE__ */ new Map();
  askEdges = /* @__PURE__ */ new Map();
  messageReceiptRoutes = /* @__PURE__ */ new Map();
  disconnectedSessions = /* @__PURE__ */ new Map();
  mailboxMessages = [];
  deliveryRecords = /* @__PURE__ */ new Map();
  pendingDirectContacts = /* @__PURE__ */ new Map();
  connections = /* @__PURE__ */ new Set();
  unregisteredConnections = /* @__PURE__ */ new Set();
  server;
  shutdownTimer = null;
  shuttingDown = false;
  maintenanceTimer = null;
  askTimeoutMs = getAskTimeoutMs();
  namespaceOwners = /* @__PURE__ */ new Map();
  nextOwnerOrder = 1;
  extensionStateManager;
  collaborationState;
  peerLinks;
  federationRoster;
  federationOriginEpoch = randomUUID8();
  federationConversations = new FederationConversations(join9(PARLEY_DIR, "conversation-dispatches"));
  federationOrigin;
  federationSendDedup = /* @__PURE__ */ new Map();
  federationPendingSends = new PendingPeerSendTracker();
  federationInFlightMessageIds = /* @__PURE__ */ new Set();
  federationSendSweepTimer = null;
  /** New provenance metadata is opt-in for local clients: older validators
   * accept only the original strict federation tuple. Peer roster frames never
   * carry these local projection fields. */
  writeBrokerFrame(socket, value) {
    const client = [...this.sessions.values()].find((session) => session.socket === socket);
    if (client && !client.clientFeatures.has(PERSON_PROVENANCE_FEATURE) && isRecord6(value) && isRecord6(value.message) && isRecord6(value.message.provenance) && value.message.provenance.type === "session_person") {
      const { provenance: _provenance, ...message } = value.message;
      value = { ...value, message };
    }
    if (client && !client.clientFeatures.has(FEDERATED_CONVERSATION_FEATURE) && isRecord6(value)) {
      const project = (session) => {
        if (!isRecord6(session) || !isRecord6(session.federation)) return session;
        const { conversation: _conversation, originEpoch: _originEpoch, ...federation } = session.federation;
        return { ...session, federation };
      };
      value = {
        ...value,
        ...value.session ? { session: project(value.session) } : {},
        ...value.from ? { from: project(value.from) } : {},
        ...value.recipient ? { recipient: project(value.recipient) } : {},
        ...Array.isArray(value.sessions) ? { sessions: value.sessions.map(project) } : {}
      };
    }
    writeMessage(socket, value);
  }
  supportsRemoteConversations(session) {
    return [FEDERATED_CONVERSATION_FEATURE, CONVERSATION_CONTRACT_FEATURE, EXACT_SEND_FEATURE].every((feature) => session.clientFeatures.has(feature));
  }
  supportsCompactionAwareness(session) {
    return session.clientFeatures.has(COMPACTION_AWARENESS_FEATURE);
  }
  directContactPlan(scopeId, observer, peer, includePeerContext = true, requestedPeerSessionId) {
    const snapshot = this.collaborationState.readContact(scopeId, observer.id, peer.id);
    const notice = snapshot.compactedSinceLastContact && snapshot.lastContactGeneration !== void 0 ? {
      peerSessionId: peer.id,
      ...peer.name ? { peerName: peer.name } : {},
      ...requestedPeerSessionId && requestedPeerSessionId !== peer.id ? { requestedPeerSessionId } : {},
      generation: snapshot.peerGeneration,
      previousGeneration: snapshot.lastContactGeneration,
      compactedAt: snapshot.peerCompactedAt
    } : void 0;
    return {
      ...scopeId ? { scopeId } : {},
      observerSessionId: observer.id,
      peerSessionId: peer.id,
      observedPeerGeneration: snapshot.peerGeneration,
      durableBaseline: snapshot.lastContactGeneration === void 0,
      ...notice ? { notice } : {}
    };
  }
  trackDirectContact(observerKey, plan, token = randomUUID8()) {
    this.prunePendingDirectContacts();
    while (this.pendingDirectContacts.size >= MAX_PENDING_DIRECT_CONTACTS) {
      const oldest = this.pendingDirectContacts.keys().next().value;
      if (oldest === void 0) break;
      this.pendingDirectContacts.delete(oldest);
    }
    if (plan.durableBaseline) {
      this.collaborationState.stageFirstContactBaseline(
        plan.scopeId,
        plan.observerSessionId,
        plan.peerSessionId,
        plan.observedPeerGeneration,
        token
      );
    }
    this.pendingDirectContacts.set(token, { observerKey, plan, createdAt: Date.now() });
    return token;
  }
  acknowledgeDirectContact(observerKey, token, scopeId, observerSessionId) {
    const pending = this.pendingDirectContacts.get(token);
    if (pending && pending.observerKey !== observerKey) return "unknown";
    try {
      const accepted = pending?.plan.durableBaseline ? this.collaborationState.acceptStagedFirstContactBaseline(scopeId, observerSessionId, token) : pending ? this.commitDirectContact(pending.plan) : this.collaborationState.acceptStagedFirstContactBaseline(scopeId, observerSessionId, token);
      if (accepted) {
        this.pendingDirectContacts.delete(token);
        return "accepted";
      }
      return pending ? "retry" : "unknown";
    } catch (error) {
      console.error("Failed to acknowledge staged collaboration baseline:", error);
      return "retry";
    }
  }
  prunePendingDirectContacts(now = Date.now()) {
    for (const [token, pending] of this.pendingDirectContacts) {
      if (now - pending.createdAt > DIRECT_CONTACT_TOKEN_RETENTION_MS) {
        this.pendingDirectContacts.delete(token);
      }
    }
  }
  commitDirectContact(plan) {
    try {
      this.collaborationState.recordAcceptedContact(
        plan.scopeId,
        plan.observerSessionId,
        plan.peerSessionId,
        plan.observedPeerGeneration,
        plan.durableBaseline
      );
      return true;
    } catch (error) {
      console.error("Failed to record direct collaboration contact:", error);
      return false;
    }
  }
  prepareSenderContact(observerKey, plan) {
    if (!plan) return {};
    if (!plan.durableBaseline) {
      return { clientToken: this.trackDirectContact(observerKey, plan) };
    }
    const stagedBaselineToken = randomUUID8();
    this.collaborationState.stageFirstContactBaseline(
      plan.scopeId,
      plan.observerSessionId,
      plan.peerSessionId,
      plan.observedPeerGeneration,
      stagedBaselineToken
    );
    return { stagedBaselineToken };
  }
  finalizeSenderContact(plan, stagedBaselineToken) {
    if (!plan?.durableBaseline || !stagedBaselineToken) return;
    if (!this.collaborationState.acceptStagedFirstContactBaseline(
      plan.scopeId,
      plan.observerSessionId,
      stagedBaselineToken
    )) {
      throw new Error("Failed to commit first-contact collaboration baseline");
    }
  }
  start() {
    const onListening = () => {
      if (typeof LISTEN_TARGET === "string") {
        restrictParleyRuntimeFile(LISTEN_TARGET);
      } else {
        const address = this.server.address();
        if (!address || typeof address === "string") {
          throw new Error("Parley TCP broker started without a TCP address");
        }
        const endpoint = {
          transport: "tcp",
          host: LISTEN_TARGET.host,
          port: address.port,
          stateId: BROKER_STATE_ID
        };
        writeFileSync5(PORT_PATH, `${JSON.stringify(endpoint)}
`, { mode: PARLEY_RUNTIME_FILE_MODE });
        restrictParleyRuntimeFile(PORT_PATH);
      }
      writeFileSync5(PID_PATH, String(process.pid), { mode: PARLEY_RUNTIME_FILE_MODE });
      restrictParleyRuntimeFile(PID_PATH);
      console.log(`Parley broker started (pid: ${process.pid})`);
      this.scheduleShutdownCheck();
    };
    if (typeof LISTEN_TARGET === "string") {
      this.server.listen(LISTEN_TARGET, onListening);
    } else {
      this.server.listen({ host: LISTEN_TARGET.host, port: LISTEN_TARGET.port }, onListening);
    }
    process.on("SIGTERM", () => this.shutdown());
    process.on("SIGINT", () => this.shutdown());
    this.maintenanceTimer = setInterval(() => {
      this.pruneMailboxMessages();
      this.pruneDisconnectedSessions();
      this.prunePendingDirectContacts();
    }, MAILBOX_SWEEP_INTERVAL_MS);
    this.maintenanceTimer.unref?.();
  }
  handleConnection(socket) {
    if (this.shuttingDown) {
      socket.destroy();
      return;
    }
    this.connections.add(socket);
    let sessionKey = null;
    let connectionRole = "unregistered";
    let preparedPeer = null;
    let startingPeer = null;
    let dialAbortController = null;
    let peerLinkId = null;
    let registrationTimeout = null;
    const armRegistrationTimeout = (timeoutMs = REGISTRATION_TIMEOUT_MS) => {
      if (registrationTimeout) {
        clearTimeout(registrationTimeout);
      }
      this.unregisteredConnections.delete(socket);
      this.unregisteredConnections.add(socket);
      this.evictOldestUnregisteredConnections(socket);
      registrationTimeout = setTimeout(() => {
        if (connectionRole === "unregistered" || connectionRole === "peer-prepared" || connectionRole === "peer-starting") socket.destroy();
      }, timeoutMs);
      registrationTimeout.unref?.();
    };
    const clearRegistrationTimeout = () => {
      if (registrationTimeout) {
        clearTimeout(registrationTimeout);
        registrationTimeout = null;
      }
      this.unregisteredConnections.delete(socket);
    };
    armRegistrationTimeout();
    const connection = {
      socket,
      tokens: RATE_LIMIT_CAPACITY,
      lastRefillAt: Date.now(),
      ackTokens: ACK_RATE_LIMIT_CAPACITY,
      lastAckRefillAt: Date.now()
    };
    const reader = createMessageReader((msg) => {
      const isDirectContactAck = typeof msg === "object" && msg !== null && "type" in msg && msg.type === "direct_contact_seen";
      if (!(isDirectContactAck ? this.consumeAckToken(connection) : this.consumeToken(connection))) {
        this.writeBrokerFrame(socket, { type: "error", error: "Parley broker rate limit exceeded" });
        socket.destroy(new Error("Parley broker rate limit exceeded"));
        return;
      }
      const record = typeof msg === "object" && msg !== null && !Array.isArray(msg) ? msg : void 0;
      const claimedType = typeof record?.type === "string" ? record.type : void 0;
      if (connectionRole === "peer") {
        this.peerLinks.handlePostHandshakeMessage(peerLinkId, msg);
        return;
      }
      if (connectionRole === "control") {
        throw new Error("Broker control connections accept exactly one request");
      }
      if (connectionRole === "peer-starting") {
        clearRegistrationTimeout();
        if (!startingPeer) throw new Error("Outbound peer authority missing");
        const { requestId, prepared } = startingPeer;
        startingPeer = null;
        let link;
        try {
          link = this.peerLinks.acceptOutbound(socket, msg, prepared);
        } catch (error) {
          const failure = error instanceof FederationPeerError ? error : new FederationPeerError("E_HANDSHAKE_FAILED", "Peer start failed", { cause: error });
          connectionRole = "control";
          this.writeBrokerFrame(socket, { type: "broker_start_peer_result", requestId, ok: false, code: failure.code, error: failure.message });
          socket.end();
          return;
        }
        connectionRole = "peer";
        peerLinkId = link.linkId;
        this.writeBrokerFrame(socket, { type: "broker_start_peer_result", requestId, ok: true, linkId: link.linkId });
        this.peerLinks.activateLink(link.linkId);
        return;
      }
      if (connectionRole === "peer-prepared") {
        clearRegistrationTimeout();
        if (!preparedPeer) throw new Error("Prepared peer authority missing");
        if (!isPeerHello(msg)) {
          const features = Array.isArray(record?.features) ? record.features : [];
          const code = record?.protocol !== FEDERATION_PROTOCOL_NAME || record.version !== FEDERATION_PROTOCOL_VERSION ? "E_VERSION_UNSUPPORTED" : FEDERATION_REQUIRED_FEATURES.some((feature) => !features.includes(feature)) ? "E_FEATURE_UNSUPPORTED" : "E_INVALID_REQUEST";
          this.writeBrokerFrame(socket, {
            type: "peer_hello_ack",
            protocol: FEDERATION_PROTOCOL_NAME,
            version: FEDERATION_PROTOCOL_VERSION,
            linkId: preparedPeer.linkId,
            accepted: false,
            code,
            error: code === "E_VERSION_UNSUPPORTED" ? "Unsupported peer protocol version" : code === "E_FEATURE_UNSUPPORTED" ? "Unsupported or invalid peer features" : "Invalid peer hello"
          });
          connectionRole = "control";
          preparedPeer = null;
          socket.end();
          return;
        }
        const accepted = this.peerLinks.acceptInbound(socket, msg, preparedPeer);
        preparedPeer = null;
        this.writeBrokerFrame(socket, accepted.ack);
        if (!accepted.link) {
          connectionRole = "control";
          socket.end();
          return;
        }
        connectionRole = "peer";
        peerLinkId = accepted.link.linkId;
        this.peerLinks.activateLink(accepted.link.linkId);
        return;
      }
      if (connectionRole === "unregistered" && claimedType === "broker_start_peer") {
        connectionRole = "control";
        clearRegistrationTimeout();
        if (typeof LISTEN_TARGET !== "string" && record?.stateId !== BROKER_STATE_ID) {
          throw new Error("Invalid parley TCP endpoint credentials");
        }
        const requestId = record && isFederationCorrelationId(record.requestId) ? record.requestId : void 0;
        if (!isBrokerStartPeerRequest(msg)) {
          if (requestId) this.writeBrokerFrame(socket, { type: "broker_start_peer_result", requestId, ok: false, code: "E_INVALID_REQUEST", error: "Invalid broker start peer request" });
          else this.writeBrokerFrame(socket, { type: "error", error: "Invalid broker start peer request" });
          socket.end();
          return;
        }
        try {
          this.enforceCanonicalFederationOrigin(msg.localOrigin);
          const prepared = this.peerLinks.prepareOutbound(msg);
          startingPeer = { requestId: msg.requestId, prepared };
          connectionRole = "peer-starting";
          this.cancelShutdownTimer();
          armRegistrationTimeout(PEER_PREPARED_TIMEOUT_MS);
          this.writeBrokerFrame(socket, this.peerLinks.outboundHello(prepared));
        } catch (error) {
          startingPeer = null;
          connectionRole = "control";
          const failure = error instanceof FederationPeerError ? error : new FederationPeerError("E_INVALID_REQUEST", "Peer start failed", { cause: error });
          this.writeBrokerFrame(socket, { type: "broker_start_peer_result", requestId: msg.requestId, ok: false, code: failure.code, error: failure.message });
          socket.end();
        }
        return;
      }
      if (connectionRole === "unregistered" && claimedType === "broker_dial_peer") {
        connectionRole = "control";
        clearRegistrationTimeout();
        const requiresEndpointAuth = typeof LISTEN_TARGET !== "string";
        if (requiresEndpointAuth && record?.stateId !== BROKER_STATE_ID) {
          throw new Error("Invalid parley TCP endpoint credentials");
        }
        const requestId = record && isFederationCorrelationId(record.requestId) ? record.requestId : void 0;
        if (!isBrokerDialPeerRequest(msg)) {
          if (requestId) this.writeBrokerFrame(socket, { type: "broker_dial_peer_result", requestId, ok: false, code: "E_INVALID_REQUEST", error: "Invalid broker dial peer request" });
          else this.writeBrokerFrame(socket, { type: "error", error: "Invalid broker dial peer request" });
          socket.end();
          return;
        }
        this.cancelShutdownTimer();
        const controller = new AbortController();
        dialAbortController = controller;
        void this.handleDialPeerControl(socket, msg, controller.signal).finally(() => {
          if (dialAbortController === controller) dialAbortController = null;
        });
        return;
      }
      if (connectionRole === "unregistered" && claimedType === "broker_list_scopes") {
        connectionRole = "control";
        clearRegistrationTimeout();
        const requiresEndpointAuth = typeof LISTEN_TARGET !== "string";
        if (requiresEndpointAuth && record?.stateId !== BROKER_STATE_ID) {
          throw new Error("Invalid parley TCP endpoint credentials");
        }
        const requestId = record && isFederationCorrelationId(record.requestId) ? record.requestId : void 0;
        if (!isBrokerListScopesRequest(msg)) {
          if (requestId) this.writeBrokerFrame(socket, { type: "broker_list_scopes_result", requestId, ok: false, code: "E_INVALID_REQUEST", error: "Invalid broker list scopes request" });
          else this.writeBrokerFrame(socket, { type: "error", error: "Invalid broker list scopes request" });
          socket.end();
          return;
        }
        let result;
        try {
          result = {
            type: "broker_list_scopes_result",
            requestId: msg.requestId,
            ok: true,
            localOrigin: this.getCanonicalFederationOrigin(),
            scopes: this.summarizeScopes(),
            ...msg.include?.includes("mergedScopes") ? { mergedScopes: [...MERGED_SCOPES].sort() } : {}
          };
        } catch (error) {
          result = {
            type: "broker_list_scopes_result",
            requestId: msg.requestId,
            ok: false,
            code: "E_INVALID_REQUEST",
            error: error instanceof Error ? error.message : String(error)
          };
        }
        this.writeBrokerFrame(socket, result);
        socket.end();
        return;
      }
      if (connectionRole === "unregistered" && claimedType === "broker_accept_peer") {
        connectionRole = "control";
        clearRegistrationTimeout();
        const requiresEndpointAuth = typeof LISTEN_TARGET !== "string";
        if (requiresEndpointAuth && record?.stateId !== BROKER_STATE_ID) {
          throw new Error("Invalid parley TCP endpoint credentials");
        }
        const requestId = record && isFederationCorrelationId(record.requestId) ? record.requestId : void 0;
        if (!isBrokerAcceptPeerRequest(msg)) {
          if (requestId) this.writeBrokerFrame(socket, { type: "broker_accept_peer_result", requestId, ok: false, code: "E_INVALID_REQUEST", error: "Invalid broker accept peer request" });
          else this.writeBrokerFrame(socket, { type: "error", error: "Invalid broker accept peer request" });
          socket.end();
          return;
        }
        try {
          this.enforceCanonicalFederationOrigin(msg.localOrigin);
          preparedPeer = this.peerLinks.prepareInbound(msg);
          connectionRole = "peer-prepared";
          this.cancelShutdownTimer();
          armRegistrationTimeout(PEER_PREPARED_TIMEOUT_MS);
          this.writeBrokerFrame(socket, { type: "broker_accept_peer_result", requestId: msg.requestId, ok: true, linkId: msg.linkId });
        } catch (error) {
          const failure = error instanceof FederationPeerError ? error : new FederationPeerError("E_INVALID_REQUEST", "Peer preparation failed", { cause: error });
          this.writeBrokerFrame(socket, { type: "broker_accept_peer_result", requestId: msg.requestId, ok: false, code: failure.code, error: failure.message });
          socket.end();
        }
        return;
      }
      if (connectionRole === "unregistered" && claimedType === "peer_hello") {
        if (typeof LISTEN_TARGET !== "string") {
          throw new Error("Invalid parley TCP endpoint credentials");
        }
        connectionRole = "control";
        clearRegistrationTimeout();
        if (record && isFederationCorrelationId(record.linkId)) {
          this.writeBrokerFrame(socket, {
            type: "peer_hello_ack",
            protocol: FEDERATION_PROTOCOL_NAME,
            version: FEDERATION_PROTOCOL_VERSION,
            linkId: record.linkId,
            accepted: false,
            code: "E_NOT_PREPARED",
            error: "Destination broker was not prepared by the transport authority"
          });
        }
        socket.end();
        return;
      }
      this.handleMessage(socket, msg, sessionKey, (id) => {
        sessionKey = id;
        connectionRole = id ? "client" : "unregistered";
        if (id) clearRegistrationTimeout();
        else armRegistrationTimeout();
      });
    }, (error) => {
      socket.destroy(error);
    });
    socket.on("data", reader);
    socket.on("close", () => {
      clearRegistrationTimeout();
      dialAbortController?.abort();
      dialAbortController = null;
      this.connections.delete(socket);
      if (peerLinkId) {
        this.peerLinks.removeInbound(peerLinkId, socket);
        peerLinkId = null;
      }
      if (sessionKey) {
        const existing = this.sessions.get(sessionKey);
        if (existing?.socket === socket) {
          this.rememberDisconnectedSession(existing);
          this.sessions.delete(sessionKey);
          this.pruneMessageReceiptRoutes();
          this.broadcastScoped({ type: "session_left", sessionId: existing.info.id }, existing.info, sessionKey, existing.scopeId);
          this.federationRoster.reconcileLocalRoster();
          this.recomputeNamespaceOwners();
        }
      }
      this.scheduleShutdownCheck();
    });
    socket.on("error", (error) => {
      console.error("Socket error:", error);
    });
  }
  evictOldestUnregisteredConnections(currentSocket) {
    while (this.unregisteredConnections.size > MAX_UNREGISTERED_CONNECTIONS) {
      const [oldest] = this.unregisteredConnections;
      if (!oldest) {
        return;
      }
      if (oldest === currentSocket && this.unregisteredConnections.size === 1) {
        return;
      }
      this.unregisteredConnections.delete(oldest);
      oldest.destroy();
    }
  }
  consumeToken(connection, now = Date.now()) {
    const elapsedMs = now - connection.lastRefillAt;
    if (elapsedMs > 0) {
      connection.tokens = Math.min(
        RATE_LIMIT_CAPACITY,
        connection.tokens + elapsedMs * RATE_LIMIT_REFILL_PER_SECOND / 1e3
      );
      connection.lastRefillAt = now;
    }
    if (connection.tokens < 1) {
      return false;
    }
    connection.tokens -= 1;
    return true;
  }
  consumeAckToken(connection, now = Date.now()) {
    const elapsedMs = now - connection.lastAckRefillAt;
    if (elapsedMs > 0) {
      connection.ackTokens = Math.min(
        ACK_RATE_LIMIT_CAPACITY,
        connection.ackTokens + elapsedMs * ACK_RATE_LIMIT_REFILL_PER_SECOND / 1e3
      );
      connection.lastAckRefillAt = now;
    }
    if (connection.ackTokens < 1) return false;
    connection.ackTokens -= 1;
    return true;
  }
  cancelShutdownTimer() {
    if (!this.shutdownTimer) return;
    clearTimeout(this.shutdownTimer);
    this.shutdownTimer = null;
  }
  scheduleShutdownCheck() {
    if (this.shuttingDown || this.shutdownTimer) return;
    this.shutdownTimer = setTimeout(() => {
      this.shutdownTimer = null;
      if (this.sessions.size === 0 && this.peerLinks.size === 0) {
        console.log("No sessions connected, shutting down");
        this.shutdown();
      }
    }, 5e3);
  }
  listLocallyOwnedFederationSessions() {
    return [...this.sessions.values()].map((session) => ({
      ownership: "local",
      exportEligible: !session.info.isSubagent,
      conversationCapable: this.supportsRemoteConversations(session),
      localScopeId: session.scopeId ?? null,
      info: session.info
    }));
  }
  /**
   * The canonical federation origin belongs to this broker install. First
   * federation use adopts a controller-supplied canonical id when offered, or
   * mints a fresh install identity; afterwards every controller must present
   * exactly it.
   */
  getCanonicalFederationOrigin(preferred) {
    if (this.federationOrigin) return { id: this.federationOrigin.originId };
    const originId = preferred && isCanonicalFederationOriginId(preferred.id) ? preferred.id : mintFederationOriginId();
    this.federationOrigin = { originId, mintedAt: Date.now() };
    try {
      persistFederationOrigin(PARLEY_DIR, this.federationOrigin);
    } catch (error) {
      this.federationOrigin = void 0;
      throw new FederationPeerError("E_INVALID_REQUEST", "Failed to persist the canonical federation origin", { cause: error });
    }
    return { id: originId };
  }
  enforceCanonicalFederationOrigin(requested) {
    const canonical = this.getCanonicalFederationOrigin(requested);
    if (canonical.id !== requested.id) {
      throw new FederationPeerError(
        "E_ORIGIN_MISMATCH",
        `Local federation origin is fixed to ${canonical.id} for this broker install; read it with broker_list_scopes.`
      );
    }
  }
  summarizeScopes() {
    const counts = /* @__PURE__ */ new Map();
    for (const session of this.sessions.values()) {
      counts.set(session.namedScopeId ?? null, (counts.get(session.namedScopeId ?? null) ?? 0) + 1);
    }
    return [...counts.entries()].map(([scopeId, liveSessions]) => ({ scopeId, liveSessions })).sort((left, right) => (left.scopeId ?? "").localeCompare(right.scopeId ?? "")).slice(0, 64);
  }
  handleFederationPeerMessage(link, value) {
    if (isPeerSendRequest(value)) {
      this.handleFederationPeerSend(link, value);
      return;
    }
    if (isPeerSendResult(value)) {
      this.handleFederationPeerSendResult(link, value);
      return;
    }
    if (!link.features.includes(FEDERATION_ROSTER_FEATURE)) {
      throw new FederationPeerError("E_FEATURE_UNSUPPORTED", "Peer sent a roster frame without negotiating roster support");
    }
    if (!this.federationRoster.handlePeerFrame(link.linkId, value)) {
      throw new FederationPeerError("E_FEATURE_UNSUPPORTED", "Unsupported federation peer frame");
    }
  }
  conversationEndpoints(link, local, imported) {
    const tuple = this.federationRoster.findExportedTuple(link.linkId, imported.info.federation.remoteScopeAlias, local.info.id);
    if (!tuple || !this.supportsRemoteConversations(local) || !local.info.endpointEpoch || !imported.info.endpointEpoch) return;
    return {
      local: {
        originId: link.localOrigin.id,
        originEpoch: this.federationOriginEpoch,
        scopeAlias: tuple.scopeAlias,
        stableSessionId: tuple.stableSessionId,
        endpointEpoch: local.info.endpointEpoch
      },
      remote: {
        originId: link.remoteOrigin.id,
        originEpoch: imported.originEpoch,
        scopeAlias: imported.info.federation.remoteScopeAlias,
        stableSessionId: imported.info.federation.remoteStableSessionId,
        endpointEpoch: imported.info.endpointEpoch
      }
    };
  }
  /** Destination side: resolve a peer send against broker-authoritative state and deliver. */
  handleFederationPeerSend(link, frame) {
    if (!link.features.includes(FEDERATION_SEND_FEATURE) || !link.features.includes(FEDERATION_ROSTER_FEATURE)) {
      throw new FederationPeerError("E_FEATURE_UNSUPPORTED", "Peer sent a routed send without negotiating send support");
    }
    if (frame.originId !== link.remoteOrigin.id) {
      throw new FederationPeerError("E_ORIGIN_MISMATCH", "Routed send origin does not match the peer link");
    }
    const fail = (code, error) => {
      this.writeBrokerFrame(link.socket, this.peerSendResultFrame(link, frame.sendId, false, code, error));
    };
    let dedup = this.federationSendDedup.get(link.linkId);
    if (!dedup) {
      dedup = new PeerSendDedup();
      this.federationSendDedup.set(link.linkId, dedup);
    }
    if (!dedup.observe(frame.sendId)) {
      fail("E_SEND_DUPLICATE", "This routed send was already observed on the link");
      return;
    }
    const sender = this.federationRoster.findImportedByRemoteTuple(
      link.linkId,
      frame.senderScopeAlias,
      frame.senderStableSessionId
    );
    if (!sender) {
      fail("E_SEND_UNAUTHORIZED", "Sending session is not present in the federated roster for this link");
      return;
    }
    const binding = link.scopeBindings.find((candidate) => candidate.localScopeAlias === frame.targetScopeAlias);
    if (!binding) {
      fail("E_SEND_INVALID", "Target scope alias was never exported on this link");
      return;
    }
    const target = this.sessions.get(scopedSessionKey(effectiveScope(binding.localScopeId ?? void 0, MERGED_SCOPES), frame.targetStableSessionId));
    if (!target || !sameScope(target.scopeId, sender.localScopeId ?? void 0) || !canSeeSession(sender.info, target.info)) {
      fail("E_SEND_TARGET_NOT_FOUND", "Target session is not visible to the sending session on this broker");
      return;
    }
    if (frame.targetEndpointEpoch !== void 0) {
      if (!link.features.includes(FEDERATION_EXACT_SEND_FEATURE)) {
        fail("E_SEND_UNSUPPORTED", "Endpoint-pinned delivery was not negotiated on this link");
        return;
      }
      if (target.info.endpointEpoch !== frame.targetEndpointEpoch) {
        fail("E_SEND_TARGET_REBOUND", "Target endpoint changed before delivery");
        return;
      }
    }
    const conversation = frame.senderOriginEpoch !== void 0;
    if (!conversation && sender.info.federation.conversation && this.supportsRemoteConversations(target)) {
      fail("E_SEND_INVALID", "Capable endpoints require an author-qualified conversation envelope");
      return;
    }
    let endpoints;
    if (conversation) {
      if (!sender.info.federation.conversation || !this.supportsRemoteConversations(target)) {
        fail("E_SEND_UNSUPPORTED", "Text conversations are not supported by both endpoints");
        return;
      }
      endpoints = this.conversationEndpoints(link, target, sender);
      const identity = decodeConversationMessageId(frame.message.id);
      if (!endpoints || !identity || !sameConversationEndpoint(identity, endpoints.remote) || frame.senderOriginEpoch !== endpoints.remote.originEpoch || frame.senderEndpointEpoch !== endpoints.remote.endpointEpoch) {
        fail("E_SEND_UNAUTHORIZED", "Conversation author incarnation does not match the sending endpoint");
        return;
      }
      if (frame.targetOriginEpoch !== endpoints.local.originEpoch) {
        fail("E_SEND_TARGET_REBOUND", "Target broker incarnation changed before delivery");
        return;
      }
      if (frame.message.replyTo && !this.federationConversations.permitsReply(frame.message.replyTo, endpoints.remote, endpoints.local)) {
        fail("E_SEND_UNAUTHORIZED", "Reply does not reverse a recorded conversation edge at these endpoint incarnations");
        return;
      }
    }
    const now = Date.now();
    const deliveredMessage = {
      id: frame.message.id,
      timestamp: frame.message.timestamp,
      brokerReceivedAt: now,
      brokerDeliveredAt: now,
      content: { text: frame.message.text },
      ...conversation ? {
        ...frame.message.replyTo ? { replyTo: frame.message.replyTo, completesAsk: conversationCompletesAsk(frame.message) } : {},
        ...frame.message.expectsReply !== void 0 ? { expectsReply: frame.message.expectsReply } : {},
        ...frame.message.senderWaitMode ? { senderWaitMode: frame.message.senderWaitMode } : {},
        ...frame.message.expectsReply ? { replyDeadline: now + this.askTimeoutMs } : {}
      } : {}
    };
    if (!this.deliveryEnvelopeFits(sender.info, deliveredMessage)) {
      fail("E_SEND_INVALID", "Message exceeds the enriched delivery frame limit");
      return;
    }
    {
      const dispatchId = conversation ? frame.message.id : JSON.stringify(["incoming", frame.originId, frame.senderScopeAlias, frame.senderStableSessionId, frame.message.id]);
      try {
        if (this.federationConversations.beginDispatch(dispatchId) === "existing") {
          fail("E_SEND_DUPLICATE", "This retained message may already have been delivered");
          return;
        }
      } catch {
        fail("E_SEND_INVALID", "Could not persist the conversation dispatch barrier");
        return;
      }
      if (conversation && endpoints) this.federationConversations.retain(frame.message.id, endpoints.remote, endpoints.local);
    }
    this.writeBrokerFrame(target.socket, { type: "message", from: sender.info, message: deliveredMessage });
    this.writeBrokerFrame(link.socket, this.peerSendResultFrame(link, frame.sendId, true, void 0, void 0, now));
  }
  /** Origin side: a correlated result is the only acceptance of delivery. */
  handleFederationPeerSendResult(link, frame) {
    if (!link.features.includes(FEDERATION_SEND_FEATURE)) {
      throw new FederationPeerError("E_FEATURE_UNSUPPORTED", "Peer sent a routed send result without negotiating send support");
    }
    if (frame.originId !== link.remoteOrigin.id) {
      throw new FederationPeerError("E_ORIGIN_MISMATCH", "Routed send result origin does not match the peer link");
    }
    const pending = this.federationPendingSends.peek(frame.sendId);
    if (!pending) return;
    if (pending.linkId !== link.linkId) {
      return;
    }
    this.federationPendingSends.resolve(frame.sendId);
    if (frame.ok) {
      this.federationInFlightMessageIds.delete(JSON.stringify([pending.senderKey, pending.messageId]));
      this.recordDelivery(pending.senderKey, pending.messageId, pending.fingerprint, "socket_delivered", void 0, void 0, false, void 0, void 0, void 0, pending.recipient);
      const session = this.sessions.get(pending.senderKey);
      if (session) this.writeDeliverySuccess(session.socket, pending.messageId, "socket_delivered", void 0, void 0, { recipient: pending.recipient });
      return;
    }
    if (pending.dispatchId && frame.code !== "E_SEND_DUPLICATE") {
      try {
        this.federationConversations.settleNotDelivered(pending.dispatchId, pending.dispatchAlias);
      } catch {
        this.failPendingFederatedSend(pending, "Destination confirmed nondelivery, but retry admission could not be persisted", "E_CONVERSATION_PERSISTENCE", false, true);
        return;
      }
    }
    this.failPendingFederatedSend(
      pending,
      frame.error,
      frame.code === "E_SEND_TARGET_REBOUND" ? "E_TARGET_REBOUND" : frame.code,
      frame.code === "E_SEND_TARGET_DISCONNECTED" || frame.code === "E_SEND_TARGET_REBOUND",
      frame.code !== "E_SEND_DUPLICATE"
    );
  }
  peerSendResultFrame(link, sendId, ok, code, error, deliveredAt) {
    return {
      type: "peer_send_result",
      protocol: FEDERATION_PROTOCOL_NAME,
      version: FEDERATION_PROTOCOL_VERSION,
      originId: link.localOrigin.id,
      sendId,
      ...ok ? { ok: true, deliveredAt: deliveredAt ?? Date.now() } : { ok: false, code, error }
    };
  }
  failPendingFederatedSend(pending, reason, code, retryable, outcomeKnown = true) {
    this.federationInFlightMessageIds.delete(JSON.stringify([pending.senderKey, pending.messageId]));
    this.recordDelivery(pending.senderKey, pending.messageId, pending.fingerprint, outcomeKnown ? "failed" : "unknown", reason, code, outcomeKnown && retryable, void 0, void 0, void 0, pending.recipient);
    const session = this.sessions.get(pending.senderKey);
    if (session) this.writeDeliveryFailure(session.socket, pending.messageId, reason, code, retryable, outcomeKnown, { recipient: pending.recipient });
  }
  ensureFederationSendSweep() {
    if (this.federationSendSweepTimer) return;
    this.federationSendSweepTimer = setInterval(() => {
      for (const expired of this.federationPendingSends.expire()) {
        this.failPendingFederatedSend(expired, "Remote federation acknowledgement timed out; delivery may have occurred", "E_DELIVERY_UNKNOWN", false, false);
      }
      if (this.federationPendingSends.size === 0 && this.federationSendSweepTimer) {
        clearInterval(this.federationSendSweepTimer);
        this.federationSendSweepTimer = null;
      }
    }, 1e3);
    this.federationSendSweepTimer.unref?.();
  }
  /**
   * Origin side of a routed federation direct send: validates the v1 direct
   * send contract, replays/records like a local delivery, and reports the
   * client outcome only when the destination broker's correlated result
   * arrives (or the link drops / the correlation deadline passes).
   */
  attemptFederatedSend(socket, currentKey, fromSession, qualifiedId, message, contactKind, imported, targetEndpointEpoch, allowLegacyResolution = false) {
    const fingerprint = this.deliveryFingerprint(message, qualifiedId, contactKind);
    const inFlightKey = JSON.stringify([currentKey, message.id]);
    if (this.federationInFlightMessageIds.has(inFlightKey)) {
      this.writeDeliveryFailure(socket, message.id, "A delivery for this message id is already in flight; its outcome is not yet known", "E_MESSAGE_ID_REUSE", false, false, { recipient: imported.info });
      return;
    }
    if (this.replayOrReject(socket, currentKey, message.id, fingerprint)) return;
    const reject = (reason, code, retryable = false) => {
      this.recordDelivery(currentKey, message.id, fingerprint, "failed", reason, code, retryable);
      this.writeDeliveryFailure(socket, message.id, reason, code, retryable);
    };
    if (contactKind !== "direct") {
      reject("Broadcast remains host-local in federation v1", "E_INVALID_MESSAGE");
      return;
    }
    if (message.supersedes) {
      reject("Remote supersession is not supported", "E_INVALID_MESSAGE");
      return;
    }
    if (message.content.attachments?.length) {
      reject("Attachments cannot cross federation yet", "E_INVALID_MESSAGE");
      return;
    }
    if (message.content.text.length > FEDERATION_SEND_TEXT_MAX_LENGTH) {
      reject(`Message text exceeds the ${FEDERATION_SEND_TEXT_MAX_LENGTH} character federated delivery limit`, "E_INVALID_MESSAGE");
      return;
    }
    const link = this.peerLinks.getLink(imported.linkId);
    if (!link || !link.features.includes(FEDERATION_SEND_FEATURE) || !link.features.includes(FEDERATION_ROSTER_FEATURE)) {
      reject("Remote session is roster-only on this link until both brokers negotiate routed delivery", "E_TARGET_NOT_FOUND");
      return;
    }
    if (targetEndpointEpoch !== void 0 && !link.features.includes(FEDERATION_EXACT_SEND_FEATURE)) {
      if (!allowLegacyResolution) {
        reject("Remote broker does not support endpoint-pinned delivery", "E_SEND_UNSUPPORTED");
        return;
      }
      targetEndpointEpoch = void 0;
    }
    const senderTuple = this.federationRoster.findExportedTuple(link.linkId, imported.info.federation.remoteScopeAlias, fromSession.info.id);
    if (!senderTuple) {
      reject("This session is not visible to remote peers", "E_SEND_UNAUTHORIZED");
      return;
    }
    const identity = decodeConversationMessageId(message.id);
    const conversation = Boolean(identity || message.expectsReply || message.replyTo || this.supportsRemoteConversations(fromSession) && imported.info.federation.conversation);
    const endpoints = conversation ? this.conversationEndpoints(link, fromSession, imported) : void 0;
    if (conversation) {
      if (!imported.info.federation.conversation) {
        reject("Remote peer does not support text conversations", "E_SEND_UNSUPPORTED");
        return;
      }
      if (!identity || !endpoints || !sameConversationEndpoint(identity, endpoints.local)) {
        reject("Conversation ID does not authenticate this author incarnation; preflight is required", "E_CONVERSATION_AUTHOR");
        return;
      }
      if (!this.federationConversations.isPrepared(message.id, endpoints.local, endpoints.remote)) {
        reject("Conversation preparation does not match these author and recipient incarnations", "E_CONVERSATION_TARGET");
        return;
      }
      if (message.replyTo && !this.federationConversations.permitsReply(message.replyTo, endpoints.local, endpoints.remote)) {
        reject(`No thread for that message between these sessions as they are now connected. A broker restart (this one started at ${BROKER_STARTED_AT}) or either session reconnecting ends earlier threads; send a new message instead of a reply`, "E_REPLY_TARGET");
        return;
      }
      targetEndpointEpoch = endpoints.remote.endpointEpoch;
    }
    const sendId = randomUUID8();
    const dispatchId = JSON.stringify(["outgoing", currentKey, message.id]);
    const aliasCandidate = conversation ? this.federationConversations.preparedScalarAlias(message.id) ?? identity?.nonce : void 0;
    const candidateDispatchAlias = aliasCandidate !== void 0 ? JSON.stringify(["outgoing", currentKey, aliasCandidate]) : void 0;
    let dispatchAlias;
    try {
      if (candidateDispatchAlias !== void 0 && this.federationConversations.hasDispatchAlias(dispatchId, candidateDispatchAlias)) {
        dispatchAlias = candidateDispatchAlias;
      }
    } catch (error) {
      this.writeDeliveryFailure(socket, message.id, error.message, "E_CONVERSATION_STATE_FAILURE", false, false);
      return;
    }
    const acceptedPartner = this.partnerDeliveryRecords(currentKey, message.id).some(([, record]) => record.state === "socket_delivered" || record.state === "queued");
    if (acceptedPartner) {
      reject("An associated caller identity was already accepted; it cannot be dispatched as a new operation", "E_MESSAGE_ID_REUSE");
      return;
    }
    const pending = this.federationPendingSends.add(sendId, {
      linkId: link.linkId,
      messageId: message.id,
      senderKey: currentKey,
      dispatchId,
      ...dispatchAlias !== void 0 ? { dispatchAlias } : {},
      recipient: { ...imported.info },
      fingerprint
    });
    if (!pending) {
      reject("The broker is saturated with pending remote deliveries; retry shortly", "E_TARGET_DISCONNECTED", true);
      return;
    }
    const frame = {
      type: "peer_send",
      protocol: FEDERATION_PROTOCOL_NAME,
      version: FEDERATION_PROTOCOL_VERSION,
      originId: link.localOrigin.id,
      sendId,
      senderScopeAlias: senderTuple.scopeAlias,
      senderStableSessionId: senderTuple.stableSessionId,
      targetScopeAlias: imported.info.federation.remoteScopeAlias,
      targetStableSessionId: imported.info.federation.remoteStableSessionId,
      ...targetEndpointEpoch !== void 0 ? { targetEndpointEpoch } : {},
      ...conversation && endpoints ? {
        senderEndpointEpoch: endpoints.local.endpointEpoch,
        senderOriginEpoch: endpoints.local.originEpoch,
        targetOriginEpoch: endpoints.remote.originEpoch
      } : {},
      message: {
        id: message.id,
        timestamp: message.timestamp,
        text: message.content.text,
        ...conversation ? {
          ...message.replyTo ? { replyTo: message.replyTo, completesAsk: conversationCompletesAsk(message) } : {},
          ...message.expectsReply !== void 0 ? { expectsReply: message.expectsReply } : {},
          ...message.senderWaitMode ? { senderWaitMode: message.senderWaitMode } : {}
        } : {}
      }
    };
    if (!isPeerSendRequest(frame)) {
      this.federationPendingSends.resolve(sendId);
      reject("Message cannot be represented safely for federated delivery", "E_INVALID_MESSAGE");
      return;
    }
    {
      try {
        if (this.federationConversations.beginDispatch(dispatchId, dispatchAlias) === "existing") {
          this.federationPendingSends.resolve(sendId);
          this.failPendingFederatedSend(pending, "This retained message may already have been dispatched", "E_DELIVERY_UNKNOWN", false, false);
          return;
        }
      } catch (error) {
        this.federationPendingSends.resolve(sendId);
        reject(
          error instanceof ConversationStoreError ? error.message : "Could not persist the conversation dispatch barrier; no peer frame was written",
          error instanceof ConversationStoreError ? error.code : "E_CONVERSATION_PERSISTENCE"
        );
        return;
      }
      this.invalidatePartnerDeliveryRecords(currentKey, message.id);
      if (conversation && endpoints) this.federationConversations.retain(message.id, endpoints.local, endpoints.remote);
      this.recordDelivery(currentKey, message.id, fingerprint, "unknown", "Conversation was dispatched; acknowledgement is pending", "E_DELIVERY_UNKNOWN", false, void 0, void 0, void 0, imported.info);
    }
    this.federationInFlightMessageIds.add(inFlightKey);
    try {
      this.writeBrokerFrame(link.socket, frame);
    } catch (error) {
      this.federationPendingSends.resolve(sendId);
      this.failPendingFederatedSend(pending, `Federation send write failed: ${error instanceof Error ? error.message : String(error)}; delivery may have occurred`, "E_DELIVERY_UNKNOWN", false, false);
      return;
    }
    this.ensureFederationSendSweep();
  }
  handleImportedRosterChange(change) {
    for (const imported of change.joined) {
      this.broadcastScoped(
        { type: "session_joined", session: imported.info },
        imported.info,
        void 0,
        imported.localScopeId ?? void 0
      );
    }
    for (const imported of change.updated) {
      this.broadcastScoped(
        { type: "presence_update", session: imported.info },
        imported.info,
        void 0,
        imported.localScopeId ?? void 0
      );
    }
    for (const imported of change.left) {
      this.broadcastScoped(
        { type: "session_left", sessionId: imported.info.id },
        imported.info,
        void 0,
        imported.localScopeId ?? void 0
      );
    }
  }
  async handleDialPeerControl(socket, request, signal) {
    try {
      this.enforceCanonicalFederationOrigin(request.localOrigin);
      const link = await this.peerLinks.dial(request, signal);
      if (socket.writable && !socket.destroyed) {
        try {
          this.writeBrokerFrame(socket, {
            type: "broker_dial_peer_result",
            requestId: request.requestId,
            ok: true,
            linkId: link.linkId
          });
        } catch (error) {
          this.peerLinks.closeLink(link.linkId);
          throw error;
        }
      } else {
        this.peerLinks.closeLink(link.linkId);
      }
    } catch (error) {
      const failure = error instanceof FederationPeerError ? error : new FederationPeerError("E_DIAL_FAILED", "Peer dial failed", { cause: error });
      if (socket.writable && !socket.destroyed) {
        try {
          this.writeBrokerFrame(socket, {
            type: "broker_dial_peer_result",
            requestId: request.requestId,
            ok: false,
            code: failure.code,
            error: failure.message
          });
        } catch {
        }
      }
      this.scheduleShutdownCheck();
    } finally {
      socket.end();
    }
  }
  handleMessage(socket, msg, currentKey, setKey) {
    if (typeof msg !== "object" || msg === null || !("type" in msg) || typeof msg.type !== "string") {
      throw new Error("Invalid client message");
    }
    const clientMessage = msg;
    const requiresEndpointAuth = typeof LISTEN_TARGET !== "string";
    const hasEndpointAuth = clientMessage.stateId === BROKER_STATE_ID;
    if (clientMessage.type === "health") {
      if (typeof clientMessage.requestId !== "string") {
        throw new Error("Invalid health message");
      }
      if (requiresEndpointAuth && !hasEndpointAuth) {
        throw new Error("Invalid parley TCP endpoint credentials");
      }
      this.writeBrokerFrame(socket, {
        type: "health_ok",
        requestId: clientMessage.requestId,
        protocol: PARLEY_PROTOCOL_NAME,
        version: PARLEY_PROTOCOL_VERSION,
        broker: {
          pid: process.pid,
          instanceId: this.runtimeLease.owner.claimId,
          ...BROKER_BUILD
        }
      });
      return;
    }
    if (requiresEndpointAuth && clientMessage.type === "register" && !hasEndpointAuth) {
      throw new Error("Invalid parley TCP endpoint credentials");
    }
    if (currentKey === null && clientMessage.type !== "register") {
      throw new Error(`Received ${clientMessage.type} before register`);
    }
    switch (clientMessage.type) {
      case "register": {
        if (!isSessionRegistration(clientMessage.session)) {
          throw new Error("Invalid register message");
        }
        if (currentKey) {
          throw new Error("Received duplicate register message");
        }
        let id = randomUUID8();
        if (clientMessage.sessionId !== void 0) {
          if (!isSessionId(clientMessage.sessionId)) {
            throw new Error("Invalid register sessionId");
          }
          id = clientMessage.sessionId;
        }
        const namedScopeId = normalizeScopeId(clientMessage.scopeId);
        const scopeId = effectiveScope(namedScopeId, MERGED_SCOPES);
        const key = scopedSessionKey(scopeId, id);
        const session = clientMessage.session;
        const extensions = session.extensions;
        const rawClientFeatures = clientMessage.clientFeatures;
        if (rawClientFeatures !== void 0 && (!Array.isArray(rawClientFeatures) || rawClientFeatures.length > MAX_CLIENT_FEATURES_PER_SESSION || !rawClientFeatures.every((feature) => typeof feature === "string" && feature.length > 0 && feature.length <= 128))) {
          throw new Error("Invalid register clientFeatures");
        }
        const clientFeatures = new Set(rawClientFeatures ?? []);
        if (extensions !== void 0) {
          if (!Array.isArray(extensions) || extensions.length > MAX_EXTENSIONS_PER_SESSION) {
            throw new Error(`Invalid extensions field (maximum ${MAX_EXTENSIONS_PER_SESSION})`);
          }
          for (const extension of extensions) {
            if (!this.validateExtensionCapability(extension)) {
              throw new Error(`Invalid extension capability: ${JSON.stringify(extension)}`);
            }
          }
        }
        this.pruneDisconnectedSessions();
        this.pruneMailboxMessages();
        const previous = this.sessions.get(key);
        if (!previous && this.sessions.size >= MAX_SESSIONS) {
          this.writeBrokerFrame(socket, { type: "error", error: "Too many registered parley sessions" });
          socket.destroy();
          break;
        }
        if (previous) {
          this.pruneMessageReceiptRoutes();
          previous.socket.end();
        }
        setKey(key);
        const effectiveName = this.dedupeSessionName(session.name, scopeId, key);
        const info = {
          id,
          endpointEpoch: randomUUID8(),
          ...effectiveName !== void 0 ? { name: effectiveName } : {},
          ...session.description !== void 0 ? { description: session.description } : {},
          ...session.runtimeFallbackAlias !== void 0 ? { runtimeFallbackAlias: session.runtimeFallbackAlias } : {},
          cwd: session.cwd,
          model: session.model,
          pid: session.pid,
          startedAt: session.startedAt,
          lastActivity: session.lastActivity,
          ...session.status !== void 0 ? { status: session.status } : {},
          ...session.tmuxPane !== void 0 ? { tmuxPane: session.tmuxPane } : {},
          ...session.app !== void 0 ? { app: session.app } : {},
          ...session.isSubagent !== void 0 ? { isSubagent: session.isSubagent } : {},
          ...session.supervisorSessionId !== void 0 ? { supervisorSessionId: session.supervisorSessionId } : {},
          ...session.supervisorName !== void 0 ? { supervisorName: session.supervisorName } : {},
          ...extensions?.length ? { extensions } : {},
          trustedLocal: typeof LISTEN_TARGET === "string" && process.platform !== "win32"
        };
        const connectedSession = {
          socket,
          info,
          key,
          ...scopeId ? { scopeId } : {},
          ...namedScopeId ? { namedScopeId } : {},
          lastPresenceBroadcastAt: Date.now(),
          ownerOrder: previous?.ownerOrder ?? this.nextOwnerOrder++,
          extensions,
          clientFeatures
        };
        this.sessions.set(key, connectedSession);
        this.disconnectedSessions.delete(key);
        this.cancelShutdownTimer();
        this.writeBrokerFrame(socket, {
          type: "registered",
          sessionId: id,
          features: [EXTENSION_BUS_FEATURE, EXACT_SEND_FEATURE, EXACT_IDENTITY_SEND_FEATURE, COMPACTION_AWARENESS_FEATURE, SESSION_PROFILE_FEATURE, CONVERSATION_CONTRACT_FEATURE, FEDERATED_CONVERSATION_FEATURE, PERSON_PROVENANCE_FEATURE],
          session: info
        });
        this.broadcastScoped({ type: "session_joined", session: info }, info, key, scopeId);
        this.federationRoster.reconcileLocalRoster();
        this.recomputeNamespaceOwners();
        this.flushMailboxForSession(connectedSession);
        if (extensions) {
          for (const ext of extensions) {
            const owner = this.namespaceOwners.get(scopedExtensionKey(scopeId, ext.namespace));
            this.writeBrokerFrame(socket, {
              type: "extension_owner",
              namespace: ext.namespace,
              ...owner ? { ownerId: owner.sessionId, ownerEpoch: owner.epoch } : {}
            });
            const state = this.extensionStateManager.loadState(scopedExtensionStateNamespace(scopeId, ext.namespace));
            if (state) {
              this.writeBrokerFrame(socket, {
                type: "extension_state",
                namespace: ext.namespace,
                revision: state.revision,
                payload: state.payload
              });
            }
          }
        }
        break;
      }
      case "unregister": {
        if (!currentKey) {
          throw new Error("Received unregister before register");
        }
        const existing = this.sessions.get(currentKey);
        if (existing?.socket === socket) {
          this.rememberDisconnectedSession(existing);
          this.sessions.delete(currentKey);
          this.pruneMessageReceiptRoutes();
          this.broadcastScoped({ type: "session_left", sessionId: existing.info.id }, existing.info, currentKey, existing.scopeId);
          this.federationRoster.reconcileLocalRoster();
          this.recomputeNamespaceOwners();
          this.scheduleShutdownCheck();
        }
        setKey(null);
        break;
      }
      case "extension_capabilities_update": {
        if (!currentKey) {
          throw new Error("Received extension_capabilities_update before register");
        }
        const session = this.sessions.get(currentKey);
        if (!session || session.socket !== socket) {
          throw new Error("Extension capability session not found");
        }
        const extensions = clientMessage.extensions;
        if (!Array.isArray(extensions) || extensions.length > MAX_EXTENSIONS_PER_SESSION) {
          throw new Error(`Invalid extensions field (maximum ${MAX_EXTENSIONS_PER_SESSION})`);
        }
        for (const extension of extensions) {
          if (!this.validateExtensionCapability(extension)) {
            throw new Error(`Invalid extension capability: ${JSON.stringify(extension)}`);
          }
        }
        session.extensions = extensions;
        if (extensions.length > 0) session.info.extensions = extensions;
        else delete session.info.extensions;
        this.broadcastScoped({ type: "presence_update", session: session.info }, session.info, currentKey, session.scopeId);
        this.recomputeNamespaceOwners();
        for (const extension of extensions) {
          const owner = this.namespaceOwners.get(scopedExtensionKey(session.scopeId, extension.namespace));
          this.writeBrokerFrame(socket, {
            type: "extension_owner",
            namespace: extension.namespace,
            ...owner ? { ownerId: owner.sessionId, ownerEpoch: owner.epoch } : {}
          });
          const state = this.extensionStateManager.loadState(scopedExtensionStateNamespace(session.scopeId, extension.namespace));
          if (state) {
            this.writeBrokerFrame(socket, {
              type: "extension_state",
              namespace: extension.namespace,
              revision: state.revision,
              payload: state.payload
            });
          }
        }
        break;
      }
      case "prepare_conversation": {
        const requester = currentKey ? this.sessions.get(currentKey) : void 0;
        if (!requester || requester.socket !== socket) throw new Error("Conversation preflight requires registration");
        if (typeof clientMessage.requestId !== "string") throw new Error("Invalid conversation preflight correlation");
        const fail = (code, error, outcomeKnown = true) => this.writeBrokerFrame(socket, {
          type: "conversation_prepare_failed",
          requestId: clientMessage.requestId,
          code,
          error,
          outcomeKnown
        });
        if (!requester.clientFeatures.has(FEDERATED_CONVERSATION_FEATURE)) {
          fail("E_SEND_UNSUPPORTED", "Client did not negotiate remote conversation preflight");
          break;
        }
        if (typeof clientMessage.to !== "string" || clientMessage.messageId !== void 0 && typeof clientMessage.messageId !== "string" || clientMessage.targetEpoch !== void 0 && typeof clientMessage.targetEpoch !== "string") {
          fail("E_INVALID_TARGET", "Invalid conversation preflight");
          break;
        }
        let recipient;
        let messageId = clientMessage.messageId;
        if (messageId !== void 0) {
          this.pruneDeliveryRecords();
          const record = this.deliveryRecords.get(this.deliveryRecordKey(requester.key, messageId));
          if (record?.state === "unknown") {
            fail("E_DELIVERY_UNKNOWN", "Previous delivery outcome is unknown; its identity cannot be converted", false);
            break;
          }
          if (record && (record.state === "socket_delivered" || record.state === "queued") && !decodeConversationMessageId(messageId)) {
            fail("E_MESSAGE_ID_REUSE", "An already-accepted scalar identity cannot be converted into a new conversation identity");
            break;
          }
          try {
            if (this.hasPriorOutgoingDispatch(requester.key, messageId)) {
              if (record) {
                if (!decodeConversationMessageId(messageId)) {
                  fail("E_MESSAGE_ID_REUSE", "An already-dispatched scalar identity cannot be converted into a new conversation identity");
                  break;
                }
              } else {
                fail("E_DELIVERY_UNKNOWN", "This retained message was dispatched before; its outcome is unknown and its identity cannot be converted", false);
                break;
              }
            }
          } catch (error) {
            fail("E_CONVERSATION_STATE_FAILURE", error.message, false);
            break;
          }
        }
        const imported = this.federationRoster.findImportedByQualifiedId(clientMessage.to, requester.scopeId ?? null);
        if (imported && canSeeSession(requester.info, imported.info)) {
          recipient = imported.info;
          if (clientMessage.targetEpoch !== void 0 && recipient.endpointEpoch !== clientMessage.targetEpoch) {
            fail("E_TARGET_REBOUND", "Target endpoint changed before preflight");
            break;
          }
          const link = this.peerLinks.getLink(imported.linkId);
          const endpoints = link ? this.conversationEndpoints(link, requester, imported) : void 0;
          if (!recipient.federation?.conversation || !endpoints) {
            fail("E_SEND_UNSUPPORTED", "Remote peer does not support text conversations");
            break;
          }
          const scalarAlias = messageId !== void 0 && !decodeConversationMessageId(messageId) ? messageId : void 0;
          try {
            messageId = this.federationConversations.prepare(endpoints.local, endpoints.remote, messageId);
            if (scalarAlias !== void 0) this.federationConversations.bindDispatchAlias(
              JSON.stringify(["outgoing", requester.key, messageId]),
              JSON.stringify(["outgoing", requester.key, scalarAlias])
            );
          } catch (error) {
            fail(
              error instanceof ConversationStoreError ? error.code : error.message.startsWith("E_") ? error.message : "E_CONVERSATION_ID",
              error instanceof ConversationStoreError ? error.message : "Conversation identity or endpoint binding is invalid"
            );
            break;
          }
        } else {
          const targets = this.findSessions(clientMessage.to, requester.scopeId, requester.key);
          if (targets.length === 1) recipient = targets[0].info;
          if (!recipient) {
            fail("E_TARGET_NOT_FOUND", "Conversation recipient is not visible or is ambiguous");
            break;
          }
          if (decodeConversationMessageId(messageId)) {
            fail("E_CONVERSATION_TARGET", "A remote conversation handle cannot be rebound to a local target");
            break;
          }
          if (clientMessage.targetEpoch !== void 0 && recipient.endpointEpoch !== clientMessage.targetEpoch) {
            fail("E_TARGET_REBOUND", "Target endpoint changed before preflight");
            break;
          }
          messageId ??= randomUUID8();
        }
        this.writeBrokerFrame(socket, {
          type: "conversation_prepared",
          requestId: clientMessage.requestId,
          prepared: { messageId, author: { ...requester.info }, recipient: { ...recipient } }
        });
        break;
      }
      case "list": {
        if (typeof clientMessage.requestId !== "string") {
          throw new Error("Invalid list message");
        }
        const requester = currentKey ? this.sessions.get(currentKey) : void 0;
        if (!requester || requester.socket !== socket) {
          throw new Error("List session not found");
        }
        const localSessions = Array.from(this.sessions.values()).filter((session) => sameScope(session.scopeId, requester.scopeId) && canSeeSession(requester.info, session.info)).map((s) => s.info);
        const importedSessions = this.federationRoster.listImported().filter((session) => sameScope(session.localScopeId ?? void 0, requester.scopeId) && canSeeSession(requester.info, session.info)).map((session) => session.info);
        this.writeBrokerFrame(socket, {
          type: "sessions",
          requestId: clientMessage.requestId,
          sessions: [...localSessions, ...importedSessions]
        });
        break;
      }
      case "advertise": {
        if (!currentKey) {
          throw new Error("Received advertise before register");
        }
        const requestId = clientMessage.requestId;
        if (typeof requestId !== "string") {
          throw new Error("Invalid advertise message");
        }
        this.writeBrokerFrame(socket, {
          type: "advertise_result",
          requestId,
          ok: false,
          error: "Subagents are scoped to their supervisor tree and cannot advertise",
          code: "E_NOT_ELIGIBLE"
        });
        break;
      }
      case "send": {
        if (!currentKey) {
          throw new Error("Received send before register");
        }
        const message = clientMessage.message;
        const messageId = isAuthoredMessage(message) ? message.id : "unknown";
        if (typeof clientMessage.to !== "string" || !isAuthoredMessage(message)) {
          this.writeDeliveryFailure(socket, messageId, "Invalid message format", "E_INVALID_MESSAGE");
          break;
        }
        const contactKind = clientMessage.contactKind ?? "direct";
        if (contactKind !== "direct" && contactKind !== "broadcast") {
          this.writeDeliveryFailure(socket, message.id, "Invalid contact kind", "E_INVALID_MESSAGE");
          break;
        }
        const fromSession = this.sessions.get(currentKey);
        if (!fromSession || fromSession.socket !== socket) {
          this.writeDeliveryFailure(socket, message.id, "Sender session not found", "E_SENDER_NOT_FOUND");
          break;
        }
        this.pruneDeliveryRecords();
        if (this.rejectRetainedUnknown(socket, currentKey, message.id)) break;
        const hasTargetId = clientMessage.targetId !== void 0;
        const hasTargetEpoch = clientMessage.targetEpoch !== void 0;
        const exactIdentity = clientMessage.targetMode === "identity";
        const invalidTarget = exactIdentity ? !hasTargetId || hasTargetEpoch || !isSessionId(clientMessage.targetId) || clientMessage.to !== clientMessage.targetId : hasTargetId !== hasTargetEpoch || clientMessage.targetMode !== void 0 && (!hasTargetId || !hasTargetEpoch || clientMessage.targetMode !== "resolved" && clientMessage.targetMode !== "snapshot") || hasTargetId && (typeof clientMessage.targetId !== "string" || clientMessage.targetId.length === 0) || hasTargetEpoch && (typeof clientMessage.targetEpoch !== "string" || clientMessage.targetEpoch.length === 0);
        if (invalidTarget) {
          this.writeDeliveryFailure(socket, message.id, "Exact target requires an id and endpoint epoch, or an exact local identity without an epoch", "E_INVALID_TARGET");
          break;
        }
        if (exactIdentity && !fromSession.clientFeatures.has(EXACT_IDENTITY_SEND_FEATURE)) {
          this.writeDeliveryFailure(socket, message.id, "Client did not negotiate exact-identity delivery", "E_SEND_UNSUPPORTED");
          break;
        }
        const routingTarget = hasTargetId ? clientMessage.targetId : clientMessage.to;
        if (routingTarget.startsWith(RESERVED_SESSION_NAME_PREFIX)) {
          const fingerprint = this.deliveryFingerprint(message, routingTarget, contactKind);
          if (this.federationInFlightMessageIds.has(JSON.stringify([currentKey, message.id]))) {
            this.writeDeliveryFailure(socket, message.id, "A delivery for this message id is already in flight; its outcome is not yet known", "E_MESSAGE_ID_REUSE", false, false);
            break;
          }
          if (this.replayOrReject(socket, currentKey, message.id, fingerprint)) break;
          const imported = this.federationRoster.findImportedByQualifiedId(routingTarget, fromSession.scopeId ?? null);
          if (!imported || !canSeeSession(fromSession.info, imported.info)) {
            const reason = "Remote federation session is not present in the federated roster";
            this.recordDelivery(currentKey, message.id, fingerprint, "failed", reason, "E_TARGET_NOT_FOUND");
            this.writeDeliveryFailure(socket, message.id, reason, "E_TARGET_NOT_FOUND");
            break;
          }
          if (hasTargetEpoch && imported.info.endpointEpoch !== clientMessage.targetEpoch) {
            this.recordDelivery(currentKey, message.id, fingerprint, "failed", "Target endpoint changed before delivery", "E_TARGET_REBOUND", true);
            this.writeDeliveryFailure(socket, message.id, "Target endpoint changed before delivery", "E_TARGET_REBOUND", true);
            break;
          }
          this.attemptFederatedSend(
            socket,
            currentKey,
            fromSession,
            routingTarget,
            message,
            contactKind,
            imported,
            hasTargetEpoch ? clientMessage.targetEpoch : void 0,
            clientMessage.targetMode === "resolved"
          );
          break;
        }
        if (decodeConversationMessageId(message.id) || decodeConversationMessageId(message.replyTo)) {
          this.writeDeliveryFailure(socket, message.id, "Remote conversation handles cannot route through local scalar message tables", "E_CONVERSATION_TARGET");
          break;
        }
        const brokerReceivedAt = Date.now();
        this.pruneAskEdges();
        this.pruneMessageReceiptRoutes(brokerReceivedAt);
        const replyRoute = message.replyTo ? this.messageReceiptRoutes.get(message.replyTo) ?? this.askEdges.get(message.replyTo) : void 0;
        const completesAsk = message.completesAsk ?? Boolean(message.replyTo && !message.expectsReply);
        if (hasTargetId && hasTargetEpoch) {
          const targetId = clientMessage.targetId;
          const targetEpoch = clientMessage.targetEpoch;
          const fingerprint = this.deliveryFingerprint(message, targetId, contactKind);
          if (this.replayOrReject(socket, currentKey, message.id, fingerprint)) {
            break;
          }
          const exactTarget = this.sessions.get(scopedSessionKey(fromSession.scopeId, targetId));
          const exactTargetVisible = exactTarget ? this.isVisibleTo(currentKey, exactTarget.info) : false;
          if (!exactTarget || !exactTargetVisible) {
            this.recordDelivery(currentKey, message.id, fingerprint, "failed", "Session not found", "E_TARGET_NOT_FOUND");
            this.writeDeliveryFailure(socket, message.id, "Session not found", "E_TARGET_NOT_FOUND");
            break;
          }
          if (exactTarget.info.endpointEpoch !== targetEpoch) {
            this.recordDelivery(currentKey, message.id, fingerprint, "failed", "Target endpoint changed before delivery", "E_TARGET_REBOUND", true);
            this.writeDeliveryFailure(socket, message.id, "Target endpoint changed before delivery", "E_TARGET_REBOUND", true);
            break;
          }
          clientMessage.to = targetId;
        }
        const targets = this.findSessions(clientMessage.to, fromSession.scopeId, currentKey, exactIdentity);
        if (targets.length === 1) {
          const target = targets[0];
          const fingerprint = this.deliveryFingerprint(message, target.info.id, contactKind);
          if (this.replayOrReject(socket, currentKey, message.id, fingerprint)) {
            break;
          }
          if (message.supersedes) {
            const supersededRoute = this.messageReceiptRoutes.get(message.supersedes);
            if (!supersededRoute || supersededRoute.from !== currentKey || supersededRoute.to !== target.key) {
              this.writeDeliveryFailure(socket, message.id, "Supersede target does not match a previous message from this sender to this receiver", "E_SUPERSEDE_TARGET");
              break;
            }
          }
          if (message.replyTo && (!replyRoute || replyRoute.to !== currentKey || replyRoute.from !== target.key)) {
            this.writeDeliveryFailure(socket, message.id, replyTargetReason(Boolean(replyRoute)), "E_REPLY_TARGET");
            break;
          }
          const senderContact = contactKind === "direct" && this.supportsCompactionAwareness(fromSession) ? this.directContactPlan(fromSession.scopeId, fromSession.info, target.info) : void 0;
          const receiverContact = contactKind === "direct" && this.supportsCompactionAwareness(target) ? this.directContactPlan(fromSession.scopeId, target.info, fromSession.info) : void 0;
          const receiverContactToken = receiverContact ? randomUUID8() : void 0;
          const deliveredMessage = {
            ...message,
            brokerReceivedAt,
            brokerDeliveredAt: Date.now(),
            ...message.expectsReply ? { replyDeadline: brokerReceivedAt + this.askTimeoutMs } : {},
            // Incoming messages never carry a note about the sender; only the sender's own receipt does.
            ...receiverContactToken ? { contactToken: receiverContactToken } : {},
            ...receiverContact?.durableBaseline ? { contactBaseline: true } : {}
          };
          if (!this.checkDeliveryEnvelope(socket, currentKey, message.id, fingerprint, fromSession.info, deliveredMessage)) break;
          if (message.expectsReply) {
            this.writePendingAskRecord(message, fromSession, target.info, brokerReceivedAt);
            this.askEdges.set(message.id, {
              from: currentKey,
              to: target.key,
              ...fromSession.scopeId ? { scopeId: fromSession.scopeId } : {},
              createdAt: brokerReceivedAt
            });
          }
          const {
            clientToken: senderContactToken,
            stagedBaselineToken: senderBaselineToken
          } = this.prepareSenderContact(currentKey, senderContact);
          if (receiverContact && receiverContactToken) this.trackDirectContact(target.key, receiverContact, receiverContactToken);
          if (message.supersedes) {
            const control = {
              action: "supersede",
              messageId: message.supersedes,
              supersededBy: message.id,
              timestamp: Date.now()
            };
            this.writeBrokerFrame(target.socket, {
              type: "message_control",
              from: fromSession.info,
              control
            });
          }
          this.writeBrokerFrame(target.socket, {
            type: "message",
            from: fromSession.info,
            message: deliveredMessage
          });
          this.finalizeSenderContact(senderContact, senderBaselineToken);
          if (message.supersedes) {
            this.askEdges.delete(message.supersedes);
            this.removePendingAskRecord(message.supersedes, fromSession.scopeId);
          }
          if (message.replyTo && completesAsk) {
            this.askEdges.delete(message.replyTo);
            this.removePendingAskRecord(message.replyTo, fromSession.scopeId);
          }
          this.messageReceiptRoutes.set(message.id, {
            from: currentKey,
            to: target.key,
            createdAt: brokerReceivedAt
          });
          this.recordDelivery(
            currentKey,
            message.id,
            fingerprint,
            "socket_delivered",
            void 0,
            void 0,
            false,
            senderContact?.notice,
            senderContactToken,
            senderContact,
            target.info
          );
          this.writeDeliverySuccess(
            socket,
            message.id,
            "socket_delivered",
            senderContact?.notice,
            senderContactToken,
            { recipient: target.info }
          );
          break;
        }
        if (targets.length > 1) {
          this.writeDeliveryFailure(socket, message.id, `Multiple sessions named "${clientMessage.to}" are connected. Use the session ID instead.`, "E_AMBIGUOUS_TARGET");
          break;
        }
        const disconnectedTargets = this.findDisconnectedSessions(clientMessage.to, fromSession.scopeId, currentKey, exactIdentity);
        if (disconnectedTargets.length === 1) {
          if (contactKind === "broadcast") {
            this.writeDeliveryFailure(socket, message.id, "Broadcast recipients must still be connected", "E_TARGET_NOT_FOUND");
            break;
          }
          const disconnectedTarget = disconnectedTargets[0];
          const target = disconnectedTarget.info;
          const fingerprint = this.deliveryFingerprint(message, target.id, contactKind);
          if (this.replayOrReject(socket, currentKey, message.id, fingerprint)) {
            break;
          }
          if (message.supersedes) {
            this.writeDeliveryFailure(socket, message.id, "Supersede target is not connected", "E_SUPERSEDE_TARGET");
            break;
          }
          if (message.replyTo && (!replyRoute || replyRoute.to !== currentKey || replyRoute.from !== disconnectedTarget.key)) {
            this.writeDeliveryFailure(socket, message.id, replyTargetReason(Boolean(replyRoute)), "E_REPLY_TARGET");
            break;
          }
          if (message.expectsReply) {
            this.writeDeliveryFailure(socket, message.id, "Target session is not currently connected; asks are not queued", "E_TARGET_DISCONNECTED");
            break;
          }
          const liveMailboxTarget = this.findUniqueLiveSessionForDisconnectedSession(disconnectedTarget, currentKey);
          const acceptedTarget = liveMailboxTarget?.info ?? target;
          const senderContact = this.supportsCompactionAwareness(fromSession) ? this.directContactPlan(
            fromSession.scopeId,
            fromSession.info,
            acceptedTarget,
            liveMailboxTarget !== null,
            target.id
          ) : void 0;
          const receiverContact = !liveMailboxTarget || this.supportsCompactionAwareness(liveMailboxTarget) ? this.directContactPlan(fromSession.scopeId, acceptedTarget, fromSession.info) : void 0;
          const receiverContactToken = receiverContact ? randomUUID8() : void 0;
          const deliveredMessage = {
            ...message,
            brokerReceivedAt,
            brokerDeliveredAt: Date.now(),
            // Incoming messages never carry a note about the sender; only the sender's own receipt does.
            ...receiverContactToken ? { contactToken: receiverContactToken } : {},
            ...receiverContact?.durableBaseline ? { contactBaseline: true } : {}
          };
          if (!this.checkDeliveryEnvelope(socket, currentKey, message.id, fingerprint, fromSession.info, deliveredMessage)) break;
          const {
            clientToken: senderContactToken,
            stagedBaselineToken: senderBaselineToken
          } = this.prepareSenderContact(currentKey, senderContact);
          if (liveMailboxTarget) {
            if (receiverContact && receiverContactToken) this.trackDirectContact(liveMailboxTarget.key, receiverContact, receiverContactToken);
            this.writeBrokerFrame(liveMailboxTarget.socket, {
              type: "message",
              from: fromSession.info,
              message: deliveredMessage
            });
            this.messageReceiptRoutes.set(message.id, { from: currentKey, to: liveMailboxTarget.key, createdAt: brokerReceivedAt });
          } else {
            this.queueMailboxMessage(fromSession, disconnectedTarget, message, contactKind, brokerReceivedAt);
          }
          this.finalizeSenderContact(senderContact, senderBaselineToken);
          if (message.replyTo && completesAsk) {
            this.askEdges.delete(message.replyTo);
            this.removePendingAskRecord(message.replyTo, fromSession.scopeId);
          }
          const acceptedDelivery = liveMailboxTarget ? "socket_delivered" : "queued";
          this.recordDelivery(
            currentKey,
            message.id,
            fingerprint,
            acceptedDelivery,
            void 0,
            void 0,
            false,
            senderContact?.notice,
            senderContactToken,
            senderContact,
            acceptedTarget
          );
          this.writeDeliverySuccess(
            socket,
            message.id,
            acceptedDelivery,
            senderContact?.notice,
            senderContactToken,
            { recipient: acceptedTarget }
          );
          break;
        }
        if (disconnectedTargets.length > 1) {
          this.writeDeliveryFailure(socket, message.id, `Multiple disconnected sessions named "${clientMessage.to}" can receive queued mail. Use the session ID instead.`, "E_AMBIGUOUS_TARGET");
          break;
        }
        this.writeDeliveryFailure(socket, message.id, "Session not found", "E_TARGET_NOT_FOUND");
        break;
      }
      case "compaction_completed": {
        if (!currentKey) {
          throw new Error("Received compaction_completed before register");
        }
        if (typeof clientMessage.eventId !== "string" || clientMessage.eventId.length === 0 || clientMessage.eventId.length > 128) {
          throw new Error("Invalid compaction event ID");
        }
        const session = this.sessions.get(currentKey);
        if (!session || session.socket !== socket) {
          throw new Error("Compaction session not found");
        }
        try {
          const compactedAt = Date.now();
          const state = this.collaborationState.recordSuccessfulCompaction(
            session.scopeId,
            session.info.id,
            clientMessage.eventId,
            compactedAt
          );
          session.info.lastActivity = compactedAt;
          session.lastPresenceBroadcastAt = compactedAt;
          this.broadcastScoped(
            { type: "presence_update", session: session.info },
            session.info,
            currentKey,
            session.scopeId
          );
          this.federationRoster.reconcileLocalRoster();
          this.writeBrokerFrame(socket, {
            type: "compaction_recorded",
            eventId: clientMessage.eventId,
            generation: state.generation,
            compactedAt: state.compactedAt
          });
        } catch (error) {
          console.error("Failed to record successful compaction:", error);
          this.writeBrokerFrame(socket, {
            type: "compaction_record_failed",
            eventId: clientMessage.eventId,
            error: "Failed to persist compaction awareness"
          });
        }
        break;
      }
      case "direct_contact_seen": {
        if (!currentKey) {
          throw new Error("Received direct_contact_seen before register");
        }
        if (typeof clientMessage.token !== "string" || clientMessage.token.length === 0) {
          throw new Error("Invalid direct_contact_seen token");
        }
        const observer = this.sessions.get(currentKey);
        if (observer?.socket === socket) {
          const outcome = this.acknowledgeDirectContact(
            currentKey,
            clientMessage.token,
            observer.scopeId,
            observer.info.id
          );
          if (outcome === "accepted") {
            this.writeBrokerFrame(socket, { type: "direct_contact_recorded", token: clientMessage.token });
          } else if (outcome === "unknown") {
            this.writeBrokerFrame(socket, { type: "direct_contact_unknown", token: clientMessage.token });
          }
        }
        break;
      }
      case "message_receipt": {
        if (!currentKey) {
          throw new Error("Received message_receipt before register");
        }
        if (!isMessageReceipt(clientMessage.receipt)) {
          throw new Error("Invalid message_receipt message");
        }
        this.pruneMessageReceiptRoutes();
        const route = this.messageReceiptRoutes.get(clientMessage.receipt.messageId);
        const receiver = this.sessions.get(currentKey);
        const sender = route ? this.sessions.get(route.from) : void 0;
        if (route?.to === currentKey && receiver?.socket === socket && sender) {
          this.writeBrokerFrame(sender.socket, {
            type: "message_receipt",
            from: receiver.info,
            receipt: clientMessage.receipt
          });
        }
        break;
      }
      case "cancel_message": {
        if (!currentKey) throw new Error("Received cancel_message before register");
        if (typeof clientMessage.messageId !== "string" || typeof clientMessage.requestId !== "string" || !clientMessage.requestId.trim()) {
          throw new Error("Invalid cancel_message message");
        }
        const sender = this.sessions.get(currentKey);
        if (sender?.socket !== socket) throw new Error("Sender session not found");
        const result = this.withdrawMessage(sender, clientMessage.messageId);
        const { accepted, ...details } = result;
        this.writeBrokerFrame(socket, {
          type: accepted ? "delivered" : "delivery_failed",
          messageId: clientMessage.messageId,
          requestId: clientMessage.requestId,
          ...details
        });
        break;
      }
      case "cancel_ask": {
        if (!currentKey) throw new Error("Received cancel_ask before register");
        if (typeof clientMessage.messageId !== "string") throw new Error("Invalid cancel_ask message");
        const sender = this.sessions.get(currentKey);
        if (sender?.socket === socket) {
          this.withdrawMessage(sender, clientMessage.messageId);
        }
        break;
      }
      case "presence": {
        if (!currentKey) {
          throw new Error("Received presence before register");
        }
        const session = this.sessions.get(currentKey);
        if (session?.socket === socket) {
          let changed = false;
          const identityLocked = session.info.advertised === true;
          if (clientMessage.name !== void 0) {
            if (!isValidSessionName(clientMessage.name)) {
              throw new Error("Invalid presence name");
            }
            if (!identityLocked) {
              const effectiveName = this.dedupeSessionName(clientMessage.name, session.scopeId, currentKey);
              if (effectiveName !== void 0 && session.info.name !== effectiveName) {
                session.info.name = effectiveName;
                changed = true;
              }
            }
          }
          if (clientMessage.description !== void 0) {
            if (clientMessage.description !== null && !isValidSessionDescription(clientMessage.description)) {
              throw new Error("Invalid presence description");
            }
            if (clientMessage.description === null) {
              if (session.info.description !== void 0) {
                delete session.info.description;
                changed = true;
              }
            } else if (session.info.description !== clientMessage.description) {
              session.info.description = clientMessage.description;
              changed = true;
            }
          }
          if (clientMessage.runtimeFallbackAlias !== void 0) {
            if (typeof clientMessage.runtimeFallbackAlias !== "boolean") {
              throw new Error("Invalid presence runtimeFallbackAlias");
            }
            if (!identityLocked && session.info.runtimeFallbackAlias !== clientMessage.runtimeFallbackAlias) {
              session.info.runtimeFallbackAlias = clientMessage.runtimeFallbackAlias;
              changed = true;
            }
          }
          if (clientMessage.status !== void 0) {
            if (typeof clientMessage.status !== "string") {
              throw new Error("Invalid presence status");
            }
            if (session.info.status !== clientMessage.status) {
              session.info.status = clientMessage.status;
              changed = true;
            }
          }
          if (clientMessage.model !== void 0) {
            if (typeof clientMessage.model !== "string") {
              throw new Error("Invalid presence model");
            }
            if (session.info.model !== clientMessage.model) {
              session.info.model = clientMessage.model;
              changed = true;
            }
          }
          const now = Date.now();
          session.info.lastActivity = now;
          if (changed || now - session.lastPresenceBroadcastAt >= PRESENCE_HEARTBEAT_MS) {
            session.lastPresenceBroadcastAt = now;
            this.broadcastScoped({ type: "presence_update", session: session.info }, session.info, currentKey, session.scopeId);
            this.federationRoster.reconcileLocalRoster();
          }
        }
        break;
      }
      case "extension_publish": {
        this.handleExtensionPublish(socket, currentKey, clientMessage);
        break;
      }
      case "extension_state_commit": {
        this.handleExtensionStateCommit(socket, currentKey, clientMessage);
        break;
      }
      default:
        throw new Error(`Unknown client message type: ${clientMessage.type}`);
    }
  }
  rememberDisconnectedSession(session, now = Date.now()) {
    const { advertised, ...info } = session.info;
    this.disconnectedSessions.set(session.key, {
      info,
      key: session.key,
      ...session.scopeId ? { scopeId: session.scopeId } : {},
      disconnectedAt: now
    });
    this.pruneDisconnectedSessions(now);
  }
  pruneDisconnectedSessions(now = Date.now()) {
    for (const [sessionId, session] of this.disconnectedSessions) {
      if (now - session.disconnectedAt > DISCONNECTED_SESSION_RETENTION_MS) {
        this.disconnectedSessions.delete(sessionId);
      }
    }
  }
  // Fork: when a queued mailbox message can no longer be delivered, the
  // sender gets an explicit receipt instead of silence. Without this a send
  // to a dead session reports "delivered (queued)" and then quietly expires
  // up to a day later with no feedback at all.
  notifyMailboxUndelivered(entry, detail) {
    const sender = this.sessions.get(entry.fromKey);
    if (!sender) {
      return;
    }
    this.writeBrokerFrame(sender.socket, {
      type: "message_receipt",
      from: entry.target,
      receipt: {
        messageId: entry.message.id,
        status: "expired",
        timestamp: Date.now(),
        detail
      }
    });
  }
  pruneMailboxMessages(now = Date.now()) {
    for (let index = this.mailboxMessages.length - 1; index >= 0; index -= 1) {
      const entry = this.mailboxMessages[index];
      if (now - entry.queuedAt > MAILBOX_MESSAGE_RETENTION_MS) {
        if (entry.message.expectsReply) {
          this.askEdges.delete(entry.message.id);
          this.removePendingAskRecord(entry.message.id, entry.fromScopeId);
        }
        this.notifyMailboxUndelivered(entry, "Mailbox delivery expired: the target session never reconnected");
        this.messageReceiptRoutes.delete(entry.message.id);
        this.updateDeliveryRecord(entry.fromKey, entry.message.id, "failed", "Mailbox delivery expired", "E_DELIVERY_EXPIRED");
        this.mailboxMessages.splice(index, 1);
      }
    }
  }
  queueMailboxMessage(from, target, message, contactKind, brokerReceivedAt) {
    this.pruneMailboxMessages(brokerReceivedAt);
    while (this.mailboxMessages.length >= MAX_MAILBOX_MESSAGES) {
      const evicted = this.mailboxMessages.shift();
      if (!evicted) break;
      if (evicted.message.expectsReply) {
        this.askEdges.delete(evicted.message.id);
        this.removePendingAskRecord(evicted.message.id, evicted.fromScopeId);
      }
      this.notifyMailboxUndelivered(evicted, "Mailbox capacity evicted the delivery before the target session reconnected");
      this.messageReceiptRoutes.delete(evicted.message.id);
      this.updateDeliveryRecord(evicted.fromKey, evicted.message.id, "failed", "Mailbox capacity evicted the delivery", "E_DELIVERY_EVICTED");
    }
    this.mailboxMessages.push({
      from: { ...from.info },
      fromKey: from.key,
      ...from.scopeId ? { fromScopeId: from.scopeId } : {},
      target: { ...target.info },
      targetKey: target.key,
      ...target.scopeId ? { targetScopeId: target.scopeId } : {},
      message: { ...message, brokerReceivedAt },
      contactKind,
      queuedAt: brokerReceivedAt
    });
  }
  withdrawMessage(sender, messageId) {
    this.pruneMessageReceiptRoutes();
    this.pruneMailboxMessages();
    this.pruneDeliveryRecords();
    const record = this.deliveryRecords.get(this.deliveryRecordKey(sender.key, messageId));
    const accepted = (cancellation, recipient) => ({
      accepted: true,
      delivery: "socket_delivered",
      outcomeKnown: true,
      retryable: false,
      cancellation,
      ...recipient ? { recipient } : {}
    });
    if (record?.cancellation) return accepted(record.cancellation, record.recipient);
    const clearAsk = () => {
      const edge = this.askEdges.get(messageId);
      if (edge?.from !== sender.key) return;
      this.askEdges.delete(messageId);
      this.removePendingAskRecord(messageId, sender.scopeId);
    };
    const queuedIndex = this.mailboxMessages.findIndex((entry) => entry.message.id === messageId && entry.fromKey === sender.key);
    if (queuedIndex >= 0) {
      const [entry] = this.mailboxMessages.splice(queuedIndex, 1);
      this.updateDeliveryRecord(sender.key, messageId, "failed", "Sender removed the queued delivery", "E_DELIVERY_CANCELLED");
      if (record) record.cancellation = "removed_from_mailbox";
      clearAsk();
      return accepted("removed_from_mailbox", entry.target);
    }
    const route = this.messageReceiptRoutes.get(messageId);
    if (record?.state === "failed" && !route && record.code !== "E_DELIVERY_SUPERSEDED") {
      record.cancellation = "not_delivered";
      clearAsk();
      return accepted("not_delivered", record.recipient);
    }
    const receiver = route?.from === sender.key ? this.sessions.get(route.to) : void 0;
    if (!receiver) {
      clearAsk();
      return {
        accepted: false,
        delivery: "failed",
        outcomeKnown: true,
        retryable: false,
        code: "E_CANCELLATION_UNAVAILABLE",
        reason: "No reachable recipient or queued message owned by this session; earlier work may already have happened"
      };
    }
    try {
      this.writeBrokerFrame(receiver.socket, {
        type: "message_control",
        from: sender.info,
        control: { action: "cancel", messageId, timestamp: Date.now(), detail: "The sender withdrew this message. Work may already have happened." }
      });
    } catch {
      clearAsk();
      return {
        accepted: false,
        delivery: "unknown",
        outcomeKnown: false,
        retryable: false,
        code: "E_CANCELLATION_UNKNOWN",
        reason: "Withdrawal write failed; the recipient may or may not have received the notice",
        recipient: receiver.info
      };
    }
    clearAsk();
    if (record) record.cancellation = "withdrawal_requested";
    return accepted("withdrawal_requested", receiver.info);
  }
  writeDeliverySuccess(socket, messageId, delivery, peerCompaction, contactToken, details = {}) {
    this.writeBrokerFrame(socket, {
      type: "delivered",
      messageId,
      delivery,
      retryable: false,
      outcomeKnown: true,
      ...details,
      ...peerCompaction ? { peerCompaction } : {},
      ...contactToken ? { contactToken } : {}
    });
  }
  deliveryEnvelopeFits(from, message) {
    const bytes = serializedPayloadSize({ type: "message", from, message });
    return bytes !== null && bytes <= MAX_FRAME_BYTES;
  }
  /** Reject before any ask/contact/mailbox acceptance or receiver side effects. */
  checkDeliveryEnvelope(socket, senderKey, messageId, fingerprint, from, message) {
    if (this.deliveryEnvelopeFits(from, message)) return true;
    const reason = "Message exceeds the enriched delivery frame limit";
    this.recordDelivery(senderKey, messageId, fingerprint, "failed", reason, "E_MESSAGE_TOO_LARGE");
    this.writeDeliveryFailure(socket, messageId, reason, "E_MESSAGE_TOO_LARGE");
    return false;
  }
  writeDeliveryFailure(socket, messageId, reason, code, retryable = false, outcomeKnown = true, details = {}) {
    this.writeBrokerFrame(socket, { type: "delivery_failed", messageId, reason, delivery: outcomeKnown ? "failed" : "unknown", code, retryable: outcomeKnown && retryable, outcomeKnown, ...details });
  }
  deliveryFingerprint(message, targetId, contactKind) {
    return JSON.stringify({
      targetId,
      contactKind,
      text: message.content.text,
      attachments: message.content.attachments,
      replyTo: message.replyTo,
      expectsReply: message.expectsReply,
      completesAsk: message.completesAsk,
      senderWaitMode: message.senderWaitMode,
      supersedes: message.supersedes,
      retryOf: message.retryOf,
      provenance: message.provenance
    });
  }
  deliveryRecordKey(fromSessionId, messageId) {
    return JSON.stringify([fromSessionId, messageId]);
  }
  partnerDeliveryRecords(senderKey, messageId) {
    this.pruneDeliveryRecords();
    const dispatchId = JSON.stringify(["outgoing", senderKey, messageId]);
    return [...this.deliveryRecords].filter(([, record]) => record.senderKey === senderKey && record.messageId !== messageId && this.federationConversations.sharesDispatchIdentity(dispatchId, JSON.stringify(["outgoing", senderKey, record.messageId])));
  }
  /** A new durable admission replaces older negative verdicts of every
   * associated identity. Accepted partners prevent admission instead. */
  invalidatePartnerDeliveryRecords(senderKey, messageId) {
    for (const [key] of this.partnerDeliveryRecords(senderKey, messageId)) this.deliveryRecords.delete(key);
  }
  /** Authenticate retained ownership independently of links and ID conversion. */
  hasPriorOutgoingDispatch(fromSessionId, messageId) {
    const identity = decodeConversationMessageId(messageId);
    const sender = this.sessions.get(fromSessionId);
    const ownsAuthoredIdentity = !identity || sender && identity.stableSessionId === sender.info.id && identity.originId === this.federationOrigin?.originId;
    return Boolean(ownsAuthoredIdentity) && this.federationConversations.hasDispatched(JSON.stringify(["outgoing", fromSessionId, messageId]));
  }
  /** Passive prior-attempt rejection across every routing class. Volatile
   * receipts still provide their stronger current-process verdict. */
  rejectRetainedUnknown(socket, fromSessionId, messageId) {
    const record = this.deliveryRecords.get(this.deliveryRecordKey(fromSessionId, messageId));
    if (record) {
      if (record.outcomeKnown) return false;
      const inFlight = this.federationInFlightMessageIds.has(JSON.stringify([fromSessionId, messageId]));
      this.writeDeliveryFailure(
        socket,
        messageId,
        record.reason ?? "Previous delivery outcome is unknown",
        inFlight ? "E_MESSAGE_ID_REUSE" : record.code ?? "E_DELIVERY_UNKNOWN",
        false,
        false,
        { recipient: record.recipient, cancellation: record.cancellation }
      );
      return true;
    }
    try {
      if (!this.hasPriorOutgoingDispatch(fromSessionId, messageId)) return false;
      const inFlight = this.federationInFlightMessageIds.has(JSON.stringify([fromSessionId, messageId]));
      this.writeDeliveryFailure(
        socket,
        messageId,
        inFlight ? "A delivery for this message id is already in flight; its outcome is not yet known" : "This retained message was dispatched before; its recovered outcome is unknown",
        inFlight ? "E_MESSAGE_ID_REUSE" : "E_DELIVERY_UNKNOWN",
        false,
        false
      );
      return true;
    } catch (error) {
      this.writeDeliveryFailure(socket, messageId, error.message, "E_CONVERSATION_STATE_FAILURE", false, false);
      return true;
    }
  }
  replayOrReject(socket, fromSessionId, messageId, fingerprint) {
    this.pruneDeliveryRecords();
    const record = this.deliveryRecords.get(this.deliveryRecordKey(fromSessionId, messageId));
    if (!record) return this.rejectRetainedUnknown(socket, fromSessionId, messageId);
    if (record.fingerprint !== fingerprint) {
      this.writeDeliveryFailure(socket, messageId, "Message id was reused with different authored content", "E_MESSAGE_ID_REUSE");
      return true;
    }
    if (record.code === "E_TARGET_REBOUND" && record.retryable) {
      return false;
    }
    if (record.state === "socket_delivered" || record.state === "queued") {
      if (record.senderContact && !record.senderContact.durableBaseline && (!record.contactToken || !this.pendingDirectContacts.has(record.contactToken))) {
        record.contactToken = this.trackDirectContact(fromSessionId, record.senderContact);
      }
      this.writeDeliverySuccess(socket, messageId, record.state, record.peerCompaction, record.contactToken, { recipient: record.recipient, cancellation: record.cancellation });
    } else {
      this.writeDeliveryFailure(socket, messageId, record.reason ?? "Previous delivery failed", record.code ?? "E_DELIVERY_FAILED", record.retryable, record.outcomeKnown, { recipient: record.recipient, cancellation: record.cancellation });
    }
    return true;
  }
  recordDelivery(fromSessionId, messageId, fingerprint, state, reason, code, retryable = false, peerCompaction, contactToken, senderContact, recipient) {
    this.pruneDeliveryRecords();
    while (this.deliveryRecords.size >= MAX_DELIVERY_RECORDS) {
      const oldest = this.deliveryRecords.keys().next().value;
      if (oldest === void 0) break;
      this.deliveryRecords.delete(oldest);
    }
    this.deliveryRecords.set(this.deliveryRecordKey(fromSessionId, messageId), {
      senderKey: fromSessionId,
      messageId,
      fingerprint,
      state,
      ...reason ? { reason } : {},
      ...code ? { code } : {},
      retryable,
      outcomeKnown: state !== "unknown",
      ...recipient ? { recipient: { ...recipient } } : {},
      ...peerCompaction ? { peerCompaction } : {},
      ...contactToken ? { contactToken } : {},
      ...senderContact ? { senderContact } : {},
      createdAt: Date.now()
    });
  }
  pruneDeliveryRecords(now = Date.now()) {
    for (const [key, record] of this.deliveryRecords) {
      if (now - record.createdAt > DELIVERY_RECORD_RETENTION_MS) this.deliveryRecords.delete(key);
    }
  }
  updateDeliveryRecord(fromSessionId, messageId, state, reason, code) {
    const record = this.deliveryRecords.get(this.deliveryRecordKey(fromSessionId, messageId));
    if (!record) return;
    record.state = state;
    record.reason = reason;
    record.code = code;
    record.retryable = false;
    record.outcomeKnown = true;
  }
  flushMailboxForSession(session, now = Date.now()) {
    this.pruneMailboxMessages(now);
    const sessionName = session.info.name?.toLowerCase();
    const uniqueMailboxIdentity = this.findLiveSessionsSharingMailboxIdentity(session).length === 1;
    for (let index = 0; index < this.mailboxMessages.length; ) {
      const entry = this.mailboxMessages[index];
      if (!sameScope(entry.targetScopeId, session.scopeId)) {
        index += 1;
        continue;
      }
      const matchesId = entry.targetKey === session.key;
      const matchesSenderIdentity = Boolean(
        sessionName && sameScope(entry.fromScopeId, session.scopeId) && entry.from.name?.toLowerCase() === sessionName && sameCwd(entry.from.cwd, session.info.cwd)
      );
      const matchesUniqueName = Boolean(
        uniqueMailboxIdentity && sessionName && !matchesSenderIdentity && entry.target.name?.toLowerCase() === sessionName && sameCwd(entry.target.cwd, session.info.cwd)
      );
      if (!matchesId && !matchesUniqueName) {
        index += 1;
        continue;
      }
      const liveSender = this.sessions.get(entry.fromKey);
      if (!canSeeSession(entry.from, session.info)) {
        index += 1;
        continue;
      }
      const receiverContact = entry.contactKind === "direct" && this.supportsCompactionAwareness(session) ? this.directContactPlan(
        session.scopeId,
        session.info,
        liveSender?.info ?? entry.from,
        liveSender !== void 0
      ) : void 0;
      const receiverContactToken = receiverContact ? randomUUID8() : void 0;
      const deliveredMessage = {
        ...entry.message,
        brokerDeliveredAt: Date.now(),
        // Incoming messages never carry a note about the sender; only the sender's own receipt does.
        ...receiverContactToken ? { contactToken: receiverContactToken } : {},
        ...receiverContact?.durableBaseline ? { contactBaseline: true } : {}
      };
      if (!this.deliveryEnvelopeFits(entry.from, deliveredMessage)) {
        this.notifyMailboxUndelivered(entry, "Mailbox delivery exceeds the frame limit after receiver enrichment");
        this.messageReceiptRoutes.delete(entry.message.id);
        this.updateDeliveryRecord(entry.fromKey, entry.message.id, "failed", "Message exceeds the enriched delivery frame limit", "E_MESSAGE_TOO_LARGE");
        this.mailboxMessages.splice(index, 1);
        continue;
      }
      if (receiverContact && receiverContactToken) this.trackDirectContact(session.key, receiverContact, receiverContactToken);
      this.writeBrokerFrame(session.socket, {
        type: "message",
        from: entry.from,
        message: deliveredMessage
      });
      this.mailboxMessages.splice(index, 1);
      const edge = this.askEdges.get(entry.message.id);
      if (edge?.to === entry.targetKey) {
        edge.to = session.key;
      }
      this.messageReceiptRoutes.set(entry.message.id, {
        from: entry.fromKey,
        to: session.key,
        createdAt: entry.message.brokerReceivedAt ?? entry.queuedAt
      });
      this.updateDeliveryRecord(entry.fromKey, entry.message.id, "socket_delivered");
      const record = this.deliveryRecords.get(this.deliveryRecordKey(entry.fromKey, entry.message.id));
      if (record) record.recipient = { ...session.info };
    }
  }
  pruneAskEdges(now = Date.now()) {
    this.prunePendingAskRecords(now);
    for (const [messageId, edge] of this.askEdges) {
      if (now - edge.createdAt > this.askTimeoutMs) {
        this.askEdges.delete(messageId);
        this.removePendingAskRecord(messageId, edge.scopeId);
      }
    }
  }
  clearAskEdgesForSession(sessionKey) {
    for (const [messageId, edge] of this.askEdges) {
      if (edge.from === sessionKey || edge.to === sessionKey) {
        this.askEdges.delete(messageId);
        this.removePendingAskRecord(messageId, edge.scopeId);
      }
    }
  }
  writePendingAskRecord(message, from, target, createdAt) {
    ensurePendingAskRecordDir();
    const record = {
      askId: message.id,
      messageId: message.id,
      asker: { sessionId: from.info.id, name: from.info.name ?? null },
      target: { sessionId: target.id, name: target.name ?? null },
      question: message.content.text,
      createdAt,
      expiresAt: createdAt + this.askTimeoutMs
    };
    const filePath = scopedPendingAskRecordPath(from.scopeId, message.id);
    writeFileSync5(filePath, `${JSON.stringify(record, null, 2)}
`, { mode: PARLEY_RUNTIME_FILE_MODE });
    restrictParleyRuntimeFile(filePath);
  }
  removePendingAskRecord(messageId, scopeId) {
    try {
      unlinkSync(scopedPendingAskRecordPath(scopeId, messageId));
    } catch (error) {
      if (!isRecord6(error) || error.code !== "ENOENT") {
        throw error;
      }
    }
  }
  prunePendingAskRecords(now = Date.now()) {
    ensurePendingAskRecordDir();
    for (const entry of readdirSync2(PENDING_ASKS_DIR, { withFileTypes: true })) {
      if (!entry.isFile() || !entry.name.endsWith(".json")) {
        continue;
      }
      const filePath = join9(PENDING_ASKS_DIR, entry.name);
      let parsed;
      try {
        parsed = JSON.parse(readFileSync8(filePath, "utf-8"));
      } catch {
        unlinkSync(filePath);
        continue;
      }
      if (!isPendingAskRecord(parsed) || now > parsed.expiresAt) {
        unlinkSync(filePath);
      }
    }
  }
  // Thread ownership belongs to stable scoped identities, not one socket lifetime.
  // Keep the bounded relationship on reconnect; live receipt/control delivery still checks current sockets.
  pruneMessageReceiptRoutes(now = Date.now()) {
    for (const [messageId, route] of this.messageReceiptRoutes) {
      if (now - route.createdAt > MESSAGE_RECEIPT_ROUTE_RETENTION_MS || this.messageReceiptRoutes.size > MAX_DELIVERY_RECORDS) {
        this.messageReceiptRoutes.delete(messageId);
      }
    }
  }
  // ACL fork: every lookup path is scoped through the requester's own
  // visibility. A hidden session behaves exactly like a nonexistent one —
  // callers get "Session not found", never a distinguishable ACL error, so
  // existence of an out-of-scope session is never leaked.
  // Fork: registration/presence names are de-duplicated against every other
  // live session in the same scope (independent of ACL visibility, so no
  // observer can ever face a by-name ambiguity) so roster names are always
  // uniquely addressable. A colliding name is auto-suffixed ("name-2",
  // "name-3", ...) instead of failing at send time with E_AMBIGUOUS_TARGET.
  // Deliberate advertise claims keep the stricter reject (E_NAME_TAKEN).
  dedupeSessionName(name, scopeId, selfKey) {
    const trimmed = name?.trim();
    if (!trimmed) {
      return name;
    }
    const taken = (candidate) => {
      const lower = candidate.toLowerCase();
      return Array.from(this.sessions.values()).some(
        (session) => session.key !== selfKey && sameScope(session.scopeId, scopeId) && (session.info.name?.toLowerCase() === lower || session.info.id === candidate)
      );
    };
    if (!taken(trimmed)) {
      return name;
    }
    for (let attempt = 2; attempt < 1e3; attempt += 1) {
      const candidate = `${trimmed}-${attempt}`;
      if (!taken(candidate)) {
        return candidate;
      }
    }
    return `${trimmed}-${randomUUID8().slice(0, 8)}`;
  }
  isVisibleTo(observerKey, subject) {
    const observer = this.sessions.get(observerKey);
    if (!observer) {
      return false;
    }
    return canSeeSession(observer.info, subject);
  }
  findSessions(nameOrId, scopeId, requesterKey, exactIdentity = false) {
    const visible = (session) => this.isVisibleTo(requesterKey, session.info);
    const byId = this.sessions.get(scopedSessionKey(scopeId, nameOrId));
    if (byId) {
      return visible(byId) ? [byId] : [];
    }
    if (exactIdentity) return [];
    const lowerName = nameOrId.toLowerCase();
    const byName = Array.from(this.sessions.values()).filter((session) => sameScope(session.scopeId, scopeId) && session.info.name?.toLowerCase() === lowerName && visible(session));
    if (byName.length > 0) {
      return byName;
    }
    return Array.from(this.sessions.entries()).filter(([, session]) => sameScope(session.scopeId, scopeId) && session.info.id.startsWith(nameOrId) && visible(session)).map(([, session]) => session);
  }
  findDisconnectedSessions(nameOrId, scopeId, requesterKey, exactIdentity = false) {
    this.pruneDisconnectedSessions();
    const observer = this.sessions.get(requesterKey);
    const visible = (session) => Boolean(observer) && canSeeSession(observer.info, session.info);
    const byId = this.disconnectedSessions.get(scopedSessionKey(scopeId, nameOrId));
    if (byId) {
      return visible(byId) ? [byId] : [];
    }
    if (exactIdentity) return [];
    const lowerName = nameOrId.toLowerCase();
    const byName = Array.from(this.disconnectedSessions.values()).filter((session) => sameScope(session.scopeId, scopeId) && session.info.name?.toLowerCase() === lowerName && visible(session));
    if (byName.length > 0) {
      return byName;
    }
    return Array.from(this.disconnectedSessions.entries()).filter(([, session]) => sameScope(session.scopeId, scopeId) && session.info.id.startsWith(nameOrId) && visible(session)).map(([, session]) => session);
  }
  findUniqueLiveSessionForDisconnectedSession(disconnected, senderKey) {
    const matches = this.findLiveSessionsSharingMailboxIdentity(disconnected).filter((session) => session.key !== senderKey && this.isVisibleTo(senderKey, session.info));
    return matches.length === 1 ? matches[0] : null;
  }
  /**
   * Mailbox identity is an explicit name plus directory, never name alone. A
   * runtime fallback alias is derived from the session id rather than chosen as
   * a durable identity, so it must not transfer mail to another process. This
   * also prevents two unnamed UUIDv7 sessions started close together from
   * inheriting each other's mailbox through a shared short alias.
   *
   * Directories compare through sameCwd so a relaunch that reports the same
   * directory differently (trailing slash, "."/"..", or a symlink such as macOS
   * /tmp vs /private/tmp) still matches.
   */
  findLiveSessionsSharingMailboxIdentity(sessionInfo) {
    const lowerName = sessionInfo.info.name?.toLowerCase();
    if (!lowerName || sessionInfo.info.runtimeFallbackAlias) {
      return [];
    }
    return Array.from(this.sessions.values()).filter(
      (session) => sameScope(session.scopeId, sessionInfo.scopeId) && !session.info.runtimeFallbackAlias && session.info.name?.toLowerCase() === lowerName && sameCwd(session.info.cwd, sessionInfo.info.cwd)
    );
  }
  broadcast(msg, exclude, scopeId) {
    for (const [id, session] of this.sessions) {
      if (id !== exclude && sameScope(session.scopeId, scopeId)) {
        this.writeBrokerFrame(session.socket, msg);
      }
    }
  }
  // ACL fork: like broadcast(), but only reaches sessions that are allowed to
  // see `subject` (the session that joined/left/changed presence) within the
  // same scope. Used for session_joined, session_left, and presence_update so
  // hidden peers never leak into a client's live session cache via these push
  // events, even though the pull-based "list" response is separately filtered
  // too.
  broadcastScoped(msg, subject, exclude, scopeId) {
    for (const [id, session] of this.sessions) {
      if (id === exclude) {
        continue;
      }
      if (!sameScope(session.scopeId, scopeId)) {
        continue;
      }
      if (!canSeeSession(session.info, subject)) {
        continue;
      }
      this.writeBrokerFrame(session.socket, msg);
    }
  }
  validateExtensionCapability(cap) {
    if (typeof cap !== "object" || cap === null) {
      return false;
    }
    const c = cap;
    if (typeof c.namespace !== "string" || typeof c.ownerEligible !== "boolean") {
      return false;
    }
    return this.validateNamespace(c.namespace);
  }
  validateNamespace(ns) {
    if (ns.length === 0 || ns.length > 64) {
      return false;
    }
    if (!/^[a-z0-9]/.test(ns)) {
      return false;
    }
    if (!/^[a-z0-9][a-z0-9._/-]*$/.test(ns)) {
      return false;
    }
    return true;
  }
  recomputeNamespaceOwners() {
    const namespaces = /* @__PURE__ */ new Map();
    for (const [key, owner] of this.namespaceOwners) {
      namespaces.set(key, {
        namespace: owner.namespace,
        ...owner.scopeId ? { scopeId: owner.scopeId } : {}
      });
    }
    for (const session of this.sessions.values()) {
      for (const extension of session.extensions ?? []) {
        namespaces.set(scopedExtensionKey(session.scopeId, extension.namespace), {
          namespace: extension.namespace,
          ...session.scopeId ? { scopeId: session.scopeId } : {}
        });
      }
    }
    for (const [namespaceKey, scopedNamespace] of namespaces) {
      const { namespace, scopeId } = scopedNamespace;
      const candidates = [];
      for (const [sessionKey, session] of this.sessions) {
        if (session.extensions) {
          const hasNamespace = session.extensions.some(
            (ext) => sameScope(session.scopeId, scopeId) && ext.namespace === namespace && ext.ownerEligible
          );
          if (hasNamespace) {
            candidates.push({ sessionKey, session });
          }
        }
      }
      if (candidates.length === 0) {
        if (this.namespaceOwners.delete(namespaceKey)) {
          for (const session of this.sessions.values()) {
            const isCapable = sameScope(session.scopeId, scopeId) && session.extensions?.some((extension) => extension.namespace === namespace);
            if (isCapable) {
              this.writeBrokerFrame(session.socket, { type: "extension_owner", namespace });
            }
          }
        }
        continue;
      }
      candidates.sort((a, b) => {
        if (a.session.ownerOrder !== b.session.ownerOrder) {
          return a.session.ownerOrder - b.session.ownerOrder;
        }
        return a.session.info.id.localeCompare(b.session.info.id);
      });
      const winner = candidates[0];
      const existing = this.namespaceOwners.get(namespaceKey);
      const ownerChanged = !existing || existing.sessionKey !== winner.sessionKey;
      const socketChanged = existing && existing.socket !== winner.session.socket;
      if (ownerChanged || socketChanged) {
        const epoch = randomUUID8();
        this.namespaceOwners.set(namespaceKey, {
          namespace,
          sessionKey: winner.sessionKey,
          sessionId: winner.session.info.id,
          socket: winner.session.socket,
          epoch,
          ...scopeId ? { scopeId } : {}
        });
        for (const session of this.sessions.values()) {
          if (session.extensions?.length) {
            const isCapable = sameScope(session.scopeId, scopeId) && session.extensions.some((ext) => ext.namespace === namespace);
            if (isCapable) {
              this.writeBrokerFrame(session.socket, {
                type: "extension_owner",
                namespace,
                ownerId: winner.session.info.id,
                ownerEpoch: epoch
              });
            }
          }
        }
      }
    }
  }
  handleExtensionPublish(socket, currentKey, msg) {
    if (!currentKey) {
      throw new Error("Received extension_publish before register");
    }
    const session = this.sessions.get(currentKey);
    if (!session || session.socket !== socket) {
      this.writeBrokerFrame(socket, { type: "error", error: "Session not found" });
      return;
    }
    if (!session.extensions?.length) {
      this.writeBrokerFrame(socket, { type: "error", error: "Session has not advertised extension capability" });
      return;
    }
    const namespace = msg.namespace;
    const audience = msg.audience;
    const ownerOnly = msg.ownerOnly === true;
    const ownerEpoch = msg.ownerEpoch;
    const payload = msg.payload;
    if (typeof namespace !== "string" || !this.validateNamespace(namespace)) {
      this.writeBrokerFrame(socket, { type: "error", error: "Invalid namespace" });
      return;
    }
    if (audience !== "owner" && audience !== "capable") {
      this.writeBrokerFrame(socket, { type: "error", error: "Invalid audience" });
      return;
    }
    const payloadSize = serializedPayloadSize(payload);
    if (payloadSize === null || payloadSize > MAX_EXTENSION_MESSAGE_BYTES) {
      this.writeBrokerFrame(socket, { type: "error", error: "Invalid extension payload or payload exceeds 16 KiB limit" });
      return;
    }
    const hasCapability = session.extensions?.some((ext) => ext.namespace === namespace);
    if (!hasCapability) {
      this.writeBrokerFrame(socket, { type: "error", error: "Sender does not have capability for this namespace" });
      return;
    }
    const owner = this.namespaceOwners.get(scopedExtensionKey(session.scopeId, namespace));
    if ((audience === "owner" || ownerOnly) && !owner) {
      this.writeBrokerFrame(socket, { type: "error", error: "No owner for this namespace" });
      return;
    }
    if (ownerOnly && owner) {
      if (typeof ownerEpoch !== "string") {
        this.writeBrokerFrame(socket, { type: "error", error: "ownerEpoch required for owner-only messages" });
        return;
      }
      if (currentKey !== owner.sessionKey || socket !== owner.socket || ownerEpoch !== owner.epoch) {
        this.writeBrokerFrame(socket, { type: "error", error: "Owner validation failed" });
        return;
      }
    }
    for (const [recipientId, recipientSession] of this.sessions) {
      if (!sameScope(recipientSession.scopeId, session.scopeId)) {
        continue;
      }
      if (!recipientSession.extensions?.length) {
        continue;
      }
      const isCapable = recipientSession.extensions.some((ext) => ext.namespace === namespace);
      if (!isCapable) {
        continue;
      }
      const shouldReceive = audience === "capable" || audience === "owner" && owner !== void 0 && recipientId === owner.sessionKey && recipientSession.socket === owner.socket;
      if (shouldReceive) {
        this.writeBrokerFrame(recipientSession.socket, {
          type: "extension_message",
          namespace,
          fromSessionId: session.info.id,
          ...owner ? { ownerId: owner.sessionId, ownerEpoch: owner.epoch } : {},
          payload
        });
      }
    }
  }
  handleExtensionStateCommit(socket, currentKey, msg) {
    if (!currentKey) {
      throw new Error("Received extension_state_commit before register");
    }
    const session = this.sessions.get(currentKey);
    if (!session || session.socket !== socket) {
      this.writeBrokerFrame(socket, {
        type: "extension_state_result",
        namespace: String(msg.namespace || ""),
        committed: false,
        revision: 0,
        reason: "Session not found"
      });
      return;
    }
    if (!session.extensions?.length) {
      this.writeBrokerFrame(socket, {
        type: "extension_state_result",
        namespace: String(msg.namespace || ""),
        committed: false,
        revision: 0,
        reason: "Session has not advertised extension capability"
      });
      return;
    }
    const namespace = msg.namespace;
    const ownerEpoch = msg.ownerEpoch;
    const expectedRevision = msg.expectedRevision;
    const payload = msg.payload;
    if (typeof namespace !== "string" || !this.validateNamespace(namespace)) {
      this.writeBrokerFrame(socket, {
        type: "extension_state_result",
        namespace: String(namespace),
        committed: false,
        revision: 0,
        reason: "Invalid namespace"
      });
      return;
    }
    const stateNamespace = scopedExtensionStateNamespace(session.scopeId, namespace);
    if (typeof ownerEpoch !== "string") {
      this.writeBrokerFrame(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(stateNamespace),
        reason: "Invalid ownerEpoch"
      });
      return;
    }
    if (typeof expectedRevision !== "number" || !Number.isSafeInteger(expectedRevision) || expectedRevision < 0) {
      this.writeBrokerFrame(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(stateNamespace),
        reason: "Invalid expectedRevision"
      });
      return;
    }
    const payloadSize = serializedPayloadSize(payload);
    if (payloadSize === null || payloadSize > MAX_EXTENSION_STATE_BYTES) {
      this.writeBrokerFrame(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(stateNamespace),
        reason: "Invalid extension state or payload exceeds 64 KiB limit"
      });
      return;
    }
    const hasCapability = session.extensions?.some((ext) => ext.namespace === namespace);
    if (!hasCapability) {
      this.writeBrokerFrame(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(stateNamespace),
        reason: "Sender does not have capability for this namespace"
      });
      return;
    }
    const owner = this.namespaceOwners.get(scopedExtensionKey(session.scopeId, namespace));
    if (!owner) {
      this.writeBrokerFrame(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(stateNamespace),
        reason: "No owner for this namespace"
      });
      return;
    }
    if (currentKey !== owner.sessionKey || socket !== owner.socket || ownerEpoch !== owner.epoch) {
      this.writeBrokerFrame(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(stateNamespace),
        reason: "Owner validation failed"
      });
      return;
    }
    const result = this.extensionStateManager.commitState(stateNamespace, expectedRevision, payload);
    this.writeBrokerFrame(socket, {
      type: "extension_state_result",
      namespace,
      committed: result.committed,
      revision: result.revision,
      reason: result.reason
    });
    if (result.committed) {
      for (const recipientSession of this.sessions.values()) {
        if (!sameScope(recipientSession.scopeId, session.scopeId)) {
          continue;
        }
        if (!recipientSession.extensions?.length) {
          continue;
        }
        const isCapable = recipientSession.extensions.some((ext) => ext.namespace === namespace);
        if (isCapable) {
          this.writeBrokerFrame(recipientSession.socket, {
            type: "extension_state",
            namespace,
            revision: result.revision,
            payload
          });
        }
      }
    }
  }
  shutdown() {
    if (this.shuttingDown) return;
    this.shuttingDown = true;
    console.log("Broker shutting down");
    this.cancelShutdownTimer();
    if (this.maintenanceTimer) {
      clearInterval(this.maintenanceTimer);
      this.maintenanceTimer = null;
    }
    if (this.federationSendSweepTimer) {
      clearInterval(this.federationSendSweepTimer);
      this.federationSendSweepTimer = null;
    }
    this.peerLinks.close();
    let exitCode = 0;
    try {
      this.collaborationState.close();
    } catch (error) {
      exitCode = 1;
      console.error("Failed to flush collaboration state during shutdown:", error);
    }
    for (const connection of this.connections) connection.destroy();
    this.connections.clear();
    this.sessions.clear();
    this.askEdges.clear();
    this.messageReceiptRoutes.clear();
    this.pendingDirectContacts.clear();
    this.disconnectedSessions.clear();
    this.mailboxMessages.length = 0;
    this.server.close((error) => {
      if (error) {
        exitCode = 1;
        console.error("Failed to close broker listener during shutdown:", error);
      }
      if (typeof LISTEN_TARGET === "string" && process.platform !== "win32") {
        try {
          unlinkSync(LISTEN_TARGET);
        } catch {
        }
      }
      try {
        unlinkSync(PORT_PATH);
      } catch {
      }
      try {
        unlinkSync(PID_PATH);
      } catch {
      }
      this.runtimeLease.release();
      process.exit(exitCode);
    });
  }
};
var runtimeLease;
try {
  runtimeLease = await claimBrokerRuntime(PARLEY_DIR, LISTEN_TARGET);
} catch (error) {
  if (error instanceof BrokerRuntimeOccupiedError) {
    console.error(error.message);
    process.exit(BROKER_RUNTIME_OCCUPIED_EXIT_CODE);
  }
  throw error;
}
try {
  new ParleyBroker(runtimeLease).start();
} catch (error) {
  runtimeLease.release();
  throw error;
}
