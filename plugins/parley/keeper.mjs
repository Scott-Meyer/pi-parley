import { createRequire as __parleyCreateRequire } from "node:module"; const require = __parleyCreateRequire(import.meta.url);

// link/keeper.ts
import { appendFileSync, mkdirSync as mkdirSync4, readFileSync as readFileSync2, renameSync, rmSync, writeFileSync as writeFileSync2 } from "node:fs";
import net3 from "node:net";
import { homedir as homedir3, hostname } from "node:os";
import { join as join6 } from "node:path";

// dist/federation.js
import net2 from "node:net";
import { randomUUID as randomUUID2 } from "node:crypto";
import { open as openFile } from "node:fs/promises";
import { platform, homedir } from "node:os";
import { isAbsolute as isAbsolute2, join as join2, posix, resolve as resolve2, win32 as win322 } from "node:path";
import { Duplex as Duplex2 } from "node:stream";

// dist/broker/attachment.js
import net from "node:net";
import { randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { Duplex } from "node:stream";

// dist/broker/framing.js
var MAX_FRAME_BYTES = 1024 * 1024;
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

// dist/broker/federation-types.js
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
var FEDERATION_CORRELATION_ID_MAX_LENGTH = 128;
var FEDERATION_DISPLAY_LABEL_MAX_LENGTH = 80;
var FEDERATION_MAX_FEATURES = 16;
var FEDERATION_MAX_SCOPE_MAPPINGS = 16;
var FEDERATION_ORIGIN_ID_MAX_LENGTH = 96;
var FEDERATION_SCOPE_ALIAS_MAX_LENGTH = 64;
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

// dist/broker/federation-protocol.js
var CONTROL_OR_FORMAT_CHARACTERS = /[\p{Cc}\p{Cf}]/u;
var ORIGIN_ID = /^[a-z][a-z0-9]*(?::[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$/;
var SCOPE_ALIAS = /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/;
var CORRELATION_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{7,127}$/;
var FEATURE = /^[a-z0-9][a-z0-9.-]*$/;
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
function isFederationOrigin(value) {
  if (!isRecord(value) || !hasOnlyKeys(value, ["id"], ["label"]))
    return false;
  return isCanonicalFederationOriginId(value.id) && (value.label === void 0 || isSafeText(value.label, FEDERATION_DISPLAY_LABEL_MAX_LENGTH));
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
  if (!Array.isArray(value) || value.length < 1 || value.length > FEDERATION_MAX_SCOPE_MAPPINGS || !value.every(isFederationScopeBinding) || !hasUniqueMappingAliases(value))
    return false;
  return new Set(value.map((binding) => binding.localScopeId)).size === value.length;
}
function isFeatures(value) {
  if (!Array.isArray(value) || value.length < 1 || value.length > FEDERATION_MAX_FEATURES)
    return false;
  if (!value.every((feature) => typeof feature === "string" && feature.length <= 64 && FEATURE.test(feature)))
    return false;
  if (new Set(value).size !== value.length)
    return false;
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
function isSuppliedPeerRequest(value, type) {
  if (!isRecord(value) || !hasOnlyKeys(value, ["type", "requestId", "linkId", "localOrigin", "remoteOrigin", "scopeBindings"], ["stateId"]))
    return false;
  return value.type === type && isFederationCorrelationId(value.requestId) && isFederationCorrelationId(value.linkId) && isFederationOrigin(value.localOrigin) && isFederationOrigin(value.remoteOrigin) && value.localOrigin.id !== value.remoteOrigin.id && isScopeBindings(value.scopeBindings) && (value.stateId === void 0 || isFederationCorrelationId(value.stateId));
}
function isBrokerAcceptPeerRequest(value) {
  return isSuppliedPeerRequest(value, "broker_accept_peer");
}
function isBrokerStartPeerRequest(value) {
  return isSuppliedPeerRequest(value, "broker_start_peer");
}
function isBrokerListScopesResult(value) {
  if (!isRecord(value) || !hasOnlyKeys(value, ["type", "requestId", "ok"], ["localOrigin", "scopes", "code", "error", "mergedScopes"]))
    return false;
  if (value.mergedScopes !== void 0 && (!Array.isArray(value.mergedScopes) || value.mergedScopes.length > 64 || !value.mergedScopes.every((scope) => typeof scope === "string" && scope.length > 0 && scope.length <= 256 && !/[\p{Cc}\p{Cf}]/u.test(scope))))
    return false;
  if (value.type !== "broker_list_scopes_result" || !isFederationCorrelationId(value.requestId))
    return false;
  if (value.ok === true) {
    if (!isRecord(value.localOrigin) || !hasOnlyKeys(value.localOrigin, ["id"], []) || !isCanonicalFederationOriginId(value.localOrigin.id))
      return false;
    if (!Array.isArray(value.scopes) || value.scopes.length > 64)
      return false;
    const seen = /* @__PURE__ */ new Set();
    for (const scope of value.scopes) {
      if (!isRecord(scope) || !hasOnlyKeys(scope, ["scopeId", "liveSessions"]))
        return false;
      const scopeId = scope.scopeId;
      if (scopeId !== null && (typeof scopeId !== "string" || scopeId.length === 0 || scopeId.length > 256 || /[\p{Cc}\p{Cf}]/u.test(scopeId)))
        return false;
      if (!Number.isSafeInteger(scope.liveSessions) || scope.liveSessions < 0)
        return false;
      if (seen.has(scopeId))
        return false;
      seen.add(scopeId);
    }
    return true;
  }
  if (value.ok !== false)
    return false;
  return typeof value.code === "string" && typeof value.error === "string" && value.error.length > 0 && value.error.length <= 256;
}
function isPeerHello(value) {
  if (!isRecord(value) || !hasOnlyKeys(value, ["type", "protocol", "version", "linkId", "origin", "expectedPeerOrigin", "scopeMappings", "features"]))
    return false;
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
  if (!isRecord(value) || value.type !== "peer_hello_ack" || value.protocol !== FEDERATION_PROTOCOL_NAME || value.version !== FEDERATION_PROTOCOL_VERSION || !isFederationCorrelationId(value.linkId) || typeof value.accepted !== "boolean")
    return false;
  if (value.accepted) {
    return hasOnlyKeys(value, ["type", "protocol", "version", "linkId", "accepted", "origin", "acceptedPeerOriginId", "scopeMappings", "features"]) && isFederationOrigin(value.origin) && isCanonicalFederationOriginId(value.acceptedPeerOriginId) && value.origin.id !== value.acceptedPeerOriginId && isScopeMappings(value.scopeMappings) && isFeatures(value.features);
  }
  return hasOnlyKeys(value, ["type", "protocol", "version", "linkId", "accepted", "code", "error"]) && isFailureCode(value.code) && isSafeText(value.error, 256);
}
function isControlResult(value, type) {
  if (!isRecord(value) || value.type !== type || !isFederationCorrelationId(value.requestId) || typeof value.ok !== "boolean")
    return false;
  if (value.ok) {
    return hasOnlyKeys(value, ["type", "requestId", "ok", "linkId"]) && isFederationCorrelationId(value.linkId);
  }
  return hasOnlyKeys(value, ["type", "requestId", "ok", "code", "error"]) && isFailureCode(value.code) && isSafeText(value.error, 256);
}
function isBrokerAcceptPeerResult(value) {
  return isControlResult(value, "broker_accept_peer_result");
}
function isBrokerStartPeerResult(value) {
  return isControlResult(value, "broker_start_peer_result");
}

// dist/broker/attachment.js
var PEER_STREAM_COPY_BYTES = 64 * 1024;
var PeerStreamAttachmentError = class extends Error {
  code;
  constructor(code) {
    super(`Peer stream attachment failed (${code})`);
    this.code = code;
    this.name = "PeerStreamAttachmentError";
  }
};
function failure(code) {
  return new PeerStreamAttachmentError(code);
}
function joinStreamClose(stream) {
  return new Promise((resolve4) => {
    let timer;
    const done = () => {
      clearTimeout(timer);
      stream.off("close", done);
      resolve4();
    };
    const poll = () => {
      if (stream.closed)
        done();
      else
        timer = setTimeout(poll, 10);
    };
    stream.once("close", done);
    timer = setTimeout(poll, 0);
  });
}
function frame(value) {
  const payload = Buffer.from(JSON.stringify(value), "utf8");
  const result = Buffer.allocUnsafe(4 + payload.length);
  result.writeUInt32BE(payload.length, 0);
  payload.copy(result, 4);
  return result;
}
var OwnedAttachment = class {
  controller = new AbortController();
  streams = /* @__PURE__ */ new Set();
  monitored = /* @__PURE__ */ new Set();
  writes = /* @__PURE__ */ new Set();
  pumps = [];
  listeners = [];
  server;
  serverClosed = Promise.resolve();
  outcome;
  startupError;
  resolveCompletion;
  completion = new Promise((resolve4) => {
    this.resolveCompletion = resolve4;
  });
  constructor(...streams) {
    for (const stream of streams)
      this.own(stream);
  }
  bindSignal(signal) {
    if (signal !== void 0 && !(signal instanceof AbortSignal))
      throw failure("E_INVALID_OPTIONS");
    if (signal) {
      const onAbort = () => this.stop({ status: "closed", reason: "aborted" }, failure("E_ABORTED"));
      signal.addEventListener("abort", onAbort, { once: true });
      this.listeners.push(() => signal.removeEventListener("abort", onAbort));
      if (signal.aborted)
        onAbort();
    }
  }
  own(stream) {
    stream.pause();
    stream.allowHalfOpen = true;
    this.streams.add(stream);
    this.monitored.add(stream);
    const onError = () => {
      if (this.monitored.has(stream))
        this.stop({ status: "failure", error: failure("E_STREAM_FAILED") });
    };
    const onClose = () => {
      if (this.monitored.has(stream) && !(stream.readableEnded && stream.writableFinished)) {
        this.stop({ status: "closed", reason: "stream" }, failure("E_CLOSED"));
      }
    };
    stream.on("error", onError);
    stream.on("close", onClose);
    this.listeners.push(() => {
      stream.off("error", onError);
      stream.off("close", onClose);
    });
  }
  check() {
    if (this.outcome)
      throw this.startupError ?? failure("E_CLOSED");
  }
  stop(outcome, error) {
    if (this.outcome)
      return;
    this.outcome = outcome;
    this.startupError = error ?? (outcome.status === "failure" ? outcome.error : failure("E_CLOSED"));
    this.controller.abort();
    for (const stream of this.streams) {
      stream.pause();
      stream.destroy();
    }
    this.server?.close();
    void this.finish();
  }
  async finish() {
    await Promise.allSettled([...this.writes, ...this.pumps, ...[...this.streams].map(joinStreamClose), this.serverClosed]);
    for (const remove of this.listeners)
      remove();
    this.resolveCompletion(this.outcome);
  }
  waitReadable(stream) {
    this.check();
    return new Promise((resolve4, reject) => {
      const cleanup = () => {
        stream.off("readable", onReady);
        stream.off("end", onReady);
        stream.off("close", onClose);
        this.controller.signal.removeEventListener("abort", onAbort);
      };
      const onReady = () => {
        cleanup();
        resolve4();
      };
      const onClose = () => {
        cleanup();
        reject(failure("E_CLOSED"));
      };
      const onAbort = () => {
        cleanup();
        reject(this.startupError ?? failure("E_CLOSED"));
      };
      stream.once("readable", onReady);
      stream.once("end", onReady);
      stream.once("close", onClose);
      this.controller.signal.addEventListener("abort", onAbort, { once: true });
      if (this.controller.signal.aborted)
        onAbort();
      else if (stream.readableLength > 0 || stream.readableEnded)
        onReady();
      else if (stream.destroyed)
        onClose();
    });
  }
  async read(stream, maximum) {
    while (true) {
      this.check();
      if (stream.readableLength === 0)
        stream.read(0);
      if (stream.readableLength > 0) {
        const chunk = stream.read(Math.min(maximum, stream.readableLength));
        if (chunk !== null) {
          if (!(chunk instanceof Uint8Array))
            throw failure("E_INVALID_OPTIONS");
          return Buffer.from(chunk);
        }
      }
      if (stream.readableEnded)
        return null;
      if (stream.destroyed || !stream.readable)
        throw failure("E_CLOSED");
      await this.waitReadable(stream);
    }
  }
  async readExact(stream, length) {
    const bytes = Buffer.allocUnsafe(length);
    let offset = 0;
    while (offset < length) {
      const chunk = await this.read(stream, Math.min(length - offset, PEER_STREAM_COPY_BYTES));
      if (!chunk)
        throw failure("E_HANDSHAKE_FAILED");
      chunk.copy(bytes, offset);
      offset += chunk.length;
    }
    return bytes;
  }
  async readRawFrame(stream) {
    const header = await this.readExact(stream, 4);
    const length = header.readUInt32BE(0);
    if (length > MAX_FRAME_BYTES)
      throw failure("E_HANDSHAKE_FAILED");
    const payload = await this.readExact(stream, length);
    try {
      return { bytes: Buffer.concat([header, payload]), value: JSON.parse(payload.toString("utf8")) };
    } catch {
      throw failure("E_HANDSHAKE_FAILED");
    }
  }
  async readFrame(stream) {
    return (await this.readRawFrame(stream)).value;
  }
  admit(start) {
    this.check();
    const operation = new Promise((resolve4, reject) => {
      try {
        start((error) => error ? reject(failure("E_STREAM_FAILED")) : resolve4());
      } catch {
        reject(failure("E_STREAM_FAILED"));
      }
    });
    this.writes.add(operation);
    void operation.then(() => this.writes.delete(operation), () => this.writes.delete(operation));
    return operation;
  }
  write(stream, bytes) {
    return this.admit((callback) => {
      stream.write(bytes, callback);
    });
  }
  async connect(target) {
    this.check();
    const socket = typeof target === "string" ? net.connect(target) : net.connect({ host: target.host, port: target.port });
    this.own(socket);
    await new Promise((resolve4, reject) => {
      const cleanup = () => {
        socket.off("connect", onConnect);
        socket.off("error", onError);
        this.controller.signal.removeEventListener("abort", onAbort);
      };
      const onConnect = () => {
        cleanup();
        resolve4();
      };
      const onError = () => {
        cleanup();
        reject(failure("E_DIAL_FAILED"));
      };
      const onAbort = () => {
        cleanup();
        reject(this.startupError ?? failure("E_CLOSED"));
      };
      socket.once("connect", onConnect);
      socket.once("error", onError);
      this.controller.signal.addEventListener("abort", onAbort, { once: true });
      if (this.controller.signal.aborted)
        onAbort();
    });
    return socket;
  }
  retireControl(socket) {
    this.monitored.delete(socket);
    socket.destroy();
  }
  async listen() {
    this.check();
    let resolveAccepted;
    let rejectAccepted;
    const accepted = new Promise((resolve4, reject) => {
      resolveAccepted = resolve4;
      rejectAccepted = reject;
    });
    void accepted.catch(() => void 0);
    let consumed = false;
    const server2 = net.createServer({ allowHalfOpen: true, pauseOnConnect: true }, (socket) => {
      if (consumed || this.outcome) {
        socket.destroy();
        return;
      }
      consumed = true;
      this.own(socket);
      server2.close();
      resolveAccepted(socket);
    });
    this.server = server2;
    this.serverClosed = new Promise((resolve4) => {
      server2.once("close", resolve4);
    });
    const onAbort = () => {
      rejectAccepted(this.startupError ?? failure("E_CLOSED"));
    };
    this.controller.signal.addEventListener("abort", onAbort, { once: true });
    this.listeners.push(() => this.controller.signal.removeEventListener("abort", onAbort));
    server2.on("error", () => {
      rejectAccepted(failure("E_DIAL_FAILED"));
      this.stop({ status: "failure", error: failure("E_DIAL_FAILED") });
    });
    await new Promise((resolve4, reject) => {
      const cleanup = () => {
        server2.off("listening", onListening);
        this.controller.signal.removeEventListener("abort", onListenAbort);
      };
      const onListening = () => {
        cleanup();
        resolve4();
      };
      const onListenAbort = () => {
        cleanup();
        reject(this.startupError ?? failure("E_CLOSED"));
      };
      server2.once("listening", onListening);
      this.controller.signal.addEventListener("abort", onListenAbort, { once: true });
      server2.listen({ host: "127.0.0.1", port: 0, signal: this.controller.signal });
    });
    this.check();
    const address = server2.address();
    if (!address || typeof address === "string")
      throw failure("E_DIAL_FAILED");
    return { port: address.port, accepted };
  }
  startCopy(local2, remote) {
    this.check();
    const copy = async (source, destination) => {
      while (true) {
        const bytes = await this.read(source, PEER_STREAM_COPY_BYTES);
        if (bytes === null) {
          await this.admit((callback) => {
            destination.end(callback);
          });
          return;
        }
        await this.write(destination, bytes);
      }
    };
    for (const [source, destination] of [[local2, remote], [remote, local2]]) {
      const pump = copy(source, destination);
      this.pumps.push(pump);
    }
    void Promise.all(this.pumps).then(() => this.stop({ status: "end" }), (error) => this.stop({ status: "failure", error: error instanceof PeerStreamAttachmentError ? error : failure("E_STREAM_FAILED") }));
  }
};
async function attachPeerStreams(options) {
  const local2 = options?.local?.stream;
  const remote = options?.remote?.stream;
  const owned = new OwnedAttachment(...[.../* @__PURE__ */ new Set([local2, remote])].filter((stream) => stream instanceof Duplex));
  let timer;
  try {
    owned.bindSignal(options?.signal);
    owned.check();
    const timeoutMs = options?.handshakeTimeoutMs ?? 1e4;
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 2147483647 || local2 === remote || ![local2, remote].every((stream) => stream instanceof Duplex && !stream.readableObjectMode && !stream.writableObjectMode && !stream.readableEncoding && !stream.destroyed && stream.readable && stream.writable))
      throw failure("E_INVALID_OPTIONS");
    const localOrigin2 = { ...options.local.origin };
    const remoteOrigin = { ...options.remote.origin };
    const linkId = randomUUID();
    const start = {
      type: "broker_start_peer",
      requestId: randomUUID(),
      linkId,
      localOrigin: localOrigin2,
      remoteOrigin,
      scopeBindings: options.local.scopeBindings?.map((binding) => ({ ...binding })),
      ...options.local.stateId !== void 0 ? { stateId: options.local.stateId } : {}
    };
    const prepare = {
      type: "broker_accept_peer",
      requestId: randomUUID(),
      linkId,
      localOrigin: remoteOrigin,
      remoteOrigin: localOrigin2,
      scopeBindings: options.remote.scopeBindings?.map((binding) => ({ ...binding })),
      ...options.remote.stateId !== void 0 ? { stateId: options.remote.stateId } : {}
    };
    if (!isBrokerStartPeerRequest(start) || !isBrokerAcceptPeerRequest(prepare))
      throw failure("E_INVALID_OPTIONS");
    timer = setTimeout(() => owned.stop({ status: "failure", error: failure("E_TIMEOUT") }), timeoutMs);
    await owned.write(remote, frame(prepare));
    await owned.write(local2, frame(start));
    const prepared = await owned.readFrame(remote);
    if (!isBrokerAcceptPeerResult(prepared) || prepared.requestId !== prepare.requestId)
      throw failure("E_HANDSHAKE_FAILED");
    if (!prepared.ok)
      throw failure(prepared.code);
    if (prepared.linkId !== linkId)
      throw failure("E_HANDSHAKE_FAILED");
    const hello = await owned.readRawFrame(local2);
    if (isBrokerStartPeerResult(hello.value) && hello.value.requestId === start.requestId && !hello.value.ok) {
      throw failure(hello.value.code);
    }
    if (!isPeerHello(hello.value) || hello.value.linkId !== linkId)
      throw failure("E_HANDSHAKE_FAILED");
    await owned.write(remote, hello.bytes);
    const ack = await owned.readRawFrame(remote);
    await owned.write(local2, ack.bytes);
    const ready = await owned.readFrame(local2);
    if (!isBrokerStartPeerResult(ready) || ready.requestId !== start.requestId)
      throw failure("E_HANDSHAKE_FAILED");
    if (!ready.ok)
      throw failure(ready.code);
    if (ready.linkId !== linkId)
      throw failure("E_HANDSHAKE_FAILED");
    owned.check();
    clearTimeout(timer);
    owned.startCopy(local2, remote);
    const acked = ack.value;
    const features = Object.freeze(isPeerHelloAck(acked) && acked.accepted ? [...acked.features] : []);
    return Object.freeze({
      linkId,
      localOrigin: Object.freeze(localOrigin2),
      remoteOrigin: Object.freeze(remoteOrigin),
      features,
      completion: owned.completion,
      close: () => {
        owned.stop({ status: "closed", reason: "close" });
        return owned.completion;
      }
    });
  } catch (error) {
    const safeError = error instanceof PeerStreamAttachmentError ? error : failure("E_INVALID_OPTIONS");
    owned.stop({ status: "failure", error: safeError });
    await owned.completion;
    throw safeError;
  } finally {
    if (timer)
      clearTimeout(timer);
  }
}

// dist/broker/paths.js
import { createHash } from "crypto";
import { isAbsolute, join, resolve, win32 } from "path";
function getBrokerPipeName(agentDir) {
  let normalized = win32.normalize(agentDir).toLowerCase();
  const rootLength = win32.parse(normalized).root.length;
  while (normalized.length > rootLength && normalized.endsWith("\\")) {
    normalized = normalized.slice(0, -1);
  }
  const digest = createHash("sha256").update("pi-parley-agent-dir\0", "utf8").update(normalized, "utf8").digest("hex");
  return `pi-parley-${digest}`;
}

// dist/federation.js
var BrokerAttachmentError = class extends Error {
  code;
  constructor(code) {
    super(`Federation broker attachment failed (${code})`);
    this.code = code;
    this.name = "BrokerAttachmentError";
  }
};
var BROKER_PUBLICATION_PATH = Object.freeze(["parley", "broker.port.json"]);
var BROKER_SOCKET_PATH = Object.freeze(["parley", "broker.sock"]);
var BROKER_PUBLICATION_MAX_BYTES = 4 * 1024;
var BROKER_INSPECTION_MAX_FRAME_BYTES = 64 * 1024;
var DEFAULT_OPERATION_TIMEOUT_MS = 1e4;
var BrokerInspectionError = class extends Error {
  code;
  constructor(code) {
    super(`Federation broker inspection failed (${code})`);
    this.code = code;
    this.name = "BrokerInspectionError";
  }
};
var brokerHandles = /* @__PURE__ */ new WeakMap();
function inspectionFailure(code) {
  return new BrokerInspectionError(code);
}
function attachmentFailure(code) {
  return new BrokerAttachmentError(code);
}
function validTimeout(value) {
  return Number.isSafeInteger(value) && value > 0 && value <= 2147483647;
}
function validPathSegments(value) {
  return value.length > 0 && value.every((segment) => typeof segment === "string" && segment.length > 0 && segment !== "." && segment !== ".." && !segment.includes("/") && !segment.includes("\\"));
}
function freezeEndpoint(endpoint) {
  if (endpoint.transport === "unix") {
    return Object.freeze({ transport: "unix", path: Object.freeze([...endpoint.path]) });
  }
  return Object.freeze({ ...endpoint });
}
function frame2(value) {
  const payload = Buffer.from(JSON.stringify(value), "utf8");
  const bytes = Buffer.allocUnsafe(4 + payload.length);
  bytes.writeUInt32BE(payload.length, 0);
  payload.copy(bytes, 4);
  return bytes;
}
function joinStreamClose2(stream) {
  return new Promise((resolveClose) => {
    let timer;
    const done = () => {
      clearTimeout(timer);
      stream.off("close", done);
      resolveClose();
    };
    const poll = () => {
      if (stream.closed)
        done();
      else
        timer = setTimeout(poll, 10);
    };
    stream.once("close", done);
    timer = setTimeout(poll, 0);
  });
}
async function destroyOwnedStream(stream) {
  const ignoreError = () => void 0;
  stream.on("error", ignoreError);
  stream.pause();
  stream.destroy();
  await joinStreamClose2(stream);
  stream.off("error", ignoreError);
}
function parsePublication(bytes) {
  if (bytes.byteLength === 0 || bytes.byteLength > BROKER_PUBLICATION_MAX_BYTES)
    return void 0;
  let parsed;
  try {
    parsed = JSON.parse(Buffer.from(bytes).toString("utf8"));
  } catch {
    return void 0;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
    return void 0;
  const record = parsed;
  if (Object.keys(record).some((key) => !["transport", "host", "port", "stateId"].includes(key)) || record.transport !== "tcp" || record.host !== "127.0.0.1" || !Number.isSafeInteger(record.port) || record.port <= 0 || record.port > 65535 || !isFederationCorrelationId(record.stateId))
    return void 0;
  return {
    public: freezeEndpoint({ transport: "tcp", port: record.port }),
    stateId: record.stateId
  };
}
function deterministicEndpoint(host) {
  return host.platform === "win32" ? { public: freezeEndpoint({ transport: "pipe", name: getBrokerPipeName(host.agentDir) }) } : { public: freezeEndpoint({ transport: "unix", path: BROKER_SOCKET_PATH }) };
}
function combineOperationSignal(external, timeoutMs, onTerminal) {
  const controller = new AbortController();
  let terminalCause;
  const finish = (cause) => {
    if (terminalCause)
      return;
    terminalCause = cause;
    onTerminal?.(cause);
    controller.abort();
  };
  const onAbort = () => finish("abort");
  external?.addEventListener("abort", onAbort, { once: true });
  if (external?.aborted)
    onAbort();
  const timer = setTimeout(() => finish("timeout"), timeoutMs);
  timer.unref?.();
  return {
    signal: controller.signal,
    cause: () => terminalCause,
    dispose: () => {
      clearTimeout(timer);
      external?.removeEventListener("abort", onAbort);
    }
  };
}
async function acquireConnectedStream(openLocal, endpoint, signal, onRejectedStream) {
  const stream = await openLocal(endpoint, { signal });
  if (!(stream instanceof Duplex2))
    throw new Error("Broker access did not return a Node Duplex");
  if (signal.aborted || stream.readableObjectMode || stream.writableObjectMode || stream.readableEncoding || stream.destroyed || !stream.readable || !stream.writable) {
    onRejectedStream?.();
    await destroyOwnedStream(stream);
    throw new Error(signal.aborted ? "Broker stream acquisition was cancelled" : "Broker access returned a non-binary or closed Duplex");
  }
  return stream;
}
function guardAcquiredStream(stream, onFailure) {
  const guarded = {
    stream,
    failed: false,
    release: () => {
      stream.off("error", fail);
      stream.off("close", fail);
    }
  };
  const fail = () => {
    guarded.failed = true;
    onFailure();
  };
  stream.pause();
  stream.on("error", fail);
  stream.on("close", fail);
  if (stream.destroyed || !stream.readable || !stream.writable)
    fail();
  return guarded;
}
var IncludeUnsupportedError = class extends Error {
  constructor() {
    super("Broker predates the requested inspection facts");
  }
};
async function requestBrokerSnapshot(stream, stateId, signal, include) {
  const requestId = randomUUID2();
  const request = {
    type: "broker_list_scopes",
    requestId,
    ...stateId !== void 0 ? { stateId } : {},
    ...include ? { include: ["mergedScopes"] } : {}
  };
  return await new Promise((resolveSnapshot, rejectSnapshot) => {
    let settled = false;
    let response;
    let writeFinished = false;
    const cleanup = () => {
      stream.off("data", onData);
      stream.off("error", onError);
      stream.off("end", onEnd);
      stream.off("close", onClose);
      signal.removeEventListener("abort", onAbort);
    };
    let receivedBytes = false;
    const fail = () => {
      if (settled)
        return;
      settled = true;
      cleanup();
      rejectSnapshot(new Error("Invalid broker inspection exchange"));
    };
    const failSilent = () => {
      if (settled)
        return;
      if (receivedBytes) {
        fail();
        return;
      }
      settled = true;
      cleanup();
      rejectSnapshot(new SilentBrokerStreamError());
    };
    const maybeResolve = () => {
      if (settled || !response || !writeFinished)
        return;
      settled = true;
      cleanup();
      resolveSnapshot(response);
    };
    const reader = createMessageReader((value) => {
      if (include && !response && isBrokerListScopesResult(value) && value.requestId === requestId && !value.ok && value.code === "E_INVALID_REQUEST") {
        if (settled)
          return;
        settled = true;
        cleanup();
        rejectSnapshot(new IncludeUnsupportedError());
        return;
      }
      if (response || !isBrokerListScopesResult(value) || value.requestId !== requestId || !value.ok) {
        fail();
        return;
      }
      response = value;
    }, fail, BROKER_INSPECTION_MAX_FRAME_BYTES);
    const onData = (bytes) => {
      if (!(bytes instanceof Uint8Array)) {
        fail();
        return;
      }
      if (bytes.byteLength > 0)
        receivedBytes = true;
      reader(Buffer.from(bytes));
      maybeResolve();
    };
    const onError = () => failSilent();
    const onEnd = () => {
      if (!response)
        failSilent();
    };
    const onClose = () => {
      if (!response)
        failSilent();
    };
    const onAbort = () => fail();
    stream.on("data", onData);
    stream.once("error", onError);
    stream.once("end", onEnd);
    stream.once("close", onClose);
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) {
      onAbort();
      return;
    }
    try {
      stream.write(frame2(request), (error) => {
        if (error) {
          fail();
          return;
        }
        writeFinished = true;
        maybeResolve();
      });
    } catch {
      fail();
    }
  });
}
var SilentBrokerStreamError = class extends Error {
  constructor() {
    super("Broker stream ended before any response");
  }
};
var CandidateInspectionError = class extends Error {
  kind;
  constructor(kind) {
    super("Broker candidate inspection failed");
    this.kind = kind;
  }
};
async function inspectCandidate(openLocal, endpoint, signal, include = true) {
  let stream;
  let retryWithout = false;
  const guardError = () => void 0;
  try {
    try {
      stream = await acquireConnectedStream(openLocal, endpoint.public, signal);
    } catch {
      throw new CandidateInspectionError("unavailable");
    }
    stream.on("error", guardError);
    try {
      return await requestBrokerSnapshot(stream, endpoint.stateId, signal, include);
    } catch (error) {
      if (error instanceof IncludeUnsupportedError) {
        retryWithout = true;
        throw error;
      }
      throw new CandidateInspectionError(error instanceof SilentBrokerStreamError ? "unavailable" : "protocol");
    }
  } catch (error) {
    if (!retryWithout)
      throw error;
  } finally {
    if (stream) {
      await destroyOwnedStream(stream);
      stream.off("error", guardError);
    }
  }
  return inspectCandidate(openLocal, endpoint, signal, false);
}
async function inspectBroker(host, options = {}) {
  if (!options || typeof options !== "object")
    throw inspectionFailure("E_INVALID_OPTIONS");
  const timeoutMs = options.timeoutMs ?? DEFAULT_OPERATION_TIMEOUT_MS;
  const validAgentDir = host && typeof host === "object" && typeof host.agentDir === "string" && host.agentDir.length > 0 && host.agentDir.length <= 4096 && !host.agentDir.includes("\0") && (host.platform === "win32" ? win322.isAbsolute(host.agentDir) : posix.isAbsolute(host.agentDir));
  if (!host || typeof host !== "object" || !["darwin", "linux", "win32"].includes(host.platform) || !validAgentDir || typeof host.readAgentFile !== "function" || typeof host.openLocal !== "function" || !validTimeout(timeoutMs) || options.signal !== void 0 && !(options.signal instanceof AbortSignal)) {
    throw inspectionFailure("E_INVALID_OPTIONS");
  }
  const operation = combineOperationSignal(options.signal, timeoutMs);
  const readAgentFile = host.readAgentFile.bind(host);
  const openLocal = host.openLocal.bind(host);
  let publicationInvalid = false;
  let brokerProtocolInvalid = false;
  try {
    const checkCancellation = () => {
      const cause = operation.cause();
      if (cause === "abort")
        throw inspectionFailure("E_ABORTED");
      if (cause === "timeout")
        throw inspectionFailure("E_TIMEOUT");
    };
    const inspectEndpoint = async (endpoint) => {
      try {
        const snapshot = await inspectCandidate(openLocal, endpoint, operation.signal);
        checkCancellation();
        const handle2 = Object.freeze({
          origin: Object.freeze({ ...snapshot.localOrigin }),
          scopes: Object.freeze(snapshot.scopes.map((scope) => Object.freeze({ ...scope }))),
          ...snapshot.mergedScopes ? { mergedScopes: Object.freeze([...snapshot.mergedScopes]) } : {}
        });
        brokerHandles.set(handle2, { openLocal, endpoint });
        return handle2;
      } catch (error) {
        checkCancellation();
        if (error instanceof CandidateInspectionError && error.kind === "protocol") {
          brokerProtocolInvalid = true;
        }
        return void 0;
      }
    };
    const deterministic = deterministicEndpoint(host);
    if (host.platform !== "win32") {
      const handle2 = await inspectEndpoint(deterministic);
      if (handle2)
        return handle2;
    }
    let publicationBytes;
    try {
      publicationBytes = await readAgentFile(BROKER_PUBLICATION_PATH, {
        maxBytes: BROKER_PUBLICATION_MAX_BYTES,
        signal: operation.signal
      });
    } catch {
      checkCancellation();
      publicationBytes = void 0;
    }
    checkCancellation();
    if (publicationBytes !== void 0 && !(publicationBytes instanceof Uint8Array)) {
      throw inspectionFailure("E_DISCOVERY_FAILED");
    }
    if (publicationBytes && publicationBytes.byteLength > BROKER_PUBLICATION_MAX_BYTES) {
      throw inspectionFailure("E_DISCOVERY_FAILED");
    }
    if (publicationBytes !== void 0) {
      const published = parsePublication(publicationBytes);
      if (published) {
        const handle2 = await inspectEndpoint(published);
        if (handle2)
          return handle2;
      } else {
        publicationInvalid = true;
      }
    }
    if (host.platform === "win32") {
      const handle2 = await inspectEndpoint(deterministic);
      if (handle2)
        return handle2;
    }
    checkCancellation();
    throw inspectionFailure(publicationInvalid || brokerProtocolInvalid ? "E_BROKER_PROTOCOL" : "E_BROKER_UNAVAILABLE");
  } catch (error) {
    if (error instanceof BrokerInspectionError)
      throw error;
    throw inspectionFailure(operation.cause() === "abort" ? "E_ABORTED" : operation.cause() === "timeout" ? "E_TIMEOUT" : "E_BROKER_UNAVAILABLE");
  } finally {
    operation.dispose();
  }
}
function validatedSide(side, oppositeOrigin, role) {
  if (!side || typeof side !== "object" || !side.broker || typeof side.broker !== "object") {
    throw attachmentFailure("E_INVALID_OPTIONS");
  }
  const state = brokerHandles.get(side.broker);
  if (!state)
    throw attachmentFailure("E_INVALID_OPTIONS");
  const origin = {
    id: side.broker.origin.id,
    ...side.originLabel !== void 0 ? { label: side.originLabel } : {}
  };
  const scopeBindings = Array.isArray(side.scopeBindings) ? side.scopeBindings.map((binding) => ({ ...binding })) : [];
  const request = {
    type: role === "initiator" ? "broker_start_peer" : "broker_accept_peer",
    requestId: randomUUID2(),
    linkId: randomUUID2(),
    localOrigin: origin,
    remoteOrigin: oppositeOrigin,
    scopeBindings,
    ...state.endpoint.stateId !== void 0 ? { stateId: state.endpoint.stateId } : {}
  };
  const valid = role === "initiator" ? isBrokerStartPeerRequest(request) : isBrokerAcceptPeerRequest(request);
  if (!valid)
    throw attachmentFailure("E_INVALID_OPTIONS");
  return { state, origin, scopeBindings };
}
async function attachBrokers(options) {
  const timeoutMs = options?.timeoutMs ?? DEFAULT_OPERATION_TIMEOUT_MS;
  if (!options || typeof options !== "object" || !validTimeout(timeoutMs) || options.signal !== void 0 && !(options.signal instanceof AbortSignal) || !options.initiator?.broker?.origin || !options.acceptor?.broker?.origin || options.initiator.broker === options.acceptor.broker || options.initiator.broker.origin.id === options.acceptor.broker.origin.id) {
    throw attachmentFailure("E_INVALID_OPTIONS");
  }
  const initiator = validatedSide(options.initiator, options.acceptor.broker.origin, "initiator");
  const acceptor = validatedSide(options.acceptor, options.initiator.broker.origin, "acceptor");
  if (!bindingsMatchPeerMappings(acceptor.scopeBindings, scopeMappingsFromBindings(initiator.scopeBindings))) {
    throw attachmentFailure("E_INVALID_OPTIONS");
  }
  const startedAt = Date.now();
  let terminalCause;
  const markTerminal = (cause) => {
    terminalCause ??= cause;
  };
  const acquisition = combineOperationSignal(options.signal, timeoutMs, markTerminal);
  const siblingController = new AbortController();
  const onAcquisitionAbort = () => siblingController.abort();
  acquisition.signal.addEventListener("abort", onAcquisitionAbort, { once: true });
  if (acquisition.signal.aborted)
    onAcquisitionAbort();
  const acquireWithSibling = async (side) => {
    try {
      const stream = await acquireConnectedStream(side.state.openLocal, side.state.endpoint.public, siblingController.signal, () => {
        markTerminal("stream");
        siblingController.abort();
      });
      return guardAcquiredStream(stream, () => {
        markTerminal("stream");
        siblingController.abort();
      });
    } catch (error) {
      markTerminal("stream");
      siblingController.abort();
      throw error;
    }
  };
  const settled = await Promise.allSettled([
    acquireWithSibling(initiator),
    acquireWithSibling(acceptor)
  ]);
  acquisition.signal.removeEventListener("abort", onAcquisitionAbort);
  acquisition.dispose();
  const acquired = settled.flatMap((result) => result.status === "fulfilled" ? [result.value] : []);
  const discardAcquired = async () => {
    await Promise.all(acquired.map(async (guarded) => {
      await destroyOwnedStream(guarded.stream);
      guarded.release();
    }));
  };
  if (settled.some((result) => result.status === "rejected") || acquired.some((guarded) => guarded.failed) || terminalCause !== void 0) {
    await discardAcquired();
    throw attachmentFailure(terminalCause === "abort" ? "E_ABORTED" : terminalCause === "timeout" ? "E_TIMEOUT" : "E_STREAM_FAILED");
  }
  const [initiatorGuard, acceptorGuard] = acquired;
  if (!initiatorGuard || !acceptorGuard) {
    await discardAcquired();
    throw attachmentFailure("E_STREAM_FAILED");
  }
  const remaining = timeoutMs - (Date.now() - startedAt);
  if (remaining <= 0) {
    await discardAcquired();
    throw attachmentFailure("E_TIMEOUT");
  }
  initiatorGuard.release();
  acceptorGuard.release();
  let attached;
  try {
    attached = await attachPeerStreams({
      local: {
        stream: initiatorGuard.stream,
        origin: initiator.origin,
        scopeBindings: initiator.scopeBindings,
        ...initiator.state.endpoint.stateId !== void 0 ? { stateId: initiator.state.endpoint.stateId } : {}
      },
      remote: {
        stream: acceptorGuard.stream,
        origin: acceptor.origin,
        scopeBindings: acceptor.scopeBindings,
        ...acceptor.state.endpoint.stateId !== void 0 ? { stateId: acceptor.state.endpoint.stateId } : {}
      },
      ...options.signal ? { signal: options.signal } : {},
      handshakeTimeoutMs: Math.max(1, remaining)
    });
  } catch (error) {
    throw attachmentFailure(error instanceof PeerStreamAttachmentError ? error.code : "E_HANDSHAKE_FAILED");
  }
  const presentCompletion = (outcome) => {
    if (outcome.status !== "failure")
      return outcome;
    return { status: "failure", error: attachmentFailure(outcome.error.code) };
  };
  const completion = attached.completion.then(presentCompletion);
  return Object.freeze({
    linkId: attached.linkId,
    initiatorOrigin: attached.localOrigin,
    acceptorOrigin: attached.remoteOrigin,
    features: attached.features ?? Object.freeze([]),
    completion,
    close: () => {
      void attached.close();
      return completion;
    }
  });
}
async function readBoundedAgentFile(agentDir, relativePath, maxBytes, signal) {
  if (!validPathSegments(relativePath) || !Number.isSafeInteger(maxBytes) || maxBytes <= 0) {
    throw new Error("Invalid rooted file read");
  }
  signal.throwIfAborted();
  let handle2;
  try {
    handle2 = await openFile(join2(agentDir, ...relativePath), "r");
  } catch (error) {
    if (error.code === "ENOENT")
      return void 0;
    throw error;
  }
  try {
    const bytes = Buffer.allocUnsafe(maxBytes + 1);
    const { bytesRead } = await handle2.read(bytes, 0, bytes.length, 0);
    signal.throwIfAborted();
    if (bytesRead > maxBytes)
      throw new Error("Rooted file exceeded read bound");
    return Buffer.from(bytes.subarray(0, bytesRead));
  } finally {
    await handle2.close();
  }
}
function openLocalSocket(agentDir, endpoint, signal) {
  return new Promise((resolveSocket, rejectSocket) => {
    let socket;
    if (endpoint.transport === "tcp") {
      if (!Number.isSafeInteger(endpoint.port) || endpoint.port <= 0 || endpoint.port > 65535) {
        rejectSocket(new Error("Invalid loopback endpoint"));
        return;
      }
      socket = net2.connect({ host: "127.0.0.1", port: endpoint.port });
    } else if (endpoint.transport === "unix") {
      if (!validPathSegments(endpoint.path) || JSON.stringify(endpoint.path) !== JSON.stringify(BROKER_SOCKET_PATH)) {
        rejectSocket(new Error("Invalid rooted Unix endpoint"));
        return;
      }
      socket = net2.connect(join2(agentDir, ...endpoint.path));
    } else {
      if (endpoint.name !== getBrokerPipeName(agentDir)) {
        rejectSocket(new Error("Invalid broker pipe endpoint"));
        return;
      }
      socket = net2.connect(`\\\\.\\pipe\\${endpoint.name}`);
    }
    const cleanup = () => {
      socket.off("connect", onConnect);
      socket.off("error", onError);
      signal.removeEventListener("abort", onAbort);
    };
    const onConnect = () => {
      cleanup();
      resolveSocket(socket);
    };
    const onError = () => {
      cleanup();
      socket.destroy();
      rejectSocket(new Error("Broker endpoint connection failed"));
    };
    const onAbort = () => {
      cleanup();
      socket.destroy();
      rejectSocket(new Error("Broker endpoint connection cancelled"));
    };
    socket.once("connect", onConnect);
    socket.once("error", onError);
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted)
      onAbort();
  });
}
function createLocalBrokerAccess(options = {}) {
  if (!options || typeof options !== "object")
    throw inspectionFailure("E_INVALID_OPTIONS");
  const currentPlatform = platform();
  if (!["darwin", "linux", "win32"].includes(currentPlatform)) {
    throw inspectionFailure("E_INVALID_OPTIONS");
  }
  const env = options.env ?? process.env;
  const cwd = options.cwd ?? process.cwd();
  const homeDir = options.homeDir ?? homedir();
  const configured = options.agentDir ?? env.PI_CODING_AGENT_DIR?.trim();
  const agentDir = configured ? isAbsolute2(configured) ? configured : resolve2(cwd, configured) : resolve2(homeDir, ".pi", "agent");
  if (!agentDir || agentDir.length > 4096)
    throw inspectionFailure("E_INVALID_OPTIONS");
  const access = {
    platform: currentPlatform,
    agentDir,
    readAgentFile: (relativePath, readOptions) => readBoundedAgentFile(agentDir, relativePath, readOptions.maxBytes, readOptions.signal),
    openLocal: (endpoint, openOptions) => openLocalSocket(agentDir, endpoint, openOptions.signal)
  };
  return Object.freeze(access);
}

// broker/framing.ts
var MAX_FRAME_BYTES2 = 1024 * 1024;
function writeMessage(socket, msg) {
  const json = JSON.stringify(msg);
  const payloadLength = Buffer.byteLength(json, "utf-8");
  const frame3 = Buffer.allocUnsafe(4 + payloadLength);
  frame3.writeUInt32BE(payloadLength, 0);
  frame3.write(json, 4, payloadLength, "utf-8");
  socket.write(frame3);
}
function createMessageReader2(onMessage, onError, maxFrameBytes = MAX_FRAME_BYTES2) {
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

// broker/process-lock.ts
import { randomUUID as randomUUID3 } from "node:crypto";
import {
  closeSync,
  constants,
  fstatSync,
  mkdirSync,
  openSync,
  readSync,
  writeFileSync
} from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
var loadedNativeLocks;
function nativeLocks() {
  loadedNativeLocks ??= createRequire(import.meta.url)("fs-native-extensions");
  return loadedNativeLocks;
}
function tryAcquireProcessLock(directory) {
  mkdirSync(directory, { recursive: true, mode: 448 });
  const fd = openSync(path.join(directory, ".process.lock"), constants.O_RDWR | constants.O_CREAT, 384);
  let retained = false;
  try {
    if (!fstatSync(fd).isFile()) throw new Error("Process lock must be a regular file");
    if (!nativeLocks().tryLock(fd)) {
      return Object.freeze({ status: "occupied", owner: readDiagnosticOwner(directory) });
    }
    const owner = Object.freeze({
      pid: process.pid,
      claimId: randomUUID3(),
      acquiredAt: (/* @__PURE__ */ new Date()).toISOString()
    });
    writeFileSync(path.join(directory, ".process.owner.json"), `${JSON.stringify(owner)}
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
          closeSync(ownedDescriptor);
        }
      }
    });
    retained = true;
    return Object.freeze({ status: "acquired", lease });
  } finally {
    if (!retained) closeSync(fd);
  }
}
function readDiagnosticOwner(directory) {
  let fd = null;
  try {
    fd = openSync(path.join(directory, ".process.owner.json"), "r");
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
        closeSync(fd);
      } catch {
      }
    }
  }
}

// broker/paths.ts
import { chmodSync, mkdirSync as mkdirSync2, readFileSync } from "fs";
import { isAbsolute as isAbsolute3, join as join3, resolve as resolve3, win32 as win323 } from "path";
import { homedir as homedir2 } from "os";
var PARLEY_DIR_MODE = 448;
function getAgentDirPath(env = process.env, homeDir = homedir2(), cwd = process.cwd()) {
  const configured = env.PI_CODING_AGENT_DIR?.trim();
  if (!configured) {
    return join3(homeDir, ".pi/agent");
  }
  return isAbsolute3(configured) ? configured : resolve3(cwd, configured);
}
function getParleyDirPath(agentDir = getAgentDirPath()) {
  return join3(agentDir, "parley");
}
function ensureParleyRuntimeDir(parleyDir = getParleyDirPath(), platform2 = process.platform) {
  mkdirSync2(parleyDir, { recursive: true, mode: PARLEY_DIR_MODE });
  if (platform2 !== "win32") {
    chmodSync(parleyDir, PARLEY_DIR_MODE);
  }
}

// link/discover.ts
import { execFile } from "node:child_process";
import { promisify } from "node:util";

// link/ssh.ts
import { spawn } from "node:child_process";
import { createHash as createHash2 } from "node:crypto";
import { mkdirSync as mkdirSync3 } from "node:fs";
import { Duplex as Duplex3 } from "node:stream";
import { posix as posix2, win32 as win324, join as join4 } from "node:path";
var SshHostError = class extends Error {
  constructor(code, detail) {
    super(detail ? `${code}: ${detail}` : code);
    this.code = code;
    this.detail = detail;
    this.name = "SshHostError";
  }
  code;
  detail;
};
function parseSshDestination(input) {
  const tokens = input.trim().replace(/^ssh\s+/, "").split(/\s+/).filter(Boolean);
  const options = [];
  let host;
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (host === void 0 && token.startsWith("-") && token.length > 1) {
      const letter = token[1];
      if (KEPT_FLAGS.includes(letter) && token.length === 2) {
        options.push(token);
        continue;
      }
      if (!KEPT_OPTIONS_WITH_VALUE.includes(letter)) return void 0;
      const value = token.length > 2 ? token.slice(2) : tokens[++i];
      if (value === void 0 || letter === "o" && !keptOption(letter, value)) return void 0;
      options.push(`-${letter}`, value);
      continue;
    }
    if (host !== void 0) return void 0;
    host = token;
  }
  if (!host || host.startsWith("-")) return void 0;
  return Object.freeze({ options: Object.freeze(options), host });
}
var KEPT_OPTIONS_WITH_VALUE = "BFiJlop";
var KEPT_FLAGS = "46";
var KEPT_CONFIG = /^(user|port|hostname|proxyjump|proxycommand|identityfile|identitiesonly|certificatefile|identityagent|addressfamily|bindaddress|bindinterface|hostkeyalias|userknownhostsfile)\s*[= ]/i;
function keptOption(letter, value) {
  if (KEPT_FLAGS.includes(letter)) return value === void 0;
  if (!KEPT_OPTIONS_WITH_VALUE.includes(letter) || value === void 0) return false;
  if (letter === "o") return KEPT_CONFIG.test(value);
  if (letter === "F") return !/^(\/tmp\/|\/var\/folders\/|\/private\/|[A-Za-z]:\\.*\\Temp\\)/i.test(value);
  return true;
}
function destinationKey(destination) {
  return [...destination.options, destination.host].join(" ");
}
function destinationLabel(destination) {
  const at = destination.host.lastIndexOf("@");
  return at >= 0 ? destination.host.slice(at + 1) : destination.host;
}
async function openSshHost(destination, options = {}) {
  const ssh = options.ssh ?? "ssh";
  const sharing = await connectionSharing(ssh, destination, options);
  const base = (mux) => [
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=10",
    "-o",
    "ServerAliveInterval=15",
    "-o",
    "ServerAliveCountMax=3",
    ...sharing ? ["-o", `ControlMaster=${mux}`, ...sharing] : [],
    ...destination.options
  ];
  const run2 = (command, input, maxBytes = 64 * 1024) => runSsh(
    ssh,
    [...base("auto"), destination.host, command],
    { ...options, input, maxBytes }
  );
  const first = await run2("echo parley-probe %OS% %USERPROFILE%");
  let shell;
  let platform2;
  let home;
  const tokens = probeTokens(first.stdout);
  if (first.code === 0 && tokens[0] === "Windows_NT" && tokens[1]) {
    shell = "cmd";
    platform2 = "win32";
    home = tokens[1];
  } else {
    if (first.code === 255) throw sshFailure(first.stderr);
    const posixProbe = await run2("sh -s", 'echo parley-probe "$(uname -s)" "$HOME"\n');
    const posixTokens = probeTokens(posixProbe.stdout);
    if (posixProbe.code === 0 && posixTokens[0] && posixTokens[1]) {
      const system = posixTokens[0];
      if (system === "Darwin") platform2 = "darwin";
      else if (system === "Linux") platform2 = "linux";
      else throw new SshHostError("E_UNSUPPORTED_HOST", system);
      shell = "posix";
      home = posixTokens[1];
    } else {
      const ps = await run2("echo parley-probe $env:OS $env:USERPROFILE");
      const psTokens = probeTokens(ps.stdout);
      if (ps.code !== 0 || psTokens[0] !== "Windows_NT" || !psTokens[1]) {
        throw new SshHostError("E_UNSUPPORTED_HOST");
      }
      shell = "powershell";
      platform2 = "win32";
      home = psTokens[1];
    }
  }
  const path2 = platform2 === "win32" ? win324 : posix2;
  const agentDir = path2.join(home, ".pi", "agent");
  const exists = await run2(existsCommand(shell, path2.join(agentDir, "parley")));
  const hasParley = exists.code === 0 && exists.stdout.includes("parley-yes");
  const readAgentFile = async (relative, { maxBytes, signal }) => {
    if (relative.some((part) => part === ".." || part.includes("/") || part.includes("\\") || part.includes("\0"))) {
      throw new Error("path outside the agent directory");
    }
    const file = path2.join(agentDir, ...relative);
    const result = await runSsh(ssh, [...base("auto"), destination.host, readCommand(shell, file, maxBytes)], {
      ...options,
      signal,
      maxBytes: maxBytes + 1
    });
    if (result.code === 44) return void 0;
    if (result.code !== 0) throw new Error("remote read failed");
    if (result.bytes.byteLength > maxBytes) throw new Error("remote file too large");
    return result.bytes;
  };
  const openLocal = async (endpoint, { signal }) => {
    let target;
    if (endpoint.transport === "tcp") target = `127.0.0.1:${endpoint.port}`;
    else if (endpoint.transport === "unix") target = path2.join(agentDir, ...endpoint.path);
    else throw new Error("named pipes cannot be carried by ssh");
    return forward(ssh, [...base("no"), "-W", target, destination.host], signal, options.onSpawn);
  };
  return Object.freeze({
    destination,
    platform: platform2,
    home,
    hasParley,
    access: Object.freeze({ platform: platform2, agentDir, readAgentFile, openLocal })
  });
}
function probeTokens(stdout) {
  const words = stdout.split(/\s+/).filter(Boolean);
  const start = words.lastIndexOf("parley-probe");
  return start < 0 ? [] : words.slice(start + 1);
}
function existsCommand(shell, dir) {
  if (shell === "cmd") return `if exist "${dir}\\" (echo parley-yes) else (echo parley-no)`;
  if (shell === "powershell") return `if (Test-Path -LiteralPath '${dir}') { 'parley-yes' } else { 'parley-no' }`;
  return `test -d ${shQuote(dir)} && echo parley-yes || echo parley-no`;
}
function readCommand(shell, file, maxBytes) {
  if (shell === "cmd") return `if exist "${file}" (type "${file}") else (exit /b 44)`;
  if (shell === "powershell") {
    return `if (Test-Path -LiteralPath '${file}') { [Console]::Out.Write([IO.File]::ReadAllText('${file}')) } else { exit 44 }`;
  }
  return `if [ -f ${shQuote(file)} ]; then head -c ${maxBytes} ${shQuote(file)}; else exit 44; fi`;
}
function shQuote(value) {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}
async function connectionSharing(ssh, destination, options) {
  if (process.platform === "win32" || !options.controlDir) return void 0;
  const config2 = await runSsh(ssh, ["-G", ...destination.options, destination.host], { ...options, maxBytes: 256 * 1024 });
  const controlPath = /^controlpath\s+(.+)$/m.exec(config2.stdout)?.[1]?.trim();
  if (config2.code === 0 && controlPath && controlPath !== "none") return [];
  mkdirSync3(options.controlDir, { recursive: true, mode: 448 });
  const tag = createHash2("sha256").update(destinationKey(destination)).digest("hex").slice(0, 16);
  return ["-o", `ControlPath=${join4(options.controlDir, tag)}`, "-o", "ControlPersist=120"];
}
function runSsh(ssh, args, options) {
  return new Promise((resolve4, reject) => {
    if (options.signal?.aborted) {
      reject(new SshHostError("E_ABORTED"));
      return;
    }
    const child = spawn(ssh, args, { stdio: ["pipe", "pipe", "pipe"], windowsHide: true });
    options.onSpawn?.(child);
    const out = [];
    let outBytes = 0;
    let stderr = "";
    let done = false;
    const finish = (value) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      options.signal?.removeEventListener("abort", onAbort);
      if (value instanceof Error) reject(value);
      else resolve4(value);
    };
    const onAbort = () => {
      child.kill();
      finish(new SshHostError("E_ABORTED"));
    };
    options.signal?.addEventListener("abort", onAbort, { once: true });
    const timer = setTimeout(() => {
      child.kill();
      finish(new SshHostError("E_SSH_UNREACHABLE", "timed out"));
    }, options.timeoutMs ?? 2e4);
    child.stdout.on("data", (chunk) => {
      if (outBytes > options.maxBytes) return;
      out.push(chunk);
      outBytes += chunk.byteLength;
      if (outBytes > options.maxBytes) child.kill();
    });
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (text) => {
      if (stderr.length < 8e3) stderr += text;
    });
    child.on("error", (error) => finish(new SshHostError("E_SSH_FAILED", `could not run ssh: ${error.message}`)));
    child.on("close", (code) => {
      const bytes = Buffer.concat(out);
      finish({ code, stdout: bytes.toString("utf8"), stderr, bytes });
    });
    child.stdin.on("error", () => {
    });
    child.stdin.end(options.input ?? "");
  });
}
function sshFailure(stderr) {
  const text = stderr.trim();
  const line = text.split("\n").map((l) => l.trim()).filter(Boolean).pop() ?? "";
  if (/host key verification failed|remote host identification has changed|no .* host key is known/i.test(text)) {
    return new SshHostError("E_SSH_HOST_KEY", line);
  }
  if (/permission denied|too many authentication failures|agent refused|sign_and_send_pubkey|authentication/i.test(text)) {
    return new SshHostError("E_SSH_AUTH", line);
  }
  if (/could not resolve|connection refused|timed out|no route|network is unreachable|connection closed|unknown host|name or service/i.test(text)) {
    return new SshHostError("E_SSH_UNREACHABLE", line);
  }
  return new SshHostError("E_SSH_FAILED", line || void 0);
}
function forward(ssh, args, signal, onSpawn) {
  if (signal.aborted) return Promise.reject(new Error("aborted"));
  const child = spawn(ssh, args, { stdio: ["pipe", "pipe", "ignore"], windowsHide: true });
  onSpawn?.(child);
  const stream = new Duplex3({
    allowHalfOpen: true,
    read() {
      child.stdout.resume();
    },
    write(chunk, _encoding, callback) {
      child.stdin.write(chunk, callback);
    },
    final(callback) {
      child.stdin.end(callback);
    },
    destroy(error, callback) {
      if (child.exitCode === null && child.signalCode === null) child.kill();
      callback(error);
    }
  });
  let ended = false;
  const end = () => {
    if (!ended) {
      ended = true;
      stream.push(null);
    }
  };
  child.stdout.on("data", (chunk) => {
    if (!ended && !stream.push(chunk)) child.stdout.pause();
  });
  child.stdout.on("end", end);
  child.on("close", end);
  child.on("error", () => stream.destroy(new Error("ssh could not start")));
  child.stdin.on("error", () => {
  });
  child.stdout.on("error", () => {
  });
  return Promise.resolve(stream);
}

// link/discover.ts
var run = promisify(execFile);
var OPTIONS_WITH_VALUE = "BbcDEeFIiJLlmOoPpQRSWw";
var NOT_A_SESSION = "WOGQVs";
var PIPE_COMMANDS = /^(git-[a-z-]+|scp|rsync|sftp|sftp-server|internal-sftp)\b/;
function destinationFromArgv(argv) {
  const program = argv[0]?.split(/[\\/]/).pop()?.toLowerCase();
  if (program !== "ssh" && program !== "ssh.exe") return void 0;
  const options = [];
  for (let i = 1; i < argv.length; i++) {
    const token = argv[i];
    if (token === "--") {
      return destinationAt(argv, i + 1, options);
    }
    if (!token.startsWith("-") || token.length < 2) return destinationAt(argv, i, options);
    for (let j = 1; j < token.length; j++) {
      const letter = token[j];
      if (NOT_A_SESSION.includes(letter)) return void 0;
      if (OPTIONS_WITH_VALUE.includes(letter)) {
        let value = token.slice(j + 1);
        if (!value) {
          value = argv[++i] ?? "";
          if (!value) return void 0;
        }
        if (keptOption(letter, value)) options.push(`-${letter}`, value);
        break;
      }
      if (keptOption(letter)) options.push(`-${letter}`);
    }
  }
  return void 0;
}
function destinationAt(argv, index, options) {
  const host = argv[index];
  if (!host || host.startsWith("-")) return void 0;
  const command = argv.slice(index + 1).join(" ").trim();
  if (command && PIPE_COMMANDS.test(command)) return void 0;
  if (/^git@/.test(host)) return void 0;
  return Object.freeze({ options: Object.freeze(options), host });
}
async function listSshProcesses() {
  if (process.platform === "win32") {
    const { stdout: stdout2 } = await run("powershell.exe", [
      "-NoProfile",
      "-NonInteractive",
      "-Command",
      `Get-CimInstance Win32_Process -Filter "Name='ssh.exe'" | ForEach-Object { "$($_.ProcessId) $($_.CommandLine)" }`
    ], { timeout: 1e4, windowsHide: true, maxBuffer: 4 * 1024 * 1024 });
    return parseProcessLines(stdout2, (line) => line.replace(/"/g, "").split(/\s+/));
  }
  const { stdout } = await run("ps", ["-x", "-ww", "-o", "pid=,args="], { timeout: 1e4, maxBuffer: 16 * 1024 * 1024 });
  return parseProcessLines(stdout, (line) => line.split(/\s+/));
}
function parseProcessLines(stdout, split) {
  const found = [];
  for (const raw of stdout.split("\n")) {
    const line = raw.trim();
    const match = /^(\d+)\s+(.*)$/.exec(line);
    if (!match) continue;
    const argv = split(match[2]).filter(Boolean);
    const program = argv[0]?.split(/[\\/]/).pop()?.toLowerCase();
    if (program === "ssh" || program === "ssh.exe") found.push({ pid: Number(match[1]), argv });
  }
  return found;
}
async function listHerdrMachines(herdr = "herdr") {
  let stdout;
  try {
    ({ stdout } = await run(herdr, ["machine", "list", "--json"], { timeout: 5e3, windowsHide: true, maxBuffer: 1024 * 1024 }));
  } catch {
    return [];
  }
  let machines;
  try {
    machines = JSON.parse(stdout);
  } catch {
    return [];
  }
  if (!Array.isArray(machines)) return [];
  const found = [];
  for (const machine of machines) {
    if (machine?.enabled !== true || typeof machine.target !== "string") continue;
    const destination = herdrTarget(machine.target);
    if (destination) found.push(destination);
  }
  return found;
}
function herdrTarget(target) {
  const trimmed = target.trim();
  if (!trimmed.startsWith("ssh://")) {
    return /^[^\s-][^\s]*$/.test(trimmed) ? Object.freeze({ options: Object.freeze([]), host: trimmed }) : void 0;
  }
  let url;
  try {
    url = new URL(trimmed);
  } catch {
    return void 0;
  }
  if (!url.hostname || url.password) return void 0;
  const host = url.username ? `${decodeURIComponent(url.username)}@${url.hostname}` : url.hostname;
  return Object.freeze({ options: Object.freeze(url.port ? ["-p", url.port] : []), host });
}

// link/paths.ts
import { createHash as createHash3 } from "node:crypto";
import { join as join5, win32 as win325 } from "node:path";
function getLinkPaths(env = process.env, platform2 = process.platform) {
  const agentDir = getAgentDirPath(env);
  const parleyDir = getParleyDirPath(agentDir);
  let control = join5(parleyDir, "link.sock");
  if (platform2 === "win32") {
    const digest = createHash3("sha256").update("pi-parley-link\0").update(win325.normalize(agentDir).toLowerCase()).digest("hex");
    control = `\\\\.\\pipe\\pi-parley-link-${digest}`;
  }
  return Object.freeze({
    parleyDir,
    config: join5(parleyDir, "links.json"),
    control,
    lock: join5(parleyDir, "link.keeper"),
    sshControlDir: join5(parleyDir, "ssh"),
    log: join5(parleyDir, "link.log")
  });
}

// link/keeper.ts
var SCOPES = [{ localScopeId: null, localScopeAlias: "default", remoteScopeAlias: "default" }];
var DISCOVER_MS = 1e4;
var IDLE_EXIT_MS = Number(process.env.PI_PARLEY_LINK_IDLE_MS ?? 3e4);
var ADD_ANSWER_MS = 45e3;
var paths = getLinkPaths();
var localLabel = (process.env.PI_PARLEY_LINK_LABEL?.trim() || hostname()).replace(/\.local$/i, "");
function log(line) {
  try {
    appendFileSync(paths.log, `${(/* @__PURE__ */ new Date()).toISOString()} ${line}
`);
  } catch {
  }
}
function readConfig() {
  try {
    const parsed = JSON.parse(readFileSync2(paths.config, "utf8"));
    return {
      version: 1,
      ...typeof parsed.discover === "boolean" ? { discover: parsed.discover } : {},
      hosts: Array.isArray(parsed.hosts) ? parsed.hosts.filter((h) => typeof h?.destination === "string").map((h) => ({ destination: h.destination, via: h.via === "found" ? "found" : "added" })) : [],
      ignored: Array.isArray(parsed.ignored) ? parsed.ignored.filter((d) => typeof d === "string") : []
    };
  } catch {
    return { version: 1, hosts: [], ignored: [] };
  }
}
var discovering = () => config.discover ?? process.env.PI_PARLEY_LINK_DISCOVER_DEFAULT === "1";
var config = readConfig();
function saveConfig() {
  const tmp = `${paths.config}.${process.pid}.tmp`;
  writeFileSync2(tmp, `${JSON.stringify(config, null, 2)}
`, { mode: 384 });
  renameSync(tmp, paths.config);
}
var hosts = /* @__PURE__ */ new Map();
var ownChildren = /* @__PURE__ */ new Set();
var trackChild = (child) => {
  if (child.pid) ownChildren.add(child.pid);
  child.once("close", () => {
    if (child.pid) ownChildren.delete(child.pid);
  });
};
function deferred() {
  let resolve4;
  const promise = new Promise((r) => {
    resolve4 = r;
  });
  return { promise, resolve: resolve4 };
}
function setState(host, state, detail) {
  if (host.state !== state || host.detail !== detail) {
    host.state = state;
    host.detail = detail;
    host.since = (/* @__PURE__ */ new Date()).toISOString();
    log(`${host.label}: ${state}${detail ? ` (${detail})` : ""}`);
  }
  if (state !== "checking") {
    host.settled.resolve();
    host.settled = deferred();
  }
  if (state === "linked" && !host.everLinked) {
    host.everLinked = true;
    broadcast({ type: "event", event: "linked", label: host.label, via: host.via });
  }
}
function remembered(host) {
  return config.hosts.some((h) => h.destination === host.key);
}
function hostFor(destination, via) {
  const key = destinationKey(destination);
  let host = hosts.get(key);
  if (!host) {
    host = {
      key,
      destination,
      label: destinationLabel(destination),
      via,
      state: "checking",
      since: (/* @__PURE__ */ new Date()).toISOString(),
      settled: deferred(),
      everLinked: false
    };
    hosts.set(key, host);
  }
  if (via === "added") host.via = "added";
  return host;
}
function sshMessage(error) {
  if (error instanceof SshHostError) {
    const words = {
      E_SSH_AUTH: "ssh needs a password or key approval it can't ask for; connect once with ssh yourself, or load your key",
      E_SSH_HOST_KEY: "ssh doesn't trust this computer's host key yet; connect once with ssh yourself",
      E_SSH_UNREACHABLE: "can't reach it over ssh",
      E_UNSUPPORTED_HOST: "its ssh shell isn't one Parley can use",
      E_SSH_FAILED: "ssh failed"
    };
    const base = words[error.code] ?? error.code;
    return { state: "unreachable", detail: error.detail ? `${base}: ${error.detail}` : base };
  }
  return { state: "unreachable", detail: error instanceof Error ? error.message : String(error) };
}
function check(host) {
  host.checking ??= (async () => {
    setState(host, "checking");
    try {
      host.ssh = await openSshHost(host.destination, {
        controlDir: paths.sshControlDir,
        signal: stopping.signal,
        onSpawn: trackChild
      });
      if (!host.ssh.hasParley && !remembered(host)) {
        setState(host, "no-parley", "Parley has never run there");
        return;
      }
      if (!remembered(host)) {
        config.hosts.push({ destination: host.key, via: host.via });
        saveConfig();
      }
      setState(host, "waiting", "linking");
    } catch (error) {
      if (stopping.signal.aborted) return;
      const { state, detail } = sshMessage(error);
      setState(host, state, detail);
    } finally {
      host.checking = void 0;
      void reconcile();
    }
  })();
  return host.checking;
}
var local = { key: "local", label: localLabel, access: createLocalBrokerAccess() };
var pairs = /* @__PURE__ */ new Map();
var localOrigin;
var sleep = (ms, signal) => new Promise((resolve4) => {
  const done = () => {
    clearTimeout(timer);
    signal.removeEventListener("abort", done);
    resolve4();
  };
  const timer = setTimeout(done, ms);
  signal.addEventListener("abort", done, { once: true });
});
function codeOf(error) {
  return error?.code ?? (error instanceof Error ? error.message : String(error));
}
function waitingWords(code, a, b) {
  if (code === "E_BROKER_UNAVAILABLE") return "no Pi with Parley is running there right now";
  if (code === "E_VERSION_UNSUPPORTED" || code === "E_FEATURE_UNSUPPORTED") return "Parley versions differ; update Parley on one side";
  if (code === "E_ALREADY_CONNECTED") return `${a.label} and ${b.label} are already linked by another computer or app`;
  if (code === "E_TIMEOUT") return "timed out";
  return code;
}
function claimOrigin(side, origin) {
  if (!side.host) {
    localOrigin = origin;
    return true;
  }
  side.host.origin = origin;
  if (origin === localOrigin) {
    setState(side.host, "same-computer", "this computer");
    return false;
  }
  for (const other of hosts.values()) {
    if (other !== side.host && other.origin === origin && other.state !== "same-computer") {
      setState(side.host, "same-computer", `same computer as ${other.label}`);
      return false;
    }
  }
  return true;
}
function report(pair, state, detail) {
  pair.state = state;
  pair.detail = detail;
  if (pair.a === local && pair.b.host && pair.b.host.state !== "same-computer") {
    setState(pair.b.host, state, detail);
  }
}
async function keepLinked(pair) {
  const { a, b, controller: { signal } } = pair;
  let backoff = 2e3;
  while (!signal.aborted) {
    let code;
    try {
      const [left, right] = await Promise.all([
        inspectBroker(a.access, { signal, timeoutMs: 2e4 }),
        inspectBroker(b.access, { signal, timeoutMs: 2e4 })
      ]);
      if (!claimOrigin(a, left.origin.id) || !claimOrigin(b, right.origin.id)) {
        void reconcile();
        return;
      }
      const attachment = await attachBrokers({
        initiator: { broker: right, originLabel: b.label, scopeBindings: SCOPES },
        acceptor: { broker: left, originLabel: a.label, scopeBindings: SCOPES },
        signal,
        timeoutMs: 25e3
      });
      if (signal.aborted) {
        await attachment.close();
        break;
      }
      report(pair, "linked");
      backoff = 2e3;
      const completion = await attachment.completion;
      if (signal.aborted) break;
      code = completion.status === "failure" ? completion.error.code : "disconnected";
    } catch (error) {
      if (signal.aborted) break;
      code = codeOf(error);
    }
    if (code === "E_BROKER_UNAVAILABLE" || code === "E_STREAM_FAILED" || code === "disconnected") {
      for (const side of [a, b]) {
        if (!side.host) continue;
        try {
          await openSshHost(side.host.destination, { controlDir: paths.sshControlDir, signal, onSpawn: trackChild, timeoutMs: 15e3 });
        } catch (error) {
          if (signal.aborted) return;
          const { detail } = sshMessage(error);
          report(pair, "waiting", `${side.label}: ${detail}`);
          if (pair.a === local && side === b) setState(side.host, "unreachable", detail);
          code = void 0;
          break;
        }
      }
    }
    if (code) report(pair, "waiting", waitingWords(code, a, b));
    backoff = Math.min(backoff * 2, code === "E_BROKER_UNAVAILABLE" ? 15e3 : 6e4);
    await sleep(backoff, signal);
  }
}
function flightDeckLinksHere() {
  const home = process.env.PI_PARLEY_FLIGHTDECK_HOME?.trim() || join6(homedir3(), ".flightdeck");
  for (const name of ["parley-link.json", "parley-link-debug.json"]) {
    try {
      const status2 = JSON.parse(readFileSync2(join6(home, name), "utf8"));
      if (status2.updatedAt && Date.now() - Date.parse(status2.updatedAt) < 6e4) return true;
      if (typeof status2.pid === "number" && status2.pid > 1) {
        try {
          process.kill(status2.pid, 0);
          return true;
        } catch {
        }
      }
    } catch {
    }
  }
  return false;
}
function sides() {
  const list = [local];
  if (flightDeckLinksHere()) {
    for (const host of hosts.values()) if (host.ssh && host.state !== "same-computer") setState(host, "waiting", "FlightDeck links this computer; Parley's own links stand aside");
    return list;
  }
  for (const host of hosts.values()) {
    if (!host.ssh || host.state === "same-computer" || host.state === "no-parley" || host.checking) continue;
    if (!remembered(host)) continue;
    list.push({ key: host.key, label: host.label, access: host.ssh.access, host });
  }
  return list;
}
var herdrCache;
async function herdrMachines() {
  if (!herdrCache || Date.now() - herdrCache.at > 6e4) herdrCache = { at: Date.now(), machines: await listHerdrMachines() };
  return herdrCache.machines;
}
var reconciling = false;
var reconcileAgain = false;
async function reconcile() {
  if (reconciling) {
    reconcileAgain = true;
    return;
  }
  reconciling = true;
  try {
    do {
      reconcileAgain = false;
      for (const entry of config.hosts) {
        const destination = parseSshDestination(entry.destination);
        if (destination) hostFor(destination, entry.via);
      }
      if (discovering()) {
        try {
          const seen = [
            ...(await listSshProcesses()).filter((proc) => !ownChildren.has(proc.pid)).map((proc) => destinationFromArgv(proc.argv)),
            ...await herdrMachines()
          ];
          for (const destination of seen) {
            if (!destination) continue;
            const key = destinationKey(destination);
            if (config.ignored.includes(key) || hosts.has(key)) continue;
            const sameHost = [...hosts.values()].some((h) => h.destination.host === destination.host && h.state !== "no-parley" && h.state !== "unreachable");
            if (sameHost) continue;
            hostFor(destination, "found");
          }
        } catch (error) {
          log(`discovery failed: ${codeOf(error)}`);
        }
      }
      for (const host of hosts.values()) {
        if (host.ssh || host.checking) continue;
        const retry = host.state === "unreachable" && remembered(host) && Date.now() - Date.parse(host.since) > 6e4;
        if (host.state === "checking" || retry) void check(host);
      }
      const wanted = /* @__PURE__ */ new Map();
      const list = sides();
      for (let i = 0; i < list.length; i++) for (let j = i + 1; j < list.length; j++) {
        wanted.set(`${list[i].key}|${list[j].key}`, [list[i], list[j]]);
      }
      for (const [key, pair] of pairs) {
        if (wanted.has(key)) continue;
        pair.controller.abort();
        pairs.delete(key);
      }
      for (const [key, [a, b]] of wanted) {
        if (pairs.has(key)) continue;
        const pair = { a, b, controller: new AbortController(), state: "waiting" };
        pairs.set(key, pair);
        void keepLinked(pair);
      }
    } while (reconcileAgain && !stopping.signal.aborted);
  } finally {
    reconciling = false;
  }
}
function hostStatus(host) {
  return Object.freeze({
    destination: host.key,
    label: host.label,
    via: host.via,
    remembered: remembered(host),
    state: host.state,
    ...host.detail ? { detail: host.detail } : {},
    since: host.since
  });
}
function status() {
  return Object.freeze({
    discover: discovering(),
    label: localLabel,
    hosts: [...hosts.values()].map(hostStatus),
    relays: [...pairs.values()].filter((p) => p.a !== local).map((p) => ({
      between: [p.a.label, p.b.label],
      state: p.state,
      ...p.detail ? { detail: p.detail } : {}
    }))
  });
}
function findHost(destination) {
  const parsed = parseSshDestination(destination);
  const key = parsed ? destinationKey(parsed) : destination.trim();
  return hosts.get(key) ?? [...hosts.values()].find((h) => h.label === destination.trim() || h.destination.host === destination.trim());
}
async function untilSettled(host, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline && (host.state === "checking" || host.state === "waiting" && host.detail === "linking")) {
    await Promise.race([host.settled.promise, new Promise((r) => setTimeout(r, deadline - Date.now()))]);
  }
}
function forgetHost(host) {
  for (const [key, pair] of pairs) {
    if (pair.a.host === host || pair.b.host === host) {
      pair.controller.abort();
      pairs.delete(key);
    }
  }
  hosts.delete(host.key);
}
async function handle(request) {
  const { requestId } = request;
  const fail = (error) => ({ type: "result", requestId, ok: false, error, status: status() });
  switch (request.type) {
    case "status":
      return { type: "result", requestId, ok: true, status: status() };
    case "discover": {
      config.discover = request.on === true;
      saveConfig();
      if (!discovering()) {
        for (const host of [...hosts.values()]) if (!remembered(host)) forgetHost(host);
      }
      await reconcile();
      return { type: "result", requestId, ok: true, status: status() };
    }
    case "add": {
      const destination = parseSshDestination(request.destination ?? "");
      if (!destination) return fail("give a destination as you would to ssh: an alias, user@host, and optionally -p, -l, -i, -J, -F or -o settings");
      const key = destinationKey(destination);
      config.ignored = config.ignored.filter((d) => d !== key);
      if (!config.hosts.some((h) => h.destination === key)) config.hosts.push({ destination: key, via: "added" });
      saveConfig();
      const existing = hosts.get(key);
      if (existing && existing.state !== "linked") {
        forgetHost(existing);
      }
      const host = hostFor(destination, "added");
      await reconcile();
      await untilSettled(host, ADD_ANSWER_MS);
      return { type: "result", requestId, ok: true, status: status(), host: hostStatus(host) };
    }
    case "recheck": {
      const targets = request.destination ? [findHost(request.destination)].filter((h) => !!h) : [...hosts.values()];
      if (request.destination && targets.length === 0) return fail(`no computer called ${request.destination} here; add it first`);
      for (const host of targets) {
        if (host.state === "linked") continue;
        forgetHost(host);
        hostFor(host.destination, host.via);
      }
      await reconcile();
      const rechecked = targets.map((t) => hosts.get(t.key)).filter((h) => !!h);
      await Promise.all(rechecked.map((h) => untilSettled(h, ADD_ANSWER_MS)));
      const one = request.destination ? rechecked[0] : void 0;
      return { type: "result", requestId, ok: true, status: status(), ...one ? { host: hostStatus(one) } : {} };
    }
    case "remove": {
      const host = findHost(request.destination ?? "");
      const key = host?.key ?? request.destination?.trim();
      if (!host && !config.hosts.some((h) => h.destination === key)) return fail(`no computer called ${request.destination} here`);
      config.hosts = config.hosts.filter((h) => h.destination !== key);
      if (key && !config.ignored.includes(key)) config.ignored.push(key);
      saveConfig();
      if (host) forgetHost(host);
      await reconcile();
      return { type: "result", requestId, ok: true, status: status() };
    }
  }
}
var clients = /* @__PURE__ */ new Set();
var idleTimer;
function broadcast(event) {
  for (const socket of clients) {
    try {
      writeMessage(socket, event);
    } catch {
    }
  }
}
function armIdle() {
  clearTimeout(idleTimer);
  if (clients.size === 0) idleTimer = setTimeout(() => void stop("no Pi sessions connected"), IDLE_EXIT_MS);
}
var stopping = new AbortController();
async function stop(why) {
  if (stopping.signal.aborted) return;
  log(`stopping: ${why}`);
  stopping.abort();
  for (const pair of pairs.values()) pair.controller.abort();
  server?.close();
  for (const socket of clients) socket.destroy();
  setTimeout(() => process.exit(0), 3e3).unref();
  await new Promise((r) => setTimeout(r, 500));
  if (process.platform !== "win32") rmSync(paths.control, { force: true });
  process.exit(0);
}
var server;
async function main() {
  ensureParleyRuntimeDir(paths.parleyDir);
  mkdirSync4(paths.sshControlDir, { recursive: true, mode: 448 });
  const lock = tryAcquireProcessLock(paths.lock);
  if (lock.status === "occupied") process.exit(0);
  if (process.platform !== "win32") rmSync(paths.control, { force: true });
  server = net3.createServer((socket) => {
    clients.add(socket);
    armIdle();
    socket.on("error", () => {
    });
    socket.on("close", () => {
      clients.delete(socket);
      armIdle();
    });
    socket.on("data", createMessageReader2((value) => {
      const request = value;
      if (!request || typeof request !== "object" || typeof request.requestId !== "string") return;
      handle(request).then(
        (response) => {
          try {
            writeMessage(socket, response);
          } catch {
          }
        },
        (error) => {
          try {
            writeMessage(socket, { type: "result", requestId: request.requestId, ok: false, error: codeOf(error) });
          } catch {
          }
        }
      );
    }, () => socket.destroy()));
  });
  await new Promise((resolve4, reject) => {
    server.once("error", reject);
    server.listen(paths.control, () => resolve4());
  });
  log(`keeper ${process.pid} up as ${localLabel}`);
  process.on("SIGTERM", () => void stop("SIGTERM"));
  process.on("SIGINT", () => void stop("SIGINT"));
  armIdle();
  while (!stopping.signal.aborted) {
    await reconcile();
    await sleep(DISCOVER_MS, stopping.signal);
  }
}
void main().catch((error) => {
  log(`failed: ${error instanceof Error ? error.stack ?? error.message : String(error)}`);
  process.exit(1);
});
