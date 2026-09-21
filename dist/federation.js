import net from "node:net";
import { randomUUID } from "node:crypto";
import { open as openFile } from "node:fs/promises";
import { platform, homedir } from "node:os";
import { isAbsolute, join, posix, resolve, win32 } from "node:path";
import { Duplex } from "node:stream";
import { attachPeerStreams, PeerStreamAttachmentError, } from "./broker/attachment.js";
import { createMessageReader } from "./broker/framing.js";
import { getBrokerPipeName } from "./broker/paths.js";
import { bindingsMatchPeerMappings, isBrokerAcceptPeerRequest, isBrokerListScopesResult, isBrokerStartPeerRequest, isFederationCorrelationId, scopeMappingsFromBindings, } from "./broker/federation-protocol.js";
/** Sanitized attachment/acquisition failure. */
export class BrokerAttachmentError extends Error {
    code;
    constructor(code) {
        super(`Federation broker attachment failed (${code})`);
        this.code = code;
        this.name = "BrokerAttachmentError";
    }
}
const BROKER_PUBLICATION_PATH = Object.freeze(["parley", "broker.port.json"]);
const BROKER_SOCKET_PATH = Object.freeze(["parley", "broker.sock"]);
const BROKER_PUBLICATION_MAX_BYTES = 4 * 1024;
const BROKER_INSPECTION_MAX_FRAME_BYTES = 64 * 1024;
const DEFAULT_OPERATION_TIMEOUT_MS = 10_000;
/** Sanitized inspection failure: paths, endpoint credentials, host diagnostics,
 * and raw broker frames are deliberately omitted. */
export class BrokerInspectionError extends Error {
    code;
    constructor(code) {
        super(`Federation broker inspection failed (${code})`);
        this.code = code;
        this.name = "BrokerInspectionError";
    }
}
const brokerHandles = new WeakMap();
function inspectionFailure(code) {
    return new BrokerInspectionError(code);
}
function attachmentFailure(code) {
    return new BrokerAttachmentError(code);
}
function validTimeout(value) {
    return Number.isSafeInteger(value) && value > 0 && value <= 2_147_483_647;
}
function validPathSegments(value) {
    return value.length > 0 && value.every((segment) => typeof segment === "string"
        && segment.length > 0 && segment !== "." && segment !== ".."
        && !segment.includes("/") && !segment.includes("\\"));
}
function freezeEndpoint(endpoint) {
    if (endpoint.transport === "unix") {
        return Object.freeze({ transport: "unix", path: Object.freeze([...endpoint.path]) });
    }
    return Object.freeze({ ...endpoint });
}
function frame(value) {
    const payload = Buffer.from(JSON.stringify(value), "utf8");
    const bytes = Buffer.allocUnsafe(4 + payload.length);
    bytes.writeUInt32BE(payload.length, 0);
    payload.copy(bytes, 4);
    return bytes;
}
function joinStreamClose(stream) {
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
        // Even an already-closed stream may still have a deferred error/close event.
        timer = setTimeout(poll, 0);
    });
}
async function destroyOwnedStream(stream) {
    const ignoreError = () => undefined;
    stream.on("error", ignoreError);
    stream.pause();
    stream.destroy();
    await joinStreamClose(stream);
    stream.off("error", ignoreError);
}
function parsePublication(bytes) {
    if (bytes.byteLength === 0 || bytes.byteLength > BROKER_PUBLICATION_MAX_BYTES)
        return undefined;
    let parsed;
    try {
        parsed = JSON.parse(Buffer.from(bytes).toString("utf8"));
    }
    catch {
        return undefined;
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
        return undefined;
    const record = parsed;
    if (Object.keys(record).some((key) => !["transport", "host", "port", "stateId"].includes(key))
        || record.transport !== "tcp" || record.host !== "127.0.0.1"
        || !Number.isSafeInteger(record.port) || record.port <= 0 || record.port > 65_535
        || !isFederationCorrelationId(record.stateId))
        return undefined;
    return {
        public: freezeEndpoint({ transport: "tcp", port: record.port }),
        stateId: record.stateId,
    };
}
function deterministicEndpoint(host) {
    return host.platform === "win32"
        ? { public: freezeEndpoint({ transport: "pipe", name: getBrokerPipeName(host.agentDir) }) }
        : { public: freezeEndpoint({ transport: "unix", path: BROKER_SOCKET_PATH }) };
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
        },
    };
}
async function acquireConnectedStream(openLocal, endpoint, signal, onRejectedStream) {
    const stream = await openLocal(endpoint, { signal });
    if (!(stream instanceof Duplex))
        throw new Error("Broker access did not return a Node Duplex");
    if (signal.aborted || stream.readableObjectMode || stream.writableObjectMode
        || stream.readableEncoding || stream.destroyed || !stream.readable || !stream.writable) {
        // Latch failure before joining provider-owned teardown: that cleanup may
        // outlive the deadline and must not change the original terminal cause.
        onRejectedStream?.();
        await destroyOwnedStream(stream);
        throw new Error(signal.aborted
            ? "Broker stream acquisition was cancelled"
            : "Broker access returned a non-binary or closed Duplex");
    }
    return stream;
}
/** A fulfilled stream is already owned while its sibling may still be opening.
 * Guard that interval so an early error cannot become uncaught or leave the
 * sibling acquisition running. */
function guardAcquiredStream(stream, onFailure) {
    const guarded = {
        stream,
        failed: false,
        release: () => {
            stream.off("error", fail);
            stream.off("close", fail);
        },
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
async function requestBrokerSnapshot(stream, stateId, signal) {
    const requestId = randomUUID();
    const request = {
        type: "broker_list_scopes",
        requestId,
        ...(stateId !== undefined ? { stateId } : {}),
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
        const fail = () => {
            if (settled)
                return;
            settled = true;
            cleanup();
            rejectSnapshot(new Error("Invalid broker inspection exchange"));
        };
        const maybeResolve = () => {
            if (settled || !response || !writeFinished)
                return;
            settled = true;
            cleanup();
            resolveSnapshot(response);
        };
        const reader = createMessageReader((value) => {
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
            reader(Buffer.from(bytes));
            // Resolve only after the whole admitted chunk was checked, so a second
            // coalesced control frame cannot be ignored after a valid first frame.
            maybeResolve();
        };
        const onError = () => fail();
        const onEnd = () => { if (!response)
            fail(); };
        const onClose = () => { if (!response)
            fail(); };
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
            stream.write(frame(request), (error) => {
                if (error) {
                    fail();
                    return;
                }
                writeFinished = true;
                maybeResolve();
            });
        }
        catch {
            fail();
        }
    });
}
class CandidateInspectionError extends Error {
    kind;
    constructor(kind) {
        super("Broker candidate inspection failed");
        this.kind = kind;
    }
}
async function inspectCandidate(openLocal, endpoint, signal) {
    let stream;
    const guardError = () => undefined;
    try {
        try {
            stream = await acquireConnectedStream(openLocal, endpoint.public, signal);
        }
        catch {
            throw new CandidateInspectionError("unavailable");
        }
        // Keep an error guard for the entire owned lifetime. The request listener
        // can be removed in a write callback before Node emits its deferred error.
        stream.on("error", guardError);
        try {
            return await requestBrokerSnapshot(stream, endpoint.stateId, signal);
        }
        catch {
            throw new CandidateInspectionError("protocol");
        }
    }
    finally {
        if (stream) {
            await destroyOwnedStream(stream);
            stream.off("error", guardError);
        }
    }
}
/** Inspect one live Parley broker without exposing its endpoint credential or
 * control protocol. The first successful inspection may mint and persist the
 * broker installation's canonical federation origin. The returned handle owns
 * no stream; TCP handles become stale when that broker restarts. */
export async function inspectBroker(host, options = {}) {
    if (!options || typeof options !== "object")
        throw inspectionFailure("E_INVALID_OPTIONS");
    const timeoutMs = options.timeoutMs ?? DEFAULT_OPERATION_TIMEOUT_MS;
    const validAgentDir = host && typeof host === "object" && typeof host.agentDir === "string"
        && host.agentDir.length > 0 && host.agentDir.length <= 4096 && !host.agentDir.includes("\0")
        && (host.platform === "win32" ? win32.isAbsolute(host.agentDir) : posix.isAbsolute(host.agentDir));
    if (!host || typeof host !== "object"
        || !["darwin", "linux", "win32"].includes(host.platform)
        || !validAgentDir
        || typeof host.readAgentFile !== "function" || typeof host.openLocal !== "function"
        || !validTimeout(timeoutMs)
        || (options.signal !== undefined && !(options.signal instanceof AbortSignal))) {
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
                const handle = Object.freeze({
                    origin: Object.freeze({ ...snapshot.localOrigin }),
                    scopes: Object.freeze(snapshot.scopes.map((scope) => Object.freeze({ ...scope }))),
                });
                brokerHandles.set(handle, { openLocal, endpoint });
                return handle;
            }
            catch (error) {
                checkCancellation();
                if (error instanceof CandidateInspectionError && error.kind === "protocol") {
                    brokerProtocolInvalid = true;
                }
                return undefined;
            }
        };
        const deterministic = deterministicEndpoint(host);
        // Default POSIX brokers need no filesystem read. This also lets remote
        // hosts whose bounded-read API cannot distinguish absent from unavailable
        // inspect their ordinary Unix socket without interpreting diagnostics.
        if (host.platform !== "win32") {
            const handle = await inspectEndpoint(deterministic);
            if (handle)
                return handle;
        }
        let publicationBytes;
        try {
            publicationBytes = await readAgentFile(BROKER_PUBLICATION_PATH, {
                maxBytes: BROKER_PUBLICATION_MAX_BYTES,
                signal: operation.signal,
            });
        }
        catch {
            checkCancellation();
            // A provider may not distinguish absence from another unavailable read.
            // No diagnostics or inferred filesystem state cross this boundary.
            publicationBytes = undefined;
        }
        checkCancellation();
        if (publicationBytes !== undefined && !(publicationBytes instanceof Uint8Array)) {
            throw inspectionFailure("E_DISCOVERY_FAILED");
        }
        if (publicationBytes && publicationBytes.byteLength > BROKER_PUBLICATION_MAX_BYTES) {
            throw inspectionFailure("E_DISCOVERY_FAILED");
        }
        if (publicationBytes !== undefined) {
            const published = parsePublication(publicationBytes);
            if (published) {
                const handle = await inspectEndpoint(published);
                if (handle)
                    return handle;
            }
            else {
                publicationInvalid = true;
            }
        }
        if (host.platform === "win32") {
            const handle = await inspectEndpoint(deterministic);
            if (handle)
                return handle;
        }
        checkCancellation();
        throw inspectionFailure(publicationInvalid || brokerProtocolInvalid
            ? "E_BROKER_PROTOCOL"
            : "E_BROKER_UNAVAILABLE");
    }
    catch (error) {
        if (error instanceof BrokerInspectionError)
            throw error;
        throw inspectionFailure(operation.cause() === "abort" ? "E_ABORTED"
            : operation.cause() === "timeout" ? "E_TIMEOUT" : "E_BROKER_UNAVAILABLE");
    }
    finally {
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
        ...(side.originLabel !== undefined ? { label: side.originLabel } : {}),
    };
    const scopeBindings = Array.isArray(side.scopeBindings)
        ? side.scopeBindings.map((binding) => ({ ...binding }))
        : [];
    const request = {
        type: role === "initiator" ? "broker_start_peer" : "broker_accept_peer",
        requestId: randomUUID(),
        linkId: randomUUID(),
        localOrigin: origin,
        remoteOrigin: oppositeOrigin,
        scopeBindings,
        ...(state.endpoint.stateId !== undefined ? { stateId: state.endpoint.stateId } : {}),
    };
    const valid = role === "initiator"
        ? isBrokerStartPeerRequest(request)
        : isBrokerAcceptPeerRequest(request);
    if (!valid)
        throw attachmentFailure("E_INVALID_OPTIONS");
    return { state, origin, scopeBindings };
}
/** Connect two inspected brokers through their caller-owned host capabilities.
 * Both exact streams are opened concurrently and become Parley-owned when their
 * promises fulfill. A stale handle is never rediscovered or replayed; callers
 * reinspect and decide whether to reconnect. */
export async function attachBrokers(options) {
    const timeoutMs = options?.timeoutMs ?? DEFAULT_OPERATION_TIMEOUT_MS;
    if (!options || typeof options !== "object" || !validTimeout(timeoutMs)
        || (options.signal !== undefined && !(options.signal instanceof AbortSignal))
        || !options.initiator?.broker?.origin || !options.acceptor?.broker?.origin
        || options.initiator.broker === options.acceptor.broker
        || options.initiator.broker.origin.id === options.acceptor.broker.origin.id) {
        throw attachmentFailure("E_INVALID_OPTIONS");
    }
    // Validate every caller-controlled value before either capability is invoked.
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
    // Keep the controller private rather than exposing abort reasons to providers.
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
        }
        catch (error) {
            markTerminal("stream");
            siblingController.abort();
            throw error;
        }
    };
    const settled = await Promise.allSettled([
        acquireWithSibling(initiator),
        acquireWithSibling(acceptor),
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
    if (settled.some((result) => result.status === "rejected")
        || acquired.some((guarded) => guarded.failed)
        || terminalCause !== undefined) {
        await discardAcquired();
        throw attachmentFailure(terminalCause === "abort" ? "E_ABORTED"
            : terminalCause === "timeout" ? "E_TIMEOUT" : "E_STREAM_FAILED");
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
    // attachPeerStreams installs its lifetime guards synchronously before its
    // first await, so releasing these immediately before invocation has no gap.
    initiatorGuard.release();
    acceptorGuard.release();
    let attached;
    try {
        attached = await attachPeerStreams({
            local: {
                stream: initiatorGuard.stream,
                origin: initiator.origin,
                scopeBindings: initiator.scopeBindings,
                ...(initiator.state.endpoint.stateId !== undefined ? { stateId: initiator.state.endpoint.stateId } : {}),
            },
            remote: {
                stream: acceptorGuard.stream,
                origin: acceptor.origin,
                scopeBindings: acceptor.scopeBindings,
                ...(acceptor.state.endpoint.stateId !== undefined ? { stateId: acceptor.state.endpoint.stateId } : {}),
            },
            ...(options.signal ? { signal: options.signal } : {}),
            handshakeTimeoutMs: Math.max(1, remaining),
        });
    }
    catch (error) {
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
        completion,
        close: () => { void attached.close(); return completion; },
    });
}
async function readBoundedAgentFile(agentDir, relativePath, maxBytes, signal) {
    if (!validPathSegments(relativePath) || !Number.isSafeInteger(maxBytes) || maxBytes <= 0) {
        throw new Error("Invalid rooted file read");
    }
    signal.throwIfAborted();
    let handle;
    try {
        handle = await openFile(join(agentDir, ...relativePath), "r");
    }
    catch (error) {
        if (error.code === "ENOENT")
            return undefined;
        throw error;
    }
    try {
        const bytes = Buffer.allocUnsafe(maxBytes + 1);
        const { bytesRead } = await handle.read(bytes, 0, bytes.length, 0);
        signal.throwIfAborted();
        if (bytesRead > maxBytes)
            throw new Error("Rooted file exceeded read bound");
        return Buffer.from(bytes.subarray(0, bytesRead));
    }
    finally {
        await handle.close();
    }
}
function openLocalSocket(agentDir, endpoint, signal) {
    return new Promise((resolveSocket, rejectSocket) => {
        let socket;
        if (endpoint.transport === "tcp") {
            if (!Number.isSafeInteger(endpoint.port) || endpoint.port <= 0 || endpoint.port > 65_535) {
                rejectSocket(new Error("Invalid loopback endpoint"));
                return;
            }
            socket = net.connect({ host: "127.0.0.1", port: endpoint.port });
        }
        else if (endpoint.transport === "unix") {
            if (!validPathSegments(endpoint.path)
                || JSON.stringify(endpoint.path) !== JSON.stringify(BROKER_SOCKET_PATH)) {
                rejectSocket(new Error("Invalid rooted Unix endpoint"));
                return;
            }
            socket = net.connect(join(agentDir, ...endpoint.path));
        }
        else {
            if (endpoint.name !== getBrokerPipeName(agentDir)) {
                rejectSocket(new Error("Invalid broker pipe endpoint"));
                return;
            }
            socket = net.connect(`\\\\.\\pipe\\${endpoint.name}`);
        }
        const cleanup = () => {
            socket.off("connect", onConnect);
            socket.off("error", onError);
            signal.removeEventListener("abort", onAbort);
        };
        const onConnect = () => { cleanup(); resolveSocket(socket); };
        const onError = () => { cleanup(); socket.destroy(); rejectSocket(new Error("Broker endpoint connection failed")); };
        const onAbort = () => { cleanup(); socket.destroy(); rejectSocket(new Error("Broker endpoint connection cancelled")); };
        socket.once("connect", onConnect);
        socket.once("error", onError);
        signal.addEventListener("abort", onAbort, { once: true });
        if (signal.aborted)
            onAbort();
    });
}
/** Built-in rooted host access for standalone callers on this machine. */
export function createLocalBrokerAccess(options = {}) {
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
    const agentDir = configured
        ? (isAbsolute(configured) ? configured : resolve(cwd, configured))
        : resolve(homeDir, ".pi", "agent");
    if (!agentDir || agentDir.length > 4096)
        throw inspectionFailure("E_INVALID_OPTIONS");
    const access = {
        platform: currentPlatform,
        agentDir,
        readAgentFile: (relativePath, readOptions) => readBoundedAgentFile(agentDir, relativePath, readOptions.maxBytes, readOptions.signal),
        openLocal: (endpoint, openOptions) => openLocalSocket(agentDir, endpoint, openOptions.signal),
    };
    return Object.freeze(access);
}
