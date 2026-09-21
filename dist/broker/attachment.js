import net from "node:net";
import { randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { Duplex } from "node:stream";
import { MAX_FRAME_BYTES } from "./framing.js";
import { isBrokerAcceptPeerRequest, isBrokerAcceptPeerResult, isBrokerDialPeerRequest, isBrokerDialPeerResult, isBrokerStartPeerRequest, isBrokerStartPeerResult, isPeerHello, isFederationBridgeAttach, } from "./federation-protocol.js";
/** Maximum copied bytes in each admitted opaque write; at most one per direction. */
export const PEER_STREAM_COPY_BYTES = 64 * 1024;
/** Diagnostics deliberately contain no capabilities, endpoint credentials or raw frames. */
export class PeerStreamAttachmentError extends Error {
    code;
    constructor(code) {
        super(`Peer stream attachment failed (${code})`);
        this.code = code;
        this.name = "PeerStreamAttachmentError";
    }
}
function failure(code) {
    return new PeerStreamAttachmentError(code);
}
/** Public `closed` joins _destroy even on streams configured emitClose:false.
 * Poll only during teardown; a turn boundary also drains deferred error events. */
function joinStreamClose(stream) {
    return new Promise((resolve) => {
        let timer;
        const done = () => { clearTimeout(timer); stream.off("close", done); resolve(); };
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
async function discardOwnedStream(stream) {
    if (!(stream instanceof Duplex))
        throw failure("E_INVALID_OPTIONS");
    const onError = () => undefined;
    stream.on("error", onError);
    stream.pause();
    stream.destroy();
    await joinStreamClose(stream);
    stream.off("error", onError);
}
function frame(value) {
    const payload = Buffer.from(JSON.stringify(value), "utf8");
    const result = Buffer.allocUnsafe(4 + payload.length);
    result.writeUInt32BE(payload.length, 0);
    payload.copy(result, 4);
    return result;
}
/** Private lifecycle: all asynchronous work is observed, including shutdown races. */
class OwnedAttachment {
    controller = new AbortController();
    streams = new Set();
    monitored = new Set();
    writes = new Set();
    pumps = [];
    listeners = [];
    server;
    serverClosed = Promise.resolve();
    outcome;
    startupError;
    resolveCompletion;
    completion = new Promise((resolve) => { this.resolveCompletion = resolve; });
    constructor(...streams) {
        for (const stream of streams)
            this.own(stream);
    }
    bindSignal(signal) {
        if (signal !== undefined && !(signal instanceof AbortSignal))
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
        // Disable Node's default socket auto-end: a readable EOF must not close the
        // opposite direction before its already-admitted writes and independent EOF.
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
        this.listeners.push(() => { stream.off("error", onError); stream.off("close", onClose); });
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
        // Synchronous admission barrier, before any asynchronous cleanup.
        this.controller.abort();
        for (const stream of this.streams) {
            stream.pause();
            stream.destroy();
        }
        this.server?.close();
        // Writers are joined even when their callbacks arrive after the close event.
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
        return new Promise((resolve, reject) => {
            const cleanup = () => {
                stream.off("readable", onReady);
                stream.off("end", onReady);
                stream.off("close", onClose);
                this.controller.signal.removeEventListener("abort", onAbort);
            };
            const onReady = () => { cleanup(); resolve(); };
            const onClose = () => { cleanup(); reject(failure("E_CLOSED")); };
            const onAbort = () => { cleanup(); reject(this.startupError ?? failure("E_CLOSED")); };
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
            // read(0) asks Node to publish an EOF even when its readable queue is empty.
            // Waiting only for bytes would otherwise miss a push(null)/FIN with no tail.
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
        // Exact reads leave coalesced peer bytes in the stream's own readable queue.
        try {
            return { bytes: Buffer.concat([header, payload]), value: JSON.parse(payload.toString("utf8")) };
        }
        catch {
            throw failure("E_HANDSHAKE_FAILED");
        }
    }
    async readFrame(stream) {
        return (await this.readRawFrame(stream)).value;
    }
    admit(start) {
        this.check();
        const operation = new Promise((resolve, reject) => {
            try {
                start((error) => error ? reject(failure("E_STREAM_FAILED")) : resolve());
            }
            catch {
                reject(failure("E_STREAM_FAILED"));
            }
        });
        this.writes.add(operation);
        void operation.then(() => this.writes.delete(operation), () => this.writes.delete(operation));
        return operation;
    }
    write(stream, bytes) {
        return this.admit((callback) => { stream.write(bytes, callback); });
    }
    async connect(target) {
        this.check();
        const socket = typeof target === "string" ? net.connect(target) : net.connect({ host: target.host, port: target.port });
        this.own(socket);
        await new Promise((resolve, reject) => {
            const cleanup = () => {
                socket.off("connect", onConnect);
                socket.off("error", onError);
                this.controller.signal.removeEventListener("abort", onAbort);
            };
            const onConnect = () => { cleanup(); resolve(); };
            const onError = () => { cleanup(); reject(failure("E_DIAL_FAILED")); };
            const onAbort = () => { cleanup(); reject(this.startupError ?? failure("E_CLOSED")); };
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
        const accepted = new Promise((resolve, reject) => { resolveAccepted = resolve; rejectAccepted = reject; });
        // A failure before startup awaits this promise must not be unhandled.
        void accepted.catch(() => undefined);
        let consumed = false;
        const server = net.createServer({ allowHalfOpen: true, pauseOnConnect: true }, (socket) => {
            if (consumed || this.outcome) {
                socket.destroy();
                return;
            }
            consumed = true;
            this.own(socket);
            server.close();
            resolveAccepted(socket);
        });
        this.server = server;
        this.serverClosed = new Promise((resolve) => { server.once("close", resolve); });
        const onAbort = () => { rejectAccepted(this.startupError ?? failure("E_CLOSED")); };
        this.controller.signal.addEventListener("abort", onAbort, { once: true });
        this.listeners.push(() => this.controller.signal.removeEventListener("abort", onAbort));
        server.on("error", () => {
            rejectAccepted(failure("E_DIAL_FAILED"));
            this.stop({ status: "failure", error: failure("E_DIAL_FAILED") });
        });
        await new Promise((resolve, reject) => {
            const cleanup = () => {
                server.off("listening", onListening);
                this.controller.signal.removeEventListener("abort", onListenAbort);
            };
            const onListening = () => { cleanup(); resolve(); };
            const onListenAbort = () => { cleanup(); reject(this.startupError ?? failure("E_CLOSED")); };
            server.once("listening", onListening);
            this.controller.signal.addEventListener("abort", onListenAbort, { once: true });
            server.listen({ host: "127.0.0.1", port: 0, signal: this.controller.signal });
        });
        this.check();
        const address = server.address();
        if (!address || typeof address === "string")
            throw failure("E_DIAL_FAILED");
        return { port: address.port, accepted };
    }
    startCopy(local, remote) {
        this.check();
        const copy = async (source, destination) => {
            while (true) {
                const bytes = await this.read(source, PEER_STREAM_COPY_BYTES);
                if (bytes === null) {
                    // EOF closes only this sending half, after all admitted writes settle.
                    await this.admit((callback) => { destination.end(callback); });
                    return;
                }
                await this.write(destination, bytes);
            }
        };
        for (const [source, destination] of [[local, remote], [remote, local]]) {
            const pump = copy(source, destination);
            this.pumps.push(pump);
        }
        void Promise.all(this.pumps).then(() => this.stop({ status: "end" }), (error) => this.stop({ status: "failure", error: error instanceof PeerStreamAttachmentError ? error : failure("E_STREAM_FAILED") }));
    }
}
/** Attach the local broker to one exact authenticated destination broker stream.
 * Resolves only after both brokers accept the peer handshake. Startup rejection
 * also joins destruction of the supplied stream. Caller supplies all authority;
 * peer traffic after preparation is copied opaquely, byte-for-byte, in order. */
export async function attachPeerStream(options) {
    if (!(options.stream instanceof Duplex))
        throw failure("E_INVALID_OPTIONS");
    const owned = new OwnedAttachment(options.stream);
    let timer;
    try {
        owned.bindSignal(options.signal);
        owned.check();
        const timeoutMs = options.handshakeTimeoutMs ?? 10_000;
        if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 2_147_483_647
            || options.stream.readableObjectMode || options.stream.writableObjectMode
            || options.stream.readableEncoding || options.stream.destroyed
            || !options.stream.readable || !options.stream.writable)
            throw failure("E_INVALID_OPTIONS");
        const localOrigin = { ...options.localOrigin };
        const remoteOrigin = { ...options.remoteOrigin };
        const capability = randomBytes(32).toString("hex");
        const dial = {
            type: "broker_dial_peer", requestId: randomUUID(),
            endpoint: { transport: "tcp", host: "127.0.0.1", port: 1 }, capability,
            localOrigin, remoteOrigin,
            scopeBindings: options.localScopeBindings?.map((binding) => ({ ...binding })),
            ...(typeof options.localBroker !== "string" && options.localBroker?.stateId !== undefined ? { stateId: options.localBroker.stateId } : {}),
        };
        const prepare = {
            type: "broker_accept_peer", requestId: randomUUID(), linkId: randomUUID(),
            localOrigin: remoteOrigin, remoteOrigin: localOrigin,
            scopeBindings: options.remoteScopeBindings?.map((binding) => ({ ...binding })),
            ...(options.remoteStateId !== undefined ? { stateId: options.remoteStateId } : {}),
        };
        const target = options.localBroker;
        if (!(typeof target === "string" ? target.length > 0
            : target?.transport === "tcp" && (target.host === "127.0.0.1" || target.host === "::1")
                && Number.isSafeInteger(target.port) && target.port > 0 && target.port <= 65535)
            || !isBrokerDialPeerRequest(dial) || !isBrokerAcceptPeerRequest(prepare))
            throw failure("E_INVALID_OPTIONS");
        timer = setTimeout(() => owned.stop({ status: "failure", error: failure("E_TIMEOUT") }), timeoutMs);
        const listener = await owned.listen();
        dial.endpoint.port = listener.port;
        const control = await owned.connect(target);
        await owned.write(control, frame(dial));
        const dialResult = owned.readFrame(control);
        // A control rejection can arrive while destination preparation is waiting.
        // Stop immediately rather than leaving the owned stream until the deadline.
        void dialResult.then((value) => {
            if (!isBrokerDialPeerResult(value) || value.requestId !== dial.requestId) {
                owned.stop({ status: "failure", error: failure("E_HANDSHAKE_FAILED") });
            }
            else if (!value.ok)
                owned.stop({ status: "failure", error: failure(value.code) });
        }, (error) => {
            owned.stop({ status: "failure", error: error instanceof PeerStreamAttachmentError ? error : failure("E_HANDSHAKE_FAILED") });
        });
        // Race the attachment against a broker rejection that never reaches the listener.
        const local = await Promise.race([
            listener.accepted,
            dialResult.then((value) => {
                if (!isBrokerDialPeerResult(value) || value.requestId !== dial.requestId)
                    throw failure("E_HANDSHAKE_FAILED");
                if (!value.ok)
                    throw failure(value.code);
                throw failure("E_HANDSHAKE_FAILED"); // Success cannot precede forwarding/handshake.
            }),
        ]);
        const attach = await owned.readFrame(local);
        if (!isFederationBridgeAttach(attach) || attach.capability.length !== capability.length
            || !timingSafeEqual(Buffer.from(attach.capability), Buffer.from(capability)))
            throw failure("E_HANDSHAKE_FAILED");
        prepare.linkId = attach.linkId;
        await owned.write(options.stream, frame(prepare));
        const prepared = await owned.readFrame(options.stream);
        if (!isBrokerAcceptPeerResult(prepared) || prepared.requestId !== prepare.requestId)
            throw failure("E_HANDSHAKE_FAILED");
        if (!prepared.ok)
            throw failure(prepared.code);
        if (prepared.linkId !== attach.linkId)
            throw failure("E_HANDSHAKE_FAILED");
        owned.startCopy(local, options.stream);
        const ready = await dialResult;
        if (!isBrokerDialPeerResult(ready) || ready.requestId !== dial.requestId)
            throw failure("E_HANDSHAKE_FAILED");
        if (!ready.ok)
            throw failure(ready.code);
        if (ready.linkId !== attach.linkId)
            throw failure("E_HANDSHAKE_FAILED");
        owned.check();
        clearTimeout(timer);
        owned.retireControl(control);
        return Object.freeze({
            linkId: ready.linkId,
            localOrigin: Object.freeze(localOrigin), remoteOrigin: Object.freeze(remoteOrigin),
            completion: owned.completion,
            close: () => { owned.stop({ status: "closed", reason: "close" }); return owned.completion; },
        });
    }
    catch (error) {
        const safeError = error instanceof PeerStreamAttachmentError ? error : failure("E_INVALID_OPTIONS");
        owned.stop({ status: "failure", error: safeError });
        await owned.completion;
        throw safeError;
    }
    finally {
        if (timer)
            clearTimeout(timer);
    }
}
/** Join two exact broker streams as a direct, single-hop peer link. Both
 * streams transfer immediately, even on invalid arguments, cancellation or
 * startup rejection. No stream is reopened, retried or replayed. The remote
 * broker is prepared first; the local broker emits its actual outbound hello
 * and validates the actual remote ack. Readiness requires its start success.
 * Thereafter bytes are copied opaquely, in order, with bounded backpressure.
 * Rejection joins both streams' destruction and admitted write callbacks. */
export async function attachPeerStreams(options) {
    const local = options?.local?.stream;
    const remote = options?.remote?.stream;
    const owned = new OwnedAttachment(...[...new Set([local, remote])].filter((stream) => stream instanceof Duplex));
    let timer;
    try {
        owned.bindSignal(options?.signal);
        owned.check();
        const timeoutMs = options?.handshakeTimeoutMs ?? 10_000;
        if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 2_147_483_647
            || local === remote || ![local, remote].every((stream) => stream instanceof Duplex
            && !stream.readableObjectMode && !stream.writableObjectMode && !stream.readableEncoding
            && !stream.destroyed && stream.readable && stream.writable))
            throw failure("E_INVALID_OPTIONS");
        const localOrigin = { ...options.local.origin };
        const remoteOrigin = { ...options.remote.origin };
        const linkId = randomUUID();
        const start = {
            type: "broker_start_peer", requestId: randomUUID(), linkId,
            localOrigin, remoteOrigin,
            scopeBindings: options.local.scopeBindings?.map((binding) => ({ ...binding })),
            ...(options.local.stateId !== undefined ? { stateId: options.local.stateId } : {}),
        };
        const prepare = {
            type: "broker_accept_peer", requestId: randomUUID(), linkId,
            localOrigin: remoteOrigin, remoteOrigin: localOrigin,
            scopeBindings: options.remote.scopeBindings?.map((binding) => ({ ...binding })),
            ...(options.remote.stateId !== undefined ? { stateId: options.remote.stateId } : {}),
        };
        if (!isBrokerStartPeerRequest(start) || !isBrokerAcceptPeerRequest(prepare))
            throw failure("E_INVALID_OPTIONS");
        timer = setTimeout(() => owned.stop({ status: "failure", error: failure("E_TIMEOUT") }), timeoutMs);
        await owned.write(remote, frame(prepare));
        const prepared = await owned.readFrame(remote);
        if (!isBrokerAcceptPeerResult(prepared) || prepared.requestId !== prepare.requestId)
            throw failure("E_HANDSHAKE_FAILED");
        if (!prepared.ok)
            throw failure(prepared.code);
        if (prepared.linkId !== linkId)
            throw failure("E_HANDSHAKE_FAILED");
        await owned.write(local, frame(start));
        const hello = await owned.readRawFrame(local);
        if (isBrokerStartPeerResult(hello.value) && hello.value.requestId === start.requestId && !hello.value.ok) {
            throw failure(hello.value.code);
        }
        if (!isPeerHello(hello.value) || hello.value.linkId !== linkId)
            throw failure("E_HANDSHAKE_FAILED");
        await owned.write(remote, hello.bytes);
        const ack = await owned.readRawFrame(remote);
        // The outbound broker, not this consumer, validates the real ack. Keep its
        // exact bytes; do not manufacture authority or negotiated peer features.
        await owned.write(local, ack.bytes);
        const ready = await owned.readFrame(local);
        if (!isBrokerStartPeerResult(ready) || ready.requestId !== start.requestId)
            throw failure("E_HANDSHAKE_FAILED");
        if (!ready.ok)
            throw failure(ready.code);
        if (ready.linkId !== linkId)
            throw failure("E_HANDSHAKE_FAILED");
        owned.check();
        clearTimeout(timer);
        owned.startCopy(local, remote);
        return Object.freeze({
            linkId, localOrigin: Object.freeze(localOrigin), remoteOrigin: Object.freeze(remoteOrigin),
            completion: owned.completion,
            close: () => { owned.stop({ status: "closed", reason: "close" }); return owned.completion; },
        });
    }
    catch (error) {
        const safeError = error instanceof PeerStreamAttachmentError ? error : failure("E_INVALID_OPTIONS");
        owned.stop({ status: "failure", error: safeError });
        await owned.completion;
        throw safeError;
    }
    finally {
        if (timer)
            clearTimeout(timer);
    }
}
function combineSignals(internal, external) {
    if (external !== undefined && !(external instanceof AbortSignal))
        throw failure("E_INVALID_OPTIONS");
    if (!external)
        return { signal: internal, dispose: () => undefined };
    const combined = new AbortController();
    const onAbort = () => combined.abort(); // Do not propagate provider/caller diagnostics.
    for (const signal of [internal, external])
        signal.addEventListener("abort", onAbort, { once: true });
    if (internal.aborted || external.aborted)
        onAbort();
    return {
        signal: combined.signal,
        dispose: () => { for (const signal of [internal, external])
            signal.removeEventListener("abort", onAbort); },
    };
}
class AttachmentAdmissions {
    closed = false;
    admitted = new Set();
    closing;
    attachOwned(options) {
        return this.start(options, options.stream);
    }
    acquire(factory, binding, options) {
        return this.start(options, undefined, (signal) => factory(binding, signal));
    }
    start(options, ownedStream, acquire) {
        if (this.closed) {
            if (ownedStream)
                return discardOwnedStream(ownedStream).then(() => { throw failure("E_CLOSED"); });
            return Promise.reject(failure("E_CLOSED"));
        }
        const cancellation = new AbortController();
        let combined;
        try {
            combined = combineSignals(cancellation.signal, options.signal);
        }
        catch {
            if (ownedStream)
                return discardOwnedStream(ownedStream).then(() => { throw failure("E_INVALID_OPTIONS"); });
            return Promise.reject(failure("E_INVALID_OPTIONS"));
        }
        let resolveJoined;
        const joined = new Promise((resolve) => { resolveJoined = resolve; });
        const record = { abort: () => cancellation.abort(), joined };
        // Record before invoking any caller factory, including reentrant shutdown.
        this.admitted.add(record);
        const acquireAndAttach = async () => {
            if (combined.signal.aborted)
                throw failure(this.closed ? "E_CLOSED" : "E_ABORTED");
            let stream;
            try {
                stream = await acquire(combined.signal);
            }
            catch {
                throw failure(combined.signal.aborted ? (this.closed ? "E_CLOSED" : "E_ABORTED") : "E_STREAM_FAILED");
            }
            if (combined.signal.aborted) {
                await discardOwnedStream(stream);
                throw failure(this.closed ? "E_CLOSED" : "E_ABORTED");
            }
            return await attachPeerStream({ ...options, stream, signal: combined.signal });
        };
        // Owned-stream transfer is synchronous at invocation, not deferred behind
        // provider acquisition or another attachment's readiness.
        const ready = ownedStream
            ? attachPeerStream({ ...options, stream: ownedStream, signal: combined.signal })
            : acquireAndAttach();
        void ready.then((attachment) => attachment.completion, () => undefined).then(() => {
            combined.dispose();
            this.admitted.delete(record);
            resolveJoined();
        });
        return ready;
    }
    close() {
        if (this.closing)
            return this.closing;
        this.closed = true;
        const records = [...this.admitted];
        // Publish the joined promise before abort callbacks can reenter close().
        this.closing = Promise.all(records.map((record) => record.joined)).then(() => undefined);
        for (const record of records)
            record.abort();
        return this.closing;
    }
}
/** Caller-created, provider-neutral lifetime for explicit attachment attempts.
 * No globals, discovery, default endpoints, host interpretation or auto-enable.
 * Registration alone never invokes a factory. Close permanently revokes all
 * admissions and joins started acquisitions, late-stream disposal and links. */
export class PeerStreamController {
    closed = false;
    direct = new AttachmentAdmissions();
    providers = new Set();
    closing;
    registerProvider(factory) {
        if (this.closed)
            throw failure("E_CLOSED");
        if (typeof factory !== "function")
            throw failure("E_INVALID_OPTIONS");
        const admissions = new AttachmentAdmissions();
        this.providers.add(admissions);
        return Object.freeze({
            attach: (binding, options) => this.closed
                ? Promise.reject(failure("E_CLOSED")) : admissions.acquire(factory, binding, options),
            close: () => {
                const closed = admissions.close();
                void closed.then(() => this.providers.delete(admissions));
                return closed;
            },
        });
    }
    attachOwnedStream(options) {
        return this.direct.attachOwned(options);
    }
    close() {
        if (this.closing)
            return this.closing;
        this.closed = true;
        let resolveClosed;
        this.closing = new Promise((resolve) => { resolveClosed = resolve; });
        const joined = [this.direct.close(), ...[...this.providers].map((provider) => provider.close())];
        void Promise.all(joined).then(() => { this.providers.clear(); resolveClosed(); });
        return this.closing;
    }
}
