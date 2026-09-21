import test from "node:test";
import assert from "node:assert/strict";
import { Duplex } from "node:stream";
import {
  attachBrokers,
  BrokerAttachmentError,
  BrokerInspectionError,
  createLocalBrokerAccess,
  inspectBroker,
  type BrokerHostAccess,
  type BrokerLocalEndpoint,
} from "./federation.ts";
import { getBrokerPipeName } from "./broker/paths.ts";

function decode(bytes: Buffer): Record<string, unknown> {
  assert.ok(bytes.length >= 4);
  const length = bytes.readUInt32BE(0);
  assert.equal(bytes.length, 4 + length);
  return JSON.parse(bytes.subarray(4).toString("utf8")) as Record<string, unknown>;
}

function encode(value: unknown): Buffer {
  const payload = Buffer.from(JSON.stringify(value));
  const bytes = Buffer.allocUnsafe(4 + payload.length);
  bytes.writeUInt32BE(payload.length, 0);
  payload.copy(bytes, 4);
  return bytes;
}

class InspectionStream extends Duplex {
  request?: Record<string, unknown>;
  constructor(readonly originId: string, readonly scopeId: string | null = "flightdeck") {
    super();
  }
  override _read() {}
  override _write(bytes: Buffer, _encoding: BufferEncoding, callback: (error?: Error | null) => void) {
    this.request = decode(bytes);
    callback();
    queueMicrotask(() => {
      this.push(encode({
        type: "broker_list_scopes_result",
        requestId: this.request!.requestId,
        ok: true,
        localOrigin: { id: this.originId },
        scopes: [{ scopeId: this.scopeId, liveSessions: 2 }],
      }));
      this.push(null);
    });
  }
  override _destroy(error: Error | null, callback: (error?: Error | null) => void) {
    callback(error);
  }
}

class DeferredDestroyInspectionStream extends InspectionStream {
  readonly destructionStarted: Promise<void>;
  private startDestruction!: () => void;
  private finishDestruction!: () => void;
  private readonly destructionFinished = new Promise<void>((resolve) => { this.finishDestruction = resolve; });
  constructor(originId: string) {
    super(originId);
    this.destructionStarted = new Promise<void>((resolve) => { this.startDestruction = resolve; });
  }
  finish() { this.finishDestruction(); }
  override _destroy(error: Error | null, callback: (error?: Error | null) => void) {
    this.startDestruction();
    void this.destructionFinished.then(() => callback(error));
  }
}

function access(options: {
  platform?: BrokerHostAccess["platform"];
  agentDir?: string;
  publication?: Uint8Array;
  open(endpoint: BrokerLocalEndpoint, signal: AbortSignal): Promise<Duplex>;
  onRead?: (path: readonly string[], maxBytes: number, signal: AbortSignal) => void;
}): BrokerHostAccess {
  return {
    platform: options.platform ?? "linux",
    agentDir: options.agentDir ?? "/agent",
    async readAgentFile(path, readOptions) {
      options.onRead?.(path, readOptions.maxBytes, readOptions.signal);
      return options.publication;
    },
    openLocal(endpoint, openOptions) {
      return options.open(endpoint, openOptions.signal);
    },
  };
}

const publication = (stateId: string, port = 43123) => Buffer.from(JSON.stringify({
  transport: "tcp", host: "127.0.0.1", port, stateId,
}));

test("inspection exposes an immutable broker snapshot without endpoint credentials or wire details", async () => {
  const stateId = "state_credential_12345678";
  let openedEndpoint: BrokerLocalEndpoint | undefined;
  let inspectedStream: InspectionStream | undefined;
  const broker = await inspectBroker(access({
    platform: "win32",
    agentDir: "C:\\Users\\alice\\.pi\\agent",
    publication: publication(stateId),
    onRead(path, maxBytes, signal) {
      assert.deepEqual(path, ["parley", "broker.port.json"]);
      assert.equal(maxBytes, 4096);
      assert.equal(signal.aborted, false);
    },
    async open(endpoint) {
      openedEndpoint = endpoint;
      inspectedStream = new InspectionStream("install:550e8400-e29b-41d4-a716-446655440000");
      return inspectedStream;
    },
  }));

  assert.deepEqual(openedEndpoint, { transport: "tcp", port: 43123 });
  assert.equal("stateId" in (openedEndpoint as unknown as Record<string, unknown>), false);
  assert.equal(inspectedStream!.request!.stateId, stateId, "credential remains inside Parley's control exchange");
  assert.deepEqual(broker, {
    origin: { id: "install:550e8400-e29b-41d4-a716-446655440000" },
    scopes: [{ scopeId: "flightdeck", liveSessions: 2 }],
  });
  assert.equal(Object.isFrozen(broker), true);
  assert.equal(Object.isFrozen(broker.origin), true);
  assert.equal(Object.isFrozen(broker.scopes), true);
  assert.equal(Object.isFrozen(broker.scopes[0]), true);
  const serialized = JSON.stringify(broker);
  assert.equal(serialized.includes(stateId), false);
  assert.equal(serialized.includes("43123"), false);
});

test("invalid publication data falls back to the deterministic local endpoint without exposing paths as file authority", async () => {
  let openedEndpoint: BrokerLocalEndpoint | undefined;
  const broker = await inspectBroker(access({
    platform: "win32",
    agentDir: "C:\\Users\\alice\\.pi\\agent",
    publication: Buffer.from("not a publication record"),
    async open(endpoint) {
      openedEndpoint = endpoint;
      return new InspectionStream("install:550e8400-e29b-41d4-a716-446655440001", null);
    },
  }));
  assert.deepEqual(openedEndpoint, {
    transport: "pipe",
    name: getBrokerPipeName("C:\\Users\\alice\\.pi\\agent"),
  });
  assert.equal(broker.scopes[0]?.scopeId, null);
});

test("a connected endpoint with an invalid control response reports sanitized broker protocol failure", async () => {
  const invalid = new Duplex({
    read() {},
    write(_bytes, _encoding, callback) {
      callback();
      this.push(encode({ type: "not_a_broker_response", secret: "do not expose" }));
    },
  });
  await assert.rejects(
    inspectBroker(access({ async open() { return invalid; } })),
    (error: unknown) => error instanceof BrokerInspectionError
      && error.code === "E_BROKER_PROTOCOL" && !error.message.includes("secret"),
  );
});

test("inspection cancellation during joined stream teardown cannot publish a successful handle", async () => {
  const stream = new DeferredDestroyInspectionStream("install:550e8400-e29b-41d4-a716-446655440004");
  const cancellation = new AbortController();
  const inspection = inspectBroker(access({ async open() { return stream; } }), { signal: cancellation.signal });
  await stream.destructionStarted;
  cancellation.abort();
  stream.finish();
  await assert.rejects(inspection, (error: unknown) =>
    error instanceof BrokerInspectionError && error.code === "E_ABORTED");
});

test("inspection preserves the first timeout or abort cause while teardown is joining", async (t) => {
  for (const scenario of [
    { name: "timeout before abort", expected: "E_TIMEOUT", abortFirst: false },
    { name: "abort before timeout", expected: "E_ABORTED", abortFirst: true },
  ] as const) {
    await t.test(scenario.name, async () => {
      const stream = new DeferredDestroyInspectionStream("install:550e8400-e29b-41d4-a716-446655440005");
      const cancellation = new AbortController();
      const inspection = inspectBroker(access({ async open() { return stream; } }), {
        signal: cancellation.signal,
        timeoutMs: 5,
      });
      await stream.destructionStarted;
      if (scenario.abortFirst) cancellation.abort();
      await new Promise((resolve) => setTimeout(resolve, 15));
      if (!scenario.abortFirst) cancellation.abort();
      stream.finish();
      await assert.rejects(inspection, (error: unknown) =>
        error instanceof BrokerInspectionError && error.code === scenario.expected);
    });
  }
});

test("runtime-null options fail through the public sanitized error contract", async () => {
  const host = access({ async open() { throw new Error("must not open"); } });
  await assert.rejects(inspectBroker(host, null as never), (error: unknown) =>
    error instanceof BrokerInspectionError && error.code === "E_INVALID_OPTIONS");
  assert.throws(() => createLocalBrokerAccess(null as never), (error: unknown) =>
    error instanceof BrokerInspectionError && error.code === "E_INVALID_OPTIONS");
});

test("forged broker handles and invalid mappings fail before opening either host capability", async () => {
  let opens = 0;
  const host = access({
    async open() {
      opens++;
      return new InspectionStream("install:550e8400-e29b-41d4-a716-446655440002");
    },
  });
  const real = await inspectBroker(host);
  assert.equal(opens, 1);
  await assert.rejects(attachBrokers({
    initiator: {
      broker: real,
      scopeBindings: [{ localScopeId: "a", localScopeAlias: "left", remoteScopeAlias: "right" }],
    },
    acceptor: {
      broker: { origin: { id: "install:550e8400-e29b-41d4-a716-446655440003" }, scopes: [] },
      scopeBindings: [{ localScopeId: "b", localScopeAlias: "right", remoteScopeAlias: "left" }],
    },
  }), (error: unknown) => error instanceof BrokerAttachmentError && error.code === "E_INVALID_OPTIONS");
  assert.equal(opens, 1, "attachment validation must not invoke either open capability");
});

test("an acquired stream failure remains guarded while its sibling is still opening", async () => {
  let leftCalls = 0;
  let rightCalls = 0;
  let siblingSignal: AbortSignal | undefined;
  let releaseSibling!: (stream: Duplex) => void;
  const sibling = new Promise<Duplex>((resolve) => { releaseSibling = resolve; });
  const leftAccess = access({
    async open() {
      leftCalls++;
      if (leftCalls === 1) return new InspectionStream("install:550e8400-e29b-41d4-a716-446655440020");
      const stream = new Duplex({ read() {}, write(_bytes, _encoding, callback) { callback(); } });
      setTimeout(() => stream.destroy(new Error("private acquired stream failure")), 5);
      return stream;
    },
  });
  const rightAccess = access({
    async open(_endpoint, signal) {
      rightCalls++;
      if (rightCalls === 1) return new InspectionStream("install:550e8400-e29b-41d4-a716-446655440021");
      siblingSignal = signal;
      return await sibling;
    },
  });
  const [left, right] = await Promise.all([inspectBroker(leftAccess), inspectBroker(rightAccess)]);
  const attaching = attachBrokers({
    initiator: { broker: left, scopeBindings: [{ localScopeId: "a", localScopeAlias: "left", remoteScopeAlias: "right" }] },
    acceptor: { broker: right, scopeBindings: [{ localScopeId: "b", localScopeAlias: "right", remoteScopeAlias: "left" }] },
  });
  await new Promise((resolve) => setTimeout(resolve, 15));
  assert.equal(siblingSignal?.aborted, true, "owned stream failure cancels the pending sibling open");
  releaseSibling(new Duplex({ read() {}, write(_bytes, _encoding, callback) { callback(); } }));
  await assert.rejects(attaching, (error: unknown) =>
    error instanceof BrokerAttachmentError && error.code === "E_STREAM_FAILED");
});

test("an invalid fulfilled stream latches failure before asynchronous destruction", async () => {
  let leftCalls = 0;
  let rightCalls = 0;
  let invalidDestructionFinished = false;
  const leftAccess = access({
    async open() {
      leftCalls++;
      if (leftCalls === 1) return new InspectionStream("install:550e8400-e29b-41d4-a716-446655440030");
      return new Duplex({
        objectMode: true,
        read() {},
        write(_value, _encoding, callback) { callback(); },
        destroy(error, callback) {
          setTimeout(() => { invalidDestructionFinished = true; callback(error); }, 30);
        },
      });
    },
  });
  const rightAccess = access({
    async open() {
      rightCalls++;
      if (rightCalls === 1) return new InspectionStream("install:550e8400-e29b-41d4-a716-446655440031");
      return new Duplex({ read() {}, write(_bytes, _encoding, callback) { callback(); } });
    },
  });
  const [left, right] = await Promise.all([inspectBroker(leftAccess), inspectBroker(rightAccess)]);
  await assert.rejects(attachBrokers({
    initiator: { broker: left, scopeBindings: [{ localScopeId: "a", localScopeAlias: "left", remoteScopeAlias: "right" }] },
    acceptor: { broker: right, scopeBindings: [{ localScopeId: "b", localScopeAlias: "right", remoteScopeAlias: "left" }] },
    timeoutMs: 5,
  }), (error: unknown) => error instanceof BrokerAttachmentError && error.code === "E_STREAM_FAILED");
  assert.equal(invalidDestructionFinished, true);
});

test("a failed acquisition aborts its sibling and joins a late returned stream before rejecting", async () => {
  let leftCalls = 0;
  let rightCalls = 0;
  let rightAttachSignal: AbortSignal | undefined;
  let releaseLate!: (stream: Duplex) => void;
  const late = new Promise<Duplex>((resolve) => { releaseLate = resolve; });
  let destructionFinished = false;
  const lateStream = new Duplex({
    emitClose: false,
    read() {},
    write(_bytes, _encoding, callback) { callback(); },
    destroy(error, callback) {
      setTimeout(() => { destructionFinished = true; callback(error); }, 30);
    },
  });

  const leftAccess = access({
    async open() {
      leftCalls++;
      if (leftCalls === 1) return new InspectionStream("install:550e8400-e29b-41d4-a716-446655440010");
      throw new Error("private FlightDeck connection failure");
    },
  });
  const rightAccess = access({
    async open(_endpoint, signal) {
      rightCalls++;
      if (rightCalls === 1) return new InspectionStream("install:550e8400-e29b-41d4-a716-446655440011");
      rightAttachSignal = signal;
      return await late;
    },
  });
  const [left, right] = await Promise.all([inspectBroker(leftAccess), inspectBroker(rightAccess)]);
  const attaching = attachBrokers({
    initiator: {
      broker: left,
      scopeBindings: [{ localScopeId: "a", localScopeAlias: "left", remoteScopeAlias: "right" }],
    },
    acceptor: {
      broker: right,
      scopeBindings: [{ localScopeId: "b", localScopeAlias: "right", remoteScopeAlias: "left" }],
    },
    timeoutMs: 5,
  });
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(rightAttachSignal?.aborted, true);
  let rejected = false;
  void attaching.catch(() => { rejected = true; });
  releaseLate(lateStream);
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(lateStream.destroyed, true);
  assert.equal(rejected, false, "rejection joins the late stream's asynchronous destruction");
  await assert.rejects(attaching, (error: unknown) =>
    error instanceof BrokerAttachmentError && error.code === "E_STREAM_FAILED"
      && !error.message.includes("FlightDeck"));
  assert.equal(destructionFinished, true);
  assert.equal(lateStream.closed, true);
});
