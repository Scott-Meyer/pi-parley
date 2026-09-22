import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import { Duplex, Readable, Transform, Writable } from "node:stream";
import { createMessageReader } from "./broker/framing.ts";
import { once } from "node:events";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { attachPeerStreams, PeerStreamController, type PeerStreamAttachment } from "./broker/attachment.ts";
import { getBrokerSocketPath, getParleyDirPath, readBrokerTcpEndpoint, type BrokerConnectTarget } from "./broker/paths.ts";
import { encodeOriginQualifiedSessionIdentity } from "./broker/federation-protocol.ts";
import { writeMessage } from "./broker/framing.ts";
import type { FederationOrigin } from "./broker/federation-types.ts";
import { getTsxCliPath } from "./broker/spawn.ts";
import { createExtensionHarness, type CapturedToolResult } from "./test/extension-harness.ts";
import type { Message, SessionInfo } from "./types.ts";
import { restoreConversationHistory } from "./conversation-history.ts";

// This test process owns its fixture brokers, never the invoking agent's runtime.
for (const key of Object.keys(process.env)) {
  if ((key.startsWith("PI_PARLEY_") && !key.startsWith("PI_PARLEY_TEST_")) || key.startsWith("PI_SUBAGENT_")
    || key.startsWith("FLIGHTDECK_") || key === "PI_CODING_AGENT_DIR") delete process.env[key];
}
const repo = process.cwd();
const text = (result: CapturedToolResult) => result.content.map((part) => part.text).join("\n");

async function startBroker(agentDir: string, tcp?: boolean): Promise<ChildProcess> {
  const child = spawn(process.execPath, [getTsxCliPath(), path.join(repo, "broker/broker.ts")], {
    cwd: repo, env: { ...process.env, PI_CODING_AGENT_DIR: agentDir,
      ...(tcp === undefined ? {} : { PI_PARLEY_TRANSPORT: tcp ? "tcp" : "socket" }) }, stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "";
  child.stderr!.on("data", (data: Buffer) => { stderr = (stderr + data.toString()).slice(-4000); });
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => finish(new Error(`Broker startup timed out: ${stderr}`)), 10_000);
    const onData = (data: Buffer) => { if (data.toString().includes("Parley broker started")) finish(); };
    const onExit = () => finish(new Error(`Broker exited during startup: ${stderr}`));
    const finish = (error?: Error) => {
      clearTimeout(timer); child.stdout!.off("data", onData); child.off("exit", onExit);
      if (error) { child.kill("SIGTERM"); reject(error); } else resolve();
    };
    child.stdout!.on("data", onData); child.once("exit", onExit);
  });
  return child;
}
async function stopBroker(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  child.kill("SIGTERM");
  await exited;
}
async function inAgentDir<T>(
  dir: string,
  work: () => Promise<T>,
  transport?: "socket" | "tcp",
): Promise<T> {
  const previous = process.env.PI_CODING_AGENT_DIR;
  const previousTransport = process.env.PI_PARLEY_TRANSPORT;
  process.env.PI_CODING_AGENT_DIR = dir;
  if (transport) process.env.PI_PARLEY_TRANSPORT = transport;
  try { return await work(); }
  finally {
    if (previous === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previous;
    if (previousTransport === undefined) delete process.env.PI_PARLEY_TRANSPORT;
    else process.env.PI_PARLEY_TRANSPORT = previousTransport;
  }
}
function caller(harness: ReturnType<typeof createExtensionHarness>) {
  return (params: Record<string, unknown>) => harness.tools.find((tool) => tool.name === "parley")!
    .execute("federated-call", params, new AbortController().signal, undefined, harness.ctx);
}
async function waitUntil(predicate: () => boolean | Promise<boolean>, explanation: string): Promise<void> {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.fail(explanation);
}

// A deliberately lossy opaque provider, not a mock broker. It admits the ask
// and its real reply, but withholds just that ask's delivery acknowledgement.
function lossyProvider(socket: net.Socket) {
  const lost = new Set<string>();
  const dispatched: string[] = [];
  const observe = createMessageReader((raw) => {
    const frame = raw as { type?: string; sendId?: string; message?: { text?: string; expectsReply?: boolean } };
    if (frame.type === "peer_send" && frame.message?.expectsReply && frame.message.text?.includes("fast:lost-ack")) {
      lost.add(frame.sendId!); dispatched.push(frame.message.text);
    }
  }, (error) => socket.destroy(error));
  const decode = createMessageReader((raw) => {
    const value = raw as { type?: string; sendId?: string };
    if (value.type === "peer_send_result" && lost.has(value.sendId!)) return;
    const payload = Buffer.from(JSON.stringify(raw));
    const frame = Buffer.alloc(4 + payload.length);
    frame.writeUInt32BE(payload.length); payload.copy(frame, 4);
    incoming.push(frame);
  }, (error) => incoming.destroy(error));
  const incoming = new Transform({ transform(chunk: Buffer, _encoding, done) { decode(chunk); done(); } });
  const outgoing = new Writable({
    write(chunk: Buffer, _encoding, done) { observe(chunk); socket.write(chunk, done); },
    final(done) { socket.end(done); },
    destroy(error, done) {
      if (socket.closed) { done(error); return; }
      socket.once("close", () => done(error)); socket.destroy();
    },
  });
  socket.on("error", (error) => incoming.destroy(error));
  socket.pipe(incoming);
  return { stream: Duplex.fromWeb({ readable: Readable.toWeb(incoming), writable: Writable.toWeb(outgoing) }, { allowHalfOpen: true }), dispatched };
}

test("neutral registered provider delivers bidirectional extension asks, fast answers and retained threaded notifications", { timeout: 45_000 }, async () => {
  const { default: extension } = await import("./index.ts");
  const dirs = [0, 1].map(() => mkdtempSync(path.join(process.platform === "win32" ? tmpdir() : "/tmp", "pl-ext-")));
  const brokers: ChildProcess[] = [];
  const a = createExtensionHarness("consumer-a", { sessionId: "consumer-a" });
  const b = createExtensionHarness("consumer-b", { sessionId: "consumer-b" });
  const callA = caller(a), callB = caller(b);
  const controller = new PeerStreamController();
  const automaticReplies: Promise<CapturedToolResult>[] = [];
  try {
    for (const dir of dirs) brokers.push(await startBroker(dir, false));
    for (const [index, harness] of [a, b].entries()) {
      await inAgentDir(dirs[index]!, async () => {
        const childMetadata: Record<string, string> = index === 0 ? {
          PI_SUBAGENT_ORCHESTRATOR_TARGET: "consumer-b",
          PI_SUBAGENT_ORCHESTRATOR_SESSION_ID: "consumer-b",
          PI_SUBAGENT_RUN_ID: "fixture-run-1234",
          PI_SUBAGENT_CHILD_AGENT: "consumer",
          PI_SUBAGENT_CHILD_INDEX: "0",
          PI_SUBAGENT_PARLEY_SESSION_NAME: "subagent-consumer-fixture-run-1",
        } : {};
        try {
          Object.assign(process.env, childMetadata);
          extension(harness.pi as never);
          await harness.emitLifecycle("session_start");
          if (index === 0) {
            const advertised = await caller(harness)({ action: "advertise", name: "consumer-a" });
            assert.notEqual(advertised.details?.error, true, text(advertised));
          }
          const listed = await caller(harness)({ action: "list" });
          assert.notEqual(listed.details?.error, true);
        } finally {
          for (const key of Object.keys(childMetadata)) delete process.env[key];
        }
      }, "socket");
    }
    let acquisitions = 0;
    let faults: ReturnType<typeof lossyProvider> | undefined;
    const provider = controller.registerProvider<{ socketPath: string }>(async (binding, signal) => {
      acquisitions++;
      const stream = net.connect({ path: binding.socketPath, allowHalfOpen: true });
      try { await once(stream, "connect", { signal }); }
      catch (error) {
        const closed = stream.closed ? Promise.resolve() : once(stream, "close");
        stream.destroy(); await closed;
        throw error;
      }
      faults = lossyProvider(stream);
      return faults.stream;
    });
    assert.equal(acquisitions, 0, "registration is explicit and never auto-connects");
    const attachment = await provider.attach({ socketPath: getBrokerSocketPath(process.platform, dirs[1]) }, {
      localBroker: getBrokerSocketPath(process.platform, dirs[0]),
      localOrigin: { id: "host:consumer-a" }, remoteOrigin: { id: "host:consumer-b" },
      localScopeBindings: [{ localScopeId: null, localScopeAlias: "a", remoteScopeAlias: "b" }],
      remoteScopeBindings: [{ localScopeId: null, localScopeAlias: "b", remoteScopeAlias: "a" }],
    });
    assert.equal(acquisitions, 1, "one explicit attachment performs one acquisition");
    assert.equal(attachment.remoteOrigin.id, "host:consumer-b");
    await waitUntil(async () => /consumer-b.*text conversations/.test(text(await callA({ action: "list" })))
      && /consumer-a.*text conversations/.test(text(await callB({ action: "list" }))), "both consumers must truthfully advertise negotiated conversations");

    // Answer as soon as the host observes the request: no manual relay or slow
    // model step may be needed to install the original sender's correlation.
    for (const [harness, call] of [[a, callA], [b, callB]] as const) {
      const capture = harness.pi.sendMessage;
      harness.pi.sendMessage = (envelope, options) => {
        capture(envelope, options);
        const details = envelope.details as { message?: Message } | undefined;
        if (details?.message?.expectsReply && details.message.content.text.includes("fast:")) {
          automaticReplies.push(call({ action: "reply", replyTo: details.message.id, message: `answer:${details.message.content.text}` }));
        }
      };
    }
    for (const [asker, responder, call, answerCall, target] of [[a, b, callA, callB, "consumer-b"], [b, a, callB, callA, "consumer-a"]] as const) {
      const result = await call({ action: "ask", to: target, message: `fast:${target}` });
      assert.notEqual(result.details?.error, true, text(result));
      assert.match(text(result), new RegExp(`answer:fast:${target}`));
      const authoredAnswer = await automaticReplies.at(-1)!;
      assert.equal(authoredAnswer.details?.delivered, true, text(authoredAnswer));
      const questionId = result.details?.messageId as string;
      const answerId = result.details?.replyMessageId as string;
      assert.match(questionId, /^oqm1\./);
      assert.match(answerId, /^oqm1\./);
      const history = asker.entries.find((entry) => entry.type === "parley_ask_pending" && (entry.data as { messageId?: string }).messageId === questionId);
      assert.ok((history?.data as { endpointEpoch?: string }).endpointEpoch);
      assert.ok((history?.data as { originEpoch?: string }).originEpoch);
      assert.match(text(await answerCall({ action: "read", messageId: questionId })), new RegExp(`fast:${target}`));
      assert.match(text(await call({ action: "read", messageId: answerId })), new RegExp(`answer:fast:${target}`));
      assert.equal(asker.sentMessages.some((entry) => (entry.message.details as { message?: Message })?.message?.id === answerId), false,
        "blocking answers return through the tool, not a duplicate wakeup");
      assert.doesNotMatch(text(await answerCall({ action: "pending" })), /awaiting your reply/);
      assert.ok(responder.entries.some((entry) => entry.type === "parley_inbound_settled" && (entry.data as { messageId?: string }).messageId === questionId));
    }
    const question = await callA({ action: "ask", to: "consumer-b", message: "async release decision", blocking: false });
    const questionId = question.details?.messageId as string;
    assert.match(questionId, /^oqm1\./);
    await waitUntil(() => b.sentMessages.some((entry) => (entry.message.details as { message?: Message })?.message?.id === questionId), "async question reaches the remote consumer");
    const progress = await callB({ action: "send", to: "consumer-a", replyTo: questionId, message: "still investigating" });
    assert.equal(progress.details?.delivered, true, text(progress));
    await waitUntil(() => a.sentMessages.some((entry) => entry.message.content?.includes("still investigating")), "progress is retained without completing the question");
    assert.equal(((await callA({ action: "status" })).details?.outstandingAsks as Array<{ messageId: string }>)[0]?.messageId, questionId);
    assert.match(text(await callB({ action: "pending" })), /async release decision/);
    const recovered = restoreConversationHistory(a.entries.map((entry) => ({ type: "custom", customType: entry.type, data: entry.data })));
    assert.equal(recovered.outgoing.size, 1);
    assert.ok(recovered.outgoing.get(questionId)?.endpointEpoch);
    assert.ok(recovered.outgoing.get(questionId)?.originEpoch);
    assert.equal(recovered.incoming.get(progress.details?.messageId as string)?.message.replyTo, questionId,
      "recovery preserves the author-qualified handle and original ask correlation");
    const answer = await callB({ action: "reply", replyTo: questionId, message: "release approved" });
    assert.equal(answer.details?.delivered, true, text(answer));
    await waitUntil(async () => ((await callA({ action: "status" })).details?.outstandingAsks as unknown[]).length === 0, "only the completing answer settles the original ask");

    const notice = await callA({ action: "send", to: "consumer-b", message: "notification with retained identity" });
    assert.equal(notice.details?.delivered, true, text(notice));
    const noticeId = notice.details?.messageId as string;
    assert.match(noticeId, /^oqm1\./);
    await waitUntil(() => b.sentMessages.some((entry) => (entry.message.details as { message?: Message })?.message?.id === noticeId), "remote notification carries its canonical retained handle");
    const noticeReply = await callB({ action: "reply", replyTo: noticeId, message: "notification acknowledged" });
    assert.equal(noticeReply.details?.delivered, true, text(noticeReply));
    await waitUntil(() => a.sentMessages.some((entry) => entry.message.content?.includes("notification acknowledged")), "notification reply uses its recorded reverse edge");
    assert.match(text(await callB({ action: "read", messageId: noticeId })), /notification with retained identity/);

    const unknownWithAnswer = await callA({ action: "ask", to: "consumer-b", message: "fast:lost-ack" });
    assert.match(text(unknownWithAnswer), /answer:fast:lost-ack/, "a real correlated answer is not swallowed by a missing ask ACK");
    assert.equal(unknownWithAnswer.details?.delivery, "unknown");
    assert.equal(unknownWithAnswer.details?.outcomeKnown, false, "the independently observed answer does not manufacture a transport ACK");
    assert.equal(unknownWithAnswer.details?.retryable, false);
    assert.match(unknownWithAnswer.details?.replyMessageId as string, /^oqm1\./);
    assert.equal((await automaticReplies.at(-1)!).details?.delivered, true);
    assert.deepEqual(faults!.dispatched, ["fast:lost-ack"], "unknown acceptance never automatically replays the ask");
    assert.deepEqual((await callA({ action: "status" })).details?.outstandingAsks, []);
    const answerHooks = await a.emitLifecycleResults("tool_result", {
      toolName: "parley", ...unknownWithAnswer, isError: false,
    });
    assert.equal(answerHooks.some((result) => (result as { isError?: boolean } | undefined)?.isError === true), false,
      "the host-visible operation succeeds with its received answer, independently of unknown acceptance");
    const theme = { fg: (_name: string, value: string) => value, bold: (value: string) => value };
    const renderedAnswer = a.tools.find((tool) => tool.name === "parley")!
      .renderResult!(unknownWithAnswer, { isPartial: false, expanded: true }, theme, { isError: false })
      .render(120).join("\n");
    assert.match(renderedAnswer, /^\? /, "the receipt still visibly carries its uncertainty");
    assert.doesNotMatch(renderedAnswer, /✗/);

    const supervisorTool = a.tools.find((tool) => tool.name === "contact_supervisor")!;
    for (const question of ["fast:supervisor decision", "fast:lost-ack"]) {
      const result = await supervisorTool.execute("remote-supervisor-call", { reason: "need_decision", message: question },
        new AbortController().signal, undefined, a.ctx);
      assert.notEqual(result.details?.error, true, text(result));
      assert.match(text(result), /\*\*Reply from supervisor:\*\*/);
      assert.match(text(result), new RegExp(question));
      assert.match(result.details?.messageId as string, /^oqm1\./);
      assert.match(result.details?.replyMessageId as string, /^oqm1\./);
      const pending = a.entries.find((entry) => entry.type === "parley_ask_pending"
        && (entry.data as { messageId?: string }).messageId === result.details?.messageId);
      assert.ok((pending?.data as { endpointEpoch?: string }).endpointEpoch);
      assert.ok((pending?.data as { originEpoch?: string }).originEpoch);
      if (question === "fast:lost-ack") {
        assert.equal(result.details?.delivery, "unknown");
        assert.equal(result.details?.outcomeKnown, false);
        assert.equal(result.details?.retryable, false);
        assert.equal(faults!.dispatched.length, 2, "one dispatch per lost-ACK ask, with no replay");
      }
      const hooks = await a.emitLifecycleResults("tool_result", { toolName: "contact_supervisor", ...result, isError: false });
      assert.equal(hooks.some((hook) => (hook as { isError?: boolean } | undefined)?.isError === true), false);
      const rendered = supervisorTool.renderResult!(result, { isPartial: false, expanded: true }, theme, { isError: false })
        .render(120).join("\n");
      assert.match(rendered, question === "fast:lost-ack" ? /^\? / : /^✓ /);
      assert.doesNotMatch(rendered, /✗/);
      assert.deepEqual((await callA({ action: "status" })).details?.outstandingAsks, []);
    }
  } finally {
    await controller.close();
    for (const harness of [a, b]) await harness.emitLifecycle("session_shutdown");
    for (const broker of brokers) await stopBroker(broker);
    for (const dir of dirs) rmSync(dir, { recursive: true, force: true });
  }
});

// A host-boundary fixture: real brokers and registered AgentTools, with the
// existing in-process extension host standing in for Pi (not installed SDK proof).
async function connectFixtureBroker(target: BrokerConnectTarget): Promise<net.Socket> {
  const stream = typeof target === "string" ? net.connect(target) : net.connect(target.port, target.host);
  stream.on("error", () => undefined);
  try { await once(stream, "connect"); return stream; }
  catch (error) { stream.destroy(); throw error; }
}
async function fixtureOrigin(target: BrokerConnectTarget): Promise<FederationOrigin> {
  const stream = await connectFixtureBroker(target);
  try {
    return await new Promise((resolve, reject) => {
      const requestId = "late-qualified-origin";
      const timer = setTimeout(() => reject(new Error("Fixture origin query timed out")), 5000);
      stream.on("data", createMessageReader(raw => {
        const frame = raw as { type?: string; requestId?: string; ok?: boolean; localOrigin?: FederationOrigin };
        if (frame.type !== "broker_list_scopes_result" || frame.requestId !== requestId) return;
        clearTimeout(timer);
        if (frame.ok === true && frame.localOrigin) resolve(frame.localOrigin);
        else reject(new Error("Fixture origin query refused"));
      }, error => { clearTimeout(timer); reject(error); }));
      writeMessage(stream, { type: "broker_list_scopes", requestId,
        ...(typeof target === "string" ? {} : { stateId: target.stateId }) });
    });
  } finally { stream.destroy(); }
}

for (const tcp of [false, true]) test(`late ${tcp ? "authenticated TCP" : "socket"} mesh supports qualified AgentTool replies and fails closed on sender withdrawal`,
  { timeout: 45_000 }, async () => {
    const { default: extension } = await import("./index.ts");
    const dirs = [0, 1, 2].map(() => mkdtempSync(path.join(process.platform === "win32" ? tmpdir() : "/tmp", "pl-lqr-")));
    const brokers: ChildProcess[] = [];
    const links: PeerStreamAttachment[] = [];
    const harnesses: ReturnType<typeof createExtensionHarness>[] = [];
    const streams: net.Socket[] = [];
    type FixtureBroker = { dir: string; target: BrokerConnectTarget; origin: FederationOrigin; alias: string };
    type Consumer = { broker: FixtureBroker; id: string; harness: ReturnType<typeof createExtensionHarness>; call: ReturnType<typeof caller> };
    const qualified = (actor: Consumer) => encodeOriginQualifiedSessionIdentity({ originId: actor.broker.origin.id,
      remoteScopeAlias: actor.broker.alias, remoteStableSessionId: actor.id });
    const history = (actor: Consumer) => restoreConversationHistory(actor.harness.ctx.sessionManager.getEntries());
    const inbound = (actor: Consumer, id: string) => actor.harness.entries.filter(entry => entry.type === "parley_inbound_received"
      && (entry.data as { message: Message }).message.id === id);
    const settled = (actor: Consumer, id: string) => actor.harness.entries.some(entry => entry.type === "parley_inbound_settled"
      && (entry.data as { messageId: string }).messageId === id);
    const outstanding = async (actor: Consumer) => {
      const result = await actor.call({ action: "status" });
      assert.notEqual(result.details?.error, true, text(result));
      return result.details?.outstandingAsks as Array<{ messageId: string }>;
    };
    const list = async (actor: Consumer) => {
      const result = await actor.call({ action: "list" });
      assert.notEqual(result.details?.error, true, text(result));
      return text(result);
    };
    const admit = async (broker: FixtureBroker, id: string, name: string): Promise<Consumer> => {
      const harness = createExtensionHarness(name, { sessionId: id }); harnesses.push(harness);
      const call = caller(harness);
      const previous = process.env.PI_PARLEY_TRANSPORT;
      try {
        process.env.PI_PARLEY_TRANSPORT = tcp ? "tcp" : "socket";
        await inAgentDir(broker.dir, async () => {
          extension(harness.pi as never);
          await harness.emitLifecycle("session_start");
          const result = await call({ action: "list" });
          assert.notEqual(result.details?.error, true, text(result));
        });
      } finally {
        if (previous === undefined) delete process.env.PI_PARLEY_TRANSPORT;
        else process.env.PI_PARLEY_TRANSPORT = previous;
      }
      return { broker, id, harness, call };
    };
    const attach = async (a: FixtureBroker, b: FixtureBroker) => {
      const endpoint = async (local: FixtureBroker, remote: FixtureBroker) => {
        const stream = await connectFixtureBroker(local.target); streams.push(stream);
        return { stream, origin: local.origin,
          scopeBindings: [{ localScopeId: null, localScopeAlias: local.alias, remoteScopeAlias: remote.alias }],
          ...(typeof local.target === "string" ? {} : { stateId: local.target.stateId }) };
      };
      const link = await attachPeerStreams({ local: await endpoint(a, b), remote: await endpoint(b, a) });
      links.push(link); return link;
    };
    try {
      const machines: FixtureBroker[] = [];
      for (const [index, dir] of dirs.entries()) {
        brokers.push(await startBroker(dir, tcp));
        const target = tcp ? readBrokerTcpEndpoint(getParleyDirPath(dir)) : getBrokerSocketPath(process.platform, dir);
        machines.push({ dir, target, origin: await fixtureOrigin(target), alias: `export-${index}` });
      }
      const [ma, mb, mc] = machines as [FixtureBroker, FixtureBroker, FixtureBroker];
      // Admit two edges before any actor exists; the third arrives after C.
      const ab = await attach(ma, mb); await attach(ma, mc);
      const a = await admit(ma, "late-qualified-a", "late-a");
      const b = await admit(mb, "late-qualified-b", "late-b");
      const c = await admit(mc, "late-qualified-c", "late-c");
      await waitUntil(async () => (await list(a)).includes(qualified(b)) && (await list(a)).includes(qualified(c)), "late A roster converges");
      assert.ok(!(await list(b)).includes(qualified(c)), "B cannot import C through broker A");
      await attach(mb, mc);
      const actors = [a, b, c];
      for (const actor of actors) await waitUntil(async () => {
        const rows = await list(actor);
        return actors.filter(other => other !== actor).every(other => rows.includes(qualified(other)));
      }, "third-edge qualified roster converges");

      for (const asker of actors) for (const responder of actors) if (asker !== responder) {
        // Both selectors come from fresh registered-tool lists, not raw foreign UUIDs.
        assert.ok((await list(asker)).includes(qualified(responder)));
        const question = await asker.call({ action: "ask", to: qualified(responder), blocking: false,
          message: `late-qualified:${asker.id}:${responder.id}` });
        assert.equal(question.details?.delivered, true, text(question));
        const questionId = question.details?.messageId as string;
        assert.match(questionId, /^oqm1\./);
        await waitUntil(() => inbound(responder, questionId).length > 0, "ask reaches actual receiver history");
        assert.equal(inbound(responder, questionId).length, 1);
        const received = history(responder).incoming.get(questionId)!;
        assert.deepEqual(received.from, (inbound(responder, questionId)[0]!.data as { from: SessionInfo }).from,
          "actual inbound record and restored host-persisted context preserve identical sender identity");
        assert.equal(history(responder).persistedIncoming.has(questionId), true, "host persisted actual incoming ask");
        const freshRows = await list(responder);
        assert.ok(freshRows.includes(qualified(asker)));
        assert.equal(received.from.id, qualified(asker), "retained sender equals fresh qualified roster contact");
        const routing = received.from.federation!;
        assert.deepEqual([routing.originId, routing.remoteScopeAlias, routing.remoteStableSessionId],
          [asker.broker.origin.id, asker.broker.alias, asker.id]);
        assert.ok(received.from.endpointEpoch);
        assert.equal(received.message.expectsReply, true);
        assert.equal((await outstanding(asker)).some(ask => ask.messageId === questionId), true);
        const answer = await responder.call({ action: "reply", to: qualified(asker), replyTo: questionId,
          message: `answer:${questionId}` });
        assert.equal(answer.details?.delivered, true, text(answer));
        const answerId = answer.details?.messageId as string;
        await waitUntil(() => inbound(asker, answerId).length > 0, "actual correlated answer reaches requester");
        assert.equal(inbound(asker, answerId).length, 1);
        const deliveredAnswer = history(asker).incoming.get(answerId)!;
        assert.equal(deliveredAnswer.message.replyTo, questionId);
        assert.equal(deliveredAnswer.message.completesAsk, true);
        assert.equal(deliveredAnswer.from.id, qualified(responder));
        await waitUntil(async () => !(await outstanding(asker)).some(ask => ask.messageId === questionId), "specific completing reply settles ask");
        assert.equal(history(asker).outgoing.has(questionId), false);
        assert.equal(settled(responder, questionId), true);
      }

      const question = await a.call({ action: "ask", to: qualified(b), blocking: false, message: "withdrawal must not retarget" });
      assert.equal(question.details?.delivered, true, text(question));
      const questionId = question.details?.messageId as string;
      await waitUntil(() => inbound(b, questionId).length > 0, "withdrawal-control ask is received first");
      const original = history(b).incoming.get(questionId)!;
      await ab.close();
      await waitUntil(async () => !(await list(b)).includes(qualified(a)), "original sender is actually withdrawn");
      const replacement = await admit(mc, "different-qualified-a", "late-a");
      await waitUntil(async () => (await list(b)).includes(qualified(replacement)), "same-name different-identity actor is visible");
      assert.ok((await list(b)).includes(`• late-a (${qualified(replacement)})`), "replacement truly has the original profile name");
      assert.notEqual(qualified(replacement), original.from.id);
      const failed = await b.call({ action: "reply", to: original.from.id, replyTo: questionId, message: "must not reach replacement" });
      assert.equal(failed.details?.error, true, text(failed));
      assert.match(text(failed), /Conversation recipient is not visible or is ambiguous/);
      assert.equal((await outstanding(a)).some(ask => ask.messageId === questionId), true);
      assert.equal(history(a).outgoing.has(questionId), true);
      assert.equal(history(b).incoming.get(questionId)?.from.id, original.from.id);
      assert.equal(settled(b, questionId), false);
      assert.match(text(await b.call({ action: "pending" })), /withdrawal must not retarget/);
      // Observe a subsequent delivery through the same B–C transport. `pending`
      // only reads local state and cannot establish receiver progress.
      const marker = await b.call({ action: "send", to: qualified(replacement), message: "withdrawal-observation-marker" });
      assert.equal(marker.details?.delivered, true, text(marker));
      const markerId = marker.details?.messageId as string;
      await waitUntil(() => inbound(replacement, markerId).length > 0, "subsequent marker reaches actual replacement history");
      assert.equal(inbound(replacement, markerId).length, 1);
      assert.equal(history(replacement).incoming.get(markerId)?.from.id, qualified(b));
      assert.equal(replacement.harness.entries.some(entry => entry.type === "parley_inbound_received"
        && (entry.data as { message: Message }).message.content.text === "must not reach replacement"), false,
      "the failed reply was not misdelivered before the subsequent ordered marker");
      assert.equal(inbound(b, questionId).length, 1);
    } finally {
      await Promise.all(links.map(link => link.close()));
      for (const stream of streams) stream.destroy();
      for (const harness of harnesses) await harness.emitLifecycle("session_shutdown");
      for (const broker of brokers) await stopBroker(broker);
      for (const dir of dirs) rmSync(dir, { recursive: true, force: true });
    }
  });
