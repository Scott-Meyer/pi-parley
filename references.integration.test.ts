import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createExtensionHarness, mentions } from "./test/extension-harness.ts";
import type { Message, SessionInfo } from "./types.ts";
import { encodeOriginQualifiedSessionIdentity } from "./broker/federation-protocol.ts";

// Models address sessions and messages by readable references. These scenarios
// drive the registered tool the way a model would: only text it was shown.
const home = mkdtempSync(path.join(tmpdir(), "ic-ref-"));
process.env.HOME = home;
process.env.USERPROFILE = home;
for (const key of Object.keys(process.env)) {
  if (key.startsWith("PI_SUBAGENT_") || (key.startsWith("PI_PARLEY_") && !key.startsWith("PI_PARLEY_TEST_"))
    || key.startsWith("FLIGHTDECK_") || key === "PI_CODING_AGENT_DIR") delete process.env[key];
}
// A short reply window lets requests elapse within the scenario.
process.env.PI_PARLEY_ASK_TIMEOUT_MS = "300";
const { ParleyClient } = await import("./broker/client.ts");
const { getTsxCliPath } = await import("./broker/spawn.ts");
const { default: extension } = await import("./index.ts");
const broker = spawn(process.execPath, [getTsxCliPath(), path.resolve("broker/broker.ts")], { env: process.env, stdio: ["ignore", "pipe", "pipe"] });
let brokerErrors = "";
broker.stderr.on("data", (data) => { brokerErrors = (brokerErrors + String(data)).slice(-4000); });
const ready = new Promise<void>((resolve, reject) => {
  const timer = setTimeout(() => reject(new Error("Reference test broker startup timed out")), 10_000);
  broker.stdout.on("data", (data) => { if (String(data).includes("Parley broker started")) { clearTimeout(timer); resolve(); } });
  broker.once("exit", () => { clearTimeout(timer); reject(new Error(`Broker exited: ${brokerErrors}`)); });
});
test.before(() => ready);
test.after(async () => {
  if (broker.exitCode === null && broker.signalCode === null) { broker.kill("SIGTERM"); await once(broker, "exit"); }
  rmSync(home, { recursive: true, force: true });
});

async function until(check: () => boolean | Promise<boolean>, label: string) {
  const deadline = Date.now() + 4000;
  while (!(await check())) {
    assert.ok(Date.now() < deadline, `Timed out: ${label}`);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
function registration(name: string) {
  return { name, cwd: process.cwd(), model: "test", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() };
}
async function colleague(name: string, id?: string) {
  const peer = new ParleyClient();
  await peer.connect(registration(name), id);
  return peer;
}
async function start(harness: ReturnType<typeof createExtensionHarness>, name: string) {
  extension(harness.pi as never);
  await harness.emitLifecycle("session_start");
  const probe = await colleague(`${name}-probe`);
  let target: SessionInfo | undefined;
  try {
    await until(async () => Boolean(target = (await probe.listSessions()).find((item) => item.name === name)), `${name} registration`);
  } finally { await probe.disconnect(); }
  return target!;
}
function call(harness: ReturnType<typeof createExtensionHarness>, params: Record<string, unknown>) {
  return harness.tools.find((tool) => tool.name === "parley")!.execute("scenario", params, AbortSignal.timeout(8_000), undefined, harness.ctx);
}
function text(result: { content: { text: string }[] }) { return result.content.map((item) => item.text).join("\n"); }
function row(listing: string, reference: string): string | undefined {
  return listing.split("\n").find((line) => line.startsWith(`• ${reference} — `));
}
/** The message reference a model read in an injected incoming message. */
async function incomingReference(harness: ReturnType<typeof createExtensionHarness>, body: string): Promise<string> {
  let content: string | undefined;
  await until(() => Boolean(content = harness.sentMessages.find((item) => item.message.content?.includes(body))?.message.content), `incoming ${body}`);
  const reference = content!.match(/^Message: (#\d+)/m)?.[1];
  assert.ok(reference, `incoming message names itself by reference: ${content}`);
  return reference;
}

test("a renamed session keeps its old reference and a newcomer reusing that name gets its own", async () => {
  const harness = createExtensionHarness("rename-observer", { sessionId: "rename-observer-id" });
  const original = await colleague("alice");
  let newcomer: InstanceType<typeof ParleyClient> | undefined;
  const reachedOriginal: Message[] = [];
  original.on("message", (_from, message) => reachedOriginal.push(message));
  try {
    await start(harness, "rename-observer");
    assert.ok(row(text(await call(harness, { action: "list" })), "alice"));

    original.updatePresence({ name: "bob" });
    newcomer = await colleague("alice");
    const reachedNewcomer: Message[] = [];
    newcomer.on("message", (_from, message) => reachedNewcomer.push(message));
    let listing = "";
    await until(async () => Boolean(row(listing = text(await call(harness, { action: "list" })), "alice~2") && row(listing, "bob")), "rename and newcomer appear");
    assert.ok(!row(listing, "alice"), "the reference the model already knew is not handed to the newcomer");

    // A transcript that still says "alice" means the session it always meant.
    const sent = await call(harness, { action: "send", to: "alice", message: "for the original, now bob" });
    assert.equal(sent.details?.delivered, true, text(sent));
    await until(() => reachedOriginal.length === 1, "original receives its old reference");
    assert.equal(reachedNewcomer.length, 0, "the newcomer is never reached through the old reference");

    const toNewcomer = await call(harness, { action: "send", to: "alice~2", message: "for the newcomer" });
    assert.equal(toNewcomer.details?.delivered, true, text(toNewcomer));
    await until(() => reachedNewcomer.length === 1, "newcomer receives its own reference");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await original.disconnect();
    await newcomer?.disconnect();
  }
});

test("a colleague that drops out and returns as the same session keeps its reference and its message numbers", async () => {
  const harness = createExtensionHarness("flap-observer", { sessionId: "flap-observer-id" });
  let planner = await colleague("planner", "flapping-planner-id");
  try {
    const target = await start(harness, "flap-observer");
    await planner.send(target.id, { text: "Which release train should this join?", expectsReply: true });
    const question = await incomingReference(harness, "Which release train");

    await planner.disconnect();
    await until(async () => !row(text(await call(harness, { action: "list" })), "planner"), "planner leaves the roster");
    const quiet = text(await call(harness, { action: "pending" }));
    assert.match(quiet, /Which release train/, "roster absence is not settlement");
    assert.match(quiet, /sender currently unreachable/);

    planner = await colleague("planner", "flapping-planner-id");
    const answers: Message[] = [];
    planner.on("message", (_from, message) => answers.push(message));
    let listing = "";
    await until(async () => Boolean(row(listing = text(await call(harness, { action: "list" })), "planner")), "planner returns");
    assert.ok(!row(listing, "planner~2"), "the same identity keeps its reference");

    const reply = await call(harness, { action: "reply", replyTo: question, message: "The Thursday train" });
    assert.equal(reply.details?.delivered, true, text(reply));
    await until(() => answers.length === 1, "answer delivered after the flap");
    assert.match(answers[0]!.content.text, /Thursday train/);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await planner.disconnect();
  }
});

test("automatic context mentions an elapsed request once, while pending keeps listing it", async () => {
  const harness = createExtensionHarness("quiet-observer", { sessionId: "quiet-observer-id" });
  const requester = await colleague("requester");
  const bystander = await colleague("bystander");
  try {
    const target = await start(harness, "quiet-observer");
    await requester.send(target.id, { text: "Please review the schema change", expectsReply: true });
    const request = await incomingReference(harness, "Please review the schema change");
    await harness.emitLifecycle("agent_end");
    await new Promise((resolve) => setTimeout(resolve, 400));

    const first = text(await call(harness, { action: "send", to: "bystander", message: "status note one" }));
    assert.ok(mentions(first, request), `the elapsed request is mentioned once: ${first}`);
    assert.match(first, /reply window elapsed, not withdrawn/);

    const second = text(await call(harness, { action: "send", to: "bystander", message: "status note two" }));
    assert.ok(!mentions(second, request), `and then only counted: ${second}`);
    assert.match(second, /1 earlier unanswered request\(s\) already mentioned.*pending lists them/);

    const pending = text(await call(harness, { action: "pending" }));
    assert.ok(mentions(pending, request), "pending stays complete");
    const answered = await call(harness, { action: "reply", replyTo: request, message: "Reviewed" });
    assert.equal(answered.details?.delivered, true, text(answered));
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await requester.disconnect();
    await bystander.disconnect();
  }
});

test("an unknown message reference fails before anything is sent", async () => {
  const harness = createExtensionHarness("unknown-ref-observer", { sessionId: "unknown-ref-observer-id" });
  const peer = await colleague("unknown-ref-peer");
  const received: Message[] = [];
  peer.on("message", (_from, message) => received.push(message));
  try {
    await start(harness, "unknown-ref-observer");
    const result = await call(harness, { action: "send", to: "unknown-ref-peer", replyTo: "#999", message: "threaded to nothing" });
    assert.equal(result.details?.error, true);
    assert.match(text(result), /#999 is not a message this session knows/);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(received.length, 0);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await peer.disconnect();
  }
});

test("a restarted colleague under the same name is never silently substituted by parley, and the broker's rebinding is reported", async () => {
  const harness = createExtensionHarness("restart-observer", { sessionId: "restart-observer-id" });
  const original = await colleague("builder", "builder-before-restart");
  let restarted: InstanceType<typeof ParleyClient> | undefined;
  try {
    await start(harness, "restart-observer");
    assert.ok(row(text(await call(harness, { action: "list" })), "builder"));
    await original.disconnect();
    restarted = await colleague("builder", "builder-after-restart");
    const received: Message[] = [];
    restarted.on("message", (_from, message) => received.push(message));
    await until(async () => Boolean(row(text(await call(harness, { action: "list" })), "builder~2")), "restarted session listed");

    // Existing broker policy: mail for an absent identity may reach the unique same-name, same-cwd session.
    const rebound = await call(harness, { action: "send", to: "builder", message: "meant for whoever builds" });
    assert.equal(rebound.details?.delivered, true, text(rebound));
    assert.match(text(rebound), /builder itself was not reached: the broker's offline-mail rule delivered this to builder~2, a different session using that name in the same directory/);
    assert.doesNotMatch(text(rebound), /did not redirect/, "a rebinding is never described as no redirect");
    assert.doesNotMatch(text(rebound), /builder-(before|after)-restart/, "stable identities never reach model text");
    await until(() => received.length === 1, "restarted builder received the rebound mail");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await original.disconnect().catch(() => undefined);
    await restarted?.disconnect();
  }
});

test("addressing an absent colleague while another session elsewhere uses its name queues, names the choice, and can be withdrawn", async () => {
  const harness = createExtensionHarness("absent-observer", { sessionId: "absent-observer-id" });
  const original = await colleague("indexer", "indexer-original");
  const elsewhere = new ParleyClient();
  try {
    await start(harness, "absent-observer");
    assert.ok(row(text(await call(harness, { action: "list" })), "indexer"));
    await original.disconnect();
    await elsewhere.connect({ ...registration("indexer"), cwd: path.join(process.cwd(), "elsewhere") }, "indexer-elsewhere");
    const received: Message[] = [];
    elsewhere.on("message", (_from, message) => received.push(message));
    await until(async () => Boolean(row(text(await call(harness, { action: "list" })), "indexer~2")), "other indexer listed");

    const queued = await call(harness, { action: "send", to: "indexer", message: "for the original indexer" });
    assert.equal(queued.details?.delivery, "queued", text(queued));
    const hint = text(queued);
    assert.match(hint, /indexer is not currently reachable\. A different session, indexer~2, now uses that name.*Parley did not redirect this/s);
    const queuedRef = hint.match(/(#\d+) is queued for indexer; if you meant indexer~2, cancel \1 before sending to indexer~2/)?.[1];
    assert.ok(queuedRef, `the hint names the queued message to withdraw: ${hint}`);
    assert.match(text(await call(harness, { action: "cancel", messageId: queuedRef })), /removed from the offline mailbox/);

    const direct = await call(harness, { action: "send", to: "indexer~2", message: "for the other indexer" });
    assert.equal(direct.details?.delivered, true, text(direct));
    await until(() => received.length === 1, "chosen session receives the deliberate send");
    assert.deepEqual(received.map((message) => message.content.text), ["for the other indexer"]);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await original.disconnect().catch(() => undefined);
    await elsewhere.disconnect();
  }
});

test("an absent pinned session is never reached through another session whose identity merely overlaps it", async () => {
  const harness = createExtensionHarness("prefix-observer", { sessionId: "prefix-observer-id" });
  const original = await colleague("builder", "prefix-builder");
  const unrelated = new ParleyClient();
  const received: Message[] = [];
  unrelated.on("message", (_from, message) => received.push(message));
  try {
    await start(harness, "prefix-observer");
    assert.ok(row(text(await call(harness, { action: "list" })), "builder"));
    await original.disconnect();
    // Its ID extends the pinned one, so a generic selector would match it by prefix.
    await unrelated.connect({ ...registration("unrelated"), cwd: path.join(process.cwd(), "elsewhere") }, "prefix-builder-new");
    await until(async () => Boolean(row(text(await call(harness, { action: "list" })), "unrelated")), "unrelated session listed");

    // The broker routes the pinned identity exactly: queued for the original, never matched by prefix.
    const queued = await call(harness, { action: "send", to: "builder", message: "only for the original builder" });
    assert.equal(queued.details?.delivery, "queued", text(queued));
    assert.doesNotMatch(text(queued), /prefix-builder/, "stable identities never reach model text");
    const asked = await call(harness, { action: "ask", to: "builder", message: "still only for the original", blocking: false });
    assert.equal(asked.details?.error, true, "asks to an absent session are never sent, and never rerouted");
    assert.match(text(asked), /Session "builder" is not currently connected.*no question was sent/s);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(received.length, 0, "nothing reached the overlapping session");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await original.disconnect().catch(() => undefined);
    await unrelated.disconnect();
  }
});

test("an unknown delivery outcome to an absent session reports the unknown recipient, not a reassurance", async () => {
  const harness = createExtensionHarness("uncertain-observer", { sessionId: "uncertain-observer-id" });
  const original = await colleague("courier", "courier-before");
  let replacement: InstanceType<typeof ParleyClient> | undefined;
  const realSend = ParleyClient.prototype.send;
  try {
    await start(harness, "uncertain-observer");
    assert.ok(row(text(await call(harness, { action: "list" })), "courier"));
    await original.disconnect();
    replacement = await colleague("courier", "courier-after");
    await until(async () => Boolean(row(text(await call(harness, { action: "list" })), "courier~2")), "replacement listed");
    // The broker accepted and routed it, but the acknowledgement was lost on the way back.
    ParleyClient.prototype.send = async function (this: InstanceType<typeof ParleyClient>, target, options) {
      const receipt = await realSend.call(this, target, options);
      return { ...receipt, delivered: false, outcomeKnown: false, delivery: "unknown", recipient: undefined, reason: "acknowledgement lost" };
    };
    const uncertain = text(await call(harness, { action: "send", to: "courier", message: "maybe delivered" }));
    assert.match(uncertain, /delivery outcome unknown/i);
    assert.match(uncertain, /courier was not connected, and the recipient of this message is unknown.*Sending again could repeat it/s);
    assert.doesNotMatch(uncertain, /did not redirect|Nothing was redirected/);
    assert.doesNotMatch(uncertain, /courier-(before|after)/);
  } finally {
    ParleyClient.prototype.send = realSend;
    await harness.emitLifecycle("session_shutdown");
    await original.disconnect().catch(() => undefined);
    await replacement?.disconnect();
  }
});

test("a result that outlives its Pi session writes nothing into the session that replaced it", async () => {
  let sessionId = "generation-old";
  const harness = createExtensionHarness("generation-observer", { sessionId: () => sessionId });
  const journal: Array<{ sessionId: string; type: string; data: unknown }> = [];
  const append = harness.pi.appendEntry;
  harness.pi.appendEntry = (type: string, data: unknown) => { journal.push({ sessionId, type, data }); append(type, data); };
  (harness.ctx.sessionManager as { getEntries: () => unknown[] }).getEntries = () => journal
    .filter((entry) => entry.sessionId === sessionId)
    .map((entry, index) => ({ type: "custom", customType: entry.type, data: entry.data, id: `entry-${index}`, parentId: null, timestamp: new Date().toISOString() }));
  const peer = await colleague("slowpoke");
  const realSend = ParleyClient.prototype.send;
  let release!: () => void;
  let signalReady!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const ready = new Promise<void>((resolve) => { signalReady = resolve; });
  try {
    await start(harness, "generation-observer");
    assert.ok(row(text(await call(harness, { action: "list" })), "slowpoke"));
    ParleyClient.prototype.send = async function (this: InstanceType<typeof ParleyClient>, target, options) {
      const receipt = await realSend.call(this, target, options);
      if (options.text === "late receipt") { signalReady(); await gate; }
      return receipt;
    };
    const pending = call(harness, { action: "send", to: "slowpoke", message: "late receipt" });
    await ready;
    sessionId = "generation-new";
    await harness.emitLifecycle("session_start");
    const before = journal.length;
    release();
    const late = text(await pending);
    assert.match(late, /Message sent as generation-observer to slowpoke/, "the late result still reports what happened");
    assert.match(late, /arrived after the Pi session changed.*not recorded in the current session's history/s);
    assert.deepEqual(journal.slice(before).map((entry) => entry.type), [], "no history or reference allocation entered the new session");
    const fresh = text(await call(harness, { action: "send", to: "slowpoke", message: "first in the new session" }));
    assert.match(fresh, /Message: #1\b/, "the new session numbers its own messages from the start");

    // A question whose session is replaced while its target is still being resolved must not
    // become pending work in the next session.
    ParleyClient.prototype.send = realSend;
    const realList = ParleyClient.prototype.listSessions;
    let pauseNextList = false;
    let releaseList!: () => void;
    let listPaused!: () => void;
    const listGate = new Promise<void>((resolve) => { releaseList = resolve; });
    const listReached = new Promise<void>((resolve) => { listPaused = resolve; });
    ParleyClient.prototype.listSessions = async function (this: InstanceType<typeof ParleyClient>, options) {
      const sessions = await realList.call(this, options);
      if (pauseNextList) { pauseNextList = false; listPaused(); await listGate; }
      return sessions;
    };
    try {
      pauseNextList = true;
      const lateAsk = call(harness, { action: "ask", to: "slowpoke", message: "late question", blocking: false });
      await listReached;
      sessionId = "generation-newest";
      await harness.emitLifecycle("session_start");
      releaseList();
      await lateAsk;
      assert.match(text(await call(harness, { action: "status" })), /Outstanding asks: none/, "no ghost question in the replacement session");
    } finally {
      ParleyClient.prototype.listSessions = realList;
      releaseList();
    }
  } finally {
    ParleyClient.prototype.send = realSend;
    release?.();
    await harness.emitLifecycle("session_shutdown");
    await peer.disconnect();
  }
});

test("a received question can be given a local label and answered by it, including after reload", async () => {
  const first = createExtensionHarness("label-observer", { sessionId: "label-observer-id" });
  const restored = createExtensionHarness("label-observer", { sessionId: "label-observer-id" });
  const requester = await colleague("label-requester");
  const received: Message[] = [];
  requester.on("message", (_from, message) => received.push(message));
  try {
    const target = await start(first, "label-observer");
    const question = await requester.send(target.id, { text: "May the release proceed?", expectsReply: true });
    const reference = await incomingReference(first, "May the release proceed?");
    const labelled = text(await call(first, { action: "label", messageId: reference, label: "release-approval" }));
    assert.match(labelled, new RegExp(`Message ${reference} · release-approval`));
    assert.match(text(await call(first, { action: "pending" })), /release-approval/, "pending shows the label beside the number");
    const clash = await call(first, { action: "send", to: "label-requester", label: "release-approval", message: "must not be sent" });
    assert.equal(clash.details?.error, true);
    assert.match(text(clash), /already names another message.*Nothing was sent/s);

    const note = await call(first, { action: "send", to: "label-requester", label: "status-note", message: "Checking now" });
    assert.match(text(note), /Message: #\d+ · status-note/);
    await until(() => received.length === 1, "labelled note delivered");

    restored.entries.push(...structuredClone(first.entries));
    await first.emitLifecycle("session_shutdown");
    await start(restored, "label-observer");
    const answer = await call(restored, { action: "reply", replyTo: "release-approval", message: "Yes, proceed" });
    assert.equal(answer.details?.delivered, true, text(answer));
    await until(() => received.length === 2, "answer delivered");
    assert.equal(received[1]!.replyTo, question.id, "the label threads the canonical question on the wire");
    assert.deepEqual(received.map((message) => message.content.text), ["Checking now", "Yes, proceed"], "the clashing send never left");
  } finally {
    await first.emitLifecycle("session_shutdown");
    await restored.emitLifecycle("session_shutdown");
    await requester.disconnect();
  }
});

test("a colleague's words reach the model exactly as written", async () => {
  const harness = createExtensionHarness("verbatim-observer", { sessionId: "verbatim-observer-id" });
  const peer = await colleague("verbatim-peer", "verbatim-peer-id");
  try {
    const target = await start(harness, "verbatim-observer");
    assert.ok(row(text(await call(harness, { action: "list" })), "verbatim-peer"));
    const body = "Literal identifier verbatim-peer-id and private-use \uE000\uE001\uE002 stay intact";
    await peer.send(target.id, { text: body });
    await until(() => harness.sentMessages.some((item) => item.message.content?.includes("Literal identifier")), "message injected");
    const injected = harness.sentMessages.find((item) => item.message.content?.includes("Literal identifier"))!.message;
    assert.ok(injected.content!.includes(body), `arrival keeps the body verbatim: ${injected.content}`);
    const results = await harness.emitLifecycleResults("context", { messages: [{ role: "custom", ...injected, timestamp: Date.now() }] });
    assert.ok(results.every((result) => result === undefined), "the model boundary leaves it exactly as recorded");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await peer.disconnect();
  }
});

test("a label stays with a delivered question whose local wait ended", async () => {
  const harness = createExtensionHarness("waiting-observer", { sessionId: "waiting-observer-id" });
  const responder = await colleague("slow-responder");
  const received: Message[] = [];
  responder.on("message", (_from, message) => received.push(message));
  try {
    await start(harness, "waiting-observer");
    const waited = text(await call(harness, { action: "ask", to: "slow-responder", label: "review-approval", message: "Approve the review?" }));
    assert.match(waited, /No reply from/, "the local wait ended without an answer");
    const reuse = await call(harness, { action: "send", to: "slow-responder", label: "review-approval", message: "must not take the label" });
    assert.equal(reuse.details?.error, true, "the delivered question still owns its label");
    assert.match(text(await call(harness, { action: "status" })), /#\d+ · review-approval to slow-responder/);
    assert.deepEqual(received.map((message) => message.content.text), ["Approve the review?"]);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await responder.disconnect();
  }
});

test("a multi-target send that outlives its Pi session writes nothing into the next one", async () => {
  let sessionId = "batch-generation-old";
  const harness = createExtensionHarness("batch-generation-observer", { sessionId: () => sessionId });
  const journal: Array<{ sessionId: string; type: string }> = [];
  const append = harness.pi.appendEntry;
  harness.pi.appendEntry = (type: string, data: unknown) => { journal.push({ sessionId, type }); append(type, data); };
  const first = await colleague("batch-first");
  const second = await colleague("batch-second");
  let restoreSendToSession = (): void => undefined;
  let release!: () => void;
  let signalReady!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const ready = new Promise<void>((resolve) => { signalReady = resolve; });
  try {
    await start(harness, "batch-generation-observer");
    assert.ok(row(text(await call(harness, { action: "list" })), "batch-first"));
    const realSendToSession = ParleyClient.prototype.sendToSession;
    ParleyClient.prototype.sendToSession = async function (this: InstanceType<typeof ParleyClient>, session, options) {
      const receipt = await realSendToSession.call(this, session, options);
      if (options.text === "late batch") { signalReady(); await gate; }
      return receipt;
    };
    restoreSendToSession = () => { ParleyClient.prototype.sendToSession = realSendToSession; };
    const pending = call(harness, { action: "send", targets: ["batch-first", "batch-second"], message: "late batch" });
    await ready;
    sessionId = "batch-generation-new";
    await harness.emitLifecycle("session_start");
    const before = journal.length;
    release();
    assert.match(text(await pending), /arrived after the Pi session changed/);
    assert.deepEqual(journal.slice(before).filter((entry) => entry.sessionId === "batch-generation-new").map((entry) => entry.type), []);
  } finally {
    restoreSendToSession();
    release?.();
    await harness.emitLifecycle("session_shutdown");
    await first.disconnect();
    await second.disconnect();
  }
});

test("a message whose identifier is spelled like its sender's name never renames the sender", async () => {
  const harness = createExtensionHarness("cross-kind-observer", { sessionId: "cross-kind-observer-id" });
  const peer = await colleague("builder", "cross-kind-builder");
  try {
    const target = await start(harness, "cross-kind-observer");
    await peer.send(target.id, { messageId: "builder", text: "A message with a name-like identifier" });
    const reference = await incomingReference(harness, "A message with a name-like identifier");
    const read = text(await call(harness, { action: "read", messageId: reference }));
    assert.match(read, /^From builder /, `the sender keeps its reference: ${read}`);
    assert.match(read, new RegExp(`Message: ${reference}`));
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await peer.disconnect();
  }
});

test("a receipt names the session actually reached by its own reference, even when its identity is spelled like another's", async () => {
  const harness = createExtensionHarness("receipt-observer", { sessionId: "receipt-observer-id" });
  const first = await colleague("builder", "receipt-builder-original");
  // A different session whose canonical identity is spelled like the first one's reference.
  const bob = await colleague("Bob", "builder");
  const received: Message[] = [];
  bob.on("message", (_from, message) => received.push(message));
  try {
    await start(harness, "receipt-observer");
    const listing = text(await call(harness, { action: "list" }));
    assert.ok(row(listing, "builder") && row(listing, "Bob"), listing);
    const sent = text(await call(harness, { action: "send", to: "Bob", message: "for Bob" }));
    assert.match(sent, /Message sent as receipt-observer to Bob\./, sent);
    assert.doesNotMatch(sent, /Bob \(builder\)/, "the receipt never shows another session's reference");
    await until(() => received.length === 1, "Bob received it");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await first.disconnect();
    await bob.disconnect();
  }
});

test("an answered labelled question shows its label, and the answer's words stay exactly as written", async () => {
  const harness = createExtensionHarness("answered-label-observer", { sessionId: "answered-label-observer-id" });
  const responder = await colleague("label-responder");
  responder.on("message", (from, message) => {
    if (message.expectsReply) void responder.send(from.id, { text: "Literal #1 #2 #3 #4 #5 stays intact", replyTo: message.id, completesAsk: true });
  });
  try {
    await start(harness, "answered-label-observer");
    await call(harness, { action: "send", to: "label-responder", message: "First, a note" });
    const answered = text(await call(harness, { action: "ask", to: "label-responder", label: "review-approval", message: "Approve the review?" }));
    assert.match(answered, /Question: #\d+ · review-approval/, answered);
    assert.ok(answered.includes("Literal #1 #2 #3 #4 #5 stays intact"), `the answer is verbatim: ${answered}`);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await responder.disconnect();
  }
});
