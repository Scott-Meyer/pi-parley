// Linking computers over SSH, through the same boundary a person or model
// uses: the parley tool. "Computers" are agent directories on this machine,
// reached through a stand-in ssh (test/fake-ssh.mjs) on PATH; everything
// else is real: brokers, sessions, this computer's link keeper.
import test from "node:test";
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { copyFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import path from "node:path";
import { getTsxCliPath } from "./broker/spawn.ts";
import { getLinkPaths } from "./link/paths.ts";
import { attachBrokers, createLocalBrokerAccess, inspectBroker } from "./dist/federation.js";
import { createExtensionHarness, type CapturedToolResult } from "./test/extension-harness.ts";

for (const key of Object.keys(process.env)) {
  if ((key.startsWith("PI_PARLEY_") && !key.startsWith("PI_PARLEY_TEST_")) || key.startsWith("PI_SUBAGENT_")
    || key.startsWith("FLIGHTDECK_") || key === "PI_CODING_AGENT_DIR") delete process.env[key];
}
const repo = process.cwd();
const text = (result: CapturedToolResult) => result.content.map((part) => part.text).join("\n");

async function startBroker(agentDir: string): Promise<ChildProcess> {
  const child = spawn(process.execPath, [getTsxCliPath(), path.join(repo, "broker/broker.ts")], {
    cwd: repo, env: { ...process.env, PI_CODING_AGENT_DIR: agentDir, PI_PARLEY_TRANSPORT: "socket" }, stdio: ["ignore", "pipe", "pipe"],
  });
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("broker startup timed out")), 10_000);
    child.stdout!.on("data", (data: Buffer) => { if (data.toString().includes("Parley broker started")) { clearTimeout(timer); resolve(); } });
    child.once("exit", () => { clearTimeout(timer); reject(new Error("broker exited during startup")); });
  });
  return child;
}

async function withAgentDir<T>(dir: string, work: () => Promise<T>, scope?: string): Promise<T> {
  const previous = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = dir;
  if (scope) process.env.PI_PARLEY_SCOPE_ID = scope;
  try { return await work(); } finally {
    process.env.PI_CODING_AGENT_DIR = previous;
    delete process.env.PI_PARLEY_SCOPE_ID;
  }
}

function computers() {
  const root = mkdtempSync("/tmp/pl-link-");
  const laptopAgent = path.join(root, "laptop", ".pi", "agent");
  const boxHome = path.join(root, "box");
  const boxAgent = path.join(boxHome, ".pi", "agent");
  const bin = path.join(root, "bin");
  for (const dir of [laptopAgent, boxAgent, bin]) mkdirSync(dir, { recursive: true });
  copyFileSync(path.join(repo, "test", "fake-ssh.mjs"), path.join(bin, "ssh"));
  const savedPath = process.env.PATH;
  process.env.PATH = `${bin}${path.delimiter}${process.env.PATH}`;
  // Anything that resolves an agent directory outside withAgentDir lands here,
  // never in the real ~/.pi/agent of whoever runs the tests.
  process.env.PI_CODING_AGENT_DIR = path.join(root, "elsewhere", ".pi", "agent");
  process.env.PI_PARLEY_TEST_SSH_HOMES = JSON.stringify({ box: boxHome });
  process.env.PI_PARLEY_LINK_LABEL = "laptop";
  process.env.PI_PARLEY_LINK_IDLE_MS = "500";
  return {
    root, laptopAgent, boxAgent,
    async cleanup() {
      await eventually(() => keeperGone(laptopAgent), "the keeper stops once no session is connected", 10_000).catch(() => {});
      process.env.PATH = savedPath;
      delete process.env.PI_CODING_AGENT_DIR;
      for (const key of ["PI_PARLEY_TEST_SSH_HOMES", "PI_PARLEY_LINK_LABEL", "PI_PARLEY_LINK_IDLE_MS"]) delete process.env[key];
      if (process.env.PI_PARLEY_TEST_KEEP) console.log(`kept ${root}`); else rmSync(root, { recursive: true, force: true });
    },
  };
}

async function stopBrokers(brokers: ChildProcess[]) {
  for (const broker of brokers) { if (broker.exitCode === null) { const exited = once(broker, "exit"); broker.kill("SIGTERM"); await exited; } }
}

async function eventually(check: () => Promise<boolean>, explanation: string, timeoutMs = 20_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, 200));
  }
  assert.fail(explanation);
}

function keeperGone(agentDir: string): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = net.connect(getLinkPaths({ PI_CODING_AGENT_DIR: agentDir }).control);
    socket.once("connect", () => { socket.destroy(); resolve(false); });
    socket.once("error", () => resolve(true));
  });
}

test("a computer added by name is linked both ways, named by its label, and unlinking it removes it", { timeout: 90_000, skip: process.platform === "win32" }, async () => {
  const { default: extension } = await import("./index.ts");
  const { laptopAgent, boxAgent, cleanup } = computers();

  const notes: string[] = [];
  const ui = new Proxy({ notify: (message: string) => notes.push(message) }, {
    get: (target, key) => (key in target ? (target as any)[key] : () => undefined),
  });
  const here = createExtensionHarness("writer", { sessionId: "writer-session", hasUI: true, ui });
  const there = createExtensionHarness("reviewer", { sessionId: "reviewer-session" });
  const call = (harness: typeof here, agentDir: string, params: Record<string, unknown>) => withAgentDir(agentDir, () =>
    harness.tools.find((tool) => tool.name === "parley")!.execute("link-call", params, new AbortController().signal, undefined, harness.ctx));
  const brokers: ChildProcess[] = [];
  try {
    brokers.push(await startBroker(laptopAgent), await startBroker(boxAgent));
    for (const [harness, agentDir] of [[here, laptopAgent], [there, boxAgent]] as const) {
      await withAgentDir(agentDir, async () => {
        extension(harness.pi as never);
        await harness.emitLifecycle("session_start");
      });
      assert.notEqual((await call(harness, agentDir, { action: "list" })).details?.error, true);
    }

    const unknown = await call(here, laptopAgent, { action: "link", host: "nowhere" });
    assert.match(text(unknown), /nowhere: unreachable — can't reach it over ssh/);

    const added = await call(here, laptopAgent, { action: "link", host: "box" });
    assert.match(text(added), /box is linked; its sessions appear here as box:…/, text(added));

    // Each side names the other by its computer; its own sessions stay unprefixed.
    await eventually(async () => /• box:[^\n]*reviewer/.test(text(await call(here, laptopAgent, { action: "list" }))),
      "the box's session appears on the laptop as box:…");
    await eventually(async () => /• laptop:[^\n]*writer/.test(text(await call(there, boxAgent, { action: "list" }))),
      "the laptop's session appears on the box as laptop:…");
    const local = text(await call(here, laptopAgent, { action: "list" }));
    assert.doesNotMatch(local, /laptop:[^\n]*writer/, "this computer's own sessions carry no prefix");
    assert.ok(notes.some((note) => note === "Parley linked box"), `the person hears about it: ${notes.join(" | ")}`);
    assert.ok(here.sentMessages.every((sent) => !String(sent.message.content ?? "").includes("linked box")), "nothing about links enters the model's context");

    const reviewerRef = /• (box:[^\s]*reviewer)/.exec(text(await call(here, laptopAgent, { action: "list" })))![1]!;
    const sent = await call(here, laptopAgent, { action: "send", to: reviewerRef, message: "draft is ready" });
    assert.notEqual(sent.details?.error, true, text(sent));
    await eventually(async () => there.sentMessages.some((m) => String(m.message.content ?? "").includes("draft is ready")),
      "a message crosses the link");

    const shown = await call(here, laptopAgent, { action: "link" });
    assert.match(text(shown), /• box \(box, added\): linked/);

    const removed = await call(here, laptopAgent, { action: "unlink", host: "box" });
    assert.doesNotMatch(text(removed), /• box/);
    await eventually(async () => !/box:/.test(text(await call(here, laptopAgent, { action: "list" }))),
      "the box's sessions leave the laptop's list once unlinked");
  } finally {
    for (const harness of [here, there]) await harness.emitLifecycle("session_shutdown").catch(() => {});
    await stopBrokers(brokers);
    await cleanup();
  }
});

test("the SSH link and another owner's link for its own sessions sit side by side, and neither knocks the other off", { timeout: 90_000, skip: process.platform === "win32" }, async () => {
  const { default: extension } = await import("./index.ts");
  const { laptopAgent, boxAgent, cleanup } = computers();
  const sessions = [
    { harness: createExtensionHarness("writer", { sessionId: "writer-session" }), agentDir: laptopAgent },
    { harness: createExtensionHarness("reviewer", { sessionId: "reviewer-session" }), agentDir: boxAgent },
    // An application (FlightDeck, say) puts its own sessions in a scope and links that scope itself.
    { harness: createExtensionHarness("app-writer", { sessionId: "app-writer-session" }), agentDir: laptopAgent, scope: "app" },
    { harness: createExtensionHarness("app-reviewer", { sessionId: "app-reviewer-session" }), agentDir: boxAgent, scope: "app" },
  ];
  const [writer, , appWriter] = sessions;
  const call = (session: typeof writer, params: Record<string, unknown>) => withAgentDir(session!.agentDir, () =>
    session!.harness.tools.find((tool) => tool.name === "parley")!.execute("link-call", params, new AbortController().signal, undefined, session!.harness.ctx), session!.scope);
  const listed = async (session: typeof writer) => text(await call(session, { action: "list" }));
  const brokers: ChildProcess[] = [];
  const appLinks: Awaited<ReturnType<typeof attachBrokers>>[] = [];
  const app = [{ localScopeId: "app", localScopeAlias: "app", remoteScopeAlias: "app" }];
  const appAttach = async () => {
    const [laptop, box] = await Promise.all([laptopAgent, boxAgent].map((agentDir) => inspectBroker(createLocalBrokerAccess({ agentDir }))));
    return attachBrokers({ initiator: { broker: laptop!, originLabel: "laptop", scopeBindings: app }, acceptor: { broker: box!, originLabel: "box", scopeBindings: app } });
  };
  try {
    brokers.push(await startBroker(laptopAgent), await startBroker(boxAgent));
    for (const session of sessions) {
      await withAgentDir(session.agentDir, async () => {
        extension(session.harness.pi as never);
        await session.harness.emitLifecycle("session_start");
      }, session.scope);
      assert.notEqual((await call(session, { action: "list" })).details?.error, true);
    }
    appLinks.push(await appAttach());
    await eventually(async () => /• box:[^\n]*app-reviewer/.test(await listed(appWriter)), "the application's link carries its sessions");

    assert.match(text(await call(writer, { action: "link", host: "box" })), /box is linked/);
    await eventually(async () => /• box:[^\n]*reviewer/.test(await listed(writer)), "the SSH link carries ordinary sessions");
    assert.match(await listed(appWriter), /• box:[^\n]*app-reviewer/, "the application's sessions are still linked");
    assert.doesNotMatch(await listed(writer), /app-reviewer/, "each link carries only its own sessions");

    // Later, a second owner tries to link the application's sessions too: refused, nothing dropped.
    await new Promise((r) => setTimeout(r, 10_500));
    await assert.rejects(appAttach(), (error: unknown) => (error as { code?: string }).code === "E_ALREADY_CONNECTED");
    await new Promise((r) => setTimeout(r, 3_500));
    assert.match(await listed(appWriter), /• box:[^\n]*app-reviewer/, "an answering link is kept");
    assert.match(await listed(writer), /• box:[^\n]*reviewer/);
  } finally {
    for (const link of appLinks) await link.close();
    for (const { harness } of sessions) await harness.emitLifecycle("session_shutdown").catch(() => {});
    await stopBrokers(brokers);
    await cleanup();
  }
});

test("with nothing set up, a session links the computer this one already has an ssh session open to, and says so", { timeout: 90_000, skip: process.platform === "win32" }, async () => {
  const { default: extension } = await import("./index.ts");
  const { laptopAgent, boxAgent, cleanup } = computers();
  process.env.PI_PARLEY_LINK_BY_DEFAULT = "1";
  // A person's interactive session, not a test runner's.
  const testContext = process.env.NODE_TEST_CONTEXT;
  delete process.env.NODE_TEST_CONTEXT;
  // Someone's terminal: `ssh box`, still connected. (A process that ps shows as ssh.)
  const terminal = spawn(process.execPath, ["-e", "setTimeout(()=>{},120000)", "box"], { argv0: "ssh", stdio: "ignore" });
  const notes: string[] = [];
  const ui = new Proxy({ notify: (message: string) => notes.push(message) }, {
    get: (target, key) => (key in target ? (target as any)[key] : () => undefined),
  });
  const here = createExtensionHarness("writer", { sessionId: "writer-session", hasUI: true, ui });
  const there = createExtensionHarness("reviewer", { sessionId: "reviewer-session" });
  const call = (harness: typeof here, agentDir: string, params: Record<string, unknown>) => withAgentDir(agentDir, () =>
    harness.tools.find((tool) => tool.name === "parley")!.execute("link-call", params, new AbortController().signal, undefined, harness.ctx));
  const brokers: ChildProcess[] = [];
  try {
    brokers.push(await startBroker(laptopAgent), await startBroker(boxAgent));
    // The box's session is headless, so it doesn't go looking on its own.
    for (const [harness, agentDir] of [[there, boxAgent], [here, laptopAgent]] as const) {
      await withAgentDir(agentDir, async () => {
        extension(harness.pi as never);
        await harness.emitLifecycle("session_start");
      });
      assert.notEqual((await call(harness, agentDir, { action: "list" })).details?.error, true);
    }
    await eventually(async () => /• box:[^\n]*reviewer/.test(text(await call(here, laptopAgent, { action: "list" }))),
      "the box's session appears without anyone adding it", 40_000);
    assert.ok(notes.includes("Parley linked box"), `the person hears about it: ${notes.join(" | ")}`);
    assert.match(text(await call(here, laptopAgent, { action: "link" })), /• box \(box, found\): linked/);

    // Turning it off is remembered; the computer it found stays linked until removed.
    assert.match(text(await call(here, laptopAgent, { action: "link", discover: false })), /ssh connections to: off/);
  } finally {
    terminal.kill();
    delete process.env.PI_PARLEY_LINK_BY_DEFAULT;
    if (testContext !== undefined) process.env.NODE_TEST_CONTEXT = testContext;
    for (const harness of [here, there]) await harness.emitLifecycle("session_shutdown").catch(() => {});
    await stopBrokers(brokers);
    await cleanup();
  }
});

test("during the switch away from an app scope, a merged computer shows everyone once, and a not-yet-merged one keeps what it had", { timeout: 90_000, skip: process.platform === "win32" }, async () => {
  const { default: extension } = await import("./index.ts");
  const { laptopAgent, boxAgent, cleanup } = computers();
  // The box's broker is still on the old rules: its "flightdeck" scope stays separate.
  mkdirSync(path.join(boxAgent, "parley"), { recursive: true });
  writeFileSync(path.join(boxAgent, "parley", "config.json"), JSON.stringify({ mergedScopes: [] }));
  const sessions = [
    { name: "old-tab", agentDir: laptopAgent, scope: "flightdeck" },
    { name: "new-tab", agentDir: laptopAgent },
    { name: "box-old-tab", agentDir: boxAgent, scope: "flightdeck" },
    { name: "box-new-tab", agentDir: boxAgent },
  ].map((spec) => ({ ...spec, harness: createExtensionHarness(spec.name, { sessionId: `${spec.name}-session` }) }));
  const listed = async (name: string) => {
    const session = sessions.find((candidate) => candidate.name === name)!;
    return text(await withAgentDir(session.agentDir, () => session.harness.tools.find((tool) => tool.name === "parley")!
      .execute("merge-call", { action: "list" }, new AbortController().signal, undefined, session.harness.ctx), session.scope));
  };
  const brokers: ChildProcess[] = [];
  let link: Awaited<ReturnType<typeof attachBrokers>> | undefined;
  try {
    brokers.push(await startBroker(laptopAgent), await startBroker(boxAgent));
    for (const session of sessions) {
      await withAgentDir(session.agentDir, async () => {
        extension(session.harness.pi as never);
        await session.harness.emitLifecycle("session_start");
      }, session.scope);
      await listed(session.name); // connects now, in its own agent directory and scope
    }
    const both = [
      { localScopeId: "flightdeck", localScopeAlias: "flightdeck", remoteScopeAlias: "flightdeck" },
      { localScopeId: null, localScopeAlias: "default", remoteScopeAlias: "default" },
    ];
    const [laptop, box] = await Promise.all([laptopAgent, boxAgent].map((agentDir) => inspectBroker(createLocalBrokerAccess({ agentDir }))));
    link = await attachBrokers({ initiator: { broker: laptop!, originLabel: "laptop", scopeBindings: both }, acceptor: { broker: box!, originLabel: "box", scopeBindings: both } });

    if (process.env.PI_PARLEY_TEST_DEBUG) { await new Promise((r) => setTimeout(r, 3000)); for (const s2 of sessions) console.log(`--- ${s2.name}\n${await listed(s2.name)}`); }
    // On the merged laptop, old and new tabs are one roster, and each box session appears once.
    for (const name of ["old-tab", "new-tab"]) {
      await eventually(async () => /box:box-old-tab/.test(await listed(name)) && /box:box-new-tab/.test(await listed(name)), `${name} sees both box sessions`);
      const rows = await listed(name);
      assert.match(rows, new RegExp(`• ${name === "old-tab" ? "new-tab" : "old-tab"} `), `${name} sees the other laptop tab`);
      assert.equal(rows.match(/box:box-old-tab/g)?.length, 1, "once, not once per binding");
    }
    // The box hasn't merged yet: its old tab still sees the laptop's old tab, as before.
    await eventually(async () => /laptop:old-tab/.test(await listed("box-old-tab")), "the box's old tab keeps its old peers");
    assert.match(await listed("box-new-tab"), /laptop:new-tab/);
  } finally {
    await link?.close();
    for (const { harness } of sessions) await harness.emitLifecycle("session_shutdown").catch(() => {});
    await stopBrokers(brokers);
    await cleanup();
  }
});
