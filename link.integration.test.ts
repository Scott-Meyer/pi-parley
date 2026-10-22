// Linking computers over SSH, through the same boundary a person or model
// uses: the parley tool. "Computers" are agent directories on this machine,
// reached through a stand-in ssh (test/fake-ssh.mjs) on PATH; everything
// else is real: brokers, sessions, this computer's link keeper.
import test from "node:test";
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import net from "node:net";
import path from "node:path";
import { getTsxCliPath } from "./broker/spawn.ts";
import { getLinkPaths } from "./link/paths.ts";
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

async function withAgentDir<T>(dir: string, work: () => Promise<T>): Promise<T> {
  const previous = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = dir;
  try { return await work(); } finally {
    if (previous === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = previous;
  }
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
  const root = mkdtempSync("/tmp/pl-link-");
  const laptopAgent = path.join(root, "laptop", ".pi", "agent");
  const boxHome = path.join(root, "box");
  const boxAgent = path.join(boxHome, ".pi", "agent");
  const bin = path.join(root, "bin");
  for (const dir of [laptopAgent, boxAgent, bin]) mkdirSync(dir, { recursive: true });
  copyFileSync(path.join(repo, "test", "fake-ssh.mjs"), path.join(bin, "ssh"));
  const saved = { PATH: process.env.PATH };
  process.env.PATH = `${bin}${path.delimiter}${process.env.PATH}`;
  process.env.PI_PARLEY_TEST_SSH_HOMES = JSON.stringify({ box: boxHome });
  process.env.PI_PARLEY_LINK_LABEL = "laptop";
  process.env.PI_PARLEY_LINK_IDLE_MS = "500";

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
    await eventually(() => keeperGone(laptopAgent), "the keeper stops once no session is connected", 10_000).catch(() => {});
    for (const broker of brokers) { if (broker.exitCode === null) { const exited = once(broker, "exit"); broker.kill("SIGTERM"); await exited; } }
    process.env.PATH = saved.PATH;
    for (const key of ["PI_PARLEY_TEST_SSH_HOMES", "PI_PARLEY_LINK_LABEL", "PI_PARLEY_LINK_IDLE_MS"]) delete process.env[key];
    if (!existsSync(path.join(root, "keep"))) rmSync(root, { recursive: true, force: true });
  }
});
