// pi-parley/host, through the boundary an application embedding Parley uses: several
// hosted sessions in one process, deliveries the application may refuse, the model
// boundary (context), and a bundle with no Parley files beside it.
import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHostedSession, type Delivery, type DeliveryOutcome, type HostedSession } from "./host.ts";

const repo = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");

async function eventually(check: () => Promise<boolean> | boolean, what: string, timeoutMs = 20_000): Promise<void> {
  const until = Date.now() + timeoutMs;
  for (;;) {
    if (await check()) return;
    if (Date.now() > until) throw new Error(`timed out: ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 150));
  }
}

/** A computer of its own, and an app on it that hosts sessions in this process. */
function app() {
  const root = mkdtempSync("/tmp/pl-host-");
  const previous = { ...process.env };
  for (const key of Object.keys(process.env)) if (key.startsWith("PI_PARLEY_") || key.startsWith("FLIGHTDECK_")) delete process.env[key];
  process.env.PI_CODING_AGENT_DIR = path.join(root, "agent");
  const sessions: HostedSession[] = [];
  const inboxes = new Map<string, Delivery[]>();
  const host = async (name: string, options: { deliver?: (delivery: Delivery) => DeliveryOutcome | Promise<DeliveryOutcome> | void; env?: Record<string, string | undefined> } = {}) => {
    const inbox: Delivery[] = [];
    inboxes.set(name, inbox);
    const session = await createHostedSession({
      cwd: root,
      defaultName: () => name,
      app: () => "Test App",
      stateFile: (id) => path.join(root, "state", `${id}.jsonl`),
      publishSessionEnv: false,
      ...(options.env ? { env: options.env } : {}),
      deliver: (delivery) => {
        const outcome = options.deliver?.(delivery);
        if (!outcome || ("accepted" in outcome && outcome.accepted)) inbox.push(delivery);
        return outcome;
      },
    });
    sessions.push(session);
    return session;
  };
  const listed = async (session: HostedSession) => (await session.call({ action: "list" })).text;
  const cleanup = async () => {
    for (const session of sessions) await session.stop().catch(() => {});
    for (const key of Object.keys(process.env)) if (!(key in previous)) delete process.env[key];
    Object.assign(process.env, previous);
    rmSync(root, { recursive: true, force: true });
  };
  return { root, host, listed, inbox: (name: string) => inboxes.get(name) ?? [], cleanup };
}

const rows = (text: string, name: string) => text.split("\n").filter((row) => row.startsWith(`• ${name} `)).length;

test("sessions hosted in one process each keep the identity they began with, even when the process inherited one", { timeout: 60_000, skip: process.platform === "win32" }, async () => {
  const { host, listed, inbox, cleanup } = app();
  // The process was started with an identity of its own; neither session takes it.
  process.env.PI_PARLEY_STABLE_ID = "inherited-identity";
  try {
    const alpha = await host("alpha", { env: {} });
    // These two read the process environment, inherited identity and all. Had either taken
    // it, they'd be one identity, knocking each other off the broker.
    const beta = await host("beta");
    const gamma = await host("gamma");
    await alpha.begin("alpha-session");
    await beta.begin("beta-session");
    await gamma.begin("gamma-session");
    await eventually(async () => {
      const seen = await listed(alpha);
      return rows(seen, "beta") === 1 && rows(seen, "gamma") === 1 && rows(await listed(beta), "gamma") === 1;
    }, "each lists the others, once");
    assert.equal(process.env.PI_PARLEY_SESSION_ID, undefined, "neither writes its id into the process environment");

    const ask = await alpha.call({ action: "ask", to: "beta", message: "are you there?", blocking: false });
    assert.equal(ask.isError, false, ask.text);
    await eventually(() => inbox("beta").some((delivery) => /are you there\?/.test(delivery.text)), "beta gets the ask");
    const asked = inbox("beta").find((delivery) => /are you there\?/.test(delivery.text))!;
    assert.equal(asked.customType, "parley_message");
    assert.equal((asked.details as { from?: { name?: string; app?: string } }).from?.name, "alpha", "the delivery says who it's from");
    assert.equal((asked.details as { from?: { app?: string } }).from?.app, "Test App");
    const reply = await beta.call({ action: "reply", message: "here" });
    assert.equal(reply.isError, false, reply.text);
    assert.ok(reply.details, "tool results carry Parley's structured details");
    await eventually(() => inbox("alpha").some((delivery) => /here/.test(delivery.text)), "alpha gets the answer");

    // One leaving doesn't take the other with it.
    await alpha.stop();
    await eventually(async () => rows(await listed(beta), "alpha") === 0, "alpha has left");
    assert.match((await beta.call({ action: "status" })).text, /Connected: Yes/);
    assert.equal(process.env.PI_PARLEY_STABLE_ID, "inherited-identity", "and nothing restored or rewrote the process environment");
    assert.equal(process.env.PI_PARLEY_SESSION_ID, undefined);
  } finally {
    await cleanup();
  }
});

test("a session the host replaces keeps its identity, names and history", { timeout: 60_000, skip: process.platform === "win32" }, async () => {
  const { host, listed, inbox, cleanup } = app();
  try {
    const alpha = await host("alpha", { env: {} });
    await alpha.begin("alpha-session");
    let beta = await host("beta", { env: {} });
    await beta.begin("beta-session");
    await eventually(async () => rows(await listed(alpha), "beta") === 1, "alpha sees beta");
    assert.equal((await alpha.call({ action: "ask", to: "beta", message: "first question", blocking: false })).isError, false);
    await eventually(() => inbox("beta").some((delivery) => /first question/.test(delivery.text)), "beta gets it");

    // The person renamed the chat; then the application rebuilds beta's agent: the old session
    // stops, and a new one begins with the same id and the chat's original default name.
    beta.setName("beta-renamed");
    await eventually(async () => rows(await listed(alpha), "beta-renamed") === 1, "alpha sees the new name");
    await beta.stop();
    beta = await host("beta", { env: {} });
    await beta.begin("beta-session");
    await eventually(async () => rows(await listed(alpha), "beta-renamed") === 1 && rows(await listed(alpha), "beta") === 0,
      `beta is back, once, under the name it was given: ${await listed(alpha)}`);
    assert.equal(beta.name(), "beta-renamed", "the rename came back from its history");
    // The question it was asked before is still the one it answers.
    const reply = await beta.call({ action: "reply", message: "answer after the rebuild" });
    assert.equal(reply.isError, false, reply.text);
    await eventually(() => inbox("alpha").some((delivery) => /answer after the rebuild/.test(delivery.text)), "the answer reaches alpha");
  } finally {
    await cleanup();
  }
});

test("a delivery the host refuses isn't counted as shown: Parley tries again, and the model sees it once", { timeout: 60_000, skip: process.platform === "win32" }, async () => {
  const { host, listed, inbox, cleanup } = app();
  try {
    let refusing = true;
    const offered: string[] = [];
    const alpha = await host("alpha", { env: {} });
    const beta = await host("beta", {
      env: {},
      deliver: (delivery) => {
        offered.push(delivery.text);
        return refusing ? { accepted: false, reason: "the chat is paused" } : { accepted: true };
      },
    });
    await alpha.begin("alpha-session");
    await beta.begin("beta-session");
    await eventually(async () => rows(await listed(alpha), "beta") === 1, "alpha sees beta");
    await alpha.call({ action: "send", to: "beta", message: "while you were paused", blocking: false });
    await eventually(() => offered.length >= 2, "Parley offers it again after the refusal");
    assert.equal(inbox("beta").length, 0, "nothing was accepted yet");

    // The model's next call still gets it, through context, before the host takes it.
    const context = await beta.context([{ role: "user", content: "carry on", timestamp: Date.now() }]);
    const shown = context.filter((message) => message.role === "custom" && message.customType === "parley_message");
    assert.equal(shown.length, 1, JSON.stringify(context.map((message) => message.role)));
    assert.match(JSON.stringify(shown[0]!.content), /while you were paused/);

    // Once the model has seen it, accepting doesn't hand it over a second time.
    refusing = false;
    const before = offered.length;
    await new Promise((resolve) => setTimeout(resolve, 2500));
    assert.equal(offered.length, before, "not offered again once the model has it");
    const again = await beta.context([{ role: "user", content: "carry on", timestamp: Date.now() }, ...shown]);
    assert.equal(again.filter((message) => message.role === "custom" && message.customType === "parley_message").length, 1, "and the copy the host kept isn't doubled");
  } finally {
    await cleanup();
  }
});

test("an application bundles pi-parley/host into one file and runs Parley's broker from it, with nothing beside it but the lock addon", { timeout: 120_000, skip: process.platform === "win32" }, async () => {
  const { build } = await import("esbuild");
  const { parleyBundleDefine } = await import("./bundle-define.mjs");
  const root = mkdtempSync("/tmp/pl-host-bundle-");
  try {
    const appDir = path.join(root, "app");
    mkdirSync(appDir);
    const entry = path.join(root, "entry.ts");
    writeFileSync(entry, `
import path from "node:path";
import { fileURLToPath } from "node:url";
import { configureParleyHost, createHostedSession, runBroker } from ${JSON.stringify(path.join(repo, "hosts", "host.ts"))};
const self = fileURLToPath(import.meta.url);
const dir = path.dirname(self);
configureParleyHost({
  brokerLauncher: { command: process.execPath, args: [self, "--parley-broker"] },
  startupLock: false,
  lockAddon: path.join(dir, "lock.node"),
});
if (process.argv.includes("--parley-broker")) {
  await runBroker();
} else {
  const inbox = [];
  const make = async (name) => {
    const session = await createHostedSession({ cwd: dir, defaultName: () => name, stateFile: (id) => path.join(dir, id + ".jsonl"), env: {}, publishSessionEnv: false, deliver: (d) => { inbox.push([name, d.text]); } });
    await session.begin(name + "-session");
    return session;
  };
  const one = await make("one"), two = await make("two");
  for (let i = 0; i < 100 && !/• two /.test((await one.call({ action: "list" })).text); i++) await new Promise((r) => setTimeout(r, 150));
  await one.call({ action: "send", to: "two", message: "from the bundle" });
  for (let i = 0; i < 100 && !inbox.some(([to, text]) => to === "two" && text.includes("from the bundle")); i++) await new Promise((r) => setTimeout(r, 150));
  console.log(inbox.some(([to, text]) => to === "two" && text.includes("from the bundle")) ? "DELIVERED" : "MISSING");
  await one.stop(); await two.stop();
  process.exit(0);
}
`);
    await build({
      entryPoints: [entry], outfile: path.join(appDir, "app.mjs"), bundle: true, platform: "node", format: "esm", target: "node20",
      define: parleyBundleDefine(),
      banner: { js: 'import { createRequire as __appRequire } from "node:module"; const require = __appRequire(import.meta.url);' },
      logLevel: "warning",
    });
    // The lock addon is the only file shipped beside the bundle.
    const addonDir = path.dirname(createRequire(import.meta.url).resolve("fs-native-extensions"));
    copyFileSync(path.join(addonDir, "prebuilds", `${process.platform}-${process.arch}`, "fs-native-extensions.node"), path.join(appDir, "lock.node"));

    const env = { ...process.env, PI_CODING_AGENT_DIR: path.join(root, "agent"), PI_PARLEY_LINK_BY_DEFAULT: "0" };
    for (const key of Object.keys(env)) if (key.startsWith("FLIGHTDECK_") || key === "PI_PARLEY_SCOPE_ID") delete env[key];
    const child = spawn(process.execPath, [path.join(appDir, "app.mjs")], { cwd: appDir, env, stdio: ["ignore", "pipe", "pipe"] });
    let out = "", err = "";
    child.stdout.on("data", (chunk) => { out += chunk; });
    child.stderr.on("data", (chunk) => { err += chunk; });
    const code = await new Promise<number | null>((resolve) => child.on("close", resolve));
    assert.equal(code, 0, err);
    assert.match(out, /DELIVERED/, `${out}\n${err}`);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("with several asks open, a bare reply answers the one the host most recently took, without context()", { timeout: 60_000, skip: process.platform === "win32" }, async () => {
  const { host, listed, inbox, cleanup } = app();
  try {
    const alpha = await host("alpha", { env: {} });
    const gamma = await host("gamma", { env: {} });
    const beta = await host("beta", { env: {} });
    for (const [session, id] of [[alpha, "alpha-session"], [gamma, "gamma-session"], [beta, "beta-session"]] as const) await session.begin(id);
    await eventually(async () => rows(await listed(beta), "alpha") === 1 && rows(await listed(beta), "gamma") === 1, "beta sees both");

    // Beta takes alpha's question in one turn...
    await alpha.call({ action: "ask", to: "beta", message: "alpha's question", blocking: false });
    await eventually(() => inbox("beta").some((delivery) => /alpha's question/.test(delivery.text)), "beta takes alpha's question");
    beta.setBusy(true); beta.setBusy(false);
    // ...and gamma's in the next, and answers without saying which.
    await gamma.call({ action: "ask", to: "beta", message: "gamma's question", blocking: false });
    await eventually(() => inbox("beta").some((delivery) => /gamma's question/.test(delivery.text)), "beta takes gamma's question");
    beta.setBusy(true);
    const reply = await beta.call({ action: "reply", message: "answering the latest" });
    assert.equal(reply.isError, false, reply.text);
    await eventually(() => inbox("gamma").some((delivery) => /answering the latest/.test(delivery.text)), "gamma gets the answer");
    assert.equal(inbox("alpha").some((delivery) => /answering the latest/.test(delivery.text)), false, "not alpha");
  } finally {
    await cleanup();
  }
});

test("a delivery carries what to show a person: the colleague's words, and the message they answer", { timeout: 60_000, skip: process.platform === "win32" }, async () => {
  const { host, listed, inbox, cleanup } = app();
  try {
    const alpha = await host("alpha", { env: {} });
    const beta = await host("beta", { env: {} });
    await alpha.begin("alpha-session");
    await beta.begin("beta-session");
    await eventually(async () => rows(await listed(alpha), "beta") === 1, "alpha sees beta");
    await alpha.call({ action: "ask", to: "beta", message: "Which port does the dev server use?", blocking: false });
    await eventually(() => inbox("beta").length > 0, "beta gets the ask");
    const asked = inbox("beta")[0]!;
    assert.equal(asked.display?.text, "Which port does the dev server use?");
    assert.match(asked.text, /\*\*From alpha\*\*/, "the model still gets the full envelope");
    await beta.call({ action: "reply", message: "8043" });
    await eventually(() => inbox("alpha").some((delivery) => delivery.display?.text === "8043"), "alpha gets the answer");
    const answer = inbox("alpha").find((delivery) => delivery.display?.text === "8043")!;
    assert.equal(answer.display?.inReplyTo, "Which port does the dev server use?");
  } finally {
    await cleanup();
  }
});
