// Parley inside other agents, through the boundary those agents use: an MCP
// client (standing in for Claude Code or Codex) starts `parley mcp <host>`,
// lists and calls the parley tool, and receives messages the way the host
// would (Claude's inbox socket, a `codex queue` command, or hook output).
import test from "node:test";
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { chmodSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import path from "node:path";
import { createInterface } from "node:readline";
import { startHostedSession } from "./hosted-session.ts";

const repo = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
// The source by default; PARLEY_TEST_HOST_BIN=plugins/parley/parley.mjs checks the built plugin.
const bin = process.env.PARLEY_TEST_HOST_BIN ? path.resolve(repo, process.env.PARLEY_TEST_HOST_BIN) : path.join(repo, "bin", "parley.mjs");

function cleanEnv(extra: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (key.startsWith("PI_PARLEY_") || key.startsWith("PI_SUBAGENT_") || key.startsWith("FLIGHTDECK_") || key.startsWith("CLAUDE") || key.startsWith("CODEX") || key === "PI_CODING_AGENT_DIR") continue;
    env[key] = value;
  }
  return { ...env, PI_PARLEY_LINK_BY_DEFAULT: "0", ...extra };
}

/** A minimal MCP client over the server's stdio. */
function mcpClient(child: ChildProcess) {
  let next = 1;
  const waiting = new Map<number, (message: any) => void>();
  createInterface({ input: child.stdout! }).on("line", (line) => {
    const message = JSON.parse(line);
    if (message.id !== undefined) { waiting.get(message.id)?.(message); waiting.delete(message.id); }
  });
  const request = (method: string, params: Record<string, unknown> = {}) => new Promise<any>((resolve) => {
    const id = next++;
    waiting.set(id, resolve);
    child.stdin!.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
  });
  return {
    request,
    async start() {
      const init = await request("initialize", { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "stand-in", version: "1" } });
      child.stdin!.write(`${JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" })}\n`);
      return init;
    },
    call: async (args: Record<string, unknown>) => {
      const result = await request("tools/call", { name: "parley", arguments: args });
      return { text: result.result.content.map((part: { text: string }) => part.text).join("\n"), isError: result.result.isError };
    },
  };
}

async function eventually(check: () => boolean | Promise<boolean>, what: string, timeoutMs = 20_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) { if (await check()) return; await new Promise((r) => setTimeout(r, 100)); }
  assert.fail(what);
}

/** A colleague in an ordinary Parley session on the same broker. */
async function colleague(agentDir: string, root: string) {
  // This process's Parley lives in the test's agent directory for the whole test.
  process.env.PI_CODING_AGENT_DIR = agentDir;
  process.env.PI_PARLEY_LINK_BY_DEFAULT = "0";
  const received: string[] = [];
  const session = await startHostedSession({
    sessionId: "colleague-session", cwd: root, defaultName: () => "colleague",
    stateFile: () => path.join(root, "colleague.jsonl"), deliver: (delivery) => received.push(delivery.text),
  });
  return { session, received };
}

test("in Claude Code, the parley tool works through MCP, and a message wakes the session through its inbox", { timeout: 60_000, skip: process.platform === "win32" }, async () => {
  const root = mkdtempSync("/tmp/pl-hosts-");
  const agentDir = path.join(root, "agent");
  mkdirSync(agentDir, { recursive: true });
  // Claude's cross-session inbox, standing in: records what's written to it.
  const inboxFrames: any[] = [];
  const inboxPath = path.join(root, "inbox.sock");
  const inbox = net.createServer((socket) => {
    let buffer = "";
    socket.on("data", (chunk) => { buffer += chunk; });
    socket.on("end", () => { for (const line of buffer.split("\n").filter(Boolean)) inboxFrames.push(JSON.parse(line)); });
  }).listen(inboxPath);
  const server = spawn(process.execPath, [bin, "mcp", "claude"], {
    cwd: root, stdio: ["pipe", "pipe", "pipe"],
    env: cleanEnv({ PI_CODING_AGENT_DIR: agentDir, CLAUDE_CODE_SESSION_ID: "claude-session-1", CLAUDE_PROJECT_DIR: root,
      CLAUDE_CODE_MESSAGING_SOCKET: inboxPath, CLAUDE_CODE_MESSAGING_TOKEN: "token-123" }),
  });
  let stderr = ""; server.stderr!.on("data", (d) => { stderr += d; });
  const client = mcpClient(server);
  let other: Awaited<ReturnType<typeof colleague>> | undefined;
  try {
    const init = await client.start();
    assert.equal(init.result.serverInfo.name, "parley");
    const listed = await client.request("tools/list");
    assert.deepEqual(listed.result.tools.map((tool: { name: string }) => tool.name), ["parley"], stderr);
    assert.ok(listed.result.tools[0].inputSchema.properties.action, "the tool's schema is Parley's own");

    other = await colleague(agentDir, root);
    let roster = "";
    await eventually(async () => /• colleague/.test(roster = (await client.call({ action: "list" })).text), "the colleague appears");
    assert.match(roster, new RegExp(`• ${path.basename(root)}:claude .*\\[self`), "named after its project, as a Claude session");

    // A colleague asks; the idle Claude session is woken through its inbox.
    await other.session.call({ action: "ask", to: `${path.basename(root)}:claude`, message: "Which build is green?", blocking: false });
    await eventually(() => inboxFrames.some((frame) => frame.type === "user"), "the ask reaches Claude's inbox");
    assert.deepEqual(inboxFrames[0], { type: "auth", token: "token-123" });
    const delivered = inboxFrames.find((frame) => frame.type === "user");
    assert.match(delivered.message.content, /From colleague[\s\S]*Which build is green\?/);
    assert.equal(delivered.priority, "next");

    // Claude answers with the tool, and the answer reaches the colleague threaded.
    const reply = await client.call({ action: "reply", message: "main is green" });
    assert.equal(reply.isError, false, reply.text);
    await eventually(() => other!.received.some((text) => /main is green/.test(text) && /Reply to your message/.test(text)), "the answer arrives as a reply");
  } finally {
    await other?.session.stop();
    delete process.env.PI_CODING_AGENT_DIR;
    server.kill("SIGTERM");
    inbox.close();
    rmSync(root, { recursive: true, force: true });
  }
});

test("in Codex, the server learns its thread from the first hook, wakes it with codex queue, and hooks hand over what couldn't wake it", { timeout: 60_000, skip: process.platform === "win32" }, async () => {
  const root = mkdtempSync("/tmp/pl-hosts-");
  const agentDir = path.join(root, "agent");
  mkdirSync(agentDir, { recursive: true });
  // `codex`, standing in: records queued messages; can be told to refuse.
  const queued = path.join(root, "queued.jsonl");
  const fakeCodex = path.join(root, "codex");
  writeFileSync(fakeCodex, `#!/bin/sh\nif [ "$1" = queue ] && [ "$2" = --help ]; then exit 0; fi\nif [ -f ${root}/refuse ]; then exit 1; fi\nprintf '%s\\n' "$*" >> ${queued}\n`);
  chmodSync(fakeCodex, 0o755);
  const env = cleanEnv({ PI_CODING_AGENT_DIR: agentDir, PI_PARLEY_CODEX_BIN: fakeCodex });
  const server = spawn(process.execPath, [bin, "mcp", "codex"], { cwd: root, stdio: ["pipe", "pipe", "pipe"], env });
  let stderr = ""; server.stderr!.on("data", (d) => { stderr += d; });
  const client = mcpClient(server);
  // Hooks run as the host's children, like the server: here, this test process's.
  const hook = (event: string, payload: Record<string, unknown>) => new Promise<string>((resolve) => {
    const child = spawn(process.execPath, [bin, "hook", event], { stdio: ["pipe", "pipe", "inherit"], env });
    let out = ""; child.stdout!.on("data", (d) => { out += d; });
    child.on("close", () => resolve(out.trim()));
    child.stdin!.end(JSON.stringify({ hook_event_name: event, session_id: "thread-42", ...payload }));
  });
  let other: Awaited<ReturnType<typeof colleague>> | undefined;
  try {
    // Codex runs SessionStart while its MCP servers start; it may come first.
    await hook("SessionStart", { source: "startup" });
    await client.start();
    const listed = await client.request("tools/list");
    assert.equal(listed.result.tools[0]?.name, "parley", stderr);

    other = await colleague(agentDir, root);
    await other.session.call({ action: "send", to: `${path.basename(root)}:codex`, message: "deploy finished" });
    await eventually(() => { try { return /queue --thread thread-42 --message [\s\S]*deploy finished/.test(readFileSync(queued, "utf8")); } catch { return false; } },
      "the message is queued into the thread the hook named");

    // When Codex can't queue, the next turn boundary hands the message over.
    writeFileSync(path.join(root, "refuse"), "");
    await other.session.call({ action: "send", to: `${path.basename(root)}:codex`, message: "tests failing on main" });
    let handed = "";
    await eventually(async () => (handed = await hook("Stop", {})) !== "", "the next turn boundary hands the message over");
    const stop = JSON.parse(handed);
    assert.equal(stop.decision, "block");
    assert.match(stop.reason, /tests failing on main/);
    assert.equal(await hook("Stop", {}), "", "handed over once");
  } finally {
    await other?.session.stop();
    delete process.env.PI_CODING_AGENT_DIR;
    server.kill("SIGTERM");
    rmSync(root, { recursive: true, force: true });
  }
});

test("two Codex sessions under one shared app server each get their own hooks and wakes", { timeout: 60_000, skip: process.platform === "win32" }, async () => {
  const root = mkdtempSync("/tmp/pl-hosts-");
  const agentDir = path.join(root, "agent");
  const queued = path.join(root, "queued.jsonl");
  const fakeCodex = path.join(root, "codex");
  mkdirSync(agentDir, { recursive: true });
  // One line per queued message: "<thread> <message on one line>".
  writeFileSync(fakeCodex, `#!/bin/sh\nif [ "$1" = queue ] && [ "$2" = --help ]; then exit 0; fi\nprintf '%s %s\\n' "$3" "$(printf '%s' "$5" | tr '\\n' ' ')" >> ${queued}\n`);
  chmodSync(fakeCodex, 0o755);
  const env = cleanEnv({ PI_CODING_AGENT_DIR: agentDir, PI_PARLEY_CODEX_BIN: fakeCodex });
  // Like Codex's daemon: one parent (this process) for both sessions' servers and hooks.
  const projects = ["alpha", "beta"].map((name) => { const dir = path.join(root, name); mkdirSync(dir); return dir; });
  const servers = projects.map((cwd) => spawn(process.execPath, [bin, "mcp", "codex"], { cwd, stdio: ["pipe", "pipe", "pipe"], env }));
  const clients = servers.map(mcpClient);
  const hook = (event: string, cwd: string, threadId: string) => new Promise<string>((resolve) => {
    const child = spawn(process.execPath, [bin, "hook", event], { cwd, stdio: ["pipe", "pipe", "inherit"], env });
    let out = ""; child.stdout!.on("data", (d) => { out += d; });
    child.on("close", () => resolve(out.trim()));
    child.stdin!.end(JSON.stringify({ hook_event_name: event, session_id: threadId, cwd }));
  });
  let other: Awaited<ReturnType<typeof colleague>> | undefined;
  try {
    await Promise.all(clients.map((client) => client.start()));
    await new Promise((r) => setTimeout(r, 500));
    await hook("SessionStart", projects[1]!, "thread-beta");
    await hook("SessionStart", projects[0]!, "thread-alpha");
    await Promise.all(clients.map((client) => client.request("tools/list")));
    other = await colleague(agentDir, root);
    await eventually(async () => /alpha:codex[\s\S]*beta:codex|beta:codex[\s\S]*alpha:codex/.test((await other!.session.call({ action: "list" })).text), "both sessions appear");
    await other.session.call({ action: "send", to: "alpha:codex", message: "for alpha" });
    await other.session.call({ action: "send", to: "beta:codex", message: "for beta" });
    await eventually(() => { try { const text = readFileSync(queued, "utf8"); return /^thread-alpha .*for alpha/m.test(text) && /^thread-beta .*for beta/m.test(text) && !/^thread-alpha .*for beta/m.test(text); } catch { return false; } },
      "each message is queued into its own session's thread");
  } finally {
    await other?.session.stop();
    delete process.env.PI_CODING_AGENT_DIR;
    for (const server of servers) server.kill("SIGTERM");
    rmSync(root, { recursive: true, force: true });
  }
});

test("a server no host session claims (a health check, an app server's spare) lists its tool but never appears as a session", { timeout: 60_000, skip: process.platform === "win32" }, async () => {
  const root = mkdtempSync("/tmp/pl-hosts-");
  const agentDir = path.join(root, "agent");
  mkdirSync(agentDir, { recursive: true });
  const env = cleanEnv({ PI_CODING_AGENT_DIR: agentDir });
  const servers = ["claude", "codex"].map((host) => spawn(process.execPath, [bin, "mcp", host], { cwd: root, stdio: ["pipe", "pipe", "pipe"], env }));
  let other: Awaited<ReturnType<typeof colleague>> | undefined;
  try {
    for (const client of servers.map(mcpClient)) {
      await client.start();
      const listed = await client.request("tools/list");
      assert.equal(listed.result.tools[0]?.name, "parley", "the host still sees the tool at once");
    }
    other = await colleague(agentDir, root);
    await new Promise((r) => setTimeout(r, 6_000));
    const roster = (await other.session.call({ action: "list" })).text;
    assert.doesNotMatch(roster, /:claude|:codex/, roster);
  } finally {
    await other?.session.stop();
    delete process.env.PI_CODING_AGENT_DIR;
    for (const server of servers) server.kill("SIGTERM");
    rmSync(root, { recursive: true, force: true });
  }
});

test("a Codex session whose server started with a bare environment takes its names and scope from its first hook", { timeout: 60_000, skip: process.platform === "win32" }, async () => {
  const root = mkdtempSync("/tmp/pl-hosts-");
  const agentDir = path.join(root, "agent");
  mkdirSync(agentDir, { recursive: true });
  // The server gets nothing (as Codex's plugin servers do); the hook carries the tab's environment.
  const serverEnv = cleanEnv({ PI_CODING_AGENT_DIR: agentDir });
  const tabEnv = { ...serverEnv, FLIGHTDECK_WORKSPACE_NAME: "mistfall", FLIGHTDECK_TAB_NAME: "builder", FLIGHTDECK_TAB_ID: "t9" };
  const server = spawn(process.execPath, [bin, "mcp", "codex"], { cwd: root, stdio: ["pipe", "pipe", "pipe"], env: serverEnv });
  const client = mcpClient(server);
  let other: Awaited<ReturnType<typeof colleague>> | undefined;
  try {
    await client.start();
    await client.request("tools/list");
    await new Promise<void>((resolve) => {
      const child = spawn(process.execPath, [bin, "hook", "SessionStart"], { cwd: root, stdio: ["pipe", "ignore", "inherit"], env: tabEnv });
      child.on("close", () => resolve());
      child.stdin!.end(JSON.stringify({ hook_event_name: "SessionStart", session_id: "thread-7", cwd: root }));
    });
    other = await colleague(agentDir, root);
    await eventually(async () => /• mistfall:builder .*Codex in FlightDeck/.test((await other!.session.call({ action: "list" })).text),
      "named after its FlightDeck tab, and shown as Codex in FlightDeck");
  } finally {
    await other?.session.stop();
    delete process.env.PI_CODING_AGENT_DIR;
    server.kill("SIGTERM");
    rmSync(root, { recursive: true, force: true });
  }
});

test("when a guess gave a Codex session to a spare server, the server its tool calls come through takes it over", { timeout: 60_000, skip: process.platform === "win32" }, async () => {
  const root = mkdtempSync("/tmp/pl-hosts-");
  const agentDir = path.join(root, "agent");
  mkdirSync(agentDir, { recursive: true });
  const env = cleanEnv({ PI_CODING_AGENT_DIR: agentDir, PI_PARLEY_CODEX_BIN: "/nonexistent/codex" });
  // Like Codex's app server: a pre-started spare, then the session's real server, same folder.
  const spare = spawn(process.execPath, [bin, "mcp", "codex"], { cwd: root, stdio: ["pipe", "pipe", "pipe"], env });
  await new Promise((r) => setTimeout(r, 300));
  const real = spawn(process.execPath, [bin, "mcp", "codex"], { cwd: root, stdio: ["pipe", "pipe", "pipe"], env });
  const [spareClient, realClient] = [spare, real].map(mcpClient);
  const hook = (event: string) => new Promise<string>((resolve) => {
    const child = spawn(process.execPath, [bin, "hook", event], { cwd: root, stdio: ["pipe", "pipe", "inherit"], env });
    let out = ""; child.stdout!.on("data", (d) => { out += d; });
    child.on("close", () => resolve(out.trim()));
    child.stdin!.end(JSON.stringify({ hook_event_name: event, session_id: "thread-9", cwd: root }));
  });
  let other: Awaited<ReturnType<typeof colleague>> | undefined;
  try {
    await Promise.all([spareClient.start(), realClient.start()]);
    await Promise.all([spareClient.request("tools/list"), realClient.request("tools/list")]);
    await hook("SessionStart"); // the guess: oldest unclaimed in the folder, the spare
    other = await colleague(agentDir, root);
    const name = `${path.basename(root)}:codex`;
    await eventually(async () => new RegExp(`• ${name} `).test((await other!.session.call({ action: "list" })).text), "the session appears");
    // A message waits (no codex to wake it) in whichever server holds the session.
    await other.session.call({ action: "send", to: name, message: "waiting for you" });
    await new Promise((r) => setTimeout(r, 500));

    // The model's first tool call comes through the real server, naming its thread.
    const status = await realClient.request("tools/call", { name: "parley", arguments: { action: "status" }, _meta: { threadId: "thread-9" } });
    assert.equal(status.result.isError, false, JSON.stringify(status.result));
    assert.match(status.result.content[0].text, new RegExp(`Session: ${name}`));
    // One session, not two; and what the spare held is handed over at the next boundary.
    const roster = (await other.session.call({ action: "list" })).text;
    assert.equal(roster.split("\n").filter((row) => row.startsWith(`• ${name} `)).length, 1, roster);
    let stop = "";
    await eventually(async () => (stop = await hook("Stop")) !== "", "the real server hands the waiting message over");
    assert.match(JSON.parse(stop).reason, /waiting for you/);
  } finally {
    await other?.session.stop();
    delete process.env.PI_CODING_AGENT_DIR;
    for (const server of [spare, real]) server.kill("SIGTERM");
    rmSync(root, { recursive: true, force: true });
  }
});
