// A Pi session in a herdr pane and herdr's names, kept in step. herdr here is
// a stand-in speaking its documented socket API (checked against herdr 0.9.5).
import test from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import net from "node:net";
import path from "node:path";
import { registerHerdrNames } from "./names.ts";

interface World {
  workspaces: Record<string, { workspace_id: string; label: string }>;
  tabs: Record<string, { tab_id: string; workspace_id: string; label: string; pane_count: number }>;
  panes: Record<string, { pane_id: string; tab_id: string; workspace_id: string; label?: string | null }>;
}

async function standInHerdr(world: World) {
  const dir = mkdtempSync("/tmp/pl-herdr-");
  const socketPath = path.join(dir, "herdr.sock");
  const subscribers = new Set<net.Socket>();
  const push = (event: string, data: Record<string, unknown>) => {
    for (const socket of subscribers) socket.write(`${JSON.stringify({ event, data })}\n`);
  };
  const server = net.createServer((socket) => {
    let buffer = "";
    socket.on("data", (chunk) => {
      buffer += chunk.toString();
      let newline: number;
      while ((newline = buffer.indexOf("\n")) >= 0) {
        const { id, method, params } = JSON.parse(buffer.slice(0, newline));
        buffer = buffer.slice(newline + 1);
        const reply = (result: unknown) => socket.write(`${JSON.stringify({ id, result })}\n`);
        if (method === "events.subscribe") { subscribers.add(socket); socket.on("close", () => subscribers.delete(socket)); reply({ type: "subscription_started" }); }
        else if (method === "pane.get") reply({ type: "pane_info", pane: world.panes[params.pane_id] });
        else if (method === "tab.get") reply({ type: "tab_info", tab: world.tabs[params.tab_id] });
        else if (method === "workspace.get") reply({ type: "workspace_info", workspace: world.workspaces[params.workspace_id] });
        else if (method === "tab.rename") { world.tabs[params.tab_id]!.label = params.label; reply({ type: "tab_info", tab: world.tabs[params.tab_id] }); push("tab_renamed", { tab_id: params.tab_id, label: params.label }); }
        else if (method === "pane.rename") { world.panes[params.pane_id]!.label = params.label; reply({ type: "pane_info", pane: world.panes[params.pane_id] }); }
        else socket.write(`${JSON.stringify({ id, error: { code: "unknown_method", message: method } })}\n`);
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(socketPath, resolve));
  return {
    socketPath,
    renameTab(tabId: string, label: string) { world.tabs[tabId]!.label = label; push("tab_renamed", { tab_id: tabId, label, workspace_id: "w1" }); },
    renameWorkspace(id: string, label: string) { world.workspaces[id]!.label = label; push("workspace_renamed", { workspace_id: id, label }); },
    async close() { for (const s of subscribers) s.destroy(); await new Promise((r) => server.close(r)); rmSync(dir, { recursive: true, force: true }); },
  };
}

/** The part of Pi this uses: session names, lifecycle events, the extension bus. */
function standInPi(name?: string) {
  const bus = new EventEmitter();
  const handlers = new Map<string, ((event: unknown) => unknown)[]>();
  const pi = {
    sessionName: name,
    events: { on: (c: string, h: (p: unknown) => void) => { bus.on(c, h); return () => bus.off(c, h); }, emit: (c: string, p: unknown) => bus.emit(c, p) },
    on(event: string, handler: (event: unknown) => unknown) { handlers.set(event, [...(handlers.get(event) ?? []), handler]); },
    getSessionName() { return pi.sessionName; },
    setSessionName(next: string) { pi.sessionName = next; for (const h of handlers.get("session_info_changed") ?? []) void h({ name: next }); },
    async emit(event: string) { for (const h of handlers.get(event) ?? []) await h({}); },
  };
  return pi;
}

const world = (paneCount = 1): World => ({
  workspaces: { w1: { workspace_id: "w1", label: "mistfall" } },
  tabs: { "w1:t1": { tab_id: "w1:t1", workspace_id: "w1", label: "writer", pane_count: paneCount } },
  panes: {
    "w1:p1": { pane_id: "w1:p1", tab_id: "w1:t1", workspace_id: "w1", label: null },
    "w1:p2": { pane_id: "w1:p2", tab_id: "w1:t1", workspace_id: "w1", label: null },
  },
});
const paneEnv = (socketPath: string, extra: NodeJS.ProcessEnv = {}) => ({ HERDR_ENV: "1", HERDR_SOCKET_PATH: socketPath, HERDR_PANE_ID: "w1:p1", ...extra });
const until = async (check: () => boolean, what: string) => {
  for (let i = 0; i < 100 && !check(); i++) await new Promise((r) => setTimeout(r, 20));
  assert.ok(check(), what);
};

test("a session in a herdr pane is named workspace:tab, follows herdr's renames, and renaming it renames its tab", async () => {
  const state = world();
  const herdr = await standInHerdr(state);
  const pi = standInPi();
  try {
    registerHerdrNames(pi as never, { env: paneEnv(herdr.socketPath) });
    await pi.emit("session_start");
    assert.equal(pi.sessionName, "mistfall:writer");

    herdr.renameTab("w1:t1", "reviewer");
    await until(() => pi.sessionName === "mistfall:reviewer", "a tab renamed in herdr renames the session");
    herdr.renameWorkspace("w1", "parley");
    await until(() => pi.sessionName === "parley:reviewer", "so does a workspace rename");

    pi.setSessionName("Ellis");
    await until(() => state.tabs["w1:t1"]!.label === "Ellis", "renaming the session renames its own tab");
    assert.equal(pi.sessionName, "parley:Ellis");
  } finally {
    await pi.emit("session_shutdown");
    await herdr.close();
  }
});

test("in a tab shared with other panes, renaming the session labels only its own pane; a resumed session keeps its name", async () => {
  const state = world(2);
  const herdr = await standInHerdr(state);
  const pi = standInPi("Juno");
  try {
    registerHerdrNames(pi as never, { env: paneEnv(herdr.socketPath) });
    await pi.emit("session_start");
    assert.equal(pi.sessionName, "mistfall:Juno", "the resumed name wins and herdr follows");
    assert.equal(state.panes["w1:p1"]!.label, "Juno");
    assert.equal(state.tabs["w1:t1"]!.label, "writer", "the shared tab keeps its label");

    pi.setSessionName("mistfall:Ellis");
    await until(() => state.panes["w1:p1"]!.label === "Ellis", "the pane takes the new name");
    assert.equal(state.tabs["w1:t1"]!.label, "writer");
    assert.equal(state.panes["w1:p2"]!.label, null, "the other pane is untouched");
  } finally {
    await pi.emit("session_shutdown");
    await herdr.close();
  }
});

test("subagents in the pane don't rename herdr, and FlightDeck's own sessions are left to FlightDeck", async () => {
  const state = world();
  const herdr = await standInHerdr(state);
  const child = standInPi("reviewer");
  const flightdeck = standInPi("pi-parley:pi-parley");
  try {
    registerHerdrNames(child as never, { env: paneEnv(herdr.socketPath, { PI_SUBAGENT_CHILD: "1" }) });
    await child.emit("session_start");
    assert.equal(child.sessionName, "mistfall:writer:reviewer");
    assert.equal(state.tabs["w1:t1"]!.label, "writer");

    registerHerdrNames(flightdeck as never, { env: paneEnv(herdr.socketPath, { FLIGHTDECK_TAB_ID: "t1" }) });
    await flightdeck.emit("session_start");
    assert.equal(flightdeck.sessionName, "pi-parley:pi-parley");
  } finally {
    await child.emit("session_shutdown");
    await herdr.close();
  }
});
