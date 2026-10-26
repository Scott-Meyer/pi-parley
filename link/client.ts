// A Pi session's side of this computer's link keeper (see keeper.ts).
//
// `linksConfigured()` says whether this computer links to others at all, so
// sessions that never asked for it never start anything. `connectLinkKeeper()`
// starts the keeper if needed and returns a connection: requests go through
// it, and holding it open keeps the keeper running while this session lives.
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import net from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createMessageReader, writeMessage } from "../broker/framing.ts";
import { ensureParleyRuntimeDir, getAgentDirPath } from "../broker/paths.ts";
import { getTsxCliPath, getWindowsBrokerCommandLine, writeWindowsHiddenLauncher } from "../broker/spawn.ts";
import { getLinkPaths } from "./paths.ts";
import type { LinkEvent, LinkRequest, LinkResponse, LinkStatus } from "./protocol.ts";

export type { LinkEvent, LinkHostStatus, LinkStatus } from "./protocol.ts";

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never;
export type LinkCommand = DistributiveOmit<LinkRequest, "requestId">;

export interface LinkKeeperConnection {
  request(command: LinkCommand, options?: { timeoutMs?: number }): Promise<LinkResponse>;
  /** Called for every keeper event while connected. */
  onEvent(listener: (event: LinkEvent) => void): () => void;
  /** Resolves when the keeper goes away or close() is called. */
  readonly closed: Promise<void>;
  close(): void;
}

/** Whether this computer links with others: any computer remembered, or discovery on,
 * as last set with /parley link on|off, else byDefault. */
export function linksConfigured(env: NodeJS.ProcessEnv = process.env, byDefault = false): boolean {
  let parsed: { discover?: unknown; hosts?: unknown } | undefined;
  try { parsed = JSON.parse(readFileSync(getLinkPaths(env).config, "utf8")); } catch { parsed = undefined; }
  if (Array.isArray(parsed?.hosts) && parsed.hosts.length > 0) return true;
  return typeof parsed?.discover === "boolean" ? parsed.discover : byDefault;
}

const KEEPER_PATH = join(dirname(fileURLToPath(import.meta.url)), "keeper.ts");
const EXTENSION_DIR = join(dirname(fileURLToPath(import.meta.url)), "..");

function startKeeper(env: NodeJS.ProcessEnv, discoverByDefault: boolean) {
  const paths = getLinkPaths(env);
  ensureParleyRuntimeDir(paths.parleyDir);
  const childEnv = {
    ...env, PI_CODING_AGENT_DIR: getAgentDirPath(env), NODE_NO_WARNINGS: "1",
    PI_PARLEY_LINK_DISCOVER_DEFAULT: discoverByDefault ? "1" : "0",
  };
  if (process.platform === "win32") {
    const launcher = writeWindowsHiddenLauncher(
      getWindowsBrokerCommandLine(KEEPER_PATH, EXTENSION_DIR),
      join(paths.parleyDir, "link-launch.vbs"),
    );
    spawn("wscript.exe", ["//E:VBScript", launcher], { detached: true, stdio: "ignore", windowsHide: true, env: childEnv }).unref();
    return;
  }
  // Bun hosts (OMP) run the keeper under Node, like the broker.
  const node = /^node(js)?$/i.test(process.execPath.split("/").pop() ?? "") ? process.execPath : "node";
  spawn(node, [getTsxCliPath(EXTENSION_DIR), KEEPER_PATH], {
    detached: true, stdio: "ignore", cwd: EXTENSION_DIR, env: childEnv,
  }).unref();
}

function connectOnce(path: string): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    const socket = net.connect(path);
    socket.once("connect", () => { socket.off("error", reject); resolve(socket); });
    socket.once("error", reject);
  });
}

/** Connects to this computer's keeper, starting it when it isn't running. */
export async function connectLinkKeeper(options: { env?: NodeJS.ProcessEnv; timeoutMs?: number; discoverByDefault?: boolean } = {}): Promise<LinkKeeperConnection> {
  // A snapshot: where this session's agent directory is mustn't change mid-connect.
  const env = { ...(options.env ?? process.env) };
  const paths = getLinkPaths(env);
  const deadline = Date.now() + (options.timeoutMs ?? 10_000);
  let socket: net.Socket | undefined;
  let started = false;
  while (!socket) {
    try {
      socket = await connectOnce(paths.control);
    } catch (error) {
      if (Date.now() > deadline) throw new Error("Parley's link keeper didn't start", { cause: error });
      if (!started) { startKeeper(env, options.discoverByDefault === true); started = true; }
      await new Promise((r) => setTimeout(r, 150));
    }
  }

  const pending = new Map<string, (response: LinkResponse) => void>();
  const listeners = new Set<(event: LinkEvent) => void>();
  let resolveClosed!: () => void;
  const closed = new Promise<void>((r) => { resolveClosed = r; });
  const connected = socket;
  connected.on("data", createMessageReader((value) => {
    const message = value as LinkResponse | LinkEvent;
    if (message?.type === "event") { for (const listener of listeners) listener(message); return; }
    if (message?.type === "result") { pending.get(message.requestId)?.(message); pending.delete(message.requestId); }
  }, () => connected.destroy()));
  connected.on("error", () => {});
  connected.on("close", () => {
    for (const settle of pending.values()) settle({ type: "result", requestId: "", ok: false, error: "the link keeper went away" });
    pending.clear();
    resolveClosed();
  });
  connected.unref();

  return {
    request(command, requestOptions = {}) {
      const requestId = randomUUID();
      return new Promise<LinkResponse>((resolve) => {
        const timer = setTimeout(() => {
          pending.delete(requestId);
          resolve({ type: "result", requestId, ok: false, error: "the link keeper didn't answer in time" });
        }, requestOptions.timeoutMs ?? 60_000);
        pending.set(requestId, (response) => { clearTimeout(timer); resolve(response); });
        try { writeMessage(connected, { ...command, requestId }); } catch { /* close settles it */ }
      });
    },
    onEvent(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    closed,
    close() { connected.destroy(); },
  };
}

/** Plain words for a person or a model reading link status. */
export function describeLinks(status: LinkStatus): string {
  const lines = [`This computer is ${status.label}. Finding computers it has ssh connections to: ${status.discover ? "on" : "off"}.`];
  if (status.hosts.length === 0) lines.push("No other computers yet.");
  for (const host of status.hosts) {
    const how = host.via === "found" ? "found" : "added";
    lines.push(`• ${host.label} (${host.destination}, ${how}${host.remembered ? "" : ", not remembered"}): ${host.state}${host.detail ? ` — ${host.detail}` : ""}`);
  }
  for (const relay of status.relays) {
    lines.push(`• ${relay.between[0]} ↔ ${relay.between[1]} through this computer: ${relay.state}${relay.detail ? ` — ${relay.detail}` : ""}`);
  }
  return lines.join("\n");
}
