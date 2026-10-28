// How a host's hooks reach the Parley server serving the same host session.
//
// Hosts start MCP servers and hook commands as children of one process: the
// session itself (Claude Code), or an app server shared by many sessions
// (Codex's daemon, the ChatGPT app). So each server listens on its own socket
// under a folder named after that parent, with a note of its folder and, once
// known, its host session id. A hook walks up its ancestry to the first such
// folder and picks the server that serves its session id; for a session no
// server has claimed yet, the oldest unclaimed server in the same folder.
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import { join } from "node:path";
import { getParleyDirPath } from "../broker/paths.ts";
import { createMessageReader, writeMessage } from "../broker/framing.ts";

export interface HookRequest { readonly event: string; readonly payload: Record<string, unknown> }
/** What the hook prints for its host, if anything. */
export interface HookResponse { readonly output?: Record<string, unknown> }

interface ServerNote { readonly pid: number; readonly cwd: string; readonly startedAt: number; sessionId?: string }

function hostsDir(env: NodeJS.ProcessEnv = process.env): string {
  return join(getParleyDirPath(env.PI_CODING_AGENT_DIR?.trim() || undefined), "hosts");
}

function parentDir(hostPid: number, env?: NodeJS.ProcessEnv): string {
  return join(hostsDir(env), String(hostPid));
}

export interface ControlEndpoint {
  /** Record the host session this server serves, so hooks for it come here. */
  claim(sessionId: string): void;
  close(): void;
}

/** Serve hook requests for this server, under the host process that started it. */
export function serveControl(hostPid: number, cwd: string, handle: (request: HookRequest) => Promise<HookResponse>): ControlEndpoint {
  const dir = parentDir(hostPid);
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  const socketPath = join(dir, `${process.pid}.sock`);
  const notePath = join(dir, `${process.pid}.json`);
  rmSync(socketPath, { force: true });
  const note: ServerNote = { pid: process.pid, cwd, startedAt: Date.now() };
  const writeNote = () => writeFileSync(notePath, JSON.stringify(note), { mode: 0o600 });
  writeNote();
  const server = net.createServer((socket) => {
    socket.on("error", () => {});
    socket.on("data", createMessageReader((value) => {
      void handle(value as HookRequest).then(
        (response) => { try { writeMessage(socket, response); } catch { /* gone */ } socket.end(); },
        () => socket.end(),
      );
    }, () => socket.destroy()));
  });
  server.listen(socketPath);
  server.unref();
  return {
    claim(sessionId) { if (note.sessionId !== sessionId) { note.sessionId = sessionId; writeNote(); } },
    close() { server.close(); rmSync(socketPath, { force: true }); rmSync(notePath, { force: true }); },
  };
}

/** This process's ancestors, nearest first. */
function ancestors(): number[] {
  const chain: number[] = [];
  let pid = process.ppid;
  for (let depth = 0; depth < 8 && pid > 1; depth++) {
    chain.push(pid);
    try {
      pid = Number(execFileSync("ps", ["-o", "ppid=", "-p", String(pid)], { encoding: "utf8", timeout: 2_000 }).trim());
    } catch {
      break;
    }
  }
  return chain;
}

function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch { return false; }
}

/** The server a hook for this session belongs to, if any. */
function chooseServer(dir: string, sessionId: string | undefined, cwd: string | undefined): ServerNote | undefined {
  const notes: ServerNote[] = [];
  for (const name of readdirSync(dir)) {
    if (!name.endsWith(".json")) continue;
    try {
      const note = JSON.parse(readFileSync(join(dir, name), "utf8")) as ServerNote;
      if (alive(note.pid)) notes.push(note); else { rmSync(join(dir, name), { force: true }); rmSync(join(dir, `${note.pid}.sock`), { force: true }); }
    } catch { /* being written */ }
  }
  if (sessionId) {
    const claimed = notes.find((note) => note.sessionId === sessionId);
    if (claimed) return claimed;
  }
  const unclaimed = notes.filter((note) => !note.sessionId).sort((a, b) => a.startedAt - b.startedAt);
  return unclaimed.find((note) => cwd && samePath(note.cwd, cwd)) ?? (notes.length === 1 ? notes[0] : unclaimed.length === 1 ? unclaimed[0] : undefined);
}

function samePath(a: string, b: string): boolean {
  const strip = (p: string) => p.replace(/^\/private(?=\/)/, "").replace(/\/+$/, "");
  return strip(a) === strip(b);
}

/** Send one hook event to this host session's Parley server. Undefined when none runs.
 * A hook that arrives before any server listens leaves the session id for it. */
export async function sendHook(request: HookRequest, timeoutMs = 5_000): Promise<HookResponse | undefined> {
  const sessionId = typeof request.payload.session_id === "string" && request.payload.session_id ? request.payload.session_id : undefined;
  const cwd = typeof request.payload.cwd === "string" ? request.payload.cwd : process.cwd();
  const dir = ancestors().map((pid) => parentDir(pid)).find((candidate) => existsSync(candidate));
  const server = dir ? chooseServer(dir, sessionId, cwd) : undefined;
  if (!dir || !server) {
    if (sessionId) {
      try {
        const early = parentDir(process.ppid);
        mkdirSync(early, { recursive: true, mode: 0o700 });
        writeFileSync(join(early, "early-session"), JSON.stringify({ sessionId, cwd }), { mode: 0o600 });
      } catch { /* best effort */ }
    }
    return undefined;
  }
  return new Promise((resolve) => {
    const socket = net.connect(join(dir, `${server.pid}.sock`));
    const done = (value?: HookResponse) => { clearTimeout(timer); socket.destroy(); resolve(value); };
    const timer = setTimeout(() => done(), timeoutMs);
    socket.on("connect", () => writeMessage(socket, request));
    socket.on("data", createMessageReader((value) => done(value as HookResponse), () => done()));
    socket.on("error", () => done());
    socket.on("close", () => done());
  });
}

/** The host session id a hook saw before this host's server was listening. */
export function takeEarlySessionId(hostPid: number, cwd: string): string | undefined {
  const file = join(parentDir(hostPid), "early-session");
  try {
    const early = JSON.parse(readFileSync(file, "utf8")) as { sessionId?: string; cwd?: string };
    if (!early.sessionId || (early.cwd && !samePath(early.cwd, cwd))) return undefined;
    rmSync(file, { force: true });
    return early.sessionId;
  } catch {
    return undefined;
  }
}
