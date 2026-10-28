// How a host's hooks reach the Parley server running in the same host session.
//
// Claude Code and Codex start their MCP servers and their hook commands as
// children of the same host process, so the server listens on a socket named
// after that process, and a hook finds it by walking up its own ancestry.
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import { join } from "node:path";
import { getParleyDirPath } from "../broker/paths.ts";
import { createMessageReader, writeMessage } from "../broker/framing.ts";

export interface HookRequest { readonly event: string; readonly payload: Record<string, unknown> }
/** What the hook prints for its host, if anything. */
export interface HookResponse { readonly output?: Record<string, unknown> }

export function controlSocketPath(hostPid: number, env: NodeJS.ProcessEnv = process.env): string {
  return join(getParleyDirPath(env.PI_CODING_AGENT_DIR?.trim() || undefined), "hosts", `${hostPid}.sock`);
}

/** Serve hook requests for the host process that started this server. */
export function serveControl(hostPid: number, handle: (request: HookRequest) => Promise<HookResponse>): () => void {
  const path = controlSocketPath(hostPid);
  mkdirSync(join(path, ".."), { recursive: true, mode: 0o700 });
  rmSync(path, { force: true });
  const server = net.createServer((socket) => {
    socket.on("error", () => {});
    socket.on("data", createMessageReader((value) => {
      void handle(value as HookRequest).then(
        (response) => { try { writeMessage(socket, response); } catch { /* gone */ } socket.end(); },
        () => socket.end(),
      );
    }, () => socket.destroy()));
  });
  server.listen(path);
  server.unref();
  return () => { server.close(); rmSync(path, { force: true }); };
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

/** The host session id a hook saw before this host's server was listening. */
export function takeEarlySessionId(hostPid: number): string | undefined {
  const file = `${controlSocketPath(hostPid)}.session`;
  try {
    const id = readFileSync(file, "utf8").trim();
    rmSync(file, { force: true });
    return id || undefined;
  } catch {
    return undefined;
  }
}

/** Send one hook event to this host session's Parley server. Undefined when none runs.
 * A hook that arrives before the server listens leaves the session id for it. */
export async function sendHook(request: HookRequest, timeoutMs = 5_000): Promise<HookResponse | undefined> {
  const path = ancestors().map((pid) => controlSocketPath(pid)).find((candidate) => existsSync(candidate));
  if (!path) {
    const sessionId = request.payload.session_id;
    if (typeof sessionId === "string" && sessionId) {
      try {
        const file = `${controlSocketPath(process.ppid)}.session`;
        mkdirSync(join(file, ".."), { recursive: true, mode: 0o700 });
        writeFileSync(file, sessionId, { mode: 0o600 });
      } catch { /* best effort */ }
    }
    return undefined;
  }
  return new Promise((resolve) => {
    const socket = net.connect(path);
    const done = (value?: HookResponse) => { clearTimeout(timer); socket.destroy(); resolve(value); };
    const timer = setTimeout(() => done(), timeoutMs);
    socket.on("connect", () => writeMessage(socket, request));
    socket.on("data", createMessageReader((value) => done(value as HookResponse), () => done()));
    socket.on("error", () => done());
    socket.on("close", () => done());
  });
}
