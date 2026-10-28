// One host session's Parley: an MCP server for the host's agent, the hosted
// Parley session behind it, and the hook endpoint that tells it when the
// agent is working and lets it hand over messages at turn boundaries.
//
// Each host says how it identifies its session and how a message can wake an
// idle agent; everything else is shared.
import { randomUUID } from "node:crypto";
import { basename, join } from "node:path";
import { getAgentDirPath, getParleyDirPath } from "../broker/paths.ts";
import { serveControl, takeEarlySessionId, type HookRequest, type HookResponse } from "./control.ts";
import { startHostedSession, type Delivery, type HostedSession } from "./hosted-session.ts";
import { serveMcp } from "./mcp.ts";

export interface Host {
  /** "claude", "codex": names state folders and the default session name. */
  readonly id: string;
  readonly version: string;
  /** The host's session identity, if it can be known at startup. */
  initialSessionId(): string | undefined;
  /** The project directory the host session works in. */
  cwd(): string;
  /** Put a message in front of the agent now, starting a turn if it's idle.
   * Resolves true when the host accepted it. */
  wake?(text: string, session: { sessionId: string }): Promise<boolean>;
  /** Hook output that hands pending text to the agent at a turn boundary. */
  hookOutput(event: string, text: string): Record<string, unknown> | undefined;
  /** MCP capabilities beyond tools. */
  readonly capabilities?: Record<string, unknown>;
}

/** A Pi session that started this host passes its own Parley identity down;
 * the host's session is someone else and must not borrow it. */
function forgetInheritedIdentity(env: NodeJS.ProcessEnv) {
  for (const key of Object.keys(env)) {
    if (key === "PI_PARLEY_SESSION_ID" || key === "PI_PARLEY_STABLE_ID" || key.startsWith("PI_SUBAGENT_") || key === "PI_CLIENT_SESSION_ID") delete env[key];
  }
}

function defaultName(host: Host, cwd: string, env: NodeJS.ProcessEnv): string {
  const workspace = env.FLIGHTDECK_WORKSPACE_NAME?.trim();
  const tab = env.FLIGHTDECK_TAB_NAME?.trim();
  if (workspace && tab) return `${workspace}:${tab}`;
  return `${basename(cwd) || "home"}:${host.id}`;
}

const BOUNDARY_EVENTS = new Set(["UserPromptSubmit", "PostToolUse", "Stop"]);

export async function serveHost(host: Host): Promise<void> {
  // MCP owns stdout; anything else goes to the host's log.
  console.log = (...args: unknown[]) => console.error(...args);
  forgetInheritedIdentity(process.env);
  const hostPid = process.ppid;
  const cwd = host.cwd();

  // Hooks can tell us the session id when the host doesn't at startup.
  let sessionId = host.initialSessionId();
  let learnedSessionId: ((id: string) => void) | undefined;
  const sessionIdKnown = new Promise<string>((resolve) => { learnedSessionId = resolve; });

  let session: HostedSession | undefined;
  // Messages the host couldn't take straight away, oldest first.
  const pending: Delivery[] = [];

  // Where a wake goes: the host session the latest hook spoke for.
  let wakeTarget = sessionId;
  const deliver = (delivery: Delivery) => {
    const target = wakeTarget ?? sessionId;
    if (host.wake && target) {
      void host.wake(delivery.text, { sessionId: target }).then((accepted) => { if (!accepted) pending.push(delivery); });
      return;
    }
    pending.push(delivery);
  };

  const takePending = () => pending.splice(0).map((delivery) => delivery.text).join("\n\n---\n\n");

  const onHook = async ({ event, payload }: HookRequest): Promise<HookResponse> => {
    const hookSession = typeof payload.session_id === "string" ? payload.session_id : undefined;
    if (hookSession) {
      wakeTarget = hookSession;
      control.claim(hookSession);
      if (!sessionId) { sessionId = hookSession; learnedSessionId?.(hookSession); }
    }
    if (event === "UserPromptSubmit") session?.setBusy(true);
    if (event === "Stop") session?.setBusy(false);
    if (!BOUNDARY_EVENTS.has(event) || pending.length === 0) return {};
    const output = host.hookOutput(event, takePending());
    if (output && event === "Stop") session?.setBusy(true);
    return output ? { output } : {};
  };
  const control = serveControl(hostPid, cwd, onHook);
  if (sessionId) control.claim(sessionId);

  let ready!: (session: HostedSession) => void;
  const sessionReady = new Promise<HostedSession>((resolve) => { ready = resolve; });
  const mcp = serveMcp(async () => {
    const current = await sessionReady;
    return [{ ...current.tool, call: (args, signal) => current.call(args, signal) }];
  }, {
    name: "parley",
    version: host.version,
    instructions: "Parley: conversations with other agent sessions (Pi, Oh My Pi, Claude Code, Codex), on this computer and linked ones. Use the parley tool to list, send, ask and reply.",
    ...(host.capabilities ? { capabilities: host.capabilities } : {}),
    onClose: () => void shutdown(),
  });
  void mcp;

  if (!sessionId) {
    // SessionStart may have come before we listened; otherwise wait briefly for
    // it. A host without hooks still gets a session. (Health checks such as
    // `claude mcp list` start the server without a session and stop within
    // this wait, so they never appear as sessions.)
    sessionId = takeEarlySessionId(hostPid, cwd) ?? await Promise.race([
      sessionIdKnown,
      new Promise<string>((resolve) => {
        const started = Date.now();
        const poll = setInterval(() => {
          const early = takeEarlySessionId(hostPid, cwd);
          if (early || Date.now() - started > 4_000) { clearInterval(poll); resolve(early ?? `${host.id}-${randomUUID()}`); }
        }, 100);
      }),
    ]);
  }
  if (!sessionId.startsWith(`${host.id}-`)) control.claim(sessionId);
  const agentDir = getAgentDirPath(process.env);
  session = await startHostedSession({
    sessionId,
    cwd,
    defaultName: defaultName(host, cwd, process.env),
    stateFile: join(getParleyDirPath(agentDir), "hosts", host.id, `${sessionId.replace(/[^A-Za-z0-9_.-]/g, "_")}.jsonl`),
    deliver,
  });
  ready(session);

  let stopping = false;
  async function shutdown() {
    if (stopping) return;
    stopping = true;
    control.close();
    await session?.stop().catch(() => {});
    process.exit(0);
  }
  process.on("SIGTERM", () => void shutdown());
  process.on("SIGINT", () => void shutdown());
}
