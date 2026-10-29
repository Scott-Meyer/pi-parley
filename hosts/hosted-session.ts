// Parley for agents that aren't Pi (Claude Code, Codex, or any application that
// embeds it): the same Parley extension, unchanged, running on a small stand-in
// for Pi's extension API.
//
// The host's agent session gets exactly what a Pi session gets: the `parley`
// tool and its references, receipts, conversation history, naming and links.
// What Pi would show the model (incoming messages, answers) is handed to
// `deliver`; the host decides how it reaches its agent, and says whether it took
// it. What Pi would persist in its session file is kept in `stateFile`, so a
// resumed host session keeps its references and open questions.
//
// Many hosted sessions can run in one process: each has its own extension
// instance, broker connection and state, and takes its identity only from
// `begin(id)`.
import { EventEmitter } from "node:events";
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { dirname } from "node:path";
import { randomUUID } from "node:crypto";
import { registerParleyExtension } from "../index.ts";

export interface Delivery {
  /** What Pi would put in front of the model. */
  readonly text: string;
  /** The extension wants a turn to start for it (Pi's triggerTurn). */
  readonly wake: boolean;
  /** While a turn is running: "steer" (now) or "followUp" (after it). */
  readonly during?: "steer" | "followUp";
  /** What kind of thing this is: "parley_message" (from a colleague), "parley_message_control"
   * (a withdrawal or update), and other "parley_*" notices. */
  readonly customType: string;
  /** Parley's record of it. Stable: `from` (the sender's SessionInfo: name, id, cwd, app,
   * federation) and `message.id`, `message.replyTo`, `message.expectsReply`. The rest is
   * informational and may change. */
  readonly details?: unknown;
}

/** Whether the host put a delivery in front of its agent. Until it does, Parley treats the
 * message as not yet shown: the sender hears it's queued, and Parley tries again. */
export type DeliveryOutcome =
  | { readonly accepted: true }
  | { readonly accepted: false; readonly reason: string };

export interface HostedSessionOptions {
  readonly cwd: string;
  readonly model?: string;
  /** Used when the session has no name yet; worked out when it joins. */
  readonly defaultName: () => string;
  /** Where the session lives, for list rows ("Claude Desktop"); worked out when it joins. */
  readonly app?: () => string;
  /** Where a session's history lives (JSON lines), by its host session id. */
  readonly stateFile: (sessionId: string) => string;
  /** Put a delivery in front of the agent. Returning nothing means it was accepted. */
  readonly deliver: (delivery: Delivery) => DeliveryOutcome | Promise<DeliveryOutcome> | void;
  /** The environment this session is read from (FlightDeck, herdr, scope, whether it links
   * by default). Default: the process's. Pass {} for a session that is none of those. */
  readonly env?: Readonly<Record<string, string | undefined>>;
  /** false: don't put the session's id in the process environment. For processes that run
   * several sessions. */
  readonly publishSessionEnv?: boolean;
  /** Parley renamed the session (its agent used `rename`). */
  readonly nameChanged?: (name: string) => void;
}

export interface ToolOutcome {
  readonly text: string;
  readonly isError: boolean;
  /** Parley's structured result: references, recipients, receipts. Informational. */
  readonly details?: unknown;
}

/** A message in Pi's shape (AgentMessage): role "user", "assistant", "toolResult", "custom"
 * (with customType and details), "compactionSummary", and so on. */
export type HostedContextMessage = Record<string, unknown> & { readonly role: string };

export interface HostedSession {
  /** Join Parley as the host session with this id (Claude's session id, Codex's thread id). Once. */
  begin(sessionId: string): Promise<void>;
  readonly started: () => boolean;
  readonly tool: { readonly name: string; readonly description: string; readonly inputSchema: Record<string, unknown> };
  /** How the tool is meant to be used, for hosts that take server instructions. */
  readonly instructions: string;
  call(args: Record<string, unknown>, signal?: AbortSignal): Promise<ToolOutcome>;
  /** The messages about to go to the model, in Pi's shape; returns what to send instead.
   * Makes references readable, drops duplicate copies of a message, adds any delivery the
   * host hasn't accepted yet, and marks which messages are being answered. Accepted
   * deliveries kept as custom messages (customType, details) get all of that. */
  context(messages: readonly HostedContextMessage[]): Promise<HostedContextMessage[]>;
  /** The host's agent started or finished a turn. */
  setBusy(busy: boolean): void;
  /** The person renamed the session in the host. */
  setName(name: string): void;
  /** The host's agent now uses this model. */
  setModel(id: string): void;
  /** The host is compacting the agent's context. */
  compaction(phase: "start" | "end" | "failed"): void;
  readonly name: () => string | undefined;
  stop(): Promise<void>;
}

type Entry =
  | { kind: "entry"; type: string; data: unknown; at: string }
  | { kind: "message"; message: { customType?: string; content?: unknown; details?: unknown }; at: string }
  | { kind: "name"; name: string; at: string };

interface ToolDefinition {
  name: string;
  description: string;
  parameters: unknown;
  promptSnippet?: string;
  promptGuidelines?: string[];
  execute(id: string, params: unknown, signal: AbortSignal | undefined, onUpdate: undefined, ctx: unknown): Promise<{ content: { type: string; text?: string }[]; details?: unknown; isError?: boolean }>;
}

type SentMessage = { customType?: string; content?: unknown; details?: unknown };

/** Which incoming message a delivery carries, so one isn't handed over twice at once. */
function deliveryKey(message: SentMessage): string | undefined {
  const details = message.details as { message?: { id?: string }; control?: { messageId?: string; action?: string } } | undefined;
  if (message.customType === "parley_message" && details?.message?.id) return `message:${details.message.id}`;
  if (message.customType === "parley_message_control" && details?.control?.messageId) return `control:${details.control.messageId}:${details.control.action ?? ""}`;
  return undefined;
}

const RETRY_FIRST_MS = 1_000;
const RETRY_MAX_MS = 30_000;

/** Sets up Parley for a host session without joining yet: the tool is known
 * at once, and `begin` joins once the host says which session this is. */
export async function createHostedSession(options: HostedSessionOptions): Promise<HostedSession> {
  let sessionId: string | undefined;
  let stateFile: string | undefined;
  let history: Entry[] = [];
  const record = (entry: Entry) => {
    history.push(entry);
    if (stateFile) appendFileSync(stateFile, `${JSON.stringify(entry)}\n`, { mode: 0o600 });
  };

  let sessionName: string | undefined;
  let app: string | undefined;
  let busy = false;
  const model = { id: options.model ?? "unknown" };
  const bus = new EventEmitter();
  const handlers = new Map<string, ((event: unknown, ctx: unknown) => unknown)[]>();
  const tools: ToolDefinition[] = [];
  // Deliveries the host is still deciding on, and ones it refused, by incoming message.
  const deciding = new Set<string>();
  const refused = new Map<string, { retryAt: number; waitMs: number }>();

  const renamed = (name: string) => {
    sessionName = name;
    record({ kind: "name", name, at: new Date().toISOString() });
    void emit("session_info_changed", { type: "session_info_changed", name });
  };

  const handOver = (message: SentMessage, delivery: { triggerTurn?: boolean; deliverAs?: string } | undefined) => {
    const text = typeof message.content === "string" ? message.content
      : Array.isArray(message.content) ? message.content.map((part: { text?: string }) => part?.text ?? "").join("\n") : "";
    const key = deliveryKey(message);
    if (key && deciding.has(key)) return;
    const backoff = key ? refused.get(key) : undefined;
    if (backoff && Date.now() < backoff.retryAt) throw new Error("The host isn't taking this message yet");
    const accept = () => {
      if (key) refused.delete(key);
      record({ kind: "message", message, at: new Date().toISOString() });
    };
    const refuse = (reason: string) => {
      if (!key) return;
      const waitMs = Math.min(RETRY_MAX_MS, backoff ? backoff.waitMs * 2 : RETRY_FIRST_MS);
      refused.set(key, { retryAt: Date.now() + waitMs, waitMs });
      return reason;
    };
    if (!text.trim()) { accept(); return; }
    const outcome = options.deliver({
      text,
      wake: delivery?.triggerTurn === true,
      ...(delivery?.deliverAs === "steer" || delivery?.deliverAs === "followUp" ? { during: delivery.deliverAs } : {}),
      customType: message.customType ?? "parley",
      ...(message.details !== undefined ? { details: message.details } : {}),
    });
    if (outcome && typeof (outcome as Promise<DeliveryOutcome>).then === "function") {
      // Parley confirms it on its next check once accepted, and tries again after a refusal.
      if (key) deciding.add(key);
      void (outcome as Promise<DeliveryOutcome>).then(
        (result) => { if (result.accepted) accept(); else refuse(result.reason); },
        (error: unknown) => { refuse(error instanceof Error ? error.message : String(error)); },
      ).finally(() => { if (key) deciding.delete(key); });
      return;
    }
    const result = outcome as DeliveryOutcome | undefined;
    if (!result || result.accepted) { accept(); return; }
    const reason = refuse(result.reason);
    // Like Pi when it can't take a message: Parley keeps it, tells the sender, and tries again.
    if (reason !== undefined) throw new Error(reason);
  };

  const pi = {
    getSessionName: () => sessionName,
    setSessionName: (name: string) => {
      if (name === sessionName) return;
      renamed(name);
      options.nameChanged?.(name);
    },
    events: {
      on: (channel: string, handler: (payload: unknown) => void) => { bus.on(channel, handler); return () => bus.off(channel, handler); },
      emit: (channel: string, payload: unknown) => bus.emit(channel, payload),
    },
    on: (event: string, handler: (event: unknown, ctx: unknown) => unknown) => {
      handlers.set(event, [...(handlers.get(event) ?? []), handler]);
      return () => handlers.set(event, (handlers.get(event) ?? []).filter((h) => h !== handler));
    },
    registerTool: (tool: ToolDefinition) => { tools.push(tool); },
    getActiveTools: () => tools.map((tool) => tool.name),
    setActiveTools: () => undefined,
    // A host has its own commands, shortcuts and rendering.
    registerCommand: () => undefined,
    registerShortcut: () => undefined,
    registerMessageRenderer: () => undefined,
    sendMessage: handOver,
    appendEntry: (type: string, data: unknown) => record({ kind: "entry", type, data, at: new Date().toISOString() }),
  };

  const ctx = {
    cwd: options.cwd,
    model,
    hasUI: false,
    isIdle: () => !busy,
    abort: () => undefined,
    ui: undefined,
    sessionManager: {
      getSessionId: () => sessionId ?? "",
      getEntries: () => history.flatMap((entry, index): Record<string, unknown>[] => {
        if (entry.kind === "entry") return [{ type: "custom", customType: entry.type, data: entry.data, id: `entry-${index}`, parentId: index > 0 ? `entry-${index - 1}` : null, timestamp: entry.at }];
        if (entry.kind === "message") return [{ type: "custom_message", ...entry.message, id: `message-${index}`, parentId: null, timestamp: entry.at }];
        return [];
      }),
    },
  };

  async function emit(event: string, payload: unknown = {}): Promise<unknown[]> {
    const results: unknown[] = [];
    for (const handler of handlers.get(event) ?? []) {
      try { results.push(await handler(payload, ctx)); } catch (error) { console.error(`[parley] ${event} handler failed: ${error instanceof Error ? error.message : String(error)}`); }
    }
    return results;
  }

  registerParleyExtension(pi as never, {
    resolveApp: () => app,
    identity: "session",
    ...(options.env ? { env: options.env } : {}),
    ...(options.publishSessionEnv === false ? { publishSessionEnv: false } : {}),
  });
  const tool = tools.find((candidate) => candidate.name === "parley");
  if (!tool) throw new Error("Parley registered no parley tool");

  const instructions = [tool.promptSnippet, ...(tool.promptGuidelines ?? [])].filter(Boolean).join("\n");
  return {
    async begin(id) {
      if (sessionId) return;
      sessionId = id;
      stateFile = options.stateFile(id);
      mkdirSync(dirname(stateFile), { recursive: true, mode: 0o700 });
      history = existsSync(stateFile)
        ? readFileSync(stateFile, "utf8").split("\n").filter(Boolean).flatMap((line) => { try { return [JSON.parse(line) as Entry]; } catch { return []; } })
        : [];
      sessionName = [...history].reverse().find((entry): entry is Extract<Entry, { kind: "name" }> => entry.kind === "name")?.name ?? options.defaultName();
      app = options.app?.();
      await emit("session_start", { type: "session_start", reason: history.length > 0 ? "resume" : "startup" });
    },
    started: () => sessionId !== undefined,
    tool: {
      name: tool.name,
      description: tool.description,
      // TypeBox schemas are JSON Schema; drop its symbol-keyed metadata.
      inputSchema: JSON.parse(JSON.stringify(tool.parameters)) as Record<string, unknown>,
    },
    instructions,
    async call(args, signal) {
      if (!sessionId) return { text: "Parley is still starting in this session; try again in a moment.", isError: true };
      const toolCallId = `host-${randomUUID()}`;
      await emit("tool_execution_start", { toolCallId, toolName: tool.name, args });
      let result: Awaited<ReturnType<ToolDefinition["execute"]>>;
      try {
        result = await tool.execute(toolCallId, args, signal, undefined, ctx);
      } catch (error) {
        await emit("tool_execution_end", { toolCallId, toolName: tool.name, isError: true });
        return { text: `Parley failed: ${error instanceof Error ? error.message : String(error)}`, isError: true };
      }
      await emit("tool_result", { toolCallId, toolName: tool.name, input: args, content: result.content, details: result.details, isError: result.isError ?? false });
      await emit("tool_execution_end", { toolCallId, toolName: tool.name, isError: result.isError ?? false });
      const text = result.content.map((part) => part.text ?? "").filter(Boolean).join("\n");
      const failed = result.isError === true || (result.details as { error?: unknown } | undefined)?.error === true;
      return { text, isError: failed, ...(result.details !== undefined ? { details: result.details } : {}) };
    },
    async context(messages) {
      let current = [...messages];
      for (const result of await emit("context", { type: "context", messages: current })) {
        const next = (result as { messages?: HostedContextMessage[] } | undefined)?.messages;
        if (Array.isArray(next)) current = next;
      }
      return current;
    },
    setBusy(next) {
      if (next === busy) return;
      busy = next;
      void (async () => {
        await emit(next ? "agent_start" : "agent_end", { type: next ? "agent_start" : "agent_end", messages: [] });
        if (next) await emit("turn_start", { type: "turn_start" });
      })();
    },
    setName(name) {
      const trimmed = name.trim();
      if (!trimmed || trimmed === sessionName) return;
      renamed(trimmed);
    },
    setModel(id) {
      if (!id || id === model.id) return;
      model.id = id;
      void emit("model_select", { type: "model_select", model: { id } });
    },
    compaction(phase) {
      void emit(phase === "start" ? "session_before_compact" : phase === "end" ? "session_compact" : "session_compact_failed", { type: phase });
    },
    name: () => sessionName,
    async stop() {
      await emit("session_shutdown", { type: "session_shutdown" });
    },
  };
}

/** Set up and join at once, for hosts that know their session from the start. */
export async function startHostedSession(options: HostedSessionOptions & { sessionId: string }): Promise<HostedSession> {
  const session = await createHostedSession(options);
  await session.begin(options.sessionId);
  return session;
}
