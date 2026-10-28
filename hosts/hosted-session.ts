// Parley for agents that aren't Pi (Claude Code, Codex): the same Parley
// extension, unchanged, running on a small stand-in for Pi's extension API.
//
// The host's agent session gets exactly what a Pi session gets: the `parley`
// tool and its references, receipts, conversation history, naming and links.
// What Pi would show the model (incoming messages, answers) is handed to
// `deliver`; the host decides how it reaches its agent. What Pi would persist
// in its session file is kept in `stateFile`, so a resumed host session keeps
// its references and open questions.
import { EventEmitter } from "node:events";
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { dirname } from "node:path";
import { randomUUID } from "node:crypto";

export interface Delivery {
  /** What Pi would put in front of the model. */
  readonly text: string;
  /** The extension wants a turn to start for it (Pi's triggerTurn). */
  readonly wake: boolean;
  /** While a turn is running: "steer" (now) or "followUp" (after it). */
  readonly during?: "steer" | "followUp";
}

export interface HostedSessionOptions {
  /** The host's own session identity (Claude's session id, Codex's thread id). */
  readonly sessionId: string;
  readonly cwd: string;
  readonly model?: string;
  /** Used when the session has no name yet. */
  readonly defaultName: string;
  /** Where this session's history lives (JSON lines). */
  readonly stateFile: string;
  readonly deliver: (delivery: Delivery) => void;
}

export interface ToolOutcome { readonly text: string; readonly isError: boolean }

export interface HostedSession {
  readonly tool: { readonly name: string; readonly description: string; readonly inputSchema: Record<string, unknown> };
  /** How the tool is meant to be used, for hosts that take server instructions. */
  readonly instructions: string;
  call(args: Record<string, unknown>, signal?: AbortSignal): Promise<ToolOutcome>;
  /** The host's agent started or finished a turn. */
  setBusy(busy: boolean): void;
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

export async function startHostedSession(options: HostedSessionOptions): Promise<HostedSession> {
  mkdirSync(dirname(options.stateFile), { recursive: true, mode: 0o700 });
  const history: Entry[] = existsSync(options.stateFile)
    ? readFileSync(options.stateFile, "utf8").split("\n").filter(Boolean).flatMap((line) => { try { return [JSON.parse(line) as Entry]; } catch { return []; } })
    : [];
  const record = (entry: Entry) => {
    history.push(entry);
    appendFileSync(options.stateFile, `${JSON.stringify(entry)}\n`, { mode: 0o600 });
  };

  let sessionName = [...history].reverse().find((entry): entry is Extract<Entry, { kind: "name" }> => entry.kind === "name")?.name ?? options.defaultName;
  let busy = false;
  const bus = new EventEmitter();
  const handlers = new Map<string, ((event: unknown, ctx: unknown) => unknown)[]>();
  const tools: ToolDefinition[] = [];

  const pi = {
    getSessionName: () => sessionName,
    setSessionName: (name: string) => {
      if (name === sessionName) return;
      sessionName = name;
      record({ kind: "name", name, at: new Date().toISOString() });
      void emit("session_info_changed", { type: "session_info_changed", name });
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
    sendMessage: (message: { customType?: string; content?: unknown; details?: unknown }, delivery?: { triggerTurn?: boolean; deliverAs?: string }) => {
      record({ kind: "message", message, at: new Date().toISOString() });
      const text = typeof message.content === "string" ? message.content
        : Array.isArray(message.content) ? message.content.map((part: { text?: string }) => part?.text ?? "").join("\n") : "";
      if (!text.trim()) return;
      options.deliver({
        text,
        wake: delivery?.triggerTurn === true,
        ...(delivery?.deliverAs === "steer" || delivery?.deliverAs === "followUp" ? { during: delivery.deliverAs } : {}),
      });
    },
    appendEntry: (type: string, data: unknown) => record({ kind: "entry", type, data, at: new Date().toISOString() }),
  };

  const ctx = {
    cwd: options.cwd,
    model: { id: options.model ?? "unknown" },
    hasUI: false,
    isIdle: () => !busy,
    abort: () => undefined,
    ui: undefined,
    sessionManager: {
      getSessionId: () => options.sessionId,
      getEntries: () => history.flatMap((entry, index): Record<string, unknown>[] => {
        if (entry.kind === "entry") return [{ type: "custom", customType: entry.type, data: entry.data, id: `entry-${index}`, parentId: index > 0 ? `entry-${index - 1}` : null, timestamp: entry.at }];
        if (entry.kind === "message") return [{ type: "custom_message", ...entry.message, id: `message-${index}`, parentId: null, timestamp: entry.at }];
        return [];
      }),
    },
  };

  async function emit(event: string, payload: unknown = {}) {
    for (const handler of handlers.get(event) ?? []) {
      try { await handler(payload, ctx); } catch (error) { console.error(`[parley] ${event} handler failed: ${error instanceof Error ? error.message : String(error)}`); }
    }
  }

  const { registerParleyExtension } = await import("../index.ts");
  registerParleyExtension(pi as never);
  const tool = tools.find((candidate) => candidate.name === "parley");
  if (!tool) throw new Error("Parley registered no parley tool");
  await emit("session_start", { type: "session_start", reason: history.length > 0 ? "resume" : "startup" });

  const instructions = [tool.promptSnippet, ...(tool.promptGuidelines ?? [])].filter(Boolean).join("\n");
  return {
    tool: {
      name: tool.name,
      description: tool.description,
      // TypeBox schemas are JSON Schema; drop its symbol-keyed metadata.
      inputSchema: JSON.parse(JSON.stringify(tool.parameters)) as Record<string, unknown>,
    },
    instructions,
    async call(args, signal) {
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
      return { text, isError: failed };
    },
    setBusy(next) {
      if (next === busy) return;
      busy = next;
      void emit(next ? "agent_start" : "agent_end", { type: next ? "agent_start" : "agent_end", messages: [] });
    },
    name: () => sessionName,
    async stop() {
      await emit("session_shutdown", { type: "session_shutdown" });
    },
  };
}
