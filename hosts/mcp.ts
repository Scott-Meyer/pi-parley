// A small Model Context Protocol server over stdio: newline-delimited
// JSON-RPC, one tool, optional notifications back to the host. Enough for
// Claude Code and Codex; no SDK dependency.
// https://modelcontextprotocol.io/specification
import { createInterface } from "node:readline";

export interface McpTool {
  readonly name: string;
  readonly description: string;
  readonly inputSchema: Record<string, unknown>;
  /** meta: the request's _meta, where hosts put context (Codex: threadId). */
  call(args: Record<string, unknown>, signal: AbortSignal, meta?: Record<string, unknown>): Promise<{ text: string; isError: boolean }>;
}

export interface McpServerOptions {
  readonly name: string;
  readonly version: string;
  readonly instructions?: string;
  /** Extra capabilities, e.g. { experimental: { "claude/channel": {} } }. */
  readonly capabilities?: Record<string, unknown>;
  /** Called when the host has finished the handshake. */
  readonly onReady?: (client: { name?: string; version?: string }) => void;
  /** Called when stdin closes: the host is gone. */
  readonly onClose?: () => void;
}

export interface McpServer {
  /** Send a notification to the host. */
  notify(method: string, params: Record<string, unknown>): void;
}

export function serveMcp(tools: () => readonly McpTool[] | Promise<readonly McpTool[]>, options: McpServerOptions): McpServer {
  const write = (message: Record<string, unknown>) => process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", ...message })}\n`);
  const running = new Map<string | number, AbortController>();

  const handle = async (message: { id?: string | number; method?: string; params?: Record<string, unknown> }) => {
    const { id, method, params = {} } = message;
    const reply = (result: unknown) => { if (id !== undefined) write({ id, result }); };
    const fail = (code: number, text: string) => { if (id !== undefined) write({ id, error: { code, message: text } }); };
    switch (method) {
      case "initialize": {
        const client = (params.clientInfo ?? {}) as { name?: string; version?: string };
        reply({
          protocolVersion: typeof params.protocolVersion === "string" ? params.protocolVersion : "2025-06-18",
          capabilities: { tools: {}, ...(options.capabilities ?? {}) },
          serverInfo: { name: options.name, version: options.version },
          ...(options.instructions ? { instructions: options.instructions } : {}),
        });
        queueMicrotask(() => options.onReady?.(client));
        return;
      }
      case "ping": reply({}); return;
      case "tools/list":
        reply({ tools: (await tools()).map(({ name, description, inputSchema }) => ({ name, description, inputSchema })) });
        return;
      case "tools/call": {
        const tool = (await tools()).find((candidate) => candidate.name === params.name);
        if (!tool) { fail(-32602, `Unknown tool: ${String(params.name)}`); return; }
        const controller = new AbortController();
        if (id !== undefined) running.set(id, controller);
        try {
          const meta = params._meta && typeof params._meta === "object" ? params._meta as Record<string, unknown> : undefined;
          const outcome = await tool.call((params.arguments ?? {}) as Record<string, unknown>, controller.signal, meta);
          reply({ content: [{ type: "text", text: outcome.text }], isError: outcome.isError });
        } catch (error) {
          reply({ content: [{ type: "text", text: error instanceof Error ? error.message : String(error) }], isError: true });
        } finally {
          if (id !== undefined) running.delete(id);
        }
        return;
      }
      case "notifications/cancelled": {
        const requestId = params.requestId as string | number | undefined;
        if (requestId !== undefined) running.get(requestId)?.abort();
        return;
      }
      default:
        if (method?.startsWith("notifications/")) return;
        fail(-32601, `Method not found: ${String(method)}`);
    }
  };

  const lines = createInterface({ input: process.stdin });
  lines.on("line", (line) => {
    if (!line.trim()) return;
    let message: { id?: string | number; method?: string; params?: Record<string, unknown> };
    try { message = JSON.parse(line); } catch { return; }
    void handle(message);
  });
  lines.on("close", () => options.onClose?.());

  return { notify: (method, params) => write({ method, params }) };
}
