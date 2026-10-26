// herdr's socket API: newline-delimited JSON requests and pushed events.
// https://herdr.dev/docs/socket-api/
import net from "node:net";
import { randomUUID } from "node:crypto";

export interface HerdrPane { pane_id: string; tab_id: string; workspace_id: string; label?: string | null }
export interface HerdrTab { tab_id: string; workspace_id: string; label: string; pane_count: number }
export interface HerdrWorkspace { workspace_id: string; label: string }
export interface HerdrEvent { event: string; data: Record<string, unknown> }

/** One request on its own connection. Rejects on herdr errors, closed sockets and timeouts. */
export function herdrRequest<T = Record<string, unknown>>(socketPath: string, method: string, params: Record<string, unknown>, timeoutMs = 3_000): Promise<T> {
  return new Promise((resolve, reject) => {
    const id = `parley:${randomUUID()}`;
    const socket = net.connect(socketPath);
    let buffer = "";
    const done = (error?: Error, value?: T) => {
      clearTimeout(timer);
      socket.destroy();
      if (error) reject(error); else resolve(value as T);
    };
    const timer = setTimeout(() => done(new Error(`herdr ${method} timed out`)), timeoutMs);
    timer.unref?.();
    socket.on("connect", () => socket.write(`${JSON.stringify({ id, method, params })}\n`));
    socket.on("data", (chunk) => {
      buffer += chunk.toString("utf8");
      let newline: number;
      while ((newline = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        let message: { id?: string; result?: T; error?: { message?: string } };
        try { message = JSON.parse(line); } catch { continue; }
        if (message.id !== id) continue;
        if (message.error) done(new Error(message.error.message ?? `herdr ${method} failed`));
        else done(undefined, message.result);
      }
    });
    socket.on("error", (error) => done(error));
    socket.on("close", () => done(new Error("herdr closed the connection")));
  });
}

/** Pushed events on a long-lived connection until close(). Nothing is replayed,
 * so callers fetch current state after `onReady`. */
export function herdrSubscribe(
  socketPath: string,
  types: readonly string[],
  handlers: { onReady(): void; onEvent(event: HerdrEvent): void; onClose(): void },
): { close(): void } {
  const socket = net.connect(socketPath);
  let buffer = "";
  let ready = false;
  socket.on("connect", () => socket.write(`${JSON.stringify({
    id: `parley:subscribe:${randomUUID()}`, method: "events.subscribe",
    params: { subscriptions: types.map((type) => ({ type })) },
  })}\n`));
  socket.on("data", (chunk) => {
    buffer += chunk.toString("utf8");
    let newline: number;
    while ((newline = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      let message: { result?: { type?: string }; event?: string; data?: Record<string, unknown> };
      try { message = JSON.parse(line); } catch { continue; }
      if (!ready && message.result?.type === "subscription_started") { ready = true; handlers.onReady(); continue; }
      if (typeof message.event === "string" && message.data && typeof message.data === "object") {
        handlers.onEvent({ event: message.event, data: message.data });
      }
    }
  });
  socket.on("error", () => {});
  socket.on("close", () => handlers.onClose());
  socket.unref();
  return { close: () => socket.destroy() };
}
