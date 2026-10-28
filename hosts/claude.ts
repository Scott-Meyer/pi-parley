// Claude Code (terminal, and the desktop app's Code tab, which share config).
//
// Claude tells its MCP servers their session id, and every Claude process has
// a cross-session inbox (CLAUDE_CODE_MESSAGING_SOCKET) that its own SendMessage
// uses: a message written there reaches the agent next, starting a turn if it
// is idle. That inbox is internal to Claude Code, so hooks remain the fallback:
// a message the inbox didn't take is handed over at the next turn boundary.
import { randomBytes } from "node:crypto";
import net from "node:net";
import type { Host } from "./serve.ts";

export function claudeHost(env: NodeJS.ProcessEnv = process.env, version = "0"): Host {
  return {
    id: "claude",
    version,
    initialSessionId: () => env.CLAUDE_CODE_SESSION_ID?.trim() || undefined,
    cwd: () => env.CLAUDE_PROJECT_DIR?.trim() || process.cwd(),
    wake: (text) => sendToInbox(env, text),
    hookOutput: (event, text) => event === "Stop"
      ? { decision: "block", reason: text }
      : { hookSpecificOutput: { hookEventName: event, additionalContext: text } },
  };
}

/** Write one message into this Claude session's own inbox. */
function sendToInbox(env: NodeJS.ProcessEnv, text: string): Promise<boolean> {
  const path = env.CLAUDE_CODE_MESSAGING_SOCKET?.trim();
  if (!path) return Promise.resolve(false);
  return new Promise((resolve) => {
    const socket = net.connect(path);
    let settled = false;
    const done = (accepted: boolean) => { if (settled) return; settled = true; clearTimeout(timer); socket.destroy(); resolve(accepted); };
    const timer = setTimeout(() => done(true), 3_000);
    socket.on("connect", () => {
      const frames = [
        ...(env.CLAUDE_CODE_MESSAGING_TOKEN ? [{ type: "auth", token: env.CLAUDE_CODE_MESSAGING_TOKEN }] : []),
        { type: "user", message: { role: "user", content: text }, priority: "next", msgV: 1, msg_id: `cc-msg-${randomBytes(16).toString("hex")}` },
      ];
      socket.end(frames.map((frame) => `${JSON.stringify(frame)}\n`).join(""), () => done(true));
    });
    socket.on("error", () => done(false));
  });
}
