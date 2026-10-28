// Codex (terminal, desktop app and IDE extension, which share config).
//
// Codex doesn't tell MCP servers which thread they serve; its hooks do, so the
// server learns the thread id from the first hook. Newer Codex can queue a
// message into an existing thread from outside (`codex queue`), which starts a
// turn when it's idle; without it, messages wait for the next turn boundary.
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import type { Host } from "./serve.ts";

const run = promisify(execFile);

export function codexHost(env: NodeJS.ProcessEnv = process.env, version = "0"): Host {
  let canQueue: Promise<boolean> | undefined;
  const codex = env.PI_PARLEY_CODEX_BIN?.trim() || "codex";
  return {
    id: "codex",
    version,
    initialSessionId: () => env.CODEX_THREAD_ID?.trim() || env.CODEX_SESSION_ID?.trim() || undefined,
    cwd: () => process.cwd(),
    async wake(text, { sessionId }) {
      canQueue ??= run(codex, ["queue", "--help"], { timeout: 10_000 }).then(() => true, () => false);
      if (!await canQueue) return false;
      try {
        await run(codex, ["queue", "--thread", sessionId, "--message", text], { timeout: 15_000 });
        return true;
      } catch {
        return false;
      }
    },
    hookOutput: (event, text) => event === "Stop"
      ? { decision: "block", reason: text }
      : { hookSpecificOutput: { hookEventName: event, additionalContext: text } },
  };
}
