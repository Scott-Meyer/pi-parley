// Codex (terminal, desktop app and IDE extension, which share config).
//
// Codex doesn't tell MCP servers which thread they serve; its hooks do, so the
// server learns the thread id from the first hook. Newer Codex can queue a
// message into an existing thread from outside (`codex queue`), which starts a
// turn when it's idle; without it, messages wait for the next turn boundary.
import { execFile, execFileSync } from "node:child_process";
import { promisify } from "node:util";
import { sessionPlace } from "../index.ts";
import type { Host } from "./serve.ts";

const run = promisify(execFile);

export function codexHost(env: NodeJS.ProcessEnv = process.env, version = "0"): Host {
  let canQueue: Promise<boolean> | undefined;
  const codex = env.PI_PARLEY_CODEX_BIN?.trim() || "codex";
  return {
    id: "codex",
    version,
    initialSessionId: () => env.CODEX_THREAD_ID?.trim() || env.CODEX_SESSION_ID?.trim() || resumedThread(),
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
    app: (current) => startedByChatGptApp() ? "Codex in the ChatGPT app"
      : sessionPlace(current) ? `Codex in ${sessionPlace(current)}` : "Codex",
    hookOutput: (event, text) => event === "Stop"
      ? { decision: "block", reason: text }
      : { hookSpecificOutput: { hookEventName: event, additionalContext: text } },
  };
}

/** `codex resume <id>` continues a thread: the same Parley session as before.
 * A new session's thread isn't known until its first hook. */
function resumedThread(): string | undefined {
  try {
    const args = execFileSync("ps", ["-o", "args=", "-p", String(process.ppid)], { encoding: "utf8", timeout: 2_000 });
    return /\bresume\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b/.exec(args)?.[1];
  } catch {
    return undefined;
  }
}

/** The ChatGPT app runs Codex sessions through its own bundled app server. */
function startedByChatGptApp(): boolean {
  try {
    return /ChatGPT\.app\//.test(execFileSync("ps", ["-o", "command=", "-p", String(process.ppid)], { encoding: "utf8", timeout: 2_000 }));
  } catch {
    return false;
  }
}
