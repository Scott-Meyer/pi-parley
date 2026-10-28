// `parley connect claude|codex`: register Parley's MCP server with the host
// (through the host's own CLI) and add Parley's hooks to the host's user
// hooks file. Every hook Parley adds carries "_parley": "v1", so connecting
// again replaces them and disconnecting removes exactly them; other hooks
// (yours, FlightDeck's) are left as they are. The file is backed up first.
import { execFile } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { promisify } from "node:util";

const run = promisify(execFile);
const MARK = "_parley";
const EVENTS = ["SessionStart", "UserPromptSubmit", "PostToolUse", "Stop"] as const;

export interface ConnectOptions {
  /** This Parley installation (checkout or package). */
  readonly root: string;
  readonly args?: readonly string[];
  readonly env?: NodeJS.ProcessEnv;
  /** For tests: where the host keeps its user config. */
  readonly hooksFile?: string;
  /** For tests: skip the host CLI's MCP registration. */
  readonly skipMcp?: boolean;
}

type HookEntry = { matcher?: string; hooks: { type: string; command: string; timeout?: number; [MARK]?: string }[] };
type HooksFile = { hooks?: Record<string, HookEntry[]>; [key: string]: unknown };

function hooksFileFor(host: "claude" | "codex", env: NodeJS.ProcessEnv): string {
  if (host === "claude") return join(env.CLAUDE_CONFIG_DIR?.trim() || join(homedir(), ".claude"), "settings.json");
  return join(env.CODEX_HOME?.trim() || join(homedir(), ".codex"), "hooks.json");
}

function quote(value: string): string {
  return /^[A-Za-z0-9_./:-]+$/.test(value) ? value : `'${value.replace(/'/g, `'\\''`)}'`;
}

/** Without our entries; empty event lists dropped. */
function withoutParley(hooks: Record<string, HookEntry[]>): Record<string, HookEntry[]> {
  const kept: Record<string, HookEntry[]> = {};
  for (const [event, entries] of Object.entries(hooks)) {
    const remaining = entries
      .map((entry) => ({ ...entry, hooks: entry.hooks.filter((hook) => hook[MARK] === undefined) }))
      .filter((entry) => entry.hooks.length > 0);
    if (remaining.length > 0) kept[event] = remaining;
  }
  return kept;
}

export async function connectHost(host: "claude" | "codex", connect: boolean, options: ConnectOptions): Promise<string[]> {
  const env = options.env ?? process.env;
  const bin = join(options.root, "bin", "parley.mjs");
  const lines: string[] = [];

  // MCP server, through the host's own CLI so its config stays its own.
  if (!options.skipMcp) {
    const cli = host === "claude" ? "claude" : "codex";
    const remove = host === "claude" ? ["mcp", "remove", "--scope", "user", "parley"] : ["mcp", "remove", "parley"];
    await run(cli, remove, { timeout: 30_000 }).catch(() => undefined);
    if (connect) {
      const add = host === "claude"
        ? ["mcp", "add", "--scope", "user", "parley", "--", "node", bin, "mcp", "claude"]
        : ["mcp", "add", "parley", "--", "node", bin, "mcp", "codex"];
      await run(cli, add, { timeout: 30_000 });
      lines.push(`Registered Parley's MCP server with ${host === "claude" ? "Claude Code" : "Codex"}.`);
    } else {
      lines.push(`Removed Parley's MCP server from ${host === "claude" ? "Claude Code" : "Codex"}.`);
    }
  }

  // Hooks: when the agent is working, and turn boundaries for messages that couldn't wake it.
  const file = options.hooksFile ?? hooksFileFor(host, env);
  const existing: HooksFile = existsSync(file) ? JSON.parse(readFileSync(file, "utf8")) : {};
  const hooks = withoutParley(existing.hooks ?? {});
  if (connect) {
    for (const event of EVENTS) {
      hooks[event] = [...(hooks[event] ?? []), {
        hooks: [{ type: "command", command: `node ${quote(bin)} hook ${event}`, timeout: 10, [MARK]: "v1" }],
      }];
    }
  }
  const next: HooksFile = { ...existing, hooks };
  if (Object.keys(hooks).length === 0) delete next.hooks;
  mkdirSync(dirname(file), { recursive: true });
  if (existsSync(file)) copyFileSync(file, `${file}.before-parley`);
  const tmp = `${file}.${process.pid}.tmp`;
  writeFileSync(tmp, `${JSON.stringify(next, null, 2)}\n`);
  renameSync(tmp, file);
  lines.push(connect ? `Added Parley's hooks to ${file} (backup: ${file}.before-parley).` : `Removed Parley's hooks from ${file}.`);
  if (connect && host === "codex") lines.push("Codex asks you to trust new hooks once: run /hooks in Codex and trust Parley's.");
  if (connect) lines.push(`Start a new ${host === "claude" ? "Claude Code" : "Codex"} session to use it.`);
  return lines;
}
