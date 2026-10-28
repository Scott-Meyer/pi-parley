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
/** What a host should pass through to Parley's server: its scope and agent
 * directory, and the names FlightDeck and herdr give the session. Codex starts
 * MCP servers with a bare environment otherwise. */
export const PASS_THROUGH_ENV = [
  "PI_PARLEY_SCOPE_ID", "PI_CODING_AGENT_DIR", "PI_PARLEY_LINK_BY_DEFAULT", "PI_PARLEY_LINK_LABEL",
  "FLIGHTDECK_WORKSPACE_NAME", "FLIGHTDECK_TAB_NAME", "FLIGHTDECK_TAB_ID", "FLIGHTDECK_PANE_ID",
  "HERDR_ENV", "HERDR_SOCKET_PATH", "HERDR_PANE_ID", "HERDR_TAB_ID", "HERDR_WORKSPACE_ID",
] as const;
const CLAUDE_TOOL = "mcp__parley__parley";
const CODEX_BEGIN = "# >>> parley: managed by `parley connect codex`; `parley disconnect codex` removes it";
const CODEX_END = "# <<< parley";

export interface ConnectOptions {
  /** This Parley installation (checkout or package). */
  readonly root: string;
  readonly args?: readonly string[];
  readonly env?: NodeJS.ProcessEnv;
  /** For tests: where the host keeps its user config. */
  readonly hooksFile?: string;
  /** For tests: skip the host CLI's MCP registration. */
  readonly skipMcp?: boolean;
  /** For tests: Codex's config.toml. */
  readonly codexConfigFile?: string;
}

function tomlString(value: string): string {
  return JSON.stringify(value);
}

/** Codex: Parley's own block in config.toml, so it can carry env_vars and
 * approval settings that `codex mcp add` can't. */
function writeCodexBlock(file: string, bin: string, connect: boolean) {
  const text = existsSync(file) ? readFileSync(file, "utf8") : "";
  const begin = text.indexOf(CODEX_BEGIN);
  const end = text.indexOf(CODEX_END);
  let rest = begin >= 0 && end > begin ? `${text.slice(0, begin).trimEnd()}\n${text.slice(end + CODEX_END.length).replace(/^\n/, "")}` : text;
  if (/^\[mcp_servers\.parley[\].]/m.test(rest)) {
    throw new Error(`${file} already has an [mcp_servers.parley] Parley didn't write; remove it (codex mcp remove parley) and connect again`);
  }
  if (connect) {
    rest = `${rest.trimEnd()}\n\n${[
      CODEX_BEGIN,
      "[mcp_servers.parley]",
      `command = "node"`,
      `args = [${[bin, "mcp", "codex"].map(tomlString).join(", ")}]`,
      `env_vars = [${PASS_THROUGH_ENV.map(tomlString).join(", ")}]`,
      `default_tools_approval_mode = "approve"`,
      "startup_timeout_sec = 30",
      CODEX_END,
    ].join("\n")}\n`;
  }
  mkdirSync(dirname(file), { recursive: true });
  if (existsSync(file)) copyFileSync(file, `${file}.before-parley`);
  const tmp = `${file}.${process.pid}.tmp`;
  writeFileSync(tmp, rest.startsWith("\n") ? rest.slice(1) : rest, { mode: 0o600 });
  renameSync(tmp, file);
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

  // MCP server. Claude: through its own CLI, so its config stays its own.
  // Codex: Parley's marked block in config.toml (see writeCodexBlock).
  if (host === "claude" && !options.skipMcp) {
    await run("claude", ["mcp", "remove", "--scope", "user", "parley"], { timeout: 30_000 }).catch(() => undefined);
    if (connect) await run("claude", ["mcp", "add", "--scope", "user", "parley", "--", "node", bin, "mcp", "claude"], { timeout: 30_000 });
  }
  if (host === "codex") {
    const configFile = options.codexConfigFile ?? join(env.CODEX_HOME?.trim() || join(homedir(), ".codex"), "config.toml");
    writeCodexBlock(configFile, bin, connect);
  }
  lines.push(`${connect ? "Registered" : "Removed"} Parley's MCP server ${connect ? "with" : "from"} ${host === "claude" ? "Claude Code" : "Codex"}.`);

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
  // Claude asks before each MCP tool call unless the tool is allowed.
  if (host === "claude") {
    const permissions = { ...((existing.permissions as { allow?: string[] } | undefined) ?? {}) };
    const allow = (permissions.allow ?? []).filter((rule) => rule !== CLAUDE_TOOL);
    permissions.allow = connect ? [...allow, CLAUDE_TOOL] : allow;
    next.permissions = permissions;
  }
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
