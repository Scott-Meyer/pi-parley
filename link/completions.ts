// Suggestions for `/parley link …` as it is typed: the verbs, then computers.
// Computers come from this computer's remembered links and the Host entries
// in ~/.ssh/config (and the files it includes), never from connecting.
import { readdirSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { basename, dirname, isAbsolute, join } from "node:path";
import { getLinkPaths } from "./paths.ts";

export interface Suggestion { readonly value: string; readonly label: string; readonly description?: string }

const VERBS: readonly Suggestion[] = [
  { value: "on", label: "on", description: "Link computers this one has ssh connections to" },
  { value: "off", label: "off", description: "Stop finding computers; remembered ones stay linked" },
  { value: "add", label: "add", description: "Link a computer, as you'd type it after ssh" },
  { value: "recheck", label: "recheck", description: "Check one computer, or all, again now" },
  { value: "remove", label: "remove", description: "Stop linking a computer and forget it" },
  { value: "status", label: "status", description: "What's linked, and why anything isn't" },
];

/** Suggestions for the text after `link `. Values are that whole text, completed. */
export function linkSuggestions(text: string, env: NodeJS.ProcessEnv = process.env): Suggestion[] {
  const match = /^(\S*)(\s+(.*))?$/.exec(text);
  const verb = match?.[1] ?? "";
  if (match?.[2] === undefined) {
    return VERBS.filter((item) => item.value.startsWith(verb.toLowerCase()))
      .map((item) => ({ ...item, value: ["add", "recheck", "remove"].includes(item.value) ? `${item.value} ` : item.value }));
  }
  const typed = match[3] ?? "";
  // Git hosts are where code lives, not computers anyone runs Pi on.
  const hosts = verb === "add" ? [...new Set([...sshConfigHosts(env), ...rememberedHosts(env)])].filter((host) => !/(^|\.)(github|gitlab|bitbucket)\./i.test(host))
    : verb === "recheck" || verb === "remove" ? rememberedHosts(env) : [];
  return hosts.filter((host) => host.startsWith(typed)).slice(0, 50)
    .map((host) => ({ value: `${verb} ${host}`, label: host }));
}

function rememberedHosts(env: NodeJS.ProcessEnv): string[] {
  try {
    const parsed = JSON.parse(readFileSync(getLinkPaths(env).config, "utf8"));
    return Array.isArray(parsed?.hosts) ? parsed.hosts.map((h: { destination?: unknown }) => h?.destination).filter((d: unknown): d is string => typeof d === "string") : [];
  } catch {
    return [];
  }
}

/** Concrete Host names (no wildcards or negations) from ssh's user config. */
export function sshConfigHosts(env: NodeJS.ProcessEnv = process.env, home = env.HOME || homedir()): string[] {
  const hosts: string[] = [];
  const seen = new Set<string>();
  const visit = (file: string, depth: number) => {
    if (depth > 3 || seen.has(file)) return;
    seen.add(file);
    let text: string;
    try { text = readFileSync(file, "utf8"); } catch { return; }
    for (const raw of text.split("\n")) {
      const line = raw.replace(/#.*/, "").trim();
      const [keyword, ...values] = line.split(/\s+/);
      if (!keyword) continue;
      if (/^host$/i.test(keyword)) {
        for (const value of values) if (!/[*?!]/.test(value) && !hosts.includes(value)) hosts.push(value);
      } else if (/^include$/i.test(keyword)) {
        for (const value of values) {
          const path = isAbsolute(value) ? value : value.startsWith("~/") ? join(home, value.slice(2)) : join(home, ".ssh", value);
          if (!/[*?]/.test(path)) { visit(path, depth + 1); continue; }
          // Wildcards in the file name, as in config.d/*, are common; deeper ones aren't.
          const dir = dirname(path);
          if (/[*?]/.test(dir)) continue;
          const pattern = new RegExp(`^${basename(path).replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*").replace(/\?/g, ".")}$`);
          let names: string[] = [];
          try { names = readdirSync(dir).filter((name) => pattern.test(name)).sort(); } catch { /* no such directory */ }
          for (const name of names) visit(join(dir, name), depth + 1);
        }
      }
    }
  };
  visit(join(home, ".ssh", "config"), 0);
  return hosts;
}
