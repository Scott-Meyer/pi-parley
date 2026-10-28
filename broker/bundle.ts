// Whether this code runs from Parley's self-contained build (the Claude Code
// and Codex plugin), and how to start Parley's own processes either way.
//
// The plugin build bundles the MCP server, the broker and the link keeper into
// single scripts side by side, with the build's identity stamped in, because a
// plugin is copied as-is: no npm install, no TypeScript loader.
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

declare const PARLEY_BUNDLE: string | undefined;

export interface BundleIdentity { readonly packageVersion: string; readonly sourceId: string }

/** The build's identity when bundled; undefined when running from source. */
export const bundledBuild: BundleIdentity | undefined = typeof PARLEY_BUNDLE === "string" ? JSON.parse(PARLEY_BUNDLE) : undefined;

/** A bundled script beside this one ("broker", "keeper"), when running bundled. */
export function bundledScript(name: string): string | undefined {
  if (!bundledBuild) return undefined;
  const path = join(dirname(fileURLToPath(import.meta.url)), `${name}.mjs`);
  return existsSync(path) ? path : undefined;
}

/** Node to start Parley's processes with: this one, unless the host is Bun. */
export function nodeCommand(execPath: string = process.execPath): string {
  const name = execPath.split(/[\\/]/).pop() ?? "";
  return /^node(js)?(\.exe)?$/i.test(name) ? execPath : "node";
}
