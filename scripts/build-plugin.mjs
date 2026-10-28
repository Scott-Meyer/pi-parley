#!/usr/bin/env node
// Builds plugins/parley: Parley for Claude Code and Codex as a plugin, which
// both hosts install by copying it from git, with no npm install and no
// TypeScript loader. The MCP server, the broker and the link keeper are bundled
// into single scripts with this build's identity stamped in; the native
// file-lock addon ships beside them with prebuilds for macOS, Linux and Windows.
//
//   node scripts/build-plugin.mjs
import { cpSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { build } from "esbuild";

const repo = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const out = join(repo, "plugins", "parley");
const pkg = JSON.parse(readFileSync(join(repo, "package.json"), "utf8"));

// The broker's identity, computed the way a running broker computes it from source.
const tsx = join(repo, "node_modules", "tsx", "dist", "cli.mjs");
const identity = JSON.parse(spawnSync(process.execPath, [tsx, "-e",
  `import(${JSON.stringify(new URL("../broker/build.ts", import.meta.url).href)}).then((m) => console.log(JSON.stringify(m.getBrokerBuildIdentity(${JSON.stringify(repo)}))))`,
], { encoding: "utf8" }).stdout.trim().split("\n").pop());

rmSync(out, { recursive: true, force: true });
mkdirSync(out, { recursive: true });

await build({
  entryPoints: { parley: join(repo, "hosts", "cli.ts"), broker: join(repo, "broker", "broker.ts"), keeper: join(repo, "link", "keeper.ts") },
  outdir: out,
  outExtension: { ".js": ".mjs" },
  bundle: true,
  splitting: false,
  platform: "node",
  format: "esm",
  target: "node20",
  // Loaded at run time from node_modules beside the bundle.
  external: ["fs-native-extensions"],
  define: { PARLEY_BUNDLE: JSON.stringify(JSON.stringify(identity)) },
  // Some bundled CommonJS dependencies call require().
  banner: { js: 'import { createRequire as __parleyCreateRequire } from "node:module"; const require = __parleyCreateRequire(import.meta.url);' },
  legalComments: "none",
  logLevel: "warning",
});

// The native file-lock addon and the packages it loads with.
const prebuilds = ["darwin-arm64", "darwin-x64", "linux-arm64", "linux-x64", "win32-arm64", "win32-x64"];
for (const name of ["fs-native-extensions", "require-addon", "which-runtime", "bare-addon-resolve", "bare-module-resolve", "bare-semver"]) {
  const from = join(repo, "node_modules", name);
  if (!existsSync(from)) throw new Error(`missing dependency ${name}; run npm ci`);
  cpSync(from, join(out, "node_modules", name), {
    recursive: true,
    filter: (path) => {
      const rel = path.slice(from.length + 1);
      if (/^(src|include|test|CMakeLists\.txt|binding\.c)(\/|$)/.test(rel)) return false;
      const prebuild = /^prebuilds\/([^/]+)/.exec(rel);
      return !prebuild || prebuilds.includes(prebuild[1]);
    },
  });
}

const description = "Conversations between agent sessions (Pi, Oh My Pi, Claude Code, Codex) on your computers: named peers, asks and replies, delivery receipts.";
const about = { author: { name: "Scott Meyer", url: "https://github.com/Scott-Meyer" }, homepage: "https://github.com/Scott-Meyer/pi-parley", repository: "https://github.com/Scott-Meyer/pi-parley", license: "MIT" };
const json = (path, value) => { mkdirSync(dirname(join(out, path)), { recursive: true }); writeFileSync(join(out, path), `${JSON.stringify(value, null, 2)}\n`); };
const hookEvents = ["SessionStart", "UserPromptSubmit", "PostToolUse", "Stop"];

json("package.json", { name: "parley-plugin", private: true, type: "module", version: pkg.version });
// Claude Code: its manifest, and the plugin's .mcp.json, which Claude loads by default.
// Every server entry says `mcp auto`, so whichever manifest a host reads, it starts as itself.
json(".claude-plugin/plugin.json", { name: "parley", version: pkg.version, description, ...about });
json(".mcp.json", { mcpServers: { parley: { command: "node", args: ["${CLAUDE_PLUGIN_ROOT}/parley.mjs", "mcp", "auto"] } } });
// Codex: its compatibility manifest, the format in which it loads a plugin's hooks. In this
// format it expands no path variables for MCP servers and starts them with an empty
// environment in the session's folder, so the server command finds its own install in
// Codex's plugin cache (the newest parley-plugin there). The session's names and scope reach
// the server through its first hook, which Codex runs with the session's environment.
const locate = [
  'const fs=require("fs"),path=require("path"),os=require("os"),url=require("url");',
  'const base=path.join(process.env.CODEX_HOME||path.join(os.homedir(),".codex"),"plugins","cache");let best;',
  'for(const market of fs.readdirSync(base)){const dir=path.join(base,market,"parley");if(!fs.existsSync(dir))continue;',
  'for(const version of fs.readdirSync(dir)){const file=path.join(dir,version,"parley.mjs");',
  'try{if(JSON.parse(fs.readFileSync(path.join(dir,version,"package.json"),"utf8")).name!=="parley-plugin")continue;const at=fs.statSync(file).mtimeMs;if(!best||at>best.at)best={file,at};}catch{}}}',
  'if(!best){console.error("Parley: no installed parley plugin under "+base);process.exit(1);}',
  'process.argv=[process.argv[0],best.file,...process.argv.slice(1)];import(url.pathToFileURL(best.file).href);',
].join("");
json(".codex-plugin/plugin.json", {
  name: "parley", version: pkg.version, description, ...about,
  keywords: ["parley", "pi", "multi-agent", "collaboration"],
  hooks: "./hooks/hooks.json",
  mcpServers: "./codex-mcp.json",
  interface: { displayName: "Parley", shortDescription: "Talk to your other agent sessions", developerName: "Scott Meyer", category: "Productivity", capabilities: [] },
});
json("codex-mcp.json", { mcpServers: { parley: { command: "node", args: ["-e", locate, "mcp", "codex"], default_tools_approval_mode: "approve" } } });
// Both hosts read hooks/hooks.json; Codex sets CLAUDE_PLUGIN_ROOT for Claude-style hooks too.
json("hooks/hooks.json", { hooks: Object.fromEntries(hookEvents.map((event) => [event, [{ hooks: [{ type: "command", command: `node "\${CLAUDE_PLUGIN_ROOT}/parley.mjs" hook ${event}`, timeout: 10 }] }]])) });
writeFileSync(join(out, "README.md"), `# Parley plugin\n\nBuilt from pi-parley ${pkg.version} by \`npm run build:plugin\`; don't edit by hand. See the [pi-parley README](../../README.md#claude-code-and-codex).\n`);

// The repository as a marketplace for both hosts.
const repoJson = (path, value) => { mkdirSync(dirname(join(repo, path)), { recursive: true }); writeFileSync(join(repo, path), `${JSON.stringify(value, null, 2)}\n`); };
repoJson(".claude-plugin/marketplace.json", {
  name: "pi-parley", owner: { name: "Scott Meyer", url: "https://github.com/Scott-Meyer" },
  plugins: [{ name: "parley", source: "./plugins/parley", description, version: pkg.version }],
});
repoJson(".agents/plugins/marketplace.json", {
  name: "pi-parley",
  plugins: [{ name: "parley", source: { source: "local", path: "./plugins/parley" }, policy: { installation: "AVAILABLE", authentication: "ON_INSTALL" }, category: "Productivity" }],
});

console.log(`built plugins/parley for pi-parley ${pkg.version} (broker ${identity.sourceId.slice(0, 12)})`);
