// parley: Parley for agents that aren't Pi.
//
//   parley connect claude|codex      set up Parley in Claude Code or Codex
//   parley disconnect claude|codex   remove it again
//   parley mcp claude|codex          the MCP server those hosts start
//   parley hook <event>              what their hooks run (reads the hook's JSON on stdin)
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const version = (JSON.parse(readFileSync(join(root, "package.json"), "utf8")) as { version: string }).version;
const [command, target, ...rest] = process.argv.slice(2);

async function main() {
  switch (command) {
    case "mcp": {
      const { serveHost } = await import("./serve.ts");
      if (target === "claude") { const { claudeHost } = await import("./claude.ts"); return serveHost(claudeHost(process.env, version)); }
      if (target === "codex") { const { codexHost } = await import("./codex.ts"); return serveHost(codexHost(process.env, version)); }
      break;
    }
    case "hook": {
      const { sendHook } = await import("./control.ts");
      const input = await new Promise<string>((resolve) => { let text = ""; process.stdin.on("data", (d) => { text += d; }); process.stdin.on("end", () => resolve(text)); });
      let payload: Record<string, unknown> = {};
      try { payload = JSON.parse(input || "{}"); } catch { /* not ours to judge */ }
      const event = target ?? String(payload.hook_event_name ?? "");
      const response = await sendHook({ event, payload });
      if (response?.output) process.stdout.write(`${JSON.stringify(response.output)}\n`);
      return;
    }
    case "connect":
    case "disconnect": {
      const { connectHost } = await import("./connect.ts");
      if (target === "claude" || target === "codex") {
        const lines = await connectHost(target, command === "connect", { root, args: rest });
        console.log(lines.join("\n"));
        return;
      }
      break;
    }
  }
  console.error("usage: parley connect|disconnect claude|codex · parley mcp claude|codex · parley hook <event>");
  process.exitCode = 2;
}

main().catch((error) => {
  console.error(`parley: ${error instanceof Error ? error.message : String(error)}`);
  // A failing hook must never block its host.
  process.exitCode = command === "hook" ? 0 : 1;
});
