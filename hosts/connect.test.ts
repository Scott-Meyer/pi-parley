// `parley connect` adds Parley to a host's config and `disconnect` takes it
// out again, leaving everything else (the person's hooks, FlightDeck's) alone.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { connectHost } from "./connect.ts";

const theirs = {
  model: "opus",
  permissions: { allow: ["Bash(git status)"] },
  hooks: {
    Stop: [{ hooks: [{ type: "command", command: "/Users/me/.flightdeck/bin/flightdeck hook", _flightdeck: "v1" }] }],
    SessionEnd: [{ hooks: [{ type: "command", command: "my-own-hook" }] }],
  },
};

test("connecting Claude Code adds Parley's hooks and tool permission beside everyone else's, and disconnecting restores the rest exactly", async () => {
  const dir = mkdtempSync(path.join(tmpdir(), "pl-connect-"));
  const settings = path.join(dir, "settings.json");
  writeFileSync(settings, JSON.stringify(theirs, null, 2));
  try {
    const said = await connectHost("claude", true, { root: "/opt/parley", hooksFile: settings, skipMcp: true });
    await connectHost("claude", true, { root: "/opt/parley", hooksFile: settings, skipMcp: true }); // again: no duplicates
    const connected = JSON.parse(readFileSync(settings, "utf8"));
    assert.equal(connected.model, "opus");
    for (const event of ["SessionStart", "UserPromptSubmit", "PostToolUse", "Stop"]) {
      const ours = connected.hooks[event].flatMap((entry: any) => entry.hooks).filter((hook: any) => hook._parley);
      assert.equal(ours.length, 1, `${event}: one Parley hook`);
      assert.equal(ours[0].command, `node /opt/parley/bin/parley.mjs hook ${event}`);
    }
    assert.equal(connected.hooks.Stop[0].hooks[0]._flightdeck, "v1", "FlightDeck's hook stays, first");
    assert.deepEqual(connected.permissions.allow, ["Bash(git status)", "mcp__parley__parley"]);
    assert.ok(said.some((line) => /backup/.test(line)));
    assert.deepEqual(JSON.parse(readFileSync(`${settings}.before-parley`, "utf8")).model, "opus");

    await connectHost("claude", false, { root: "/opt/parley", hooksFile: settings, skipMcp: true });
    assert.deepEqual(JSON.parse(readFileSync(settings, "utf8")), theirs);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("connecting Codex writes one marked config block that passes the session's names through, and disconnecting removes only it", async () => {
  const dir = mkdtempSync(path.join(tmpdir(), "pl-connect-"));
  const config = path.join(dir, "config.toml");
  const hooks = path.join(dir, "hooks.json");
  const before = 'model = "gpt"\n\n[mcp_servers.docs]\nurl = "https://example.com/mcp"\n';
  writeFileSync(config, before);
  writeFileSync(hooks, JSON.stringify({ hooks: theirs.hooks }, null, 2));
  try {
    const said = await connectHost("codex", true, { root: "/opt/parley", hooksFile: hooks, codexConfigFile: config });
    await connectHost("codex", true, { root: "/opt/parley", hooksFile: hooks, codexConfigFile: config });
    const text = readFileSync(config, "utf8");
    assert.ok(text.startsWith(before.trimEnd()), "the person's config is untouched");
    assert.equal(text.match(/\[mcp_servers\.parley\]/g)?.length, 1, "one block, however often connected");
    assert.match(text, /args = \["\/opt\/parley\/bin\/parley\.mjs", "mcp", "codex"\]/);
    assert.match(text, /env_vars = \[[^\]]*"PI_PARLEY_SCOPE_ID"[^\]]*"FLIGHTDECK_TAB_NAME"/);
    assert.match(text, /default_tools_approval_mode = "approve"/);
    assert.ok(said.some((line) => /trust/.test(line)), "Codex users are told to trust the hooks once");

    await connectHost("codex", false, { root: "/opt/parley", hooksFile: hooks, codexConfigFile: config });
    assert.equal(readFileSync(config, "utf8"), before);
    assert.deepEqual(JSON.parse(readFileSync(hooks, "utf8")), { hooks: theirs.hooks });

    // A parley server someone else configured isn't silently overwritten.
    writeFileSync(config, `${before}\n[mcp_servers.parley]\ncommand = "other"\n`);
    await assert.rejects(connectHost("codex", true, { root: "/opt/parley", hooksFile: hooks, codexConfigFile: config }), /already has an \[mcp_servers\.parley\]/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
