// What `/parley` (and `/intercom`) offer as you type.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { createExtensionHarness } from "../test/extension-harness.ts";

test("/parley lists everything it does, then continues through link verbs and computers; /intercom is the same command", async () => {
  const home = mkdtempSync(path.join(tmpdir(), "pl-complete-"));
  mkdirSync(path.join(home, ".ssh", "config.d"), { recursive: true });
  writeFileSync(path.join(home, ".ssh", "config"), "Host build-vm penguin\n  User me\nHost *.internal !bastion\nInclude config.d/*\nHost github.com\n");
  writeFileSync(path.join(home, ".ssh", "config.d", "lab"), "Host lab-box\n");
  const saved = { HOME: process.env.HOME, PI_CODING_AGENT_DIR: process.env.PI_CODING_AGENT_DIR };
  process.env.HOME = home;
  process.env.PI_CODING_AGENT_DIR = path.join(home, ".pi", "agent");
  try {
    const { default: extension } = await import("../index.ts");
    const harness = createExtensionHarness("typist");
    extension(harness.pi as never);
    const values = async (name: string, prefix: string) =>
      ((await harness.commandDefinitions.get(name)!.getArgumentCompletions!(prefix)) as { value: string }[] | null)?.map((item) => item.value) ?? null;

    assert.deepEqual(await values("parley", ""), ["sessions", "link ", "id", "alias "]);
    assert.deepEqual(await values("parley", "li"), ["link "]);
    assert.deepEqual(await values("parley", "link "), ["link on", "link off", "link add ", "link recheck ", "link remove ", "link status"]);
    assert.deepEqual(await values("parley", "link add "), ["link add build-vm", "link add penguin", "link add lab-box"],
      "concrete hosts from ssh config and its includes; no patterns, no git hosts");
    assert.deepEqual(await values("parley", "link add pe"), ["link add penguin"]);
    assert.deepEqual(await values("parley", "link remove "), null, "nothing remembered yet");
    assert.deepEqual(await values("intercom", "link a"), ["link add "]);
    assert.equal(harness.commands.get("intercom"), harness.commands.get("parley"));
  } finally {
    process.env.HOME = saved.HOME;
    if (saved.PI_CODING_AGENT_DIR === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = saved.PI_CODING_AGENT_DIR;
    rmSync(home, { recursive: true, force: true });
  }
});
