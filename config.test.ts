import test from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { getConfigPath, getParleyScopeId, loadConfig } from "./config.ts";

async function withAgentDir<T>(agentDir: string, fn: () => T | Promise<T>): Promise<T> {
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  try {
    return await fn();
  } finally {
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
  }
}

test("outside FlightDeck, explicit routing scope behavior is unchanged", () => {
  assert.equal(getParleyScopeId({}), undefined);
  assert.equal(getParleyScopeId({ PI_PARLEY_SCOPE_ID: "  " }), undefined);
  assert.equal(getParleyScopeId({ PI_PARLEY_SCOPE_ID: "  team-alpha  " }), "team-alpha");
});

test("FlightDeck tabs share one scope across workspaces, machines, and explicit project scopes", () => {
  const sessions: NodeJS.ProcessEnv[] = [
    {
      FLIGHTDECK_TAB_ID: "t1",
      FLIGHTDECK_WORKSPACE_ID: "project-alpha",
      FLIGHTDECK_STATUS_SOCK: "/home/alice/.flightdeck/status.sock",
      PI_PARLEY_SCOPE_ID: "project-alpha",
    },
    {
      FLIGHTDECK_TAB_ID: "t8",
      FLIGHTDECK_WORKSPACE_ID: "project-beta",
      FLIGHTDECK_STATUS_SOCK: "/run/flightdeck-host/status.sock",
      PI_PARLEY_SCOPE_ID: "project-beta",
    },
    {
      FLIGHTDECK_TAB_ID: "t1",
      FLIGHTDECK_WORKSPACE_ID: "project-gamma",
      FLIGHTDECK_STATUS_PORT: "51234",
      FLIGHTDECK_STATUS_TOKEN: "host-status-token",
    },
  ];
  for (const session of sessions) {
    assert.equal(getParleyScopeId(session), "flightdeck");
  }
});

test("system-terminal and legacy FlightDeck tabs join without workspace metadata or a live GUI", () => {
  assert.equal(getParleyScopeId({
    FLIGHTDECK_TAB_ID: "t0",
    FLIGHTDECK_STATUS_SOCK: "/retained-terminal/gui-no-longer-running.sock",
  }), "flightdeck");
  assert.equal(getParleyScopeId({
    FLIGHTDECK_TAB_ID: "  ",
    FLIGHTDECK_PANE_ID: "t7",
    FLIGHTDECK_STATUS_SOCK: "/legacy-flightdeck/status.sock",
  }), "flightdeck");
});

test("partial FlightDeck context and leftover workspace metadata do not enroll unrelated sessions", () => {
  const partialContexts: NodeJS.ProcessEnv[] = [
    { FLIGHTDECK_TAB_ID: "t1", FLIGHTDECK_WORKSPACE_ID: "project-alpha" },
    { FLIGHTDECK_PANE_ID: "t1" },
    { FLIGHTDECK_WORKSPACE_ID: "project-alpha", FLIGHTDECK_STATUS_SOCK: "/tmp/status.sock" },
    { FLIGHTDECK_TAB_ID: "  ", FLIGHTDECK_STATUS_SOCK: "/tmp/status.sock" },
    { FLIGHTDECK_TAB_ID: "t1", FLIGHTDECK_STATUS_SOCK: "  " },
    { FLIGHTDECK_TAB_ID: "t1", FLIGHTDECK_STATUS_PORT: "51234" },
    { FLIGHTDECK_TAB_ID: "t1", FLIGHTDECK_STATUS_PORT: "51234", FLIGHTDECK_STATUS_TOKEN: "  " },
    { FLIGHTDECK_TAB_ID: "t1", FLIGHTDECK_STATUS_PORT: "0", FLIGHTDECK_STATUS_TOKEN: "token" },
    { FLIGHTDECK_TAB_ID: "t1", FLIGHTDECK_STATUS_PORT: "65536", FLIGHTDECK_STATUS_TOKEN: "token" },
    { FLIGHTDECK_TAB_ID: "t1", FLIGHTDECK_STATUS_PORT: "invalid", FLIGHTDECK_STATUS_TOKEN: "token" },
  ];
  for (const context of partialContexts) {
    assert.equal(getParleyScopeId(context), undefined);
    assert.equal(getParleyScopeId({ ...context, PI_PARLEY_SCOPE_ID: "  team-alpha  " }), "team-alpha");
  }
});

test("getConfigPath uses the centralized parley runtime directory", () => {
  assert.equal(getConfigPath("/tmp/pi-agent/parley"), join("/tmp/pi-agent", "parley", "config.json"));
});

test("loadConfig reads config below PI_CODING_AGENT_DIR", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-config-"));

  try {
    const parleyDir = join(root, "parley");
    mkdirSync(parleyDir, { recursive: true });
    writeFileSync(join(parleyDir, "config.json"), JSON.stringify({ status: "platform-test" }));

    await withAgentDir(root, () => {
      assert.equal(loadConfig().status, "platform-test");
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig defaults inboundTrigger to current auto-trigger behavior", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-config-"));
  try {
    await withAgentDir(root, () => {
      assert.equal(loadConfig().inboundTrigger, "always");
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig accepts inboundTrigger replies policy", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-config-"));
  try {
    mkdirSync(join(root, "parley"), { recursive: true });
    writeFileSync(join(root, "parley", "config.json"), JSON.stringify({ inboundTrigger: "replies" }));
    await withAgentDir(root, () => {
      assert.equal(loadConfig().inboundTrigger, "replies");
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("unknown config keys do not change supported settings", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-config-"));
  try {
    mkdirSync(join(root, "parley"), { recursive: true });
    writeFileSync(join(root, "parley", "config.json"), JSON.stringify({ unrelatedSetting: "ignored", replyHint: false }));
    await withAgentDir(root, () => {
      assert.equal(loadConfig().replyHint, false);
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig accepts a restart-stable parley id", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-config-"));
  try {
    mkdirSync(join(root, "parley"), { recursive: true });
    writeFileSync(join(root, "parley", "config.json"), JSON.stringify({ stableId: " pinned-worker " }));
    await withAgentDir(root, () => {
      assert.equal(loadConfig().stableId, "pinned-worker");
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig validates the optional default project launcher command", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-config-"));
  try {
    mkdirSync(join(root, "parley"), { recursive: true });
    writeFileSync(join(root, "parley", "config.json"), JSON.stringify({ projectLauncher: " tmux new-window -c \"{root}\" pi " }));
    await withAgentDir(root, () => {
      assert.equal(loadConfig().projectLauncher, "tmux new-window -c \"{root}\" pi");
    });

    writeFileSync(join(root, "parley", "config.json"), JSON.stringify({ projectLauncher: "" }));
    await withAgentDir(root, () => {
      assert.throws(() => loadConfig(), /"projectLauncher" must not be empty/);
    });

    writeFileSync(join(root, "parley", "config.json"), JSON.stringify({ projectLauncher: 42 }));
    await withAgentDir(root, () => {
      assert.throws(() => loadConfig(), /"projectLauncher" must be a string/);
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig rejects invalid inboundTrigger values", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-parley-config-"));
  try {
    mkdirSync(join(root, "parley"), { recursive: true });
    writeFileSync(join(root, "parley", "config.json"), JSON.stringify({ inboundTrigger: "prompt" }));

    await withAgentDir(root, () => {
      assert.throws(
        () => loadConfig(),
        /Failed to load parley config.*"inboundTrigger" must be "always", "replies", or "never"/,
      );
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
