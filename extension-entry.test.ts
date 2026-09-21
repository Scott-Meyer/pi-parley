import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import extension, { registerParleyExtension } from "./extension.ts";
import {
  PARLEY_EXTENSION_REGISTRY_READY_EVENT,
  PARLEY_OUTBOX_REQUEST_EVENT,
  PARLEY_OUTBOX_RESULT_EVENT,
  type ParleyOutboxResult,
} from "./extension-api.ts";
import { createExtensionHarness } from "./test/extension-harness.ts";

for (const key of Object.keys(process.env)) {
  if ((key.startsWith("PI_PARLEY_") && !key.startsWith("PI_PARLEY_TEST_"))
    || key.startsWith("FLIGHTDECK_") || key === "PI_CODING_AGENT_DIR") {
    delete process.env[key];
  }
}

test("the packaged actor entry is explicit, runtime-idempotent, and re-arms after shutdown", async () => {
  assert.strictEqual(extension, registerParleyExtension);
  const agentDir = mkdtempSync(path.join(tmpdir(), "pl-extension-entry-"));
  process.env.PI_CODING_AGENT_DIR = agentDir;
  const required = createExtensionHarness("flightdeck-required");
  const failed = createExtensionHarness("failed-wrapper");
  const ambient = createExtensionHarness("ambient-package");
  // Pi creates one ExtensionAPI facade per extension path, all forwarding to
  // one synchronous runtime event bus.
  failed.pi.events = ambient.pi.events = {
    on: required.pi.events.on,
    emit: required.pi.events.emit,
  };
  assert.notStrictEqual(ambient.pi.events, required.pi.events);
  let registryReadyCount = 0;
  const outboxResults: ParleyOutboxResult[] = [];
  required.pi.events.on(PARLEY_EXTENSION_REGISTRY_READY_EVENT, () => { registryReadyCount += 1; });
  required.pi.events.on(PARLEY_OUTBOX_RESULT_EVENT, (payload) => outboxResults.push(payload as ParleyOutboxResult));
  const emitOutboxRequest = (requestId: string) => required.pi.events.emit(PARLEY_OUTBOX_REQUEST_EVENT, {
    version: 1,
    requestId,
    extensionId: "registration-rollback-test",
    extensionName: "Registration rollback test",
    to: "nobody",
    message: "Must have exactly one owner.",
  });
  const settleImmediateWork = () => new Promise<void>((resolve) => setImmediate(resolve));

  try {
    failed.pi.registerMessageRenderer = () => {
      throw new Error("injected registration failure");
    };
    assert.throws(() => registerParleyExtension(failed.pi as never), /injected registration failure/);
    assert.equal(registryReadyCount, 0, "a discarded registration is never advertised as ready");
    emitOutboxRequest("after-failed-registration");
    await settleImmediateWork();
    assert.deepEqual(outboxResults, [], "a discarded registration leaves no shared outbox handler");

    registerParleyExtension(required.pi as never);
    assert.equal(registryReadyCount, 1);
    emitOutboxRequest("after-recovered-registration");
    await settleImmediateWork();
    assert.equal(outboxResults.length, 1, "only the recovered owner answers an outbox request");
    assert.equal(outboxResults[0]?.code, "session_unavailable");
    const requiredToolCount = required.tools.length;
    const requiredCommandCount = required.commands.size;
    const requiredShortcutCount = required.shortcuts.size;
    assert.ok(required.tools.some((tool) => tool.name === "parley"));
    assert.ok(required.commands.has("parley"));

    registerParleyExtension(ambient.pi as never);
    assert.equal(ambient.tools.length, 0, "an ambient physical copy cannot register a second client/tool set");
    assert.equal(ambient.commands.size, 0);
    assert.equal(ambient.shortcuts.size, 0);
    assert.equal(required.tools.length, requiredToolCount);
    assert.equal(required.commands.size, requiredCommandCount);
    assert.equal(required.shortcuts.size, requiredShortcutCount);

    await required.emitLifecycle("session_shutdown", { reason: "reload" });
    registerParleyExtension(ambient.pi as never);
    assert.ok(ambient.tools.some((tool) => tool.name === "parley"), "a fresh runtime can bind after joined shutdown");
    assert.ok(ambient.commands.has("parley"));

    registerParleyExtension(required.pi as never);
    assert.equal(required.tools.length, requiredToolCount, "the replacement owner excludes the stale physical copy");
    assert.equal(required.commands.size, requiredCommandCount);
    assert.equal(required.shortcuts.size, requiredShortcutCount);
    await ambient.emitLifecycle("session_shutdown", { reason: "quit" });
  } finally {
    delete process.env.PI_CODING_AGENT_DIR;
    rmSync(agentDir, { recursive: true, force: true });
  }
});
