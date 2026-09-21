import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const repo = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const scratch = mkdtempSync(join(tmpdir(), "pl-fed-package-"));

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: "utf8",
    maxBuffer: 20 * 1024 * 1024,
    ...options,
  });
  if (result.status !== 0) {
    throw new Error([
      `${command} ${args.join(" ")} failed with ${result.status}`,
      result.stdout,
      result.stderr,
    ].filter(Boolean).join("\n"));
  }
  return result;
}

function npm(args, options = {}) {
  return process.env.npm_execpath
    ? run(process.execPath, [process.env.npm_execpath, ...args], options)
    : run(process.platform === "win32" ? "npm.cmd" : "npm", args, options);
}

try {
  const packedDir = join(scratch, "packed");
  mkdirSync(packedDir);
  const packedResult = npm(["pack", "--json", "--pack-destination", packedDir], { cwd: repo });
  const packedReport = JSON.parse(packedResult.stdout);
  const packed = Array.isArray(packedReport) ? packedReport[0] : Object.values(packedReport)[0];
  assert.ok(packed?.filename, "npm pack did not report a tarball");
  const packedFiles = new Set(packed.files.map((file) => file.path));
  for (const required of [
    "dist/federation.js",
    "dist/federation.d.ts",
    "dist/broker/attachment.js",
    "index.ts",
    "extension-api.ts",
    "broker/attachment.ts",
  ]) {
    assert.equal(packedFiles.has(required), true, `packed package omitted ${required}`);
  }
  assert.equal(packedFiles.has("federation.ts"), false, "plain-Node facade must not resolve to its raw build source");

  const consumer = join(scratch, "consumer");
  mkdirSync(consumer);
  writeFileSync(join(consumer, "package.json"), JSON.stringify({ private: true, type: "module" }));
  npm(["install", "--silent", "--ignore-scripts", "--omit=dev", join(packedDir, packed.filename)], { cwd: consumer });
  const installed = join(consumer, "node_modules", "pi-parley");
  assert.equal(existsSync(join(installed, "dist", "federation.js")), true);

  writeFileSync(join(consumer, "consumer.ts"), `
    import type {
      ScopeBinding,
      BrokerInspection,
      BrokerAttachment,
      BrokerAttachmentCompletion,
    } from "pi-parley/federation";
    const binding: ScopeBinding = {
      localScopeId: "team",
      localScopeAlias: "local",
      remoteScopeAlias: "remote",
    };
    const inspect = (broker: BrokerInspection) => [broker.origin.id, broker.scopes.length] as const;
    const latch = async (attachment: BrokerAttachment): Promise<BrokerAttachmentCompletion> => {
      const same = attachment.close();
      return await same;
    };
    void binding; void inspect; void latch;
  `);
  run(process.execPath, [
    join(repo, "node_modules", "typescript", "bin", "tsc"),
    "--noEmit", "--module", "NodeNext", "--moduleResolution", "NodeNext",
    "--target", "ES2022", "--strict", "--skipLibCheck", "--types", "node",
    "--typeRoots", join(repo, "node_modules", "@types"), "consumer.ts",
  ], { cwd: consumer });

  const runtimeCheck = `
    import assert from "node:assert/strict";
    import { Duplex } from "node:stream";
    import {
      BrokerAttachmentError,
      BrokerInspectionError,
      createLocalBrokerAccess,
      inspectBroker,
      attachBrokers,
    } from "pi-parley/federation";
    const evaluatedAt = process.execArgv.indexOf("-e");
    const runtimeOptions = evaluatedAt >= 0 ? process.execArgv.slice(0, evaluatedAt) : process.execArgv;
    assert.equal(runtimeOptions.some(arg => arg === "--import" || arg.startsWith("--import=")
      || arg === "--loader" || arg.startsWith("--loader=") || arg.includes("tsx")), false);
    assert.equal(typeof inspectBroker, "function");
    assert.equal(typeof attachBrokers, "function");
    assert.equal(new BrokerInspectionError("E_TIMEOUT").code, "E_TIMEOUT");
    assert.equal(new BrokerAttachmentError("E_CLOSED").code, "E_CLOSED");
    const local = createLocalBrokerAccess({ agentDir: process.cwd() });
    assert.equal(typeof local.readAgentFile, "function");
    assert.equal(typeof local.openLocal, "function");

    const encode = value => {
      const payload = Buffer.from(JSON.stringify(value));
      const bytes = Buffer.allocUnsafe(4 + payload.length);
      bytes.writeUInt32BE(payload.length, 0); payload.copy(bytes, 4);
      return bytes;
    };
    const control = new Duplex({
      read() {},
      write(bytes, _encoding, callback) {
        const length = bytes.readUInt32BE(0);
        const request = JSON.parse(bytes.subarray(4, 4 + length));
        callback();
        this.push(encode({ type: "broker_list_scopes_result", requestId: request.requestId,
          ok: true, localOrigin: { id: "install:550e8400-e29b-41d4-a716-446655440099" },
          scopes: [{ scopeId: "packed", liveSessions: 1 }] }));
        this.push(null);
      },
    });
    const inspected = await inspectBroker({
      platform: process.platform,
      agentDir: process.cwd(),
      async readAgentFile() { throw new Error("POSIX default inspection should not read publication"); },
      async openLocal() { return control; },
    });
    assert.equal(inspected.origin.id, "install:550e8400-e29b-41d4-a716-446655440099");
    assert.equal(inspected.scopes[0].scopeId, "packed");
    console.log("plain Node exercised pi-parley/federation");
  `;
  const checked = run(process.execPath, ["--input-type=module", "-e", runtimeCheck], {
    cwd: consumer,
    env: { ...process.env, NODE_OPTIONS: "" },
  });
  assert.match(checked.stdout, /plain Node exercised pi-parley\/federation/);
  console.log("✓ packed federation facade imports and inspects under plain Node without a loader");
} finally {
  rmSync(scratch, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
}
