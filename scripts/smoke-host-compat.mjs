import assert from "node:assert/strict";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";

const repo = resolve(dirname(fileURLToPath(import.meta.url)), "..");
// Keep actual Unix socket paths within macOS's short sockaddr_un limit.
const scratch = mkdtempSync(join(tmpdir(), "pl-host-"));
const packed = join(scratch, "packed");
const agentDirs = [];

const hosts = [
  {
    label: "upstream",
    spec: "@mariozechner/pi-coding-agent@0.73.1",
    packagePath: "@mariozechner/pi-coding-agent",
    forbiddenPackagePaths: ["@earendil-works/pi-coding-agent", "@earendil-works/pi-tui"],
  },
  {
    label: "fork-minimum",
    spec: "@earendil-works/pi-coding-agent@0.80.3",
    packagePath: "@earendil-works/pi-coding-agent",
    forbiddenPackagePaths: ["@mariozechner/pi-coding-agent", "@mariozechner/pi-tui"],
    // Pin matching sibling packages so caret ranges do not mix SDK families.
    overrides: {
      "@earendil-works/pi-agent-core": "0.80.3",
      "@earendil-works/pi-ai": "0.80.3",
      "@earendil-works/pi-tui": "0.80.3",
    },
  },
  {
    label: "fork-current",
    spec: "@earendil-works/pi-coding-agent@0.85.1",
    packagePath: "@earendil-works/pi-coding-agent",
    forbiddenPackagePaths: ["@mariozechner/pi-coding-agent", "@mariozechner/pi-tui"],
  },
];

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

async function startRpc(command, args, options = {}) {
  const child = spawn(command, args, { ...options, stdio: ["pipe", "pipe", "pipe"] });
  const exited = new Promise((resolveExit) => child.once("exit", (code, signal) => resolveExit({ code, signal })));
  const records = [];
  const pending = new Map();
  let stdoutBuffer = "";
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => { stderr = (stderr + chunk).slice(-20_000); });
  child.stdout.setEncoding("utf8");
  child.stdout.on("data", (chunk) => {
    stdoutBuffer += chunk;
    for (;;) {
      const newline = stdoutBuffer.indexOf("\n");
      if (newline < 0) break;
      const line = stdoutBuffer.slice(0, newline).trim();
      stdoutBuffer = stdoutBuffer.slice(newline + 1);
      if (!line) continue;
      let record;
      try { record = JSON.parse(line); }
      catch { continue; }
      records.push(record);
      if (record.type === "response" && typeof record.id === "string") {
        const waiter = pending.get(record.id);
        if (waiter) {
          pending.delete(record.id);
          clearTimeout(waiter.timeout);
          waiter.resolve(record);
        }
      }
    }
  });
  child.on("exit", (code, signal) => {
    const error = new Error(`RPC host exited before responding (${code ?? signal}): ${stderr}`);
    for (const waiter of pending.values()) {
      clearTimeout(waiter.timeout);
      waiter.reject(error);
    }
    pending.clear();
  });
  let closePromise;
  async function joinExit(graceMs) {
    let timeout;
    const result = await Promise.race([
      exited,
      new Promise((resolveTimeout) => { timeout = setTimeout(() => resolveTimeout(undefined), graceMs); }),
    ]);
    clearTimeout(timeout);
    return result;
  }
  async function terminate() {
    if (child.exitCode === null && child.signalCode === null) child.stdin.end();
    let result = await joinExit(2_000);
    if (!result && child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    result ??= await exited;
    return result;
  }
  return {
    records,
    async send(message, timeoutMs = 15_000) {
      if (child.exitCode !== null || child.signalCode !== null) {
        throw new Error(`RPC host is not running: ${stderr}`);
      }
      const response = new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          pending.delete(message.id);
          reject(new Error(`RPC command ${message.id} timed out: ${stderr}`));
        }, timeoutMs);
        pending.set(message.id, { resolve, reject, timeout });
      });
      child.stdin.write(`${JSON.stringify(message)}\n`);
      return response;
    },
    async close() {
      closePromise ??= terminate();
      const { code, signal } = await closePromise;
      if (code !== 0) throw new Error(`RPC host exited with ${code ?? signal}: ${stderr}`);
    },
    async terminate() {
      closePromise ??= terminate();
      await closePromise;
    },
  };
}

try {
  mkdirSync(packed, { recursive: true });
  const packResult = run("npm", ["pack", "--silent", "--pack-destination", packed], { cwd: repo });
  const tarballName = packResult.stdout.trim().split("\n").at(-1);
  if (!tarballName) throw new Error("npm pack did not report a tarball");
  const tarball = join(packed, tarballName);

  const hostlessProject = join(scratch, "hostless");
  mkdirSync(hostlessProject, { recursive: true });
  writeFileSync(join(hostlessProject, "package.json"), JSON.stringify({ private: true }));
  run("npm", ["install", "--silent", tarball], { cwd: hostlessProject });
  assert.equal(existsSync(join(hostlessProject, "node_modules", "tsx")), true);
  assert.equal(existsSync(join(hostlessProject, "node_modules", "fs-native-extensions")), true);
  const hostlessExtension = join(hostlessProject, "node_modules", "pi-parley");
  for (const runtimeFile of ["broker/process-lock.ts", "broker/runtime-claim.ts", "broker/build.ts", "broker/broker.ts"]) {
    assert.equal(existsSync(join(hostlessExtension, runtimeFile)), true, `packed package omitted ${runtimeFile}`);
  }
  for (const hostModule of [
    "@mariozechner/pi-coding-agent",
    "@mariozechner/pi-tui",
    "@earendil-works/pi-coding-agent",
    "@earendil-works/pi-tui",
    "typebox",
  ]) {
    assert.equal(
      existsSync(join(hostlessProject, "node_modules", hostModule)),
      false,
      `hostless install pulled in optional host module ${hostModule}`,
    );
  }

  const hostlessAgent = join(hostlessProject, "agent");
  agentDirs.push(hostlessAgent);
  const coldBrokerCheck = `
    import assert from 'node:assert/strict';
    import { spawn } from 'node:child_process';
    import { once } from 'node:events';
    import { randomUUID } from 'node:crypto';
    import { ParleyClient } from ${JSON.stringify(pathToFileURL(join(hostlessExtension, "broker/client.ts")).href)};
    const broker = spawn(process.execPath, ['--import', 'tsx', ${JSON.stringify(join(hostlessExtension, "broker/broker.ts"))}], {
      env: process.env, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true,
    });
    const exited = once(broker, 'exit');
    let stdout = '', stderr = '';
    broker.stderr.on('data', chunk => { stderr = (stderr + chunk).slice(-4000); });
    const client = new ParleyClient();
    try {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => finish(new Error('Cold broker startup timed out: ' + stderr)), 10000);
        function finish(error) {
          clearTimeout(timer); broker.stdout.off('data', onData); broker.off('exit', onExit);
          error ? reject(error) : resolve();
        }
        function onData(chunk) { stdout += chunk; if (stdout.includes('Parley broker started')) finish(); }
        function onExit(code) { finish(new Error('Cold broker exited ' + code + ': ' + stderr)); }
        broker.stdout.on('data', onData); broker.once('exit', onExit);
      });
      const id = randomUUID();
      await client.connect({ cwd: process.cwd(), model: 'cold-smoke', pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, id);
      assert.deepEqual((await client.listSessions()).map(seat => seat.id), [id]);
      assert.equal((await client.reportCompactionCompleted()).generation, 1);
      await client.disconnect();
      const [code, signal] = await exited;
      assert.equal(signal, null); assert.equal(code, 0);
    } finally {
      await client.disconnect();
      if (broker.exitCode === null && broker.signalCode === null) broker.kill('SIGKILL');
      await exited;
    }
  `;
  run(process.execPath, ["--import", "tsx", "--input-type=module", "-e", coldBrokerCheck], {
    cwd: hostlessProject, timeout: 30000,
    env: { ...process.env, PI_CODING_AGENT_DIR: hostlessAgent },
  });
  console.log("✓ hostless packed install: broker started, registered a client, committed state and shut down cleanly");

  for (const host of hosts) {
    const project = join(scratch, host.label);
    const agentDir = join(project, "agent");
    agentDirs.push(agentDir);
    mkdirSync(agentDir, { recursive: true });
    writeFileSync(join(project, "package.json"), JSON.stringify({
      private: true,
      ...(host.overrides ? { overrides: host.overrides } : {}),
    }));
    run("npm", ["install", "--silent", "--ignore-scripts", "--omit=dev", host.spec, tarball], { cwd: project });

    const hostPackage = join(project, "node_modules", host.packagePath);
    if (!existsSync(hostPackage)) throw new Error(`${host.label}: expected host package is missing`);
    for (const forbiddenPackagePath of host.forbiddenPackagePaths) {
      if (existsSync(join(project, "node_modules", forbiddenPackagePath))) {
        throw new Error(`${host.label}: installing pi-parley pulled in ${forbiddenPackagePath}`);
      }
    }

    const hostManifest = JSON.parse(readFileSync(join(hostPackage, "package.json"), "utf8"));
    const extensionRoot = join(project, "node_modules", "pi-parley");
    const extensionManifest = JSON.parse(readFileSync(join(extensionRoot, "package.json"), "utf8"));
    assert.deepEqual(extensionManifest.dependencies, { "fs-native-extensions": "^1.5.1", tsx: "^4.23.13" });
    const wrapperPath = join(project, "flightdeck-wrapper.mjs");
    const resolverTracePath = join(project, "presence-resolver.jsonl");
    writeFileSync(wrapperPath, [
      'import { appendFileSync } from "node:fs";',
      'import { registerParleyExtension } from "pi-parley/extension";',
      `const tracePath = ${JSON.stringify(resolverTracePath)};`,
      'export default function flightdeck(pi) {',
      '  registerParleyExtension(pi, {',
      '    resolvePresenceName(candidate, context) {',
      '      appendFileSync(tracePath, JSON.stringify({ candidate, kind: context.kind }) + "\\n");',
      '      return context.kind === "advertised"',
      '        ? `embedded:advertised:${candidate ?? "child"}`',
      '        : `embedded:${candidate ?? "session"}`;',
      '    },',
      '  });',
      '}',
      '',
    ].join("\n"));
    const ambientExtensionRoot = join(project, "ambient-copy", "pi-parley");
    cpSync(extensionRoot, ambientExtensionRoot, { recursive: true });
    const ambientExtensionPath = join(ambientExtensionRoot, "extension.ts");
    // Publish the fixture broker before RPC stdin can end the host. Otherwise its
    // asynchronous auto-spawn can finish after cleanup's PID census and recreate
    // a directory while it is being removed. This also exercises packed spawning.
    run(process.execPath, ["--import", "tsx", "--input-type=module", "-e", `
      import { spawnBrokerIfNeeded } from ${JSON.stringify(pathToFileURL(join(extensionRoot, "broker/spawn.ts")).href)};
      await spawnBrokerIfNeeded('npx', ['--no-install', 'tsx']);
    `], { cwd: project, timeout: 15000, env: { ...process.env, PI_CODING_AGENT_DIR: agentDir } });

    // Load the physical ambient copy first. The required wrapper must configure
    // that already-claimed actor across distinct Pi API facades and module realms.
    const rpc = await startRpc(
      process.execPath,
      [
        join(hostPackage, hostManifest.bin.pi),
        "--mode", "rpc",
        "--no-session",
        "--offline",
        "--provider", "openai",
        "--model", "gpt-4o-mini",
        "--api-key", "host-compat-smoke-only",
        "--no-extensions",
        "-e", ambientExtensionPath,
        "-e", wrapperPath,
      ],
      {
        cwd: project,
        env: { ...process.env, PI_CODING_AGENT_DIR: agentDir, PI_OFFLINE: "1" },
      },
    );
    try {
      const commands = await rpc.send({ id: "commands", type: "get_commands" });
      const rename = await rpc.send({ id: "rename", type: "set_session_name", name: `${host.label}-compatible` });
      // Upstream 0.73.1 reports set_session_name to RPC consumers but not
      // ExtensionAPI.on(). Keep stdin open across the compatibility poll, then
      // inspect the real broker.
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 1_300));
    const expectedPresenceName = `embedded:${host.label}-compatible`;
    const observerCheck = `
      import assert from 'node:assert/strict';
      import { ParleyClient } from ${JSON.stringify(pathToFileURL(join(extensionRoot, "broker/client.ts")).href)};
      const client = new ParleyClient();
      try {
        await client.connect({ name: 'packed-observer', cwd: process.cwd(), model: 'packed-smoke', pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, ${JSON.stringify(`${host.label}-packed-observer`)});
        const deadline = Date.now() + 5000;
        let sessions = [];
        while (Date.now() < deadline) {
          sessions = await client.listSessions();
          if (sessions.some(session => session.name === ${JSON.stringify(expectedPresenceName)})) break;
          await new Promise(resolve => setTimeout(resolve, 25));
        }
        assert.ok(sessions.some(session => session.name === ${JSON.stringify(expectedPresenceName)}), JSON.stringify(sessions.map(session => session.name)));
      } finally { await client.disconnect(); }
    `;
    run(process.execPath, ["--import", "tsx", "--input-type=module", "-e", observerCheck], {
      cwd: project,
      timeout: 15000,
      env: { ...process.env, PI_CODING_AGENT_DIR: agentDir },
    });
    const state = await rpc.send({ id: "state", type: "get_state" });
    await rpc.close();

    const nameEvent = rpc.records.find(
      (record) => record.type === "session_info_changed" && record.name === `${host.label}-compatible`,
    );
    const availableCommands = commands?.data?.commands ?? commands?.data ?? [];
    const parleyCommands = availableCommands.filter((command) => command.name === "parley" || /^parley:\d+$/.test(command.name));
    if (!commands?.success || parleyCommands.length !== 1) {
      throw new Error(`${host.label}: bundled wrapper and ambient physical copy did not deduplicate Parley registration`);
    }
    const resolverCalls = readFileSync(resolverTracePath, "utf8").trim().split("\n").filter(Boolean).map((line) => JSON.parse(line));
    if (!rename?.success || !nameEvent || !state?.success || state.data?.sessionName !== `${host.label}-compatible`
      || !resolverCalls.some((call) => call.kind === "session" && call.candidate === `${host.label}-compatible`)) {
      throw new Error(`${host.label}: real host rename did not publish through packed presence-name policy`);
    }
      console.log(`✓ ${host.label} ${hostManifest.version}: packed ambient-first actor resolved real host rename presence`);
    } finally {
      await rpc.terminate();
    }
  }
} finally {
  const brokerPids = agentDirs.flatMap((agentDir) => {
    const pidPath = join(agentDir, "parley", "broker.pid");
    if (!existsSync(pidPath)) return [];
    const pid = Number.parseInt(readFileSync(pidPath, "utf8").trim(), 10);
    return Number.isInteger(pid) && pid > 0 ? [pid] : [];
  });
  for (const pid of brokerPids) {
    try { process.kill(pid, "SIGTERM"); } catch { /* already stopped */ }
  }
  for (const pid of brokerPids) {
    const deadline = Date.now() + 2_000;
    while (Date.now() < deadline) {
      try { process.kill(pid, 0); } catch { break; }
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 25);
    }
    try {
      process.kill(pid, 0);
      process.kill(pid, "SIGKILL");
    } catch { /* stopped after SIGTERM */ }
  }
  // The default tsx launcher is the broker process's detached parent. Give it
  // time to observe the child exit before deleting its cwd and returning.
  if (brokerPids.length > 0) {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
  }
  if (process.env.PI_PARLEY_KEEP_SMOKE_TMP === "1") {
    console.log(`Kept smoke workspace: ${scratch}`);
  } else {
    rmSync(scratch, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
  }
}
