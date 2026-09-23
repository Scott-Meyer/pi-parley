// ACL fork: end-to-end regression coverage for subagent visibility scoping.
//
// Policy under test:
//   - A main session sees every other main session, plus only the subagent
//     children it personally supervises.
//   - A subagent session sees only its own supervisor -- never siblings,
//     never unrelated mains.
// Coverage includes both supervisorSessionId matching and the supervisorName
// fallback (the stableId mismatch case), plus send-path enforcement, not just
// the list response.
import assert from "node:assert/strict";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import test from "node:test";
import type { SessionRegistration } from "../types.ts";
import { ParleyClient } from "./client.ts";

const repoDir = process.cwd();
const TSX_BIN = process.env.PI_PARLEY_TEST_TSX_BIN
  ?? path.join(repoDir, "node_modules", "tsx", "dist", "cli.mjs");

function baseRegistration(name: string): SessionRegistration {
  return {
    name,
    cwd: "/test",
    model: "test-model",
    pid: process.pid,
    startedAt: Date.now(),
    lastActivity: Date.now(),
  };
}

async function startBroker(agentDir: string): Promise<ChildProcessWithoutNullStreams> {
  const broker = spawn(
    process.execPath,
    [TSX_BIN, path.join(repoDir, "broker", "broker.ts")],
    {
      cwd: repoDir,
      env: { ...process.env, PI_CODING_AGENT_DIR: agentDir },
      stdio: ["pipe", "pipe", "pipe"],
    },
  );
  const ready = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("Broker startup timed out")), 10_000);
    broker.stdout.on("data", (chunk: Buffer) => {
      if (chunk.toString().includes("Parley broker started")) {
        clearTimeout(timeout);
        resolve();
      }
    });
    broker.once("exit", (code, signal) => {
      clearTimeout(timeout);
      reject(new Error(`Broker exited before startup (${code ?? signal})`));
    });
  });
  await ready;
  return broker;
}

async function stopBroker(broker: ChildProcessWithoutNullStreams): Promise<void> {
  if (broker.exitCode !== null) return;
  broker.kill("SIGTERM");
  await once(broker, "exit");
}

function idsOf(sessions: { id: string }[]): Set<string> {
  return new Set(sessions.map((s) => s.id));
}

test("subagent ACL: list/send scoping by supervisorSessionId and supervisorName fallback", { concurrency: false, timeout: 30_000 }, async () => {
  const agentDir = mkdtempSync(path.join(tmpdir(), "pi-parley-acl-"));
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  // The client library resolves the broker connect target from THIS
  // process's env, independent of the env passed only to the spawned broker
  // subprocess. Without this, clients silently connect to any real live
  // broker at the default location instead of the isolated test broker.
  process.env.PI_CODING_AGENT_DIR = agentDir;
  const broker = await startBroker(agentDir);
  const clients: ParleyClient[] = [];

  try {
    const mainAId = randomUUID();
    const mainBId = randomUUID();
    const mainA = new ParleyClient();
    const mainB = new ParleyClient();
    clients.push(mainA, mainB);
    await mainA.connect(baseRegistration("main-a"), mainAId);
    await mainB.connect(baseRegistration("main-b"), mainBId);

    // child1OfA matches its supervisor by session ID.
    const child1OfA = new ParleyClient();
    clients.push(child1OfA);
    await child1OfA.connect({
      ...baseRegistration("child-1-of-a"),
      isSubagent: true,
      supervisorSessionId: mainAId,
      supervisorName: "main-a",
    });

    // child2OfA is a sibling of child1OfA under the same supervisor mainA.
    const child2OfA = new ParleyClient();
    clients.push(child2OfA);
    await child2OfA.connect({
      ...baseRegistration("child-2-of-a"),
      isSubagent: true,
      supervisorSessionId: mainAId,
      supervisorName: "main-a",
    });

    // childOfB deliberately carries a WRONG supervisorSessionId (simulating
    // the stableId-mismatch case: pi-subagents passed the parent's raw pi
    // session id, but the parent registered with pi-parley under a
    // different stable id) and must still resolve via supervisorName.
    const childOfB = new ParleyClient();
    clients.push(childOfB);
    await childOfB.connect({
      ...baseRegistration("child-of-b"),
      isSubagent: true,
      supervisorSessionId: randomUUID(),
      supervisorName: "main-b",
    });

    // --- list scoping ---
    const mainASees = idsOf(await mainA.listSessions());
    const mainBSees = idsOf(await mainB.listSessions());
    const child1ASees = idsOf(await child1OfA.listSessions());
    const child2ASees = idsOf(await child2OfA.listSessions());
    const childBSees = idsOf(await childOfB.listSessions());

    assert.ok(mainASees.has(mainA.sessionId!), "main A sees itself");
    assert.ok(mainASees.has(mainB.sessionId!), "main A sees main B");
    assert.ok(mainASees.has(child1OfA.sessionId!), "main A sees its child 1");
    assert.ok(mainASees.has(child2OfA.sessionId!), "main A sees its child 2");
    assert.ok(!mainASees.has(childOfB.sessionId!), "main A does NOT see main B's child");

    assert.ok(mainBSees.has(mainA.sessionId!), "main B sees main A");
    assert.ok(mainBSees.has(childOfB.sessionId!), "main B sees its own child (via name fallback)");
    assert.ok(!mainBSees.has(child1OfA.sessionId!), "main B does NOT see main A's child 1");
    assert.ok(!mainBSees.has(child2OfA.sessionId!), "main B does NOT see main A's child 2");

    assert.deepEqual(child1ASees, new Set([child1OfA.sessionId!, child2OfA.sessionId!, mainA.sessionId!]), "child 1 of A sees itself, its sibling, and its supervisor");
    assert.deepEqual(child2ASees, new Set([child1OfA.sessionId!, child2OfA.sessionId!, mainA.sessionId!]), "child 2 of A sees itself, its sibling, and its supervisor");
    assert.deepEqual(childBSees, new Set([childOfB.sessionId!, mainB.sessionId!]), "child of B sees only itself + its supervisor (name-fallback matched)");

    // --- send-path enforcement (not just list) ---
    // send() never rejects on a broker-side delivery failure; it resolves
    // with { delivered: false, delivery: "failed", ... }. A hidden target
    // must fail exactly like a nonexistent one ("Session not found"), never
    // with a distinguishable ACL-denied reason.

    // Sibling children CAN reach each other within the same supervisor tree.
    const siblingSend = await child1OfA.send(child2OfA.sessionId!, { text: "hi sibling" });
    assert.equal(siblingSend.delivered, true, "child 1 of A can send to sibling child 2 of A");
    assert.equal(siblingSend.delivery, "socket_delivered");

    // Children of different parents CANNOT reach each other.
    const crossTreeSend = await child1OfA.send(childOfB.sessionId!, { text: "hi stranger" });
    assert.equal(crossTreeSend.delivered, false, "child of A cannot send to child of B by ID");
    assert.match(crossTreeSend.reason ?? "", /not found/i);

    // A child cannot reach another main.
    const childToOtherMain = await child1OfA.send(mainB.sessionId!, { text: "hi other main" });
    assert.equal(childToOtherMain.delivered, false, "child of A cannot send to main B by ID");
    assert.match(childToOtherMain.reason ?? "", /not found/i);

    // A main cannot reach another main's child, even by exact session ID.
    const crossMainSend = await mainA.send(childOfB.sessionId!, { text: "hi other child" });
    assert.equal(crossMainSend.delivered, false, "main A cannot send to main B's child by ID");
    assert.match(crossMainSend.reason ?? "", /not found/i);

    // A child can reach its own supervisor.
    const toSupervisor = await child1OfA.send(mainA.sessionId!, { text: "status update" });
    assert.equal(toSupervisor.delivered, true);
    assert.equal(toSupervisor.delivery, "socket_delivered");

    // A main can reach its own child.
    const toOwnChild = await mainB.send(childOfB.sessionId!, { text: "go do X" });
    assert.equal(toOwnChild.delivered, true);
    assert.equal(toOwnChild.delivery, "socket_delivered");

    // Mains still see and can reach each other normally.
    const mainToMain = await mainA.send(mainB.sessionId!, { text: "peer to peer" });
    assert.equal(mainToMain.delivered, true);
    assert.equal(mainToMain.delivery, "socket_delivered");

    // --- advertise retirement: subagents cannot advertise ---
    const childAdvertise = await child1OfA.advertise("child-public");
    assert.equal(childAdvertise.ok, false);
    assert.equal(childAdvertise.code, "E_NOT_ELIGIBLE");

    const mainAdvertise = await mainA.advertise("main-public");
    assert.equal(mainAdvertise.ok, false);
    assert.equal(mainAdvertise.code, "E_NOT_ELIGIBLE");
  } finally {
    for (const client of clients) {
      try {
        await client.disconnect();
      } catch {
        // best-effort cleanup
      }
    }
    await stopBroker(broker);
    rmSync(agentDir, { recursive: true, force: true });
    if (previousAgentDir === undefined) {
      delete process.env.PI_CODING_AGENT_DIR;
    } else {
      process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    }
  }
});
