import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import path from "node:path";
import { once } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { attachPeerStreams, type PeerStreamAttachment } from "./attachment.ts";
import { ParleyClient } from "./client.ts";
import { createMessageReader, writeMessage } from "./framing.ts";
import { encodeOriginQualifiedSessionIdentity } from "./federation-protocol.ts";
import type { FederationOrigin, BrokerScopeSummary } from "./federation-types.ts";
import { getBrokerSocketPath, getParleyDirPath, readBrokerTcpEndpoint, type BrokerConnectTarget } from "./paths.ts";
import { getTsxCliPath } from "./spawn.ts";
import type { BrokerMessage, SessionInfo } from "../types.ts";

const sharedScope = "flightdeck";
const delay = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
type Inbound = Extract<BrokerMessage, { type: "message" }>;
interface Broker {
  name: string;
  agentDir: string;
  target: BrokerConnectTarget;
  origin: FederationOrigin;
}
interface Actor {
  broker: Broker;
  client: ParleyClient;
  id: string;
  received: Inbound[];
}

function isolatedEnv(agentDir: string, tcp: boolean): NodeJS.ProcessEnv {
  const env = { ...process.env };
  // This is an ordinary library fixture, not an activated FlightDeck Pi proof.
  for (const key of Object.keys(env)) if (key.startsWith("FLIGHTDECK_")) delete env[key];
  delete env.PI_PARLEY_SCOPE_ID;
  return { ...env, PI_CODING_AGENT_DIR: agentDir, PI_PARLEY_TRANSPORT: tcp ? "tcp" : "socket", PI_PARLEY_TCP: String(tcp) };
}

async function connect(target: BrokerConnectTarget): Promise<net.Socket> {
  const socket = typeof target === "string" ? net.connect(target) : net.connect(target.port, target.host);
  socket.on("error", () => undefined);
  try { await once(socket, "connect"); return socket; }
  catch (error) { socket.destroy(); throw error; }
}

async function catalog(target: BrokerConnectTarget): Promise<{ localOrigin: FederationOrigin; scopes: BrokerScopeSummary[] }> {
  const socket = await connect(target);
  const requestId = randomUUID();
  try {
    return await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("Scope catalog response timed out")), 5_000);
      const reader = createMessageReader(value => {
        const result = value as Record<string, unknown>;
        if (result.type !== "broker_list_scopes_result" || result.requestId !== requestId) return;
        clearTimeout(timer);
        if (result.ok !== true) reject(new Error("Scope catalog refused"));
        else resolve(result as unknown as { localOrigin: FederationOrigin; scopes: BrokerScopeSummary[] });
      }, error => { clearTimeout(timer); reject(error); });
      socket.on("data", reader);
      writeMessage(socket, { type: "broker_list_scopes", requestId, ...(typeof target === "string" ? {} : { stateId: target.stateId }) });
    });
  } finally { socket.destroy(); }
}

async function fixture(t: test.TestContext, tcp: boolean) {
  const root = mkdtempSync(path.join(process.platform === "win32" ? tmpdir() : "/tmp", "plr-"));
  const children: ChildProcess[] = [];
  const clients: ParleyClient[] = [];
  const links: PeerStreamAttachment[] = [];
  const streams: net.Socket[] = [];
  t.after(async () => {
    await Promise.all(links.map(link => link.close()));
    for (const socket of streams) socket.destroy();
    await Promise.all(clients.map(client => client.disconnect()));
    await Promise.all(children.map(async child => {
      if (child.exitCode !== null || child.signalCode !== null) return;
      const exited = once(child, "exit");
      child.kill("SIGTERM");
      await exited;
    }));
    rmSync(root, { recursive: true, force: true });
  });
  const brokers: Broker[] = [];
  for (const name of ["a", "b", "c"]) {
    const agentDir = path.join(root, name);
    const child = spawn(process.execPath, [getTsxCliPath(), path.join(process.cwd(), "broker/broker.ts")], {
      cwd: process.cwd(), env: isolatedEnv(agentDir, tcp), stdio: ["ignore", "pipe", "pipe"],
    });
    children.push(child);
    let stderr = "";
    child.stderr!.on("data", (bytes: Buffer) => { stderr = (stderr + bytes.toString()).slice(-4000); });
    await new Promise<void>((resolve, reject) => {
      const cleanup = () => { clearTimeout(timer); child.stdout!.off("data", onData); child.off("exit", onExit); };
      const onData = (bytes: Buffer) => { if (bytes.toString().includes("Parley broker started")) { cleanup(); resolve(); } };
      const onExit = () => { cleanup(); reject(new Error(`Isolated broker startup failed: ${stderr}`)); };
      const timer = setTimeout(() => { cleanup(); reject(new Error(`Isolated broker startup timed out: ${stderr}`)); }, 10_000);
      child.stdout!.on("data", onData); child.once("exit", onExit);
    });
    const target = tcp ? readBrokerTcpEndpoint(getParleyDirPath(agentDir)) : getBrokerSocketPath(process.platform, agentDir);
    brokers.push({ name, agentDir, target, origin: (await catalog(target)).localOrigin });
  }
  const actor = async (broker: Broker, scope: string, id: string): Promise<Actor> => {
    const client = new ParleyClient(); clients.push(client);
    const received: Inbound[] = [];
    client.on("message", (from: Inbound["from"], message: Inbound["message"]) => received.push({ type: "message", from, message }));
    const changed = [...new Set([...Object.keys(process.env).filter(key => key.startsWith("FLIGHTDECK_")),
      "PI_CODING_AGENT_DIR", "PI_PARLEY_SCOPE_ID", "PI_PARLEY_TRANSPORT", "PI_PARLEY_TCP"])];
    const saved = changed.map(key => [key, process.env[key]] as const);
    try {
      for (const key of changed) delete process.env[key];
      Object.assign(process.env, { PI_CODING_AGENT_DIR: broker.agentDir, PI_PARLEY_SCOPE_ID: scope,
        PI_PARLEY_TRANSPORT: tcp ? "tcp" : "socket", PI_PARLEY_TCP: String(tcp) });
      await client.connect({ name: id, cwd: process.cwd(), model: "roster-lifecycle-fixture", pid: process.pid,
        startedAt: Date.now(), lastActivity: Date.now() }, id);
    } finally {
      for (const [key, value] of saved) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
    }
    return { broker, client, id, received };
  };
  const attach = async (local: Broker, remote: Broker) => {
    const endpoint = async (broker: Broker) => {
      const stream = await connect(broker.target); streams.push(stream);
      return { stream, origin: broker.origin,
        scopeBindings: [{ localScopeId: sharedScope, localScopeAlias: sharedScope, remoteScopeAlias: sharedScope }],
        ...(typeof broker.target === "string" ? {} : { stateId: broker.target.stateId }) };
    };
    const link = await attachPeerStreams({ local: await endpoint(local), remote: await endpoint(remote) });
    links.push(link);
    return link;
  };
  return { brokers, actor, attach };
}

function qualified(actor: Actor): string {
  return encodeOriginQualifiedSessionIdentity({ originId: actor.broker.origin.id,
    remoteScopeAlias: sharedScope, remoteStableSessionId: actor.id });
}

async function roster(actor: Actor, remotes: Actor[]): Promise<SessionInfo[]> {
  const expected = [actor.id, ...remotes.map(qualified)].sort();
  let last: SessionInfo[] = [];
  const deadline = Date.now() + 5_000;
  do {
    last = await actor.client.listSessions();
    if (JSON.stringify(last.map(row => row.id).sort()) === JSON.stringify(expected)) {
      for (const remote of remotes) {
        const row = last.find(row => row.id === qualified(remote))!;
        assert.equal(row.federation?.originId, remote.broker.origin.id);
        assert.equal(row.federation?.remoteStableSessionId, remote.id);
        assert.equal(row.federation?.remoteScopeAlias, sharedScope);
        assert.ok(row.endpointEpoch, "qualified roster carries a usable endpoint lifetime");
      }
      return last;
    }
    await delay(10);
  } while (Date.now() < deadline);
  assert.deepEqual(last.map(row => row.id).sort(), expected, `${actor.id} qualified roster did not converge`);
  return last;
}

async function deliver(from: Actor, to: Actor) {
  const target = (await from.client.listSessions()).find(row => row.id === qualified(to));
  assert.ok(target, "delivery uses a discovered qualified contact");
  const text = `late-roster:${from.id}:${to.id}:${randomUUID()}`;
  const result = await from.client.sendToSession(target, { text });
  assert.equal(result.delivered, true, JSON.stringify(result));
  // A fresh correlated receiver list is an ordered consumer-boundary barrier.
  await to.client.listSessions();
  const inbound = to.received.filter(row => row.message.id === result.id);
  assert.equal(inbound.length, 1, "one actual inbound event accompanies this finite delivery");
  assert.equal(inbound[0]!.message.content.text, text);
  assert.equal(inbound[0]!.from.id, qualified(from));
}

for (const tcp of [false, true]) test(`supplied ${tcp ? "authenticated TCP" : "local socket"} streams converge after empty admission, late actors and zero-to-reregistration`,
  { timeout: 45_000 }, async t => {
    const f = await fixture(t, tcp);
    const [a, b, c] = f.brokers as [Broker, Broker, Broker];
    const privateActors = [];
    for (const broker of f.brokers) privateActors.push(await f.actor(broker, "private-unmapped", `${broker.name}-private`));
    // Two edges are admitted while neither endpoint has a shared-scope actor.
    await f.attach(a, b);
    await f.attach(a, c);
    for (const actor of privateActors) await roster(actor, []);

    const firstA = await f.actor(a, sharedScope, "a-shared");
    await roster(firstA, []);
    const firstB = await f.actor(b, sharedScope, "b-shared");
    await roster(firstA, [firstB]); await roster(firstB, [firstA]);
    const firstC = await f.actor(c, sharedScope, "c-shared");
    await roster(firstA, [firstB, firstC]); await roster(firstB, [firstA]); await roster(firstC, [firstA]);
    // No broker transit: B/C discover one another only after their own later edge.
    await f.attach(b, c);
    const first = [firstA, firstB, firstC];
    for (const actor of first) await roster(actor, first.filter(other => other !== actor));
    for (const broker of f.brokers) assert.equal((await catalog(broker.target)).scopes.find(scope => scope.scopeId === sharedScope)?.liveSessions, 1);
    for (const actor of privateActors) await roster(actor, []);
    for (const from of first) for (const to of first) if (from !== to) await deliver(from, to);

    const oldEpochs = first.map(actor => actor.client.getSelfSession()!.endpointEpoch);
    await firstB.client.disconnect();
    await roster(firstA, [firstC]); await roster(firstC, [firstA]);
    await firstA.client.disconnect(); await firstC.client.disconnect();
    for (const broker of f.brokers) {
      const scopes = (await catalog(broker.target)).scopes;
      assert.equal(scopes.find(scope => scope.scopeId === sharedScope)?.liveSessions ?? 0, 0,
        "all shared actors have unregistered; existing stream admissions remain owned");
    }
    for (const actor of privateActors) await roster(actor, []);

    const againA = await f.actor(a, sharedScope, "a-shared");
    await roster(againA, []);
    const againB = await f.actor(b, sharedScope, "b-shared");
    await roster(againA, [againB]); await roster(againB, [againA]);
    const againC = await f.actor(c, sharedScope, "c-shared");
    const again = [againA, againB, againC];
    for (const [index, actor] of again.entries()) {
      assert.equal(qualified(actor), qualified(first[index]!));
      assert.notEqual(actor.client.getSelfSession()!.endpointEpoch, oldEpochs[index], "new endpoint lifetime, stable qualified contact");
      await roster(actor, again.filter(other => other !== actor));
    }
    for (const actor of privateActors) await roster(actor, []);
    for (const from of again) for (const to of again) if (from !== to) await deliver(from, to);
  });
