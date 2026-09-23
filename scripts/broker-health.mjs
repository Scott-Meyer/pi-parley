#!/usr/bin/env node
// Report the Parley broker running for this user's Pi agent directory, and
// whether it matches the source in this checkout. Read-only. Plain Node; the
// expected identity additionally needs this checkout's node_modules (tsx).
//
//   node scripts/broker-health.mjs
//
// Prints one JSON object: { running, expected, stale }.
import net from "node:net";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const agentDir = process.env.PI_CODING_AGENT_DIR || path.join(os.homedir(), ".pi", "agent");
const runtimeDir = path.join(agentDir, "parley");

function endpoint() {
  try {
    const published = JSON.parse(fs.readFileSync(path.join(runtimeDir, "broker.port.json"), "utf8"));
    return { target: { host: published.host, port: published.port }, stateId: published.stateId };
  } catch {
    return { target: path.join(runtimeDir, "broker.sock") };
  }
}

function health() {
  const { target, stateId } = endpoint();
  return new Promise((resolve) => {
    const socket = net.connect(target);
    const payload = Buffer.from(JSON.stringify({ type: "health", requestId: "broker-health", ...(stateId ? { stateId } : {}) }));
    const header = Buffer.alloc(4);
    header.writeUInt32BE(payload.length);
    let buffer = Buffer.alloc(0);
    const done = (value) => { socket.destroy(); resolve(value); };
    socket.on("connect", () => socket.write(Buffer.concat([header, payload])));
    socket.on("data", (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      if (buffer.length < 4 || buffer.length < 4 + buffer.readUInt32BE(0)) return;
      const frame = JSON.parse(buffer.subarray(4, 4 + buffer.readUInt32BE(0)).toString("utf8"));
      done(frame.type === "health_ok" ? frame.broker : { error: "unexpected response", frame });
    });
    socket.on("error", (error) => done({ error: `no broker (${error.code ?? error.message})` }));
    setTimeout(() => done({ error: "timeout" }), 3000).unref();
  });
}

function expected() {
  const tsx = path.join(packageRoot, "node_modules", "tsx", "dist", "cli.mjs");
  if (!fs.existsSync(tsx)) return { error: "install this checkout's dependencies to compute the expected identity" };
  const script = `import(${JSON.stringify(new URL("../broker/build.ts", import.meta.url).href)}).then((m) => console.log(JSON.stringify(m.getBrokerBuildIdentity(${JSON.stringify(packageRoot)}))))`;
  const result = spawnSync(process.execPath, [tsx, "-e", script], { encoding: "utf8" });
  try { return JSON.parse(result.stdout.trim().split("\n").pop()); } catch { return { error: (result.stderr || "could not compute").trim().slice(0, 300) }; }
}

const running = await health();
const want = expected();
const stale = running.sourceId && want.sourceId ? running.sourceId !== want.sourceId : null;
console.log(JSON.stringify({ running, expected: want, stale }, null, 2));
