import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { getBrokerConnectTarget, getBrokerListenTarget } from "./broker/paths.ts";

const terminal = { FLIGHTDECK_TAB_ID: "tab-1", FLIGHTDECK_STATUS_PORT: "1234", FLIGHTDECK_STATUS_TOKEN: "desktop-token" };

test("Windows FlightDeck participants use the broker's authenticated loopback publication", () => {
  const root = mkdtempSync(path.join(tmpdir(), "parley-fd-windows-"));
  const parleyDir = path.join(root, "parley");
  mkdirSync(parleyDir);
  const endpoint = { transport: "tcp", host: "127.0.0.1", port: 2345, stateId: "published-endpoint-credential" };
  try {
    assert.deepEqual(getBrokerListenTarget("win32", terminal), { transport: "tcp", host: "127.0.0.1", port: 0 });
    assert.throws(() => getBrokerConnectTarget("win32", terminal, parleyDir));
    writeFileSync(path.join(parleyDir, "broker.port.json"), JSON.stringify(endpoint));
    assert.deepEqual(getBrokerConnectTarget("win32", terminal, parleyDir), endpoint);
    assert.deepEqual(getBrokerConnectTarget("win32", {}, parleyDir), endpoint,
      "ordinary and FlightDeck clients share the same broker whichever starts first");
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test("Windows clients share a default; explicit named pipes and POSIX Unix sockets remain available", () => {
  assert.deepEqual(getBrokerListenTarget("win32", {}), getBrokerListenTarget("win32", terminal));
  assert.deepEqual(getBrokerListenTarget("win32", { FLIGHTDECK_TAB_ID: "partial" }), getBrokerListenTarget("win32", terminal));
  assert.equal(typeof getBrokerListenTarget("win32", { PI_PARLEY_TRANSPORT: "socket" }), "string");
  assert.equal(typeof getBrokerListenTarget("linux", terminal), "string");
  assert.equal(typeof getBrokerListenTarget("darwin", terminal), "string");
});
