// This computer's link keeper: keeps its Parley broker linked with the
// brokers of other computers it reaches over SSH, and links those computers
// with each other through it, every pair, like FlightDeck does for its hosts.
//
// Pi sessions start it and keep it alive by staying connected; it stops a
// little while after the last one leaves, closing its links so the broker
// can idle out as usual. One keeper per agent directory, held by a lock.
//
//   node --import tsx link/keeper.ts
import { appendFileSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import { hostname } from "node:os";
import type { ChildProcess } from "node:child_process";
import {
  attachBrokers, createLocalBrokerAccess, inspectBroker,
  type BrokerHostAccess, type ScopeBinding,
} from "../dist/federation.js";
import { createMessageReader, writeMessage } from "../broker/framing.ts";
import { tryAcquireProcessLock } from "../broker/process-lock.ts";
import { ensureParleyRuntimeDir } from "../broker/paths.ts";
import { destinationFromArgv, listHerdrMachines, listSshProcesses } from "./discover.ts";
import { getLinkPaths } from "./paths.ts";
import type { LinkEvent, LinkHostState, LinkHostStatus, LinkRequest, LinkResponse, LinkStatus } from "./protocol.ts";
import {
  destinationKey, destinationLabel, openSshHost, parseSshDestination, SshHostError,
  type SshDestination, type SshHost,
} from "./ssh.ts";

/** Ordinary sessions: no PI_PARLEY_SCOPE_ID. Applications that scope their
 * sessions (FlightDeck) link those scopes themselves. */
const SCOPES: ScopeBinding[] = [{ localScopeId: null, localScopeAlias: "default", remoteScopeAlias: "default" }];
const DISCOVER_MS = 10_000;
const IDLE_EXIT_MS = Number(process.env.PI_PARLEY_LINK_IDLE_MS ?? 30_000);
const ADD_ANSWER_MS = 45_000;

const paths = getLinkPaths();
const localLabel = (process.env.PI_PARLEY_LINK_LABEL?.trim() || hostname()).replace(/\.local$/i, "");

function log(line: string) {
  try { appendFileSync(paths.log, `${new Date().toISOString()} ${line}\n`); } catch { /* diagnostics only */ }
}

// ---------------------------------------------------------------- config

interface RememberedHost {
  readonly destination: string;
  readonly via: "added" | "found";
}
interface Config {
  version: 1;
  /** Set by /parley link on|off; absent means this computer's default. */
  discover?: boolean;
  hosts: RememberedHost[];
  /** Removed by the person or an AI; not found automatically again until added. */
  ignored: string[];
}

function readConfig(): Config {
  try {
    const parsed = JSON.parse(readFileSync(paths.config, "utf8"));
    return {
      version: 1,
      ...(typeof parsed.discover === "boolean" ? { discover: parsed.discover } : {}),
      hosts: Array.isArray(parsed.hosts) ? parsed.hosts.filter((h: any) => typeof h?.destination === "string")
        .map((h: any) => ({ destination: h.destination, via: h.via === "found" ? "found" : "added" })) : [],
      ignored: Array.isArray(parsed.ignored) ? parsed.ignored.filter((d: unknown) => typeof d === "string") : [],
    };
  } catch {
    return { version: 1, hosts: [], ignored: [] };
  }
}

/** Whether discovery is on: as last set, else the default the starting session passed. */
const discovering = () => config.discover ?? process.env.PI_PARLEY_LINK_DISCOVER_DEFAULT === "1";

let config = readConfig();
function saveConfig() {
  const tmp = `${paths.config}.${process.pid}.tmp`;
  writeFileSync(tmp, `${JSON.stringify(config, null, 2)}\n`, { mode: 0o600 });
  renameSync(tmp, paths.config);
}

// ---------------------------------------------------------------- hosts

interface Host {
  readonly key: string;
  readonly destination: SshDestination;
  readonly label: string;
  via: "added" | "found";
  state: LinkHostState;
  detail?: string;
  since: string;
  ssh?: SshHost;
  origin?: string;
  checking?: Promise<void>;
  /** Resolves whenever this host's state settles after a change. */
  settled: { promise: Promise<void>; resolve: () => void };
  everLinked: boolean;
}

const hosts = new Map<string, Host>();
const ownChildren = new Set<number>();
const trackChild = (child: ChildProcess) => {
  if (child.pid) ownChildren.add(child.pid);
  child.once("close", () => { if (child.pid) ownChildren.delete(child.pid); });
};

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => { resolve = r; });
  return { promise, resolve };
}

function setState(host: Host, state: LinkHostState, detail?: string) {
  if (host.state !== state || host.detail !== detail) {
    host.state = state;
    host.detail = detail;
    host.since = new Date().toISOString();
    log(`${host.label}: ${state}${detail ? ` (${detail})` : ""}`);
  }
  if (state !== "checking") {
    host.settled.resolve();
    host.settled = deferred();
  }
  if (state === "linked" && !host.everLinked) {
    host.everLinked = true;
    broadcast({ type: "event", event: "linked", label: host.label, via: host.via });
  }
}

function remembered(host: Host) {
  return config.hosts.some((h) => h.destination === host.key);
}

function hostFor(destination: SshDestination, via: "added" | "found"): Host {
  const key = destinationKey(destination);
  let host = hosts.get(key);
  if (!host) {
    host = {
      key, destination, label: destinationLabel(destination), via,
      state: "checking", since: new Date().toISOString(), settled: deferred(), everLinked: false,
    };
    hosts.set(key, host);
  }
  if (via === "added") host.via = "added";
  return host;
}

function sshMessage(error: unknown): { state: LinkHostState; detail: string } {
  if (error instanceof SshHostError) {
    const words: Record<string, string> = {
      E_SSH_AUTH: "ssh needs a password or key approval it can't ask for; connect once with ssh yourself, or load your key",
      E_SSH_HOST_KEY: "ssh doesn't trust this computer's host key yet; connect once with ssh yourself",
      E_SSH_UNREACHABLE: "can't reach it over ssh",
      E_UNSUPPORTED_HOST: "its ssh shell isn't one Parley can use",
      E_SSH_FAILED: "ssh failed",
    };
    const base = words[error.code] ?? error.code;
    return { state: "unreachable", detail: error.detail ? `${base}: ${error.detail}` : base };
  }
  return { state: "unreachable", detail: error instanceof Error ? error.message : String(error) };
}

/** Connect over ssh and learn the computer; then its links start. */
function check(host: Host): Promise<void> {
  host.checking ??= (async () => {
    setState(host, "checking");
    try {
      host.ssh = await openSshHost(host.destination, {
        controlDir: paths.sshControlDir, signal: stopping.signal, onSpawn: trackChild,
      });
      if (!host.ssh.hasParley && !remembered(host)) {
        setState(host, "no-parley", "Parley has never run there");
        return;
      }
      if (!remembered(host)) {
        config.hosts.push({ destination: host.key, via: host.via });
        saveConfig();
      }
      // Links decide the state from here.
      setState(host, "waiting", "linking");
    } catch (error) {
      if (stopping.signal.aborted) return;
      const { state, detail } = sshMessage(error);
      setState(host, state, detail);
    } finally {
      host.checking = undefined;
      void reconcile();
    }
  })();
  return host.checking;
}

// ---------------------------------------------------------------- links

type Side = { key: string; label: string; access: BrokerHostAccess; host?: Host };
const local: Side = { key: "local", label: localLabel, access: createLocalBrokerAccess() };

interface Pair { a: Side; b: Side; controller: AbortController; state: "linked" | "waiting"; detail?: string }
const pairs = new Map<string, Pair>();
let localOrigin: string | undefined;

const sleep = (ms: number, signal: AbortSignal) => new Promise<void>((resolve) => {
  const done = () => { clearTimeout(timer); signal.removeEventListener("abort", done); resolve(); };
  const timer = setTimeout(done, ms);
  signal.addEventListener("abort", done, { once: true });
});

function codeOf(error: unknown): string {
  return (error as { code?: string })?.code ?? (error instanceof Error ? error.message : String(error));
}

function waitingWords(code: string, a: Side, b: Side): string {
  if (code === "E_BROKER_UNAVAILABLE") return "no Pi with Parley is running there right now";
  if (code === "E_VERSION_UNSUPPORTED" || code === "E_FEATURE_UNSUPPORTED") return "Parley versions differ; update Parley on one side";
  if (code === "E_ALREADY_CONNECTED") return `${a.label} and ${b.label} are already linked by another computer or app`;
  if (code === "E_TIMEOUT") return "timed out";
  return code;
}

/** Records a side's broker identity; false when it's a computer already present. */
function claimOrigin(side: Side, origin: string): boolean {
  if (!side.host) { localOrigin = origin; return true; }
  side.host.origin = origin;
  if (origin === localOrigin) {
    setState(side.host, "same-computer", "this computer");
    return false;
  }
  for (const other of hosts.values()) {
    if (other !== side.host && other.origin === origin && other.state !== "same-computer") {
      setState(side.host, "same-computer", `same computer as ${other.label}`);
      return false;
    }
  }
  return true;
}

function report(pair: Pair, state: "linked" | "waiting", detail?: string) {
  pair.state = state;
  pair.detail = detail;
  // Only links that include this computer describe a host's own state.
  if (pair.a === local && pair.b.host && pair.b.host.state !== "same-computer") {
    setState(pair.b.host, state, detail);
  }
}

async function keepLinked(pair: Pair) {
  const { a, b, controller: { signal } } = pair;
  let backoff = 2_000;
  while (!signal.aborted) {
    let code: string | undefined;
    try {
      const [left, right] = await Promise.all([
        inspectBroker(a.access, { signal, timeoutMs: 20_000 }),
        inspectBroker(b.access, { signal, timeoutMs: 20_000 }),
      ]);
      if (!claimOrigin(a, left.origin.id) || !claimOrigin(b, right.origin.id)) { void reconcile(); return; }
      // The acceptor decides first, seeing what the other broker supports, so
      // this computer's broker (as current as this keeper) accepts.
      const attachment = await attachBrokers({
        initiator: { broker: right, originLabel: b.label, scopeBindings: SCOPES },
        acceptor: { broker: left, originLabel: a.label, scopeBindings: SCOPES },
        signal,
        timeoutMs: 25_000,
      });
      if (signal.aborted) { await attachment.close(); break; }
      report(pair, "linked");
      backoff = 2_000;
      const completion = await attachment.completion;
      if (signal.aborted) break;
      code = completion.status === "failure" ? completion.error.code : "disconnected";
    } catch (error) {
      if (signal.aborted) break;
      code = codeOf(error);
    }
    // An absent broker over ssh can also be ssh failing; ask ssh which.
    if (code === "E_BROKER_UNAVAILABLE" || code === "E_STREAM_FAILED" || code === "disconnected") {
      for (const side of [a, b]) {
        if (!side.host) continue;
        try {
          await openSshHost(side.host.destination, { controlDir: paths.sshControlDir, signal, onSpawn: trackChild, timeoutMs: 15_000 });
        } catch (error) {
          if (signal.aborted) return;
          const { detail } = sshMessage(error);
          report(pair, "waiting", `${side.label}: ${detail}`);
          if (pair.a === local && side === b) setState(side.host, "unreachable", detail);
          code = undefined;
          break;
        }
      }
    }
    if (code) report(pair, "waiting", waitingWords(code, a, b));
    backoff = Math.min(backoff * 2, code === "E_BROKER_UNAVAILABLE" ? 15_000 : 60_000);
    await sleep(backoff, signal);
  }
}

function sides(): Side[] {
  const list: Side[] = [local];
  for (const host of hosts.values()) {
    if (!host.ssh || host.state === "same-computer" || host.state === "no-parley" || host.checking) continue;
    if (!remembered(host)) continue;
    list.push({ key: host.key, label: host.label, access: host.ssh.access, host });
  }
  return list;
}

// herdr's saved machines change rarely; ask now and then, not every pass.
let herdrCache: { at: number; machines: SshDestination[] } | undefined;
async function herdrMachines(): Promise<SshDestination[]> {
  if (!herdrCache || Date.now() - herdrCache.at > 60_000) herdrCache = { at: Date.now(), machines: await listHerdrMachines() };
  return herdrCache.machines;
}

let reconciling = false;
let reconcileAgain = false;
async function reconcile(): Promise<void> {
  if (reconciling) { reconcileAgain = true; return; }
  reconciling = true;
  try {
    do {
      reconcileAgain = false;
      // Remembered hosts are always wanted; found ones while discovery is on.
      for (const entry of config.hosts) {
        const destination = parseSshDestination(entry.destination);
        if (destination) hostFor(destination, entry.via);
      }
      if (discovering()) {
        try {
          const seen = [
            ...(await listSshProcesses()).filter((proc) => !ownChildren.has(proc.pid)).map((proc) => destinationFromArgv(proc.argv)),
            ...await herdrMachines(),
          ];
          for (const destination of seen) {
            if (!destination) continue;
            const key = destinationKey(destination);
            if (config.ignored.includes(key) || hosts.has(key)) continue;
            // Prefer the plainest spelling when several reach the same host.
            const sameHost = [...hosts.values()].some((h) => h.destination.host === destination.host && h.state !== "no-parley" && h.state !== "unreachable");
            if (sameHost) continue;
            hostFor(destination, "found");
          }
        } catch (error) {
          log(`discovery failed: ${codeOf(error)}`);
        }
      }
      for (const host of hosts.values()) {
        if (host.ssh || host.checking) continue;
        // Remembered computers that were unreachable are tried again now and then.
        const retry = host.state === "unreachable" && remembered(host) && Date.now() - Date.parse(host.since) > 60_000;
        if (host.state === "checking" || retry) void check(host);
      }

      const wanted = new Map<string, [Side, Side]>();
      const list = sides();
      for (let i = 0; i < list.length; i++) for (let j = i + 1; j < list.length; j++) {
        wanted.set(`${list[i]!.key}|${list[j]!.key}`, [list[i]!, list[j]!]);
      }
      for (const [key, pair] of pairs) {
        if (wanted.has(key)) continue;
        pair.controller.abort();
        pairs.delete(key);
      }
      for (const [key, [a, b]] of wanted) {
        if (pairs.has(key)) continue;
        const pair: Pair = { a, b, controller: new AbortController(), state: "waiting" };
        pairs.set(key, pair);
        void keepLinked(pair);
      }
    } while (reconcileAgain && !stopping.signal.aborted);
  } finally {
    reconciling = false;
  }
}

// ---------------------------------------------------------------- requests

function hostStatus(host: Host): LinkHostStatus {
  return Object.freeze({
    destination: host.key,
    label: host.label,
    via: host.via,
    remembered: remembered(host),
    state: host.state,
    ...(host.detail ? { detail: host.detail } : {}),
    since: host.since,
  });
}

function status(): LinkStatus {
  return Object.freeze({
    discover: discovering(),
    label: localLabel,
    hosts: [...hosts.values()].map(hostStatus),
    relays: [...pairs.values()].filter((p) => p.a !== local).map((p) => ({
      between: [p.a.label, p.b.label] as const, state: p.state, ...(p.detail ? { detail: p.detail } : {}),
    })),
  });
}

function findHost(destination: string): Host | undefined {
  const parsed = parseSshDestination(destination);
  const key = parsed ? destinationKey(parsed) : destination.trim();
  return hosts.get(key) ?? [...hosts.values()].find((h) => h.label === destination.trim() || h.destination.host === destination.trim());
}

async function untilSettled(host: Host, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline && (host.state === "checking" || (host.state === "waiting" && host.detail === "linking"))) {
    await Promise.race([host.settled.promise, new Promise((r) => setTimeout(r, deadline - Date.now()))]);
  }
}

function forgetHost(host: Host) {
  for (const [key, pair] of pairs) {
    if (pair.a.host === host || pair.b.host === host) { pair.controller.abort(); pairs.delete(key); }
  }
  hosts.delete(host.key);
}

async function handle(request: LinkRequest): Promise<LinkResponse> {
  const { requestId } = request;
  const fail = (error: string): LinkResponse => ({ type: "result", requestId, ok: false, error, status: status() });
  switch (request.type) {
    case "status":
      return { type: "result", requestId, ok: true, status: status() };
    case "discover": {
      config.discover = request.on === true;
      saveConfig();
      if (!discovering()) {
        // Found hosts that never linked aren't kept; remembered ones stay.
        for (const host of [...hosts.values()]) if (!remembered(host)) forgetHost(host);
      }
      await reconcile();
      return { type: "result", requestId, ok: true, status: status() };
    }
    case "add": {
      const destination = parseSshDestination(request.destination ?? "");
      if (!destination) return fail("give a destination as you would to ssh: an alias, user@host, and optionally -p, -l, -i, -J, -F or -o settings");
      const key = destinationKey(destination);
      config.ignored = config.ignored.filter((d) => d !== key);
      if (!config.hosts.some((h) => h.destination === key)) config.hosts.push({ destination: key, via: "added" });
      saveConfig();
      const existing = hosts.get(key);
      if (existing && existing.state !== "linked") { forgetHost(existing); }
      const host = hostFor(destination, "added");
      await reconcile();
      await untilSettled(host, ADD_ANSWER_MS);
      return { type: "result", requestId, ok: true, status: status(), host: hostStatus(host) };
    }
    case "recheck": {
      const targets = request.destination
        ? [findHost(request.destination)].filter((h): h is Host => !!h)
        : [...hosts.values()];
      if (request.destination && targets.length === 0) return fail(`no computer called ${request.destination} here; add it first`);
      for (const host of targets) {
        if (host.state === "linked") continue;
        forgetHost(host);
        hostFor(host.destination, host.via);
      }
      await reconcile();
      const rechecked = targets.map((t) => hosts.get(t.key)).filter((h): h is Host => !!h);
      await Promise.all(rechecked.map((h) => untilSettled(h, ADD_ANSWER_MS)));
      const one = request.destination ? rechecked[0] : undefined;
      return { type: "result", requestId, ok: true, status: status(), ...(one ? { host: hostStatus(one) } : {}) };
    }
    case "remove": {
      const host = findHost(request.destination ?? "");
      const key = host?.key ?? request.destination?.trim();
      if (!host && !config.hosts.some((h) => h.destination === key)) return fail(`no computer called ${request.destination} here`);
      config.hosts = config.hosts.filter((h) => h.destination !== key);
      if (key && !config.ignored.includes(key)) config.ignored.push(key);
      saveConfig();
      if (host) forgetHost(host);
      await reconcile();
      return { type: "result", requestId, ok: true, status: status() };
    }
  }
}

// ---------------------------------------------------------------- control

const clients = new Set<net.Socket>();
let idleTimer: NodeJS.Timeout | undefined;

function broadcast(event: LinkEvent) {
  for (const socket of clients) { try { writeMessage(socket, event); } catch { /* gone */ } }
}

function armIdle() {
  clearTimeout(idleTimer);
  if (clients.size === 0) idleTimer = setTimeout(() => void stop("no Pi sessions connected"), IDLE_EXIT_MS);
}

const stopping = new AbortController();
async function stop(why: string) {
  if (stopping.signal.aborted) return;
  log(`stopping: ${why}`);
  stopping.abort();
  for (const pair of pairs.values()) pair.controller.abort();
  server?.close();
  for (const socket of clients) socket.destroy();
  setTimeout(() => process.exit(0), 3_000).unref();
  // Give attachments a moment to close cleanly.
  await new Promise((r) => setTimeout(r, 500));
  if (process.platform !== "win32") rmSync(paths.control, { force: true });
  process.exit(0);
}

let server: net.Server | undefined;

async function main() {
  ensureParleyRuntimeDir(paths.parleyDir);
  mkdirSync(paths.sshControlDir, { recursive: true, mode: 0o700 });
  const lock = tryAcquireProcessLock(paths.lock);
  if (lock.status === "occupied") process.exit(0);
  if (process.platform !== "win32") rmSync(paths.control, { force: true });

  server = net.createServer((socket) => {
    clients.add(socket);
    armIdle();
    socket.on("error", () => {});
    socket.on("close", () => { clients.delete(socket); armIdle(); });
    socket.on("data", createMessageReader((value) => {
      const request = value as LinkRequest;
      if (!request || typeof request !== "object" || typeof (request as any).requestId !== "string") return;
      handle(request).then(
        (response) => { try { writeMessage(socket, response); } catch { /* gone */ } },
        (error) => { try { writeMessage(socket, { type: "result", requestId: request.requestId, ok: false, error: codeOf(error) }); } catch { /* gone */ } },
      );
    }, () => socket.destroy()));
  });
  await new Promise<void>((resolve, reject) => {
    server!.once("error", reject);
    server!.listen(paths.control, () => resolve());
  });
  log(`keeper ${process.pid} up as ${localLabel}`);
  process.on("SIGTERM", () => void stop("SIGTERM"));
  process.on("SIGINT", () => void stop("SIGINT"));
  armIdle();

  while (!stopping.signal.aborted) {
    await reconcile();
    await sleep(DISCOVER_MS, stopping.signal);
  }
}

void main().catch((error) => {
  log(`failed: ${error instanceof Error ? error.stack ?? error.message : String(error)}`);
  process.exit(1);
});
