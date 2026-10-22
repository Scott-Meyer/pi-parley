// Reach another computer's Parley broker over the person's own OpenSSH.
//
// A destination is whatever the person would type after `ssh`: an alias from
// ~/.ssh/config, user@host, plus connection options such as -p or -J. This
// module turns one into a BrokerHostAccess for pi-parley/federation: bounded
// file reads run as small remote commands, and broker streams are `ssh -W`
// stdio forwards to the remote Unix socket or loopback port. Nothing is
// installed or listened on anywhere. It never prompts: a destination that
// needs a password or a host-key decision fails with a reason instead.
import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync } from "node:fs";
import { Duplex } from "node:stream";
import { posix, win32, join } from "node:path";
import type { BrokerHostAccess, BrokerLocalEndpoint } from "../dist/federation.js";

export interface SshDestination {
  /** Connection options, as typed: ["-p", "2222", "-J", "jump"]. */
  readonly options: readonly string[];
  /** Alias or [user@]host. */
  readonly host: string;
}

export type SshHostErrorCode =
  | "E_SSH_AUTH" // needs a password, key, or agent approval it could not get
  | "E_SSH_HOST_KEY" // unknown or changed host key; run ssh once yourself
  | "E_SSH_UNREACHABLE" // name, route, refusal or timeout
  | "E_SSH_FAILED" // ssh failed for another reason; detail has its words
  | "E_UNSUPPORTED_HOST" // no shell this module can talk to
  | "E_ABORTED";

export class SshHostError extends Error {
  constructor(readonly code: SshHostErrorCode, readonly detail?: string) {
    super(detail ? `${code}: ${detail}` : code);
    this.name = "SshHostError";
  }
}

export interface SshHost {
  readonly destination: SshDestination;
  readonly platform: BrokerHostAccess["platform"];
  readonly home: string;
  /** Whether Parley has ever run there (its state directory exists). */
  readonly hasParley: boolean;
  readonly access: BrokerHostAccess;
}

export interface SshOptions {
  /** Where this computer may keep SSH connection-sharing sockets. */
  readonly controlDir?: string;
  /** ssh executable; defaults to `ssh` on PATH. */
  readonly ssh?: string;
  readonly signal?: AbortSignal;
  readonly timeoutMs?: number;
  /** Told about every ssh child, so a caller can recognise its own processes. */
  readonly onSpawn?: (child: ChildProcess) => void;
}

type Shell = "posix" | "cmd" | "powershell";

/** Splits what someone typed after `ssh` into options and the destination.
 * A remote command is not accepted: this is a place to connect, not a command. */
export function parseSshDestination(input: string): SshDestination | undefined {
  const tokens = input.trim().replace(/^ssh\s+/, "").split(/\s+/).filter(Boolean);
  const options: string[] = [];
  let host: string | undefined;
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i]!;
    if (host === undefined && token.startsWith("-") && token.length > 1) {
      const letter = token[1]!;
      if (KEPT_FLAGS.includes(letter) && token.length === 2) { options.push(token); continue; }
      if (!KEPT_OPTIONS_WITH_VALUE.includes(letter)) return undefined;
      const value = token.length > 2 ? token.slice(2) : tokens[++i];
      if (value === undefined || (letter === "o" && !keptOption(letter, value))) return undefined;
      options.push(`-${letter}`, value);
      continue;
    }
    if (host !== undefined) return undefined;
    host = token;
  }
  if (!host || host.startsWith("-")) return undefined;
  return Object.freeze({ options: Object.freeze(options), host });
}

/** Options that say where ssh connects and as whom, worth repeating. Forwards,
 * terminals, agents, verbosity, backgrounding and connection sharing are not:
 * they belong to whatever session they came from. */
export const KEPT_OPTIONS_WITH_VALUE = "BFiJlop";
export const KEPT_FLAGS = "46";
const KEPT_CONFIG = /^(user|port|hostname|proxyjump|proxycommand|identityfile|identitiesonly|certificatefile|identityagent|addressfamily|bindaddress|bindinterface|hostkeyalias|userknownhostsfile)\s*[= ]/i;

/** Whether one option and its value is worth repeating (see above). -o keeps
 * only settings about where and as whom; -F keeps only lasting config files. */
export function keptOption(letter: string, value?: string): boolean {
  if (KEPT_FLAGS.includes(letter)) return value === undefined;
  if (!KEPT_OPTIONS_WITH_VALUE.includes(letter) || value === undefined) return false;
  if (letter === "o") return KEPT_CONFIG.test(value);
  if (letter === "F") return !/^(\/tmp\/|\/var\/folders\/|\/private\/|[A-Za-z]:\\.*\\Temp\\)/i.test(value);
  return true;
}

/** One stable spelling of a destination, for remembering and deduplication. */
export function destinationKey(destination: SshDestination): string {
  return [...destination.options, destination.host].join(" ");
}

/** What a person would call this computer: the host part of the destination. */
export function destinationLabel(destination: SshDestination): string {
  const at = destination.host.lastIndexOf("@");
  return at >= 0 ? destination.host.slice(at + 1) : destination.host;
}

/** Connects once to learn the computer's platform and home, without prompting. */
export async function openSshHost(destination: SshDestination, options: SshOptions = {}): Promise<SshHost> {
  const ssh = options.ssh ?? "ssh";
  const sharing = await connectionSharing(ssh, destination, options);
  const base = (mux: "auto" | "no") => [
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=10",
    "-o", "ServerAliveInterval=15",
    "-o", "ServerAliveCountMax=3",
    ...(sharing ? ["-o", `ControlMaster=${mux}`, ...sharing] : []),
    ...destination.options,
  ];
  const run = (command: string, input?: string, maxBytes = 64 * 1024) => runSsh(
    ssh, [...base("auto"), destination.host, command], { ...options, input, maxBytes },
  );

  // cmd expands %OS%; sh, fish and PowerShell print it literally.
  const first = await run("echo parley-probe %OS% %USERPROFILE%");
  let shell: Shell;
  let platform: SshHost["platform"];
  let home: string;
  const tokens = probeTokens(first.stdout);
  if (first.code === 0 && tokens[0] === "Windows_NT" && tokens[1]) {
    shell = "cmd"; platform = "win32"; home = tokens[1];
  } else {
    if (first.code === 255) throw sshFailure(first.stderr);
    const posixProbe = await run("sh -s", 'echo parley-probe "$(uname -s)" "$HOME"\n');
    const posixTokens = probeTokens(posixProbe.stdout);
    if (posixProbe.code === 0 && posixTokens[0] && posixTokens[1]) {
      const system = posixTokens[0];
      if (system === "Darwin") platform = "darwin";
      else if (system === "Linux") platform = "linux";
      else throw new SshHostError("E_UNSUPPORTED_HOST", system);
      shell = "posix"; home = posixTokens[1];
    } else {
      const ps = await run("echo parley-probe $env:OS $env:USERPROFILE");
      const psTokens = probeTokens(ps.stdout);
      if (ps.code !== 0 || psTokens[0] !== "Windows_NT" || !psTokens[1]) {
        throw new SshHostError("E_UNSUPPORTED_HOST");
      }
      shell = "powershell"; platform = "win32"; home = psTokens[1];
    }
  }

  const path = platform === "win32" ? win32 : posix;
  const agentDir = path.join(home, ".pi", "agent");
  const exists = await run(existsCommand(shell, path.join(agentDir, "parley")));
  const hasParley = exists.code === 0 && exists.stdout.includes("parley-yes");

  const readAgentFile: BrokerHostAccess["readAgentFile"] = async (relative, { maxBytes, signal }) => {
    if (relative.some((part) => part === ".." || part.includes("/") || part.includes("\\") || part.includes("\0"))) {
      throw new Error("path outside the agent directory");
    }
    const file = path.join(agentDir, ...relative);
    const result = await runSsh(ssh, [...base("auto"), destination.host, readCommand(shell, file, maxBytes)], {
      ...options, signal, maxBytes: maxBytes + 1,
    });
    if (result.code === 44) return undefined;
    if (result.code !== 0) throw new Error("remote read failed");
    if (result.bytes.byteLength > maxBytes) throw new Error("remote file too large");
    return result.bytes;
  };

  const openLocal: BrokerHostAccess["openLocal"] = async (endpoint: BrokerLocalEndpoint, { signal }) => {
    let target: string;
    if (endpoint.transport === "tcp") target = `127.0.0.1:${endpoint.port}`;
    else if (endpoint.transport === "unix") target = path.join(agentDir, ...endpoint.path);
    else throw new Error("named pipes cannot be carried by ssh");
    return forward(ssh, [...base("no"), "-W", target, destination.host], signal, options.onSpawn);
  };

  return Object.freeze({
    destination,
    platform,
    home,
    hasParley,
    access: Object.freeze({ platform, agentDir, readAgentFile, openLocal }),
  });
}

function probeTokens(stdout: string): string[] {
  // Login banners can precede the answer; PowerShell prints one word a line.
  const words = stdout.split(/\s+/).filter(Boolean);
  const start = words.lastIndexOf("parley-probe");
  return start < 0 ? [] : words.slice(start + 1);
}

function existsCommand(shell: Shell, dir: string): string {
  if (shell === "cmd") return `if exist "${dir}\\" (echo parley-yes) else (echo parley-no)`;
  if (shell === "powershell") return `if (Test-Path -LiteralPath '${dir}') { 'parley-yes' } else { 'parley-no' }`;
  return `test -d ${shQuote(dir)} && echo parley-yes || echo parley-no`;
}

function readCommand(shell: Shell, file: string, maxBytes: number): string {
  if (shell === "cmd") return `if exist "${file}" (type "${file}") else (exit /b 44)`;
  if (shell === "powershell") {
    return `if (Test-Path -LiteralPath '${file}') { [Console]::Out.Write([IO.File]::ReadAllText('${file}')) } else { exit 44 }`;
  }
  return `if [ -f ${shQuote(file)} ]; then head -c ${maxBytes} ${shQuote(file)}; else exit 44; fi`;
}

function shQuote(value: string): string {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

/** Share one SSH connection per computer, so a link's many short commands and
 * streams don't each log in. A destination the person already shares keeps
 * their settings; Windows' OpenSSH can't share at all. */
async function connectionSharing(ssh: string, destination: SshDestination, options: SshOptions): Promise<string[] | undefined> {
  if (process.platform === "win32" || !options.controlDir) return undefined;
  const config = await runSsh(ssh, ["-G", ...destination.options, destination.host], { ...options, maxBytes: 256 * 1024 });
  const controlPath = /^controlpath\s+(.+)$/m.exec(config.stdout)?.[1]?.trim();
  if (config.code === 0 && controlPath && controlPath !== "none") return [];
  mkdirSync(options.controlDir, { recursive: true, mode: 0o700 });
  // Unix socket paths are short; %C is already a hash of the connection.
  const tag = createHash("sha256").update(destinationKey(destination)).digest("hex").slice(0, 8);
  return ["-o", `ControlPath=${join(options.controlDir, `${tag}-%C`).slice(0, 100)}`, "-o", "ControlPersist=120"];
}

interface SshResult { code: number | null; stdout: string; stderr: string; bytes: Buffer }

function runSsh(
  ssh: string,
  args: string[],
  options: SshOptions & { input?: string; maxBytes: number },
): Promise<SshResult> {
  return new Promise((resolve, reject) => {
    if (options.signal?.aborted) { reject(new SshHostError("E_ABORTED")); return; }
    const child = spawn(ssh, args, { stdio: ["pipe", "pipe", "pipe"], windowsHide: true });
    options.onSpawn?.(child);
    const out: Buffer[] = [];
    let outBytes = 0;
    let stderr = "";
    let done = false;
    const finish = (value: SshResult | Error) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      options.signal?.removeEventListener("abort", onAbort);
      if (value instanceof Error) reject(value); else resolve(value);
    };
    const onAbort = () => { child.kill(); finish(new SshHostError("E_ABORTED")); };
    options.signal?.addEventListener("abort", onAbort, { once: true });
    const timer = setTimeout(() => {
      child.kill();
      finish(new SshHostError("E_SSH_UNREACHABLE", "timed out"));
    }, options.timeoutMs ?? 20_000);
    child.stdout.on("data", (chunk: Buffer) => {
      if (outBytes > options.maxBytes) return;
      out.push(chunk);
      outBytes += chunk.byteLength;
      if (outBytes > options.maxBytes) child.kill();
    });
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (text: string) => { if (stderr.length < 8_000) stderr += text; });
    child.on("error", (error) => finish(new SshHostError("E_SSH_FAILED", `could not run ssh: ${error.message}`)));
    child.on("close", (code) => {
      const bytes = Buffer.concat(out);
      finish({ code, stdout: bytes.toString("utf8"), stderr, bytes });
    });
    child.stdin.on("error", () => {});
    child.stdin.end(options.input ?? "");
  });
}

function sshFailure(stderr: string): SshHostError {
  const text = stderr.trim();
  const line = text.split("\n").map((l) => l.trim()).filter(Boolean).pop() ?? "";
  if (/host key verification failed|remote host identification has changed|no .* host key is known/i.test(text)) {
    return new SshHostError("E_SSH_HOST_KEY", line);
  }
  if (/permission denied|too many authentication failures|agent refused|sign_and_send_pubkey|authentication/i.test(text)) {
    return new SshHostError("E_SSH_AUTH", line);
  }
  if (/could not resolve|connection refused|timed out|no route|network is unreachable|connection closed|unknown host|name or service/i.test(text)) {
    return new SshHostError("E_SSH_UNREACHABLE", line);
  }
  return new SshHostError("E_SSH_FAILED", line || undefined);
}

/** A byte stream to one endpoint on the remote computer. ssh can't say when
 * the far end connects, so this resolves at once; a failed forward simply
 * ends the stream, which Parley reads as "no broker there". */
function forward(
  ssh: string,
  args: string[],
  signal: AbortSignal,
  onSpawn?: (child: ChildProcess) => void,
): Promise<Duplex> {
  if (signal.aborted) return Promise.reject(new Error("aborted"));
  const child = spawn(ssh, args, { stdio: ["pipe", "pipe", "ignore"], windowsHide: true });
  onSpawn?.(child);
  const stream = new Duplex({
    allowHalfOpen: true,
    read() { child.stdout.resume(); },
    write(chunk, _encoding, callback) { child.stdin.write(chunk, callback); },
    final(callback) { child.stdin.end(callback); },
    destroy(error, callback) {
      if (child.exitCode === null && child.signalCode === null) child.kill();
      callback(error);
    },
  });
  let ended = false;
  const end = () => { if (!ended) { ended = true; stream.push(null); } };
  child.stdout.on("data", (chunk) => { if (!ended && !stream.push(chunk)) child.stdout.pause(); });
  child.stdout.on("end", end);
  child.on("close", end);
  child.on("error", () => stream.destroy(new Error("ssh could not start")));
  child.stdin.on("error", () => {});
  child.stdout.on("error", () => {});
  return Promise.resolve(stream);
}
