// Which computers is this one connected to right now over SSH?
//
// Terminals, editors' remote modes and other tools run `ssh <host> …`; those
// processes say where the person already goes and how they get there. Only
// connection options are kept (see ssh.ts), so repeating them never repeats a
// port forward, a terminal or a remote command. Tools that use ssh as a pipe
// for something else (git, rsync, scp, sftp) aren't computers anyone works on.
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { keptOption, type SshDestination } from "./ssh.ts";

const run = promisify(execFile);

/** Every ssh option letter that takes a value, kept or not. */
const OPTIONS_WITH_VALUE = "BbcDEeFIiJLlmOoPpQRSWw";
/** A process with one of these isn't a connection to a computer someone uses:
 * a stdio forward, a mux control command, config printing, a subsystem. */
const NOT_A_SESSION = "WOGQVs";
const PIPE_COMMANDS = /^(git-[a-z-]+|scp|rsync|sftp|sftp-server|internal-sftp)\b/;

export interface SshProcess { readonly pid: number; readonly argv: readonly string[] }

/** Parses one ssh process's arguments. Undefined when it isn't a session to a
 * computer (see above) or can't be read confidently. */
export function destinationFromArgv(argv: readonly string[]): SshDestination | undefined {
  const program = argv[0]?.split(/[\\/]/).pop()?.toLowerCase();
  if (program !== "ssh" && program !== "ssh.exe") return undefined;
  const options: string[] = [];
  for (let i = 1; i < argv.length; i++) {
    const token = argv[i]!;
    if (token === "--") { return destinationAt(argv, i + 1, options); }
    if (!token.startsWith("-") || token.length < 2) return destinationAt(argv, i, options);
    // Flags can be bundled (-tt, -vvA); the first letter taking a value ends
    // the bundle, with its value either attached or in the next argument.
    for (let j = 1; j < token.length; j++) {
      const letter = token[j]!;
      if (NOT_A_SESSION.includes(letter)) return undefined;
      if (OPTIONS_WITH_VALUE.includes(letter)) {
        let value = token.slice(j + 1);
        if (!value) { value = argv[++i] ?? ""; if (!value) return undefined; }
        if (keptOption(letter, value)) options.push(`-${letter}`, value);
        break;
      }
      if (keptOption(letter)) options.push(`-${letter}`);
    }
  }
  return undefined;
}

function destinationAt(argv: readonly string[], index: number, options: string[]): SshDestination | undefined {
  const host = argv[index];
  if (!host || host.startsWith("-")) return undefined;
  const command = argv.slice(index + 1).join(" ").trim();
  if (command && PIPE_COMMANDS.test(command)) return undefined;
  if (/^git@/.test(host)) return undefined;
  return Object.freeze({ options: Object.freeze(options), host });
}

/** Running ssh processes for this user. ps joins arguments with spaces, so
 * arguments containing spaces can split; options rarely do, and a misread
 * destination only costs one failed check. */
export async function listSshProcesses(): Promise<SshProcess[]> {
  if (process.platform === "win32") {
    const { stdout } = await run("powershell.exe", [
      "-NoProfile", "-NonInteractive", "-Command",
      "Get-CimInstance Win32_Process -Filter \"Name='ssh.exe'\" | ForEach-Object { \"$($_.ProcessId) $($_.CommandLine)\" }",
    ], { timeout: 10_000, windowsHide: true, maxBuffer: 4 * 1024 * 1024 });
    return parseProcessLines(stdout, (line) => line.replace(/"/g, "").split(/\s+/));
  }
  const { stdout } = await run("ps", ["-x", "-ww", "-o", "pid=,args="], { timeout: 10_000, maxBuffer: 16 * 1024 * 1024 });
  return parseProcessLines(stdout, (line) => line.split(/\s+/));
}

function parseProcessLines(stdout: string, split: (line: string) => string[]): SshProcess[] {
  const found: SshProcess[] = [];
  for (const raw of stdout.split("\n")) {
    const line = raw.trim();
    const match = /^(\d+)\s+(.*)$/.exec(line);
    if (!match) continue;
    const argv = split(match[2]!).filter(Boolean);
    const program = argv[0]?.split(/[\\/]/).pop()?.toLowerCase();
    if (program === "ssh" || program === "ssh.exe") found.push({ pid: Number(match[1]), argv });
  }
  return found;
}
