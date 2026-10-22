#!/usr/bin/env node
// A stand-in for OpenSSH in tests: "computers" are home directories on this
// machine, named in PI_PARLEY_TEST_SSH_HOMES ({"box": "/tmp/…/box-home"}).
// Commands run with HOME set to that directory; -W forwards stdio to a local
// Unix socket or loopback port, like `ssh -W` does on the far side.
import net from "node:net";
import { spawn } from "node:child_process";

const args = process.argv.slice(2);
const homes = JSON.parse(process.env.PI_PARLEY_TEST_SSH_HOMES ?? "{}");
const withValue = "BbcDEeFIiJLlmOoPpQRSWw";
let forward;
let printConfig = false;
let i = 0;
for (; i < args.length && args[i].startsWith("-"); i++) {
  const letter = args[i][1];
  if (letter === "G") { printConfig = true; continue; }
  if (withValue.includes(letter)) {
    const value = args[i].length > 2 ? args[i].slice(2) : args[++i];
    if (letter === "W") forward = value;
  }
}
const host = args[i] ?? "";
const command = args.slice(i + 1).join(" ");
if (printConfig) { process.stdout.write(`hostname ${host}\ncontrolpath none\n`); process.exit(0); }
const home = homes[host.replace(/^.*@/, "")];
if (!home) {
  process.stderr.write(`ssh: Could not resolve hostname ${host}: nodename nor servname provided, or not known\n`);
  process.exit(255);
}
if (forward) {
  const tcp = /^127\.0\.0\.1:(\d+)$/.exec(forward);
  const socket = tcp ? net.connect({ host: "127.0.0.1", port: Number(tcp[1]) }) : net.connect(forward);
  socket.on("error", () => { process.stderr.write("Stdio forwarding request failed: Session open refused by peer\n"); process.exit(255); });
  socket.on("connect", () => { process.stdin.pipe(socket); socket.pipe(process.stdout); });
  socket.on("close", () => process.exit(0));
  process.stdin.on("end", () => socket.end());
} else {
  spawn("sh", ["-c", command], { stdio: "inherit", env: { ...process.env, HOME: home } })
    .on("exit", (code) => process.exit(code ?? 1));
}
