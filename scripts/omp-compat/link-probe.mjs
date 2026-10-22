// An OMP session on "laptop" links "box" and sees box's OMP session as box:omp-box.
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { readFileSync, existsSync } from "node:fs";

const [omp, extension, cwd, requestLog, laptopHome, boxHome] = process.argv.slice(2);
const sessions = [];
function start(name, home) {
  const child = spawn(omp, ["--mode", "rpc", "--no-session", "--no-extensions", "-e", extension, "--model", "compat/compat-model", "--cwd", cwd],
    { env: { ...process.env, HOME: home }, stdio: ["pipe", "pipe", "pipe"] });
  const session = { name, child, stderr: "", ready: null };
  session.ready = new Promise((resolve) => {
    createInterface({ input: child.stdout }).on("line", (line) => {
      let event; try { event = JSON.parse(line); } catch { return; }
      if (event.type === "ready") resolve();
    });
  });
  child.stderr.on("data", (data) => { session.stderr += data; });
  session.prompt = (message) => child.stdin.write(`${JSON.stringify({ id: `${name}-${Date.now()}`, type: "prompt", message })}\n`);
  sessions.push(session);
  return session;
}
const requests = () => existsSync(requestLog) ? readFileSync(requestLog, "utf8").trim().split("\n").filter(Boolean).map((line) => JSON.parse(line)) : [];
const toolResult = (pattern) => requests().some((r) => r.messages.some((m) => m.role === "tool" && pattern.test(m.text)));
const until = async (predicate, ms, what) => {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) { if (predicate()) return; await new Promise((r) => setTimeout(r, 200)); }
  throw new Error(`timed out waiting for ${what}`);
};

let outcome = "fail";
try {
  const box = start("box", boxHome); await box.ready; box.prompt("OMP-BOX register");
  await until(() => toolResult(/Session: omp-box/), 30000, "box's session to register");
  const laptop = start("laptop", laptopHome); await laptop.ready; laptop.prompt("OMP-LINK add");
  await until(() => toolResult(/box is linked/), 60000, "laptop to link box");
  laptop.prompt("OMP-LINK list");
  await until(() => toolResult(/• box:omp-box/), 30000, "box's session to appear on laptop as box:omp-box");
  outcome = "pass";
} catch (error) {
  console.log(`FAILED: ${error.message}`);
  for (const r of requests()) for (const m of r.messages) if (m.role === "tool") console.log(`tool: ${m.text.slice(0, 300).replace(/\n/g, " | ")}`);
} finally {
  for (const s of sessions) s.child.kill("SIGTERM");
  for (const s of sessions) if (s.stderr.trim()) console.log(`--- ${s.name} stderr\n${s.stderr.slice(-1500)}`);
  console.log(`OMP link over SSH: ${outcome}`);
  process.exit(outcome === "pass" ? 0 : 1);
}
