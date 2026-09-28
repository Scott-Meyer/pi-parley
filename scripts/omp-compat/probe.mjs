// Two OMP sessions with Parley loaded exchange an ask and a reply through one broker.
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { readFileSync, existsSync } from "node:fs";

const [omp, extension, cwd, requestLog] = process.argv.slice(2);
const sessions = [];
function start(name) {
  const child = spawn(omp, ["--mode", "rpc", "--no-session", "--no-extensions", "-e", extension, "--model", "compat/compat-model", "--cwd", cwd],
    { env: process.env, stdio: ["pipe", "pipe", "pipe"] });
  const session = { name, child, events: [], stderr: "", ready: null };
  session.ready = new Promise((resolve) => {
    createInterface({ input: child.stdout }).on("line", (line) => {
      let event; try { event = JSON.parse(line); } catch { event = { invalid: line }; }
      session.events.push(event);
      if (event.type === "ready") resolve();
    });
  });
  child.stderr.on("data", (data) => { session.stderr += data; });
  session.prompt = (message) => child.stdin.write(`${JSON.stringify({ id: `${name}-${Date.now()}`, type: "prompt", message })}\n`);
  sessions.push(session);
  return session;
}
const requests = () => existsSync(requestLog) ? readFileSync(requestLog, "utf8").trim().split("\n").filter(Boolean).map((line) => JSON.parse(line)) : [];
const until = async (predicate, ms, what) => {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) { if (predicate()) return true; await new Promise((r) => setTimeout(r, 200)); }
  throw new Error(`timed out waiting for ${what}`);
};
const seen = (needle, decisionTool) => requests().some((request) => request.messages.some((m) => m.text.includes(needle)) && (!decisionTool || request.decision.tool?.args?.action === decisionTool));

let outcome = "fail";
try {
  const b = start("b"); await b.ready; b.prompt("OMP-B register");
  await until(() => requests().some((r) => r.decision.text?.startsWith("done:") && r.messages.some((m) => m.text.includes("OMP-B register"))), 30000, "B to register as omp-b");
  const a = start("a"); await a.ready; a.prompt("OMP-A ask");
  await until(() => seen("OMP-A ask", null) && requests().some((r) => r.messages.some((m) => m.text.includes("OMP-A ask")) && r.decision.text?.startsWith("done:")), 30000, "A's ask to be sent");
  await until(() => seen("OMP-PING-4417", "reply"), 30000, "B to receive the ask and reply");
  await until(() => requests().some((r) => r.decision.text === "received the answer"), 30000, "A to receive the reply");
  outcome = "pass";
} catch (error) {
  console.log(`FAILED: ${error.message}`);
} finally {
  for (const s of sessions) s.child.kill("SIGTERM");
  const summary = requests().map((r) => `${r.at} → ${r.decision.tool ? `tool ${JSON.stringify(r.decision.tool.args)}` : `text ${r.decision.text}`}`);
  console.log(summary.join("\n"));
  for (const s of sessions) if (s.stderr.trim()) console.log(`--- ${s.name} stderr\n${s.stderr.slice(-1500)}`);
  console.log(`OMP two-session ask/reply: ${outcome}`);
  process.exit(outcome === "pass" ? 0 : 1);
}
