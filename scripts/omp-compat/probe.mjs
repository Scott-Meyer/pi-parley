// Two OMP sessions with Parley loaded exchange an ask and a reply through one broker.
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";

export async function runProbe(label, [omp, extension, cwd, requestLog], check) {
  const sessions = [];
  async function until(predicate, ms, what) {
    const deadline = Date.now() + ms;
    do {
      for (const s of sessions) if (s.failure) throw new Error(`${s.name}: ${s.failure}`);
      if (predicate()) return;
      await new Promise((resolve) => setTimeout(resolve, 100));
    } while (Date.now() < deadline);
    throw new Error(`timed out waiting for ${what}`);
  }

  async function start(name, home = process.env.HOME) {
    const child = spawn(omp, ["--mode", "rpc", "--no-ui", "--no-session", "--no-extensions", "--offline", "--tools", "parley", "-e", extension, "--model", "compat/compat-model", "--cwd", cwd],
      { env: { ...process.env, HOME: home }, stdio: ["pipe", "pipe", "pipe"] });
    const s = { name, child, stderr: "", output: [], results: [], completed: new Set() };
    sessions.push(s);
    const fail = (message) => { s.failure ??= message; };
    child.on("error", (error) => fail(`could not start OMP: ${error.message}`));
    child.on("exit", (code, signal) => fail(`OMP exited (${signal ?? code})`));
    child.stdin.on("error", (error) => fail(`RPC stdin: ${error.message}`));
    child.stderr.on("data", (data) => { s.stderr = (s.stderr + data).slice(-3000); });
    createInterface({ input: child.stdout }).on("line", (line) => {
      s.output.push(line.slice(0, 400));
      if (s.output.length > 10) s.output.shift();
      let event;
      try { event = JSON.parse(line); } catch { return; }
      if (event.type === "response" && !event.success) {
        fail(`${event.command} (${event.id ?? "uncorrelated"}): ${event.error}`);
      } else if (event.type === "response" && event.id === "startup" && event.command === "get_state") {
        const { model, dumpTools } = event.data ?? {};
        if (model?.provider !== "compat" || model?.id !== "compat-model") fail("RPC did not select the fixture model");
        else if (!dumpTools?.some((tool) => tool.name === "parley")) fail(`Parley tool missing after loading ${extension}`);
        else s.ready = true;
      } else if (event.type === "extension_error") {
        fail(`${event.extensionPath} (${event.event}): ${event.error}`);
      } else if (event.type === "prompt_result") {
        if (event.status === "completed") s.completed.add(event.id);
        else fail(`prompt ${event.id} ${event.status}: ${event.error?.message ?? "no detail"}`);
      } else if (event.type === "tool_execution_end" && event.toolName === "parley") {
        const text = event.result?.content?.map((part) => part.text ?? "").join("\n") ?? "";
        if (event.isError || event.result?.details?.error) fail(`Parley tool failed: ${text}`);
        s.results.push(text);
      } else if (event.type === "message_end" && event.message?.role === "assistant") {
        if (event.message.content?.some((part) => part.type === "text" && part.text === "received the answer")) s.receivedAnswer = true;
      }
    });
    const send = (command) => child.stdin.write(`${JSON.stringify(command)}\n`);
    // A correlated state response proves the command loop and extension startup are complete.
    send({ id: "startup", type: "get_state" });
    let promptId = 0;
    s.prompt = (message) => {
      const id = `${name}-${++promptId}`;
      send({ id, type: "prompt", message });
      return id;
    };
    await until(() => s.ready, 15000, `${name}'s RPC startup`);
    return s;
  }

  try {
    await check({ start, until });
    console.log(`${label}: pass`);
  } catch (error) {
    console.error(`FAILED: ${error.message}`);
    for (const s of sessions) {
      if (s.results.length) console.error(`--- ${s.name} Parley results\n${s.results.join("\n").slice(-3000)}`);
      if (s.output.length) console.error(`--- ${s.name} RPC tail\n${s.output.join("\n")}`);
      if (s.stderr.trim()) console.error(`--- ${s.name} stderr\n${s.stderr}`);
    }
    console.error(`Fake model requests: ${requestLog}`);
    console.error(`${label}: fail`);
    process.exitCode = 1;
  } finally {
    for (const { child } of sessions) child.kill("SIGTERM");
    const kill = setTimeout(() => {
      for (const { child } of sessions) child.kill("SIGKILL");
    }, 1000);
    kill.unref();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await runProbe("OMP two-session ask/reply", process.argv.slice(2), async ({ start, until }) => {
    const b = await start("b");
    const registration = b.prompt("OMP-B register");
    await until(() => b.completed.has(registration) && b.results.some((text) => text.includes("Session: omp-b")),
      30000, "B to register as omp-b");
    const a = await start("a");
    a.prompt("OMP-A ask");
    // The reply may arrive before A's next model call; check the ask receipt on RPC.
    await until(() => a.results.some((text) => text.includes("Ask sent")), 30000, "A's ask to be sent");
    await until(() => b.results.some((text) => text.includes("Reply sent")), 30000, "B to receive the ask and reply");
    await until(() => a.receivedAnswer, 30000, "A to receive the reply");
  });
}
