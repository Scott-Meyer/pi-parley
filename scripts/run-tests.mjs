#!/usr/bin/env node
// Runs the given test files with tsx's test runner, in an environment of
// their own. A Pi session (or FlightDeck tab) that runs the suite exports its
// Parley identity, scope and agent directory; tests that inherit them would
// join the caller's real broker namespace instead of their fixtures.
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

const env = { ...process.env };
for (const key of Object.keys(env)) {
  if ((key.startsWith("PI_PARLEY_") && !key.startsWith("PI_PARLEY_TEST_")) || key.startsWith("PI_SUBAGENT_")
    || key.startsWith("FLIGHTDECK_") || key === "PI_CODING_AGENT_DIR" || key === "PI_CLIENT_SESSION_ID") delete env[key];
}
const tsxCli = join(dirname(createRequire(import.meta.url).resolve("tsx")), "cli.mjs");
// Integration files each start real brokers and sessions; all of them at once
// on a busy machine starve each other past their deadlines.
const concurrency = env.PARLEY_TEST_CONCURRENCY ?? "4";
const child = spawn(process.execPath, [tsxCli, "--test", `--test-concurrency=${concurrency}`, ...process.argv.slice(2)], { stdio: "inherit", env });
child.on("exit", (code, signal) => process.exit(signal ? 1 : code ?? 1));
