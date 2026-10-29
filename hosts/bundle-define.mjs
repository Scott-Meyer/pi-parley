// Build time, for an application that bundles Parley into its own files (esbuild and
// the like): the `define` that stamps in the identity of the Parley sources being
// bundled, so its broker reports the same build a broker started from source would.
//
//   import { parleyBundleDefine } from "pi-parley/host/bundle-define";
//   await esbuild.build({ ..., define: { ...parleyBundleDefine() } });
//
// Plain JavaScript, so a build script runs it on any Node; it asks Parley's own
// source (through tsx, a dependency of Parley) for the identity.
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");

export function parleyBundleIdentity() {
  const tsx = createRequire(join(root, "package.json")).resolve("tsx/cli");
  const build = pathToFileURL(join(root, "broker", "build.ts")).href;
  const result = spawnSync(process.execPath, [tsx, "-e",
    `import(${JSON.stringify(build)}).then((m) => console.log(JSON.stringify(m.getBrokerBuildIdentity(${JSON.stringify(root)}))))`,
  ], { encoding: "utf8" });
  if (result.status !== 0) throw new Error(`Couldn't work out Parley's build identity: ${result.stderr.trim()}`);
  return JSON.parse(result.stdout.trim().split("\n").pop());
}

export function parleyBundleDefine() {
  return { PARLEY_BUNDLE: JSON.stringify(JSON.stringify(parleyBundleIdentity())) };
}
