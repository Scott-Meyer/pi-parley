import { existsSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname, extname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const repo = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const dist = join(repo, "dist");
const tsc = join(repo, "node_modules", "typescript", "bin", "tsc");

rmSync(dist, { recursive: true, force: true });
const compiled = spawnSync(process.execPath, [tsc, "-p", join(repo, "tsconfig.federation.json")], {
  cwd: repo,
  stdio: "inherit",
});
if (compiled.status !== 0) process.exit(compiled.status ?? 1);

function filesUnder(directory) {
  return readdirSync(directory).flatMap((name) => {
    const path = join(directory, name);
    return statSync(path).isDirectory() ? filesUnder(path) : [path];
  });
}

for (const required of ["federation.js", "federation.d.ts"]) {
  if (!existsSync(join(dist, required))) throw new Error(`Federation build omitted dist/${required}`);
}

const emittedFiles = filesUnder(dist);
const declarationSpecifier = /((?:\bfrom\s*|\bimport\s*\()\s*["'])(\.[^"']+)\.(?:[cm]?ts|tsx)(["'])/g;
for (const file of emittedFiles.filter((path) => path.endsWith(".d.ts"))) {
  const source = readFileSync(file, "utf8");
  const normalized = source.replace(declarationSpecifier, "$1$2.js$3");
  if (normalized !== source) writeFileSync(file, normalized);
}

const relativeSpecifier = /(?:\bfrom\s*|\bimport\s*\()\s*["'](\.[^"']+)["']/g;
for (const file of emittedFiles.filter((path) => path.endsWith(".js") || path.endsWith(".d.ts"))) {
  const source = readFileSync(file, "utf8");
  for (const match of source.matchAll(relativeSpecifier)) {
    const specifier = match[1];
    if (!specifier) continue;
    if (/\.(?:[cm]?ts|tsx)$/.test(specifier)) {
      throw new Error(`${file} retains raw TypeScript specifier ${specifier}`);
    }
    if (extname(specifier) === ".js" && !existsSync(resolve(dirname(file), specifier))) {
      throw new Error(`${file} references missing compiled module ${specifier}`);
    }
  }
}
