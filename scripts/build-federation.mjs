import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
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

// pi-parley/host ships as TypeScript source, which applications bundle; its types are
// declared here too, so an application's own type checking stops at Parley's boundary.
// The public types live in four files that import only each other.
{
  const tmp = join(dist, ".host-types");
  const emitted = spawnSync(process.execPath, [tsc, "--declaration", "--emitDeclarationOnly", "--outDir", tmp,
    "--module", "nodenext", "--moduleResolution", "nodenext", "--target", "es2022", "--strict", "--skipLibCheck",
    "--allowImportingTsExtensions", "--types", "node", join(repo, "hosts", "host.ts")], { cwd: repo, stdio: "inherit" });
  if (emitted.status !== 0) process.exit(emitted.status ?? 1);
  const surface = { "hosts/host": "host", "hosts/hosted-session": "hosted-session", "broker/host-process": "host-process", "broker/runtime": "runtime" };
  const out = join(dist, "host");
  mkdirSync(out, { recursive: true });
  for (const [source, name] of Object.entries(surface)) {
    let text = readFileSync(join(tmp, `${source}.d.ts`), "utf8");
    text = text.replace(/(\bfrom\s*["'])([^"']+)(["'])/g, (whole, open, specifier, close) => {
      const target = Object.entries(surface).find(([path]) => resolve(repo, dirname(source), specifier).replace(/\.ts$/, "") === join(repo, path));
      if (!target) throw new Error(`pi-parley/host's declarations reach outside their surface: ${source} imports ${specifier}`);
      return `${open}./${target[1]}.js${close}`;
    });
    writeFileSync(join(out, `${name}.d.ts`), text);
  }
  rmSync(tmp, { recursive: true, force: true });
}
