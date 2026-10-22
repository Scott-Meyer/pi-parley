// Where this computer's link keeper keeps its state and listens.
import { createHash } from "node:crypto";
import { join, win32 } from "node:path";
import { getAgentDirPath, getParleyDirPath } from "../broker/paths.ts";

export interface LinkPaths {
  readonly parleyDir: string;
  /** Remembered computers and the discovery setting. Only the keeper writes it. */
  readonly config: string;
  /** The keeper's control endpoint: a Unix socket, or a named pipe on Windows. */
  readonly control: string;
  /** Held by the running keeper. */
  readonly lock: string;
  /** SSH connection-sharing sockets. */
  readonly sshControlDir: string;
  /** The keeper's own diagnostics. */
  readonly log: string;
}

export function getLinkPaths(env: NodeJS.ProcessEnv = process.env, platform: NodeJS.Platform = process.platform): LinkPaths {
  const agentDir = getAgentDirPath(env);
  const parleyDir = getParleyDirPath(agentDir);
  let control = join(parleyDir, "link.sock");
  if (platform === "win32") {
    const digest = createHash("sha256").update("pi-parley-link\0").update(win32.normalize(agentDir).toLowerCase()).digest("hex");
    control = `\\\\.\\pipe\\pi-parley-link-${digest}`;
  }
  return Object.freeze({
    parleyDir,
    config: join(parleyDir, "links.json"),
    control,
    lock: join(parleyDir, "link.keeper"),
    sshControlDir: join(parleyDir, "ssh"),
    log: join(parleyDir, "link.log"),
  });
}
