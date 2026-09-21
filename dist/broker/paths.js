import { createHash } from "crypto";
import { chmodSync, mkdirSync, readFileSync } from "fs";
import { isAbsolute, join, resolve, win32 } from "path";
import { homedir } from "os";
export const PARLEY_DIR_MODE = 0o700;
export const PARLEY_RUNTIME_FILE_MODE = 0o600;
export const PARLEY_TCP_HOST = "127.0.0.1";
export const PARLEY_PROTOCOL_NAME = "pi-parley";
export const PARLEY_PROTOCOL_VERSION = 1;
/** Bounded, collision-resistant Windows IPC namespace for one agent root.
 * Normalize Windows' case-insensitive path spelling before hashing so every
 * participant derives the same broker name without exposing or truncating it. */
export function getBrokerPipeName(agentDir) {
    let normalized = win32.normalize(agentDir).toLowerCase();
    const rootLength = win32.parse(normalized).root.length;
    while (normalized.length > rootLength && normalized.endsWith("\\")) {
        normalized = normalized.slice(0, -1);
    }
    const digest = createHash("sha256")
        .update("pi-parley-agent-dir\0", "utf8")
        .update(normalized, "utf8")
        .digest("hex");
    return `pi-parley-${digest}`;
}
export function getAgentDirPath(env = process.env, homeDir = homedir(), cwd = process.cwd()) {
    const configured = env.PI_CODING_AGENT_DIR?.trim();
    if (!configured) {
        return join(homeDir, ".pi/agent");
    }
    return isAbsolute(configured) ? configured : resolve(cwd, configured);
}
export function getParleyDirPath(agentDir = getAgentDirPath()) {
    return join(agentDir, "parley");
}
export function shouldUseTcpTransport(env = process.env, platform = process.platform) {
    // One Windows installation uses one default endpoint for every client.
    // Authenticated loopback can also be carried by FlightDeck; named pipes cannot.
    // Choosing by terminal context would split clients sharing the same broker.
    const transport = env.PI_PARLEY_TRANSPORT?.trim().toLowerCase();
    if (transport === "tcp")
        return true;
    if (transport === "socket")
        return false;
    const tcpOptIn = env.PI_PARLEY_TCP?.trim().toLowerCase();
    if (tcpOptIn === "1" || tcpOptIn === "true")
        return true;
    if (tcpOptIn === "0" || tcpOptIn === "false")
        return false;
    return platform === "win32";
}
export function getBrokerPortFilePath(parleyDir = getParleyDirPath()) {
    return join(parleyDir, "broker.port.json");
}
export function getBrokerSocketPath(platform = process.platform, agentDir = getAgentDirPath()) {
    if (platform === "win32") {
        return `\\\\.\\pipe\\${getBrokerPipeName(agentDir)}`;
    }
    return join(getParleyDirPath(agentDir), "broker.sock");
}
/** Read one published authenticated loopback endpoint independently of transport
 * selection. Discovery is a snapshot of where to probe, not proof of ownership.
 * Missing, interrupted or invalid records throw. */
export function readBrokerTcpEndpoint(parleyDir = getParleyDirPath()) {
    const endpointFile = getBrokerPortFilePath(parleyDir);
    const parsed = JSON.parse(readFileSync(endpointFile, "utf-8"));
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
        throw new Error(`Invalid parley TCP endpoint at ${endpointFile}: expected a JSON object`);
    }
    const endpoint = parsed;
    if (endpoint.transport !== "tcp"
        || endpoint.host !== PARLEY_TCP_HOST
        || typeof endpoint.port !== "number"
        || !Number.isSafeInteger(endpoint.port)
        || endpoint.port <= 0
        || endpoint.port > 65535
        || typeof endpoint.stateId !== "string"
        || endpoint.stateId.length === 0) {
        throw new Error(`Invalid parley TCP endpoint at ${endpointFile}`);
    }
    return Object.freeze({ transport: "tcp", host: endpoint.host, port: endpoint.port, stateId: endpoint.stateId });
}
export function getBrokerConnectTarget(platform = process.platform, env = process.env, parleyDir = getParleyDirPath(getAgentDirPath(env))) {
    return shouldUseTcpTransport(env, platform)
        ? readBrokerTcpEndpoint(parleyDir)
        : getBrokerSocketPath(platform, getAgentDirPath(env));
}
export function getBrokerListenTarget(platform = process.platform, env = process.env) {
    if (shouldUseTcpTransport(env, platform)) {
        return { transport: "tcp", host: PARLEY_TCP_HOST, port: 0 };
    }
    return getBrokerSocketPath(platform, getAgentDirPath(env));
}
export function ensureParleyRuntimeDir(parleyDir = getParleyDirPath(), platform = process.platform) {
    mkdirSync(parleyDir, { recursive: true, mode: PARLEY_DIR_MODE });
    if (platform !== "win32") {
        chmodSync(parleyDir, PARLEY_DIR_MODE);
    }
}
export function restrictParleyRuntimeFile(filePath, platform = process.platform) {
    if (platform !== "win32") {
        chmodSync(filePath, PARLEY_RUNTIME_FILE_MODE);
    }
}
