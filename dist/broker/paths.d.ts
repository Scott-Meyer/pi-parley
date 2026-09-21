export declare const PARLEY_DIR_MODE = 448;
export declare const PARLEY_RUNTIME_FILE_MODE = 384;
export declare const PARLEY_TCP_HOST = "127.0.0.1";
export declare const PARLEY_PROTOCOL_NAME = "pi-parley";
export declare const PARLEY_PROTOCOL_VERSION = 1;
export interface BrokerTcpEndpoint {
    transport: "tcp";
    host: string;
    port: number;
    stateId?: string;
}
export type BrokerConnectTarget = string | BrokerTcpEndpoint;
/** Bounded, collision-resistant Windows IPC namespace for one agent root.
 * Normalize Windows' case-insensitive path spelling before hashing so every
 * participant derives the same broker name without exposing or truncating it. */
export declare function getBrokerPipeName(agentDir: string): string;
export declare function getAgentDirPath(env?: NodeJS.ProcessEnv, homeDir?: string, cwd?: string): string;
export declare function getParleyDirPath(agentDir?: string): string;
export declare function shouldUseTcpTransport(env?: NodeJS.ProcessEnv, platform?: NodeJS.Platform): boolean;
export declare function getBrokerPortFilePath(parleyDir?: string): string;
export declare function getBrokerSocketPath(platform?: NodeJS.Platform, agentDir?: string): string;
/** Read one published authenticated loopback endpoint independently of transport
 * selection. Discovery is a snapshot of where to probe, not proof of ownership.
 * Missing, interrupted or invalid records throw. */
export declare function readBrokerTcpEndpoint(parleyDir?: string): BrokerTcpEndpoint;
export declare function getBrokerConnectTarget(platform?: NodeJS.Platform, env?: NodeJS.ProcessEnv, parleyDir?: string): BrokerConnectTarget;
export declare function getBrokerListenTarget(platform?: NodeJS.Platform, env?: NodeJS.ProcessEnv): BrokerConnectTarget;
export declare function ensureParleyRuntimeDir(parleyDir?: string, platform?: NodeJS.Platform): void;
export declare function restrictParleyRuntimeFile(filePath: string, platform?: NodeJS.Platform): void;
