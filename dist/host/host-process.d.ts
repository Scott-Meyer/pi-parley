export interface BrokerLauncher {
    readonly command: string;
    readonly args: readonly string[];
    /** Added to this process's environment for the broker. */
    readonly env?: Readonly<Record<string, string>>;
}
export interface ParleyHostProcessOptions {
    /** How this process starts a broker. Default: Node with Parley's broker script. */
    readonly brokerLauncher?: BrokerLauncher;
    /** false skips the client's startup lock. Safe: the broker's own runtime lock decides
     * between competing brokers, and a losing one exits and its client waits for the winner. */
    readonly startupLock?: boolean;
    /** Path to fs-native-extensions' native binary (the .node file), for a process that
     * can't resolve the package itself. */
    readonly lockAddon?: string;
}
/** Set how this process starts and locks Parley's broker. Call before any session starts. */
export declare function configureParleyHost(options: ParleyHostProcessOptions): void;
export declare function hostProcessOptions(): ParleyHostProcessOptions;
