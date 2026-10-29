// How this process starts and locks Parley's broker, when an application that
// embeds Parley needs something other than the defaults: a GUI app whose
// runtime isn't `node`, or one that ships the native lock addon on its own.
// It's per process because the broker is: one per computer, started by whichever
// session needs it first.

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

let current: ParleyHostProcessOptions = {};

/** Set how this process starts and locks Parley's broker. Call before any session starts. */
export function configureParleyHost(options: ParleyHostProcessOptions): void {
  current = { ...options, ...(options.brokerLauncher ? { brokerLauncher: { ...options.brokerLauncher, args: [...options.brokerLauncher.args] } } : {}) };
}

export function hostProcessOptions(): ParleyHostProcessOptions {
  return current;
}
