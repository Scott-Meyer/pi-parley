/** Run this process as the computer's Parley broker. It exits when it's done:
 * idle with no sessions, signalled, or because another broker owns the runtime
 * (BROKER_RUNTIME_OCCUPIED_EXIT_CODE). broker/broker.ts is the usual entry;
 * a host that embeds Parley can call this from its own entry instead. */
export declare function runBroker(): Promise<void>;
