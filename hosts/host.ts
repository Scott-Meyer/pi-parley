// pi-parley/host: Parley inside an application that isn't Pi.
//
// Each of the application's agent sessions gets a hosted session: the same Parley a Pi
// session has (the `parley` tool, references, receipts, history, names, links),
// joined under the id the application gives it. The application puts what Parley
// delivers in front of its agent and says whether it took it; calls `context()` just
// before each model call; and reports turns, renames, models and compaction.
//
// Many hosted sessions can share one process. configureParleyHost() says how that
// process starts and locks the computer's broker, and runBroker() lets the
// application's own entry run the broker, for a bundled app that can't ship
// Parley's broker script beside it. Build time: hosts/bundle-define.mjs.
export {
  createHostedSession,
  startHostedSession,
  type Delivery,
  type DeliveryOutcome,
  type HostedContextMessage,
  type HostedSession,
  type HostedSessionOptions,
  type ToolOutcome,
} from "./hosted-session.ts";
export { configureParleyHost, type BrokerLauncher, type ParleyHostProcessOptions } from "../broker/host-process.ts";
export { runBroker } from "../broker/runtime.ts";
