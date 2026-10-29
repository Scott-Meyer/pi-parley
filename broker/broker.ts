// The Parley broker process: one per computer (per Pi agent directory), started
// by the first session that needs it. Its logic is runtime.ts.
import { runBroker } from "./runtime.ts";

await runBroker();
