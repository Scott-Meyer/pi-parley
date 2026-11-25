import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { registerParleyExtension as register, type ParleyExtensionOptions } from "./parley-extension.ts";
import { piUI } from "./ui/pi.ts";

export { sessionIdPrefixes, sessionPlace } from "./parley-extension.ts";
export type { InboundDisplay, ParleyExtensionOptions, ParleyPresenceNameContext } from "./parley-extension.ts";

export function registerParleyExtension(pi: ExtensionAPI, options?: ParleyExtensionOptions): void {
  register(pi, piUI, options);
}

export default registerParleyExtension;
