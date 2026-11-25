import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { registerParleyExtension } from "./parley-extension.ts";
import { sendPersonMessage } from "./ui/host.ts";

export default function (pi: ExtensionAPI): void {
  registerParleyExtension(pi, {
    async pickSession(ctx, _self, sessions, reference) {
      if (sessions.length === 0) {
        ctx.ui.notify("No other Parley-connected sessions", "info");
        return;
      }
      const targets = new Map(sessions.map((session) => [reference(session), session]));
      const choice = await ctx.ui.select("Parley: choose a session", [...targets.keys()]);
      return choice === undefined ? undefined : targets.get(choice);
    },
    async compose(ctx, target, label, client) {
      const text = (await ctx.ui.input(`Parley: send to ${label}`))?.trim();
      if (text) return sendPersonMessage(client, target, text);
    },
  });
}
