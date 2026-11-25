import { Text } from "@mariozechner/pi-tui";
import { ClickToExpand } from "./click-to-expand.ts";
import { ComposeOverlay } from "./compose.ts";
import type { ParleyUI } from "./host.ts";
import { InlineMessageComponent, messageExpanded } from "./inline-message.ts";
import { SessionListOverlay } from "./session-list.ts";

export const piUI: ParleyUI = {
  shortcut: "alt+m",
  pickSession: (ctx, self, sessions, reference, origin) => ctx.ui.custom(
    (_tui, theme, keybindings, done) => new SessionListOverlay(theme, keybindings, self, sessions, done, reference, origin),
    { overlay: true, overlayOptions: { width: 88 } },
  ),
  compose: (ctx, target, label, client) => ctx.ui.custom(
    (tui, theme, keybindings, done) => new ComposeOverlay(tui, theme, keybindings, target, label, client, done),
    { overlay: true, overlayOptions: { width: 72 } },
  ),
  renderMessage: (from, message, theme, replyCommand, bodyText, expanded, references) =>
    new InlineMessageComponent(from, message, theme, replyCommand, bodyText, !messageExpanded(message.id, expanded), references),
  renderText(text, context) {
    const child = new Text(text, 0, 0);
    if (!context) return child;
    return new ClickToExpand(child, () => {
      const state = (context.state ?? {}) as { clickedOpen?: boolean };
      state.clickedOpen = !(state.clickedOpen ?? Boolean(context.expanded));
      context.state = state;
      context.invalidate?.();
    });
  },
};
