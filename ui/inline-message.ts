import type { Component } from "@mariozechner/pi-tui";
import { truncateToWidth, visibleWidth, wrapTextWithAnsi } from "@mariozechner/pi-tui";
import type { Theme } from "@mariozechner/pi-coding-agent";
import type { SessionInfo, Message } from "../types.ts";
import { formatPeerCompactionNotice } from "../compaction-awareness.ts";

/** Human-readable names for sessions and messages; defaults keep raw identities. */
export interface InlineMessageReferences {
  session(from: SessionInfo): string;
  message(id: string): string;
  /** Replace canonical identities inside generated text. */
  present?(text: string): string;
  /** Typed naming for a session identity, as the compaction notice needs it. */
  sessionName?(id: string, name?: string): string;
}

export class InlineMessageComponent implements Component {
  private from: SessionInfo;
  private message: Message;
  private theme: Theme;
  private replyCommand?: string;
  private bodyText?: string;
  private collapsed: boolean;
  // Caches assume message/bodyText never mutate after construction; theme
  // styling stays outside the caches so live theme changes apply per render.
  private collapsedPreview?: string;
  private wrappedBody?: { width: number; lines: string[] };

  constructor(
    from: SessionInfo,
    message: Message,
    theme: Theme,
    replyCommand?: string,
    bodyText?: string,
    collapsed = false,
    private readonly references?: InlineMessageReferences,
  ) {
    this.from = from;
    this.message = message;
    this.theme = theme;
    this.replyCommand = replyCommand;
    this.bodyText = bodyText;
    this.collapsed = collapsed;
  }

  invalidate(): void {}

  render(width: number): string[] {
    const lines: string[] = [];
    const borderChar = "─";
    const senderName = this.references?.session(this.from) ?? (this.from.name || this.from.id.slice(0, 8));
    const replyToLabel = this.message.replyTo
      ? this.references?.message(this.message.replyTo) ?? this.message.replyTo.slice(0, 8)
      : undefined;
    const messageLabel = this.references ? ` · ${this.references.message(this.message.id)}` : "";
    if (width < 3) {
      return [truncateToWidth(`From ${senderName}`, width)];
    }
    const bodyWidth = Math.max(1, width - 2);

    const header = ` From: ${senderName} (${this.from.cwd})${messageLabel} `;
    const headerText = truncateToWidth(this.collapsed ? `${header} Ctrl+O expands ` : header, bodyWidth, "");
    const headerPadding = Math.max(0, bodyWidth - visibleWidth(headerText));
    lines.push(
      this.theme.fg("muted", "╭") +
        this.theme.fg("toolTitle", headerText) +
        this.theme.fg("muted", `${borderChar.repeat(headerPadding)}╮`),
    );

    const frameLine = (content: string): string => {
      const text = truncateToWidth(content, bodyWidth, "");
      const padding = Math.max(0, bodyWidth - visibleWidth(text));
      return this.theme.fg("muted", "│") + text + this.theme.fg("muted", `${" ".repeat(padding)}│`);
    };

    if (this.collapsed) {
      this.collapsedPreview ??= (this.bodyText || this.message.content.text).replace(/\s+/g, " ").trim();
      lines.push(frameLine(this.theme.fg("text", this.collapsedPreview)));

      const meta: string[] = [];
      if (this.replyCommand) meta.push(`To reply: ${this.replyCommand}`);
      if (this.message.content.attachments?.length) {
        const count = this.message.content.attachments.length;
        meta.push(`${count} attachment${count === 1 ? "" : "s"}`);
      }
      if (this.message.provenance?.type === "extension_outbox") meta.push(`Via ${this.message.provenance.extensionName}`);
      if (this.message.provenance?.type === "session_person") meta.push("Written by the person there");
      if (this.message.replyTo && !this.message.expectsReply) meta.push(`Reply to ${replyToLabel}`);
      if (this.message.peerCompaction) meta.push("Sender compacted since prior direct contact");
      meta.push("Ctrl+O to expand");

      lines.push(frameLine(this.theme.fg("dim", ` ${meta.join(" · ")}`)));
      lines.push(this.theme.fg("muted", `╰${borderChar.repeat(bodyWidth)}╯`));
      return lines;
    }

    if (this.message.peerCompaction) {
      const sessionName = this.references?.sessionName?.bind(this.references);
      const rawNotice = formatPeerCompactionNotice(senderName, this.message.peerCompaction, this.from.id, sessionName);
      const notice = sessionName ? rawNotice : this.references?.present?.(rawNotice) ?? rawNotice;
      for (const line of wrapTextWithAnsi(this.theme.fg("warning", ` ${notice}`), bodyWidth)) {
        lines.push(frameLine(line));
      }
      lines.push(frameLine(""));
    }

    if (this.wrappedBody?.width !== bodyWidth) {
      this.wrappedBody = {
        width: bodyWidth,
        lines: wrapTextWithAnsi(this.bodyText || this.message.content.text, bodyWidth),
      };
    }
    for (const line of this.wrappedBody.lines) {
      lines.push(frameLine(this.theme.fg("text", line)));
    }

    if (this.replyCommand) {
      lines.push(frameLine(""));
      const replyLines = wrapTextWithAnsi(this.theme.fg("dim", ` To reply: ${this.replyCommand}`), bodyWidth);
      for (const line of replyLines) {
        lines.push(frameLine(line));
      }
    }

    if (this.message.content.attachments?.length) {
      lines.push(frameLine(""));
      for (const att of this.message.content.attachments) {
        lines.push(frameLine(this.theme.fg("dim", ` Attachment: ${att.name}`)));
      }
    }

    if (this.message.replyTo && !this.message.expectsReply) {
      lines.push(frameLine(""));
      lines.push(frameLine(this.theme.fg("dim", ` Reply to ${replyToLabel}`)));
    }

    if (this.message.provenance?.type === "extension_outbox") {
      lines.push(frameLine(""));
      lines.push(frameLine(this.theme.fg("dim", ` Via extension: ${this.message.provenance.extensionName}`)));
    }
    if (this.message.provenance?.type === "session_person") {
      lines.push(frameLine(""));
      lines.push(frameLine(this.theme.fg("dim", " Written by the person at the sending session")));
    }

    lines.push(this.theme.fg("muted", `╰${borderChar.repeat(bodyWidth)}╯`));

    return lines;
  }
}
