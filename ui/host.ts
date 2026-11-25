import type { ExtensionContext, Theme } from "@mariozechner/pi-coding-agent";
import type { ParleyClient } from "../broker/client.ts";
import { PERSON_PROVENANCE_FEATURE, type Message, type PeerCompactionNotice, type SessionInfo } from "../types.ts";

type Rows = { render(width: number): string[]; invalidate(): void };

/** Human-readable names for sessions and messages; defaults keep raw identities. */
export interface InlineMessageReferences {
  session(from: SessionInfo): string;
  message(id: string): string;
  /** Replace canonical identities inside generated text. */
  present?(text: string): string;
  /** Typed naming for a session identity, as the compaction notice needs it. */
  sessionName?(id: string, name?: string): string;
}

export interface ComposeResult {
  sent: boolean;
  messageId?: string;
  delivery?: string;
  text?: string;
  peerCompaction?: PeerCompactionNotice;
  contactToken?: string;
}

export interface ParleyUI {
  shortcut?: "alt+m";
  pickSession(ctx: ExtensionContext, self: SessionInfo, sessions: SessionInfo[], reference: (session: SessionInfo) => string, origin: (session: SessionInfo) => string | undefined): Promise<SessionInfo | undefined>;
  compose(ctx: ExtensionContext, target: SessionInfo, label: string, client: ParleyClient): Promise<ComposeResult | undefined>;
  renderText?(text: string, context?: { expanded?: boolean; state?: unknown; invalidate?: () => void }): Rows;
  renderMessage?(from: SessionInfo, message: Message, theme: Theme, replyCommand: string | undefined, bodyText: string | undefined, expanded: boolean, references: InlineMessageReferences): Rows;
}

export async function sendPersonMessage(client: ParleyClient, target: SessionInfo, text: string): Promise<ComposeResult> {
  const result = await client.send(target.id, {
    text,
    // The person typed this, not the agent. Older brokers would refuse the field.
    ...(client.supportsFeature(PERSON_PROVENANCE_FEATURE) ? { provenance: { type: "session_person" as const } } : {}),
  });
  if (!result.delivered) throw new Error(result.reason ?? "Message not delivered. Session may not exist or has disconnected.");
  return {
    sent: true,
    messageId: result.id,
    text,
    delivery: result.delivery,
    ...(result.peerCompaction ? { peerCompaction: result.peerCompaction } : {}),
    ...(result.contactToken ? { contactToken: result.contactToken } : {}),
  };
}
