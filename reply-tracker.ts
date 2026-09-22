import { getAskTimeoutMs } from "./config.ts";
import type { Message, SessionInfo } from "./types.ts";

export interface ParleyContext {
  from: SessionInfo;
  message: Message;
  receivedAt: number;
  disposition?: { state: "withdrawn" | "superseded"; replacementId?: string };
}

function senderMatchPriority(context: ParleyContext, to: string): number {
  if (context.from.id === to) return 0;
  if (context.from.name?.toLowerCase() === to.toLowerCase()) return 1;
  if (context.from.id.startsWith(to)) return 2;
  return 3;
}

function resolvePendingSender(pending: ParleyContext[], to: string, name: (to: string) => string = (value) => `"${value}"`): ParleyContext {
  const exactIdMatches = pending.filter((context) => senderMatchPriority(context, to) === 0);
  if (exactIdMatches.length === 1) {
    return exactIdMatches[0]!;
  }
  if (exactIdMatches.length > 1) {
    throw new Error(`Multiple pending asks from ${name(to)} — specify \`replyTo\` with the message reference`);
  }

  const exactNameMatches = pending.filter((context) => senderMatchPriority(context, to) === 1);
  if (exactNameMatches.length === 1) {
    return exactNameMatches[0]!;
  }
  if (exactNameMatches.length > 1) {
    throw new Error(`Multiple pending asks match sender name "${to}" — specify \`replyTo\` with the message reference`);
  }

  const idPrefixMatches = pending.filter((context) => senderMatchPriority(context, to) === 2);
  if (idPrefixMatches.length === 1) {
    return idPrefixMatches[0]!;
  }
  if (idPrefixMatches.length > 1) {
    throw new Error(`Multiple pending asks match ID prefix "${to}" — use a longer session ID prefix or specify \`replyTo\``);
  }

  throw new Error(`No pending ask from ${name(to)}`);
}

/** How conversation text names sessions and messages for a model. Defaults keep raw identities. */
export interface ConversationReferences {
  session(from: SessionInfo): string;
  message(id: string): string;
  /** False only when the sender is known to be absent from the current roster. */
  isReachable?(from: SessionInfo): boolean | undefined;
  /** Mark quoted peer text so presentation leaves it as written. */
  verbatim?(text: string): string;
}

const RAW_REFERENCES: ConversationReferences = {
  session: (from) => from.name || from.id,
  message: (id) => id,
};

export class ReplyTracker {
  private readonly messages = new Map<string, ParleyContext>();
  private readonly pendingAsks = new Map<string, ParleyContext>();
  private activeContexts: readonly ParleyContext[] = [];
  /** Requests already mentioned in automatic context while elapsed or unreachable. */
  private readonly quietAnnounced = new Set<string>();

  constructor(
    private readonly askTimeoutMs = getAskTimeoutMs(),
    private readonly references: ConversationReferences = RAW_REFERENCES,
  ) {}

  recordIncomingMessage(from: SessionInfo, message: Message, receivedAt = Date.now()): ParleyContext {
    const context = { from, message, receivedAt };
    this.messages.set(message.id, context);
    // Pending requests retain their full context until an explicit settlement.
    for (const id of this.messages.keys()) {
      if (this.messages.size <= 200) break;
      if (!this.pendingAsks.has(id) && id !== message.id) this.messages.delete(id);
    }
    if (message.expectsReply) {
      this.pendingAsks.set(message.id, context);
    }
    return context;
  }

  /** Newly surfaced messages replace the active conversation; tool-only iterations retain it.
   * Keep simultaneous candidates distinct from an absence of context. */
  activateContexts(contexts: readonly ParleyContext[]): void {
    if (contexts.length > 0) this.activeContexts = [...contexts];
  }

  /** A sender as the model knows it: its reference when this tracker has seen it, else the text given. */
  private senderName(to: string): string {
    for (const context of this.messages.values()) if (context.from.id === to) return this.references.session(context.from);
    return `"${to}"`;
  }

  clearActiveContexts(): void {
    this.activeContexts = [];
  }

  reset(): void {
    this.messages.clear();
    this.pendingAsks.clear();
    this.quietAnnounced.clear();
    this.clearActiveContexts();
  }

  /**
   * `exactSender` means `to` is a pinned identity: only that exact sender matches, never a
   * session whose name or ID merely overlaps it.
   */
  resolveReplyTarget(options: { to?: string; replyTo?: string; exactSender?: boolean }, now = Date.now()): ParleyContext {
    const priorityOf = (context: ParleyContext, to: string): number => {
      const priority = senderMatchPriority(context, to);
      return options.exactSender && priority !== 0 ? 3 : priority;
    };

    if (options.replyTo) {
      const target = this.messages.get(options.replyTo);
      if (!target) {
        throw new Error(`No retained message ${this.references.message(options.replyTo)}`);
      }
      if (options.to && priorityOf(target, options.to) === 3) {
        throw new Error(`Pending ask ${this.references.message(options.replyTo)} is not from ${this.senderName(options.to)}`);
      }
      return target;
    }

    const pending = Array.from(this.pendingAsks.values());
    if (options.to) {
      const candidates = [...this.activeContexts, ...pending];
      const priority = candidates.reduce((best, context) => Math.min(best, priorityOf(context, options.to!)), 3);
      const matches = candidates.filter((context) => priority < 3 && priorityOf(context, options.to!) === priority);
      const activeMatches = this.activeContexts.filter((context) => matches.includes(context));
      if (activeMatches.length > 0 && new Set(matches.map((context) => context.from.id)).size > 1) {
        throw new Error(`Multiple senders match ${this.senderName(options.to)} — specify \`replyTo\` with the message reference.`);
      }
      if (activeMatches.length > 1) {
        throw new Error(`Multiple active messages match ${this.senderName(options.to)} — specify \`replyTo\`.`);
      }
      // Naming the sender narrows the conversation; it must not redirect an
      // acknowledgment of a fresh note into an answer to an older question.
      if (activeMatches.length === 1) return activeMatches[0]!;
      if (pending.some((context) => priorityOf(context, options.to!) < 3)) {
        try {
          return resolvePendingSender(options.exactSender ? pending.filter((context) => context.from.id === options.to) : pending, options.to, (to) => this.senderName(to));
        } catch (error) {
          throw new Error(`${(error as Error).message}\n${this.formatConversationContext({ now, complete: true })}`);
        }
      }
      throw new Error(`No pending ask from ${this.senderName(options.to)}. Exact replyTo can identify a retained ordinary message.`);
    }

    if (this.activeContexts.length > 1) {
      throw new Error("Multiple active conversations — specify `to` or `replyTo`.");
    }
    if (this.activeContexts.length === 1) {
      return this.activeContexts[0]!;
    }

    if (pending.length === 1) {
      return pending[0]!;
    }
    if (pending.length === 0) {
      throw new Error("No active parley context to reply to");
    }

    throw new Error(`Multiple pending asks — specify \`to\` or \`replyTo\`.\n${this.formatConversationContext({ now, complete: true })}`);
  }

  findUniquePendingAskFrom(to: string, now = Date.now()): ParleyContext | null {
    const candidates = Array.from(this.pendingAsks.values()).filter((context) => {
      if (now - context.receivedAt > this.askTimeoutMs) {
        return false;
      }
      return context.from.id === to || context.from.name?.toLowerCase() === to.toLowerCase();
    });
    return candidates.length === 1 ? candidates[0]! : null;
  }

  getActiveReplyTarget(now = Date.now()): ParleyContext | null {
    const active = this.activeContexts.length === 1 ? this.activeContexts[0] : undefined;
    return active?.message.expectsReply ? active : null;
  }

  findActiveReplyTargetMismatch(to: string, now = Date.now()): ParleyContext | null {
    const activeReplyTarget = this.getActiveReplyTarget(now);
    if (!activeReplyTarget) {
      return null;
    }
    return activeReplyTarget.from.id === to ? null : activeReplyTarget;
  }

  markReplied(replyTo: string): void {
    this.dismissPendingAsk(replyTo);
  }

  dismissPendingAsk(replyTo: string): void {
    this.pendingAsks.delete(replyTo);
    this.activeContexts = this.activeContexts.filter((context) => context.message.id !== replyTo);
  }

  listPending(now = Date.now()): ParleyContext[] {
    return Array.from(this.pendingAsks.values()).sort((a, b) => a.receivedAt - b.receivedAt);
  }

  /** Retained text keeps its later withdrawal/replacement context. */
  setDisposition(messageId: string, disposition: NonNullable<ParleyContext["disposition"]>): void {
    const context = this.messages.get(messageId);
    if (context) this.messages.set(messageId, { ...context, disposition });
    this.dismissPendingAsk(messageId);
  }

  /** Full retained snapshot for recovery or explicit conversation threading. */
  getMessage(messageId: string): ParleyContext | undefined {
    return this.messages.get(messageId);
  }

  /** A response timeout is a waiting-window boundary, not completed or withdrawn work. */
  replyWindowElapsed(context: ParleyContext, now = Date.now()): boolean {
    return now > (context.message.replyDeadline ?? context.receivedAt + this.askTimeoutMs);
  }

  /**
   * Bounded adjacent context for action results; never selects or answers a request.
   * Automatic context mentions an elapsed or unreachable request once, then only counts
   * it: waiting is not settlement, and `complete` (pending, errors) always lists everything.
   */
  formatConversationContext(options: { limit?: number; previewLength?: number; now?: number; complete?: boolean } = {}): string {
    const now = options.now ?? Date.now();
    const limit = Math.max(1, Math.floor(options.limit ?? 3));
    const previewLength = Math.max(20, Math.min(300, options.previewLength ?? 120));
    const pending = this.listPending(now);
    const activeIds = new Set(this.activeContexts.map((context) => context.message.id));
    const contexts = [...this.activeContexts, ...pending.filter((item) => !activeIds.has(item.message.id))];
    if (contexts.length === 0) return "";
    const quote = this.references.verbatim ?? ((text: string) => text);
    const lines: string[] = [];
    let quieted = 0;
    let omitted = 0;
    for (const context of contexts) {
      const id = context.message.id;
      const awaiting = this.pendingAsks.has(id);
      const elapsed = awaiting && this.replyWindowElapsed(context, now);
      const unreachable = this.references.isReachable?.(context.from) === false;
      const quietKey = (elapsed || unreachable) && !activeIds.has(id) ? `${id}\0${elapsed ? "elapsed" : ""}\0${unreachable ? "unreachable" : ""}` : undefined;
      if (!options.complete && quietKey && this.quietAnnounced.has(quietKey)) {
        quieted += 1;
        continue;
      }
      if (lines.length >= limit) {
        omitted += 1;
        continue;
      }
      if (!options.complete && quietKey) this.quietAnnounced.add(quietKey);
      const preview = context.message.content.text.replace(/\s+/g, " ").trim();
      const status = awaiting
        ? elapsed ? "unanswered; reply window elapsed, not withdrawn" : "awaiting your reply"
        : "active conversation";
      const attachments = context.message.content.attachments?.length;
      lines.push(`- ${this.references.session(context.from)}, message ${this.references.message(id)} — ${status}${unreachable ? " (sender currently unreachable)" : ""}: ${quote(JSON.stringify(preview.length > previewLength ? `${preview.slice(0, previewLength - 1)}…` : preview))}${attachments ? ` [${attachments} attachment snapshot(s)]` : ""}${context.message.peerCompaction ? " [sender compacted since prior direct contact (notice at message arrival)]" : ""}`);
    }
    if (omitted > 0) lines.push(`- ${omitted} more conversation(s); pending has the complete request list.`);
    if (quieted > 0) lines.push(`- ${quieted} earlier unanswered request(s) already mentioned (reply window elapsed or sender unreachable); pending lists them.`);
    return `Conversation context (${pending.length} unanswered request${pending.length === 1 ? "" : "s"}):\n${lines.join("\n")}`;
  }
}
