import { randomBytes } from "node:crypto";
import { decodeOriginQualifiedSessionIdentity } from "./broker/federation-protocol.ts";
import { decodeConversationMessageId } from "./broker/federation-conversation.ts";

/**
 * Human-readable references for everything a model sees or types.
 *
 * Canonical identities (local UUIDs, origin-qualified `oqs1.`/`oqm1.` handles)
 * belong to the wire, to `details`, and to programmatic callers. Models get a
 * session's name (`pi:parley`, `FlightDeck VM:parley`) and message numbers
 * (`#12`), and this book owns the translation in both directions.
 *
 * A reference is pinned to the identity it was first allocated for and is never
 * recycled, not across renames, reconnects, restores, or a different session
 * later taking the same name. That session receives `name~2` instead. Old names
 * stay reserved as aliases of the identity that held them, so a transcript that
 * says `alice` resolves to that original session or fails honestly as offline;
 * it never silently means a successor.
 */

export const REFERENCE_ENTRY_TYPE = "parley_reference";

export type ReferenceRecord =
  | { kind: "session"; ref: string; id: string; base: string }
  | { kind: "message"; ref: string; id: string; number?: number; label?: boolean }
  | { kind: "origin"; ref: string; id: string };

export interface ReferencedSession {
  id: string;
  name?: string;
  runtimeFallbackAlias?: boolean;
  federation?: { remoteStableSessionId?: string; originId?: string; originLabel?: string };
}

/** Identities a structured value declared, by kind, so text mentioning them can be presented exactly. */
export interface DeclaredIdentities {
  messages: Set<string>;
  sessions: Set<string>;
}

export type MessageResolution =
  | { kind: "message"; id: string; ref: string }
  | { kind: "unknown"; ref: string };

const MESSAGE_ID_KEYS = new Set(["messageId", "replyMessageId", "replyTo", "supersedes", "retryOf", "supersededBy", "replacementId", "questionId", "requestMessageId"]);
const SESSION_ID_KEYS = new Set(["targetId"]);
// Peer-authored text is wrapped in markers so presentation leaves it verbatim. The markers carry
// an unguessable per-process nonce, so no literal text (a body, a public name, a label) can be
// mistaken for them, and any Unicode passes through untouched. They exist only between wrapping
// and presentation within this process; nothing marked is persisted or shown.
const MARKER_NONCE = randomBytes(12).toString("hex");
const VERBATIM_START = `\uE000${MARKER_NONCE}\uE001`;
const VERBATIM_END = `\uE001${MARKER_NONCE}\uE000`;
const VERBATIM_SEGMENT = new RegExp(`${VERBATIM_START}([\\s\\S]*?)${VERBATIM_END}`, "g");
const UNNAMED_BASE = "unnamed";
const LABEL_SEPARATOR = " · ";
const CANONICAL_SHAPE = /^(?:oq[sm]1\.[A-Za-z0-9_-]+|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$/;
// Identities are delimited by anything that cannot continue them: a UUID followed by "-helper" is a name, not a UUID.
const CANONICAL_TOKEN = /(?<![\w.-])(?:oqs1\.[A-Za-z0-9_-]+|oqm1\.[A-Za-z0-9_-]+|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})(?![\w-])/g;
const CONTAINS_CANONICAL = /oq[sm]1\.|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/;

function referenceKey(value: string): string {
  return value.trim().toLowerCase();
}

/** Generated names carry no identity fragments; pinning disambiguates them as unnamed~2 and so on. */
function sessionBase(session: ReferencedSession): string {
  const name = session.runtimeFallbackAlias ? undefined : session.name?.trim();
  return name || UNNAMED_BASE;
}

/** A value shaped and valid as a canonical session or message identity (UUID or origin-qualified handle). */
export function isCanonicalIdentity(value: string): boolean {
  return CANONICAL_SHAPE.test(value) && isCanonicalToken(value);
}

function isCanonicalToken(token: string): boolean {
  if (token.startsWith("oqs1.")) return decodeOriginQualifiedSessionIdentity(token) !== undefined;
  if (token.startsWith("oqm1.")) return decodeConversationMessageId(token) !== undefined;
  return true;
}

export class ReferenceBook {
  private readonly sessionOwners = new Map<string, string>();
  private readonly sessionPrimary = new Map<string, { ref: string; base: string }>();
  /** Authoritative current views: complete rosters, joins, presence, and fresh arrivals. */
  private readonly liveViews = new Map<string, ReferencedSession>();
  private liveSessions: Set<string> | undefined;
  private readonly messageOwners = new Map<string, string>();
  private readonly messagePrimary = new Map<string, string>();
  private readonly messageLabels = new Map<string, string[]>();
  private readonly pendingLabels = new Map<string, string>();
  private readonly originRefs = new Map<string, string>();
  private highWater = 0;
  private originHighWater = 0;

  constructor(private readonly persist: (record: ReferenceRecord) => void = () => undefined) {}

  /** Replay previously persisted allocations without persisting them again. */
  restore(records: Iterable<unknown>): void {
    for (const record of records) {
      if (!record || typeof record !== "object") continue;
      const value = record as Partial<ReferenceRecord> & { base?: unknown; number?: unknown; label?: unknown };
      if (typeof value.ref !== "string" || typeof value.id !== "string" || !value.ref.trim()) continue;
      if (value.kind === "session" && typeof value.base === "string") {
        this.sessionOwners.set(referenceKey(value.ref), value.id);
        this.sessionPrimary.set(value.id, { ref: value.ref, base: value.base });
      } else if (value.kind === "message") {
        this.messageOwners.set(referenceKey(value.ref), value.id);
        if (value.label === true) this.messageLabels.set(value.id, [...this.messageLabels.get(value.id) ?? [], value.ref]);
        else if (!this.messagePrimary.has(value.id)) this.messagePrimary.set(value.id, value.ref);
        const number = typeof value.number === "number" ? value.number : Number(value.ref.match(/^#(\d+)$/)?.[1]);
        if (Number.isSafeInteger(number)) this.highWater = Math.max(this.highWater, number);
      } else if (value.kind === "origin") {
        this.originRefs.set(value.id, value.ref);
        const number = Number(value.ref.match(/^remote-(\d+)$/)?.[1]);
        if (Number.isSafeInteger(number)) this.originHighWater = Math.max(this.originHighWater, number);
      }
    }
  }

  // ---------------------------------------------------------------- sessions

  /**
   * Learn current names from authoritative observations. A complete roster also defines
   * reachability; anything else only adds to it. Retained snapshots never come through here.
   */
  observeLive(sessions: Iterable<ReferencedSession>, options: { complete?: boolean } = {}): void {
    const ids = new Set<string>();
    for (const session of sessions) {
      if (!session?.id) continue;
      ids.add(session.id);
      this.liveViews.set(session.id, { ...this.liveViews.get(session.id), ...session });
      if (!options.complete) this.liveSessions?.add(session.id);
    }
    if (options.complete) this.liveSessions = ids;
  }

  observeSessionLeft(id: string): void {
    this.liveSessions?.delete(id);
  }

  /** Transport loss: reachability is unknown until the next complete roster. */
  forgetReachability(): void {
    this.liveSessions = undefined;
  }

  /** Undefined until a complete roster has been seen: absence then means unreachable, never ended. */
  isReachable(id: string): boolean | undefined {
    return this.liveSessions ? this.liveSessions.has(id) : undefined;
  }

  isKnownSession(id: string): boolean {
    return this.liveViews.has(id) || this.sessionPrimary.has(id);
  }

  /**
   * The reference a model should use for this session, allocating and pinning it on first use.
   * Only authoritative observations move the primary reference; a retained snapshot names an
   * identity only when nothing better is known, so old names never flip a session backwards.
   */
  sessionRef(session: ReferencedSession | string): string {
    const view = typeof session === "string" ? { id: session } : session;
    const live = this.liveViews.get(view.id);
    const current = this.sessionPrimary.get(view.id);
    if (!live && current) return current.ref;
    const base = sessionBase(live ?? view);
    if (current && current.base === base) return current.ref;
    const ref = this.firstAvailableRef(base, view.id);
    this.sessionOwners.set(referenceKey(ref), view.id);
    this.sessionPrimary.set(view.id, { ref, base });
    this.persist({ kind: "session", ref, id: view.id, base });
    return ref;
  }

  /** Pinned references only; unpinned text is left to ordinary live name resolution. */
  resolveSession(text: string): { id: string; ref: string } | undefined {
    const id = this.sessionOwners.get(referenceKey(text));
    return id ? { id, ref: this.sessionPrimary.get(id)?.ref ?? text.trim() } : undefined;
  }

  /**
   * When a pinned session is unreachable and a live session now uses its name,
   * name that session so a model can choose it deliberately. A hint, not a verdict.
   */
  successorHint(id: string, addressedBase?: string): string | undefined {
    if (!this.liveSessions || this.liveSessions.has(id)) return undefined;
    const base = addressedBase ?? this.sessionPrimary.get(id)?.base;
    if (!base || base === UNNAMED_BASE) return undefined;
    for (const liveId of this.liveSessions) {
      const live = this.liveViews.get(liveId);
      if (liveId !== id && live && referenceKey(sessionBase(live)) === referenceKey(base)) return this.sessionRef(live);
    }
    return undefined;
  }

  /** A readable host description: its own label, or a pinned remote-N for unlabelled origins. */
  originRef(federation: { originId: string; originLabel?: string }): string {
    const label = federation.originLabel?.trim();
    if (label) return label;
    const existing = this.originRefs.get(federation.originId);
    if (existing) return existing;
    const ref = `remote-${++this.originHighWater}`;
    this.originRefs.set(federation.originId, ref);
    this.persist({ kind: "origin", ref, id: federation.originId });
    return ref;
  }

  private firstAvailableRef(base: string, id: string): string {
    const owner = this.sessionOwners.get(referenceKey(base));
    if (owner === undefined || owner === id) return base;
    for (let suffix = 2; ; suffix++) {
      const candidate = `${base}~${suffix}`;
      const candidateOwner = this.sessionOwners.get(referenceKey(candidate));
      if (candidateOwner === undefined || candidateOwner === id) return candidate;
    }
  }

  // ---------------------------------------------------------------- messages

  /** `#N`, allocated once per canonical message and never reused. */
  messageNumber(id: string): string {
    const existing = this.messagePrimary.get(id);
    if (existing) return existing;
    const number = ++this.highWater;
    const ref = `#${number}`;
    this.messageOwners.set(referenceKey(ref), id);
    this.messagePrimary.set(id, ref);
    this.persist({ kind: "message", ref, id, number });
    return ref;
  }

  /** How a message is shown: its stable number, followed by any local labels. */
  messageRef(id: string): string {
    const number = this.messageNumber(id);
    const labels = this.messageLabels.get(id);
    return labels?.length ? [number, ...labels].join(LABEL_SEPARATOR) : number;
  }

  isKnownMessage(id: string): boolean {
    return this.messagePrimary.has(id);
  }

  /** Whether text is a reference or label this book has issued, of either kind. */
  isIssuedReference(text: string): boolean {
    return this.isIssuedSessionReference(text) || this.isIssuedMessageReference(text);
  }

  isIssuedSessionReference(text: string): boolean {
    return this.sessionOwners.has(referenceKey(text));
  }

  isIssuedMessageReference(text: string): boolean {
    const key = referenceKey(text);
    return this.messageOwners.has(key) || this.pendingLabels.has(key);
  }

  /** Undefined means the text is not written as a message reference at all. */
  resolveMessage(text: string): MessageResolution | undefined {
    const candidates = [text.trim(), ...text.split(LABEL_SEPARATOR).map((part) => part.trim())].filter(Boolean);
    for (const candidate of candidates) {
      const normalized = /^\d+$/.test(candidate) ? `#${candidate}` : candidate;
      const id = this.messageOwners.get(referenceKey(normalized));
      if (id && !this.pendingLabels.has(referenceKey(normalized))) return { kind: "message", id, ref: this.messageRef(id) };
    }
    const first = candidates[0] ?? "";
    const normalized = /^\d+$/.test(first) ? `#${first}` : first;
    return /^#\d+$/.test(normalized) || this.pendingLabels.has(referenceKey(normalized)) ? { kind: "unknown", ref: normalized } : undefined;
  }

  /** Why a label cannot name a message, or undefined when it can. */
  private labelProblem(label: string, id?: string): string | undefined {
    const trimmed = label.trim();
    if (!trimmed || trimmed !== label || /\s/.test(trimmed)) return "Labels are one word without spaces, like release-approval.";
    if (/[\p{Cc}\p{Cf}]/u.test(trimmed)) return "Labels cannot contain control or formatting characters.";
    if (trimmed.length > 64) return "Labels are at most 64 characters.";
    if (/^#?\d+$/.test(trimmed) || CANONICAL_SHAPE.test(trimmed) || trimmed.startsWith("oq")) return `"${trimmed}" looks like a message number or identity; choose a word.`;
    const owner = this.messageOwners.get(referenceKey(trimmed));
    if (this.pendingLabels.has(referenceKey(trimmed)) || (owner !== undefined && owner !== id)) return `"${trimmed}" already names another message.`;
    return undefined;
  }

  /** Give a known message a session-local label; failures change nothing. */
  labelMessage(id: string, label: string): { ok: true; ref: string } | { ok: false; reason: string } {
    const problem = this.labelProblem(label, id);
    if (problem) return { ok: false, reason: problem };
    this.messageNumber(id);
    if (this.messageOwners.get(referenceKey(label)) !== id) {
      this.messageOwners.set(referenceKey(label), id);
      this.messageLabels.set(id, [...this.messageLabels.get(id) ?? [], label]);
      this.persist({ kind: "message", ref: label, id, label: true });
    }
    return { ok: true, ref: this.messageRef(id) };
  }

  /**
   * Reserve a label for a message not created yet. Reservation is synchronous, so concurrent
   * calls cannot both claim it; bind or release once the send outcome is known.
   */
  reserveLabel(label: string): { ok: true; bind: (id: string) => void; release: () => void } | { ok: false; reason: string } {
    const problem = this.labelProblem(label);
    if (problem) return { ok: false, reason: problem };
    const key = referenceKey(label);
    this.pendingLabels.set(key, label);
    return {
      ok: true,
      bind: (id) => {
        this.pendingLabels.delete(key);
        this.labelMessage(id, label);
      },
      release: () => { this.pendingLabels.delete(key); },
    };
  }

  // ------------------------------------------------------------ presentation

  /** Mark peer-authored text so presentation leaves it exactly as written. */
  static verbatim(text: string): string {
    return `${VERBATIM_START}${text}${VERBATIM_END}`;
  }

  /**
   * Register message and session identities a structured value declares
   * (tool result details, message envelopes) so free text mentioning them can
   * be presented. Keys, not string shapes, decide what is a message. Snapshots
   * learned here never become authoritative names.
   */
  learnFrom(value: unknown, declared: DeclaredIdentities = { messages: new Set(), sessions: new Set() }, depth = 0): DeclaredIdentities {
    if (!value || typeof value !== "object" || depth > 4) return declared;
    if (Array.isArray(value)) {
      for (const item of value) this.learnFrom(item, declared, depth + 1);
      return declared;
    }
    const record = value as Record<string, unknown>;
    for (const [key, field] of Object.entries(record)) {
      if (MESSAGE_ID_KEYS.has(key) && typeof field === "string" && field) {
        this.messageNumber(field);
        declared.messages.add(field);
      } else if (SESSION_ID_KEYS.has(key) && typeof field === "string" && field) {
        declared.sessions.add(field);
      } else if (key === "peerCompaction" && field && typeof field === "object") {
        const notice = field as { peerSessionId?: unknown; peerName?: unknown; requestedPeerSessionId?: unknown };
        if (typeof notice.peerSessionId === "string") {
          this.sessionRef({ id: notice.peerSessionId, ...(typeof notice.peerName === "string" ? { name: notice.peerName } : {}) });
          declared.sessions.add(notice.peerSessionId);
        }
        if (typeof notice.requestedPeerSessionId === "string") declared.sessions.add(notice.requestedPeerSessionId);
      } else if ((key === "from" || key === "recipient" || key === "session") && field && typeof field === "object"
        && typeof (field as ReferencedSession).id === "string") {
        this.sessionRef(field as ReferencedSession);
        declared.sessions.add((field as ReferencedSession).id);
      } else if (key === "message" && field && typeof field === "object" && typeof (field as { id?: unknown }).id === "string") {
        this.messageNumber((field as { id: string }).id);
        declared.messages.add((field as { id: string }).id);
        this.learnFrom(field, declared, depth + 1);
      } else if (field && typeof field === "object") this.learnFrom(field, declared, depth + 1);
    }
    return declared;
  }

  /**
   * Replace canonical identities in model-facing text with references. Valid origin-
   * qualified handles are always replaced; bare UUIDs only when this book knows what they
   * identify; identities a structured value declared, whatever their shape, exactly.
   * Verbatim (peer-authored) segments and references already generated are never altered.
   * `name (name)` collapses. Presenting presented text changes nothing.
   */
  present(text: string, declared?: DeclaredIdentities): string {
    if (!text) return text;
    const produced = new Set<string>();
    const exact = [
      ...[...declared?.messages ?? []].map((id) => ({ id, ref: () => this.messageRef(id) })),
      ...[...declared?.sessions ?? []].map((id) => ({ id, ref: () => this.sessionRef(id) })),
    ].filter(({ id }) => id.length >= 4 && !(CANONICAL_SHAPE.test(id) && isCanonicalToken(id))
      // A declaration cannot redefine text that is already an issued reference or label.
      && !this.isIssuedReference(id)).sort((left, right) => right.id.length - left.id.length);
    // Generated references that themselves contain identity-shaped text must survive intact.
    const fragile = [...this.sessionPrimary.values()].map(({ ref }) => ref)
      .filter((ref) => CONTAINS_CANONICAL.test(ref) || exact.some(({ id }) => ref !== id && ref.includes(id)))
      .sort((left, right) => right.length - left.length);
    const delimited = (value: string): RegExp => new RegExp(`(?<![\\w.:-])${value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?![\\w:-])`, "g");
    const replaceIdentities = (segment: string): string => {
      let source = segment;
      for (const { id, ref } of exact) {
        const pattern = delimited(id);
        if (!pattern.test(source)) continue;
        const reference = ref();
        produced.add(reference);
        source = source.replace(pattern, () => reference);
      }
      return source.replace(CANONICAL_TOKEN, (token) => {
        let ref: string | undefined;
        // A structured value's declaration decides the kind, whatever this book has seen before.
        if (declared?.messages.has(token)) ref = this.messageRef(token);
        else if (declared?.sessions.has(token)) ref = this.sessionRef(token);
        else if (token.startsWith("oqs1.")) ref = isCanonicalToken(token) || this.isKnownSession(token) ? this.sessionRef(token) : undefined;
        else if (token.startsWith("oqm1.")) ref = isCanonicalToken(token) || this.messagePrimary.has(token) ? this.messageRef(token) : undefined;
        else if (this.isKnownSession(token)) ref = this.sessionRef(token);
        else if (this.messagePrimary.has(token)) ref = this.messageRef(token);
        if (!ref) return token;
        produced.add(ref);
        return ref;
      });
    };
    // Collapse `name (name)` only where this pass generated it, never inside authored text.
    const collapse = (segment: string): string => {
      let out = segment;
      for (const ref of produced) out = out.split(`${ref} (${ref})`).join(ref);
      return out;
    };
    const presentSegment = (segment: string): string => collapse(presentGeneratedSegment(segment));
    const presentGeneratedSegment = (segment: string): string => {
      if (fragile.length === 0) return replaceIdentities(segment);
      const protectedRefs = new RegExp(fragile.map((ref) => delimited(ref).source).join("|"), "g");
      let out = "";
      let cursor = 0;
      for (const match of segment.matchAll(protectedRefs)) {
        out += replaceIdentities(segment.slice(cursor, match.index)) + match[0];
        cursor = match.index! + match[0].length;
      }
      return out + replaceIdentities(segment.slice(cursor));
    };
    let result = "";
    let cursor = 0;
    for (const match of text.matchAll(VERBATIM_SEGMENT)) {
      result += presentSegment(text.slice(cursor, match.index)) + match[1]!;
      cursor = match.index! + match[0].length;
    }
    result += presentSegment(text.slice(cursor));
    return result;
  }
}
