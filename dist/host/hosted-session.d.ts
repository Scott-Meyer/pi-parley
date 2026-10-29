export interface Delivery {
    /** What Pi would put in front of the model. */
    readonly text: string;
    /** The extension wants a turn to start for it (Pi's triggerTurn). */
    readonly wake: boolean;
    /** While a turn is running: "steer" (now) or "followUp" (after it). */
    readonly during?: "steer" | "followUp";
    /** What kind of thing this is: "parley_message" (from a colleague), "parley_message_control"
     * (a withdrawal or update), and other "parley_*" notices. */
    readonly customType: string;
    /** Parley's record of it. Stable: `from` (the sender's SessionInfo: name, id, cwd, app,
     * federation) and `message.id`, `message.replyTo`, `message.expectsReply`. The rest is
     * informational and may change. */
    readonly details?: unknown;
    /** For a person: the colleague's words without the model's metadata, and a preview of
     * the message they answer. Messages and withdrawals have it; other notices don't. */
    readonly display?: {
        readonly text: string;
        readonly inReplyTo?: string;
    };
}
/** Whether the host put a delivery in front of its agent. Until it does, Parley treats the
 * message as not yet shown: the sender hears it's queued, and Parley tries again. */
export type DeliveryOutcome = {
    readonly accepted: true;
} | {
    readonly accepted: false;
    readonly reason: string;
};
export interface HostedSessionOptions {
    readonly cwd: string;
    readonly model?: string;
    /** Used when the session has no name yet; worked out when it joins. */
    readonly defaultName: () => string;
    /** Where the session lives, for list rows ("Claude Desktop"); worked out when it joins. */
    readonly app?: () => string;
    /** Where a session's history lives (JSON lines), by its host session id. */
    readonly stateFile: (sessionId: string) => string;
    /** Put a delivery in front of the agent. Returning nothing means it was accepted. */
    readonly deliver: (delivery: Delivery) => DeliveryOutcome | Promise<DeliveryOutcome> | void;
    /** The environment this session is read from (FlightDeck, herdr, scope, whether it links
     * by default). Default: the process's. Pass {} for a session that is none of those. */
    readonly env?: Readonly<Record<string, string | undefined>>;
    /** false: don't put the session's id in the process environment. For processes that run
     * several sessions. */
    readonly publishSessionEnv?: boolean;
    /** Parley renamed the session (its agent used `rename`). */
    readonly nameChanged?: (name: string) => void;
}
export interface ToolOutcome {
    readonly text: string;
    readonly isError: boolean;
    /** Parley's structured result: references, recipients, receipts. Informational. */
    readonly details?: unknown;
}
/** A message in Pi's shape (AgentMessage): role "user", "assistant", "toolResult", "custom"
 * (with customType and details), "compactionSummary", and so on. */
export type HostedContextMessage = Record<string, unknown> & {
    readonly role: string;
};
export interface HostedSession {
    /** Join Parley as the host session with this id (Claude's session id, Codex's thread id). Once. */
    begin(sessionId: string): Promise<void>;
    readonly started: () => boolean;
    readonly tool: {
        readonly name: string;
        readonly description: string;
        readonly inputSchema: Record<string, unknown>;
    };
    /** How the tool is meant to be used, for hosts that take server instructions. */
    readonly instructions: string;
    call(args: Record<string, unknown>, signal?: AbortSignal): Promise<ToolOutcome>;
    /** The messages about to go to the model, in Pi's shape; returns what to send instead.
     * Makes references readable, drops duplicate copies of a message, adds any delivery the
     * host hasn't accepted yet, and marks which messages are being answered. Accepted
     * deliveries kept as custom messages (customType, details) get all of that. */
    context(messages: readonly HostedContextMessage[]): Promise<HostedContextMessage[]>;
    /** The host's agent started or finished a turn. */
    setBusy(busy: boolean): void;
    /** The person renamed the session in the host. */
    setName(name: string): void;
    /** The host's agent now uses this model. */
    setModel(id: string): void;
    /** The host is compacting the agent's context. */
    compaction(phase: "start" | "end" | "failed"): void;
    readonly name: () => string | undefined;
    stop(): Promise<void>;
}
/** Sets up Parley for a host session without joining yet: the tool is known
 * at once, and `begin` joins once the host says which session this is. */
export declare function createHostedSession(options: HostedSessionOptions): Promise<HostedSession>;
/** Set up and join at once, for hosts that know their session from the start. */
export declare function startHostedSession(options: HostedSessionOptions & {
    sessionId: string;
}): Promise<HostedSession>;
