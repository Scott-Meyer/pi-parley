import type { SendResult } from "./broker/client.ts";
import { formatPeerCompactionNotice } from "./compaction-awareness.ts";

/** A receipt describes observed delivery, not whether a colleague read or acted on it. */
export function formatDeliveryResult(result: SendResult, context: {
  kind: "Message" | "Ask" | "Reply" | "Progress update";
  sender: string;
  target: string;
  /** How to name the observed recipient; defaults to its raw name and ID for programmatic callers. */
  sessionRef?: (recipient: NonNullable<SendResult["recipient"]>) => string;
  /** How to name the message; defaults to its raw ID. */
  messageRef?: (id: string) => string;
  /** How a compaction notice names session identities; canonical by default. */
  sessionName?: (id: string, name?: string) => string;
}): string {
  const messageName = context.messageRef ?? ((id: string) => id);
  const recipient = result.recipient;
  const actualTarget = recipient
    ? context.sessionRef ? context.sessionRef(recipient) : `${recipient.name || recipient.id} (${recipient.id})`
    : context.target;
  const identity = `as ${context.sender} to ${actualTarget}`;
  const lines: string[] = [];
  if (!result.outcomeKnown || result.delivery === "unknown") {
    lines.push(`${context.kind} delivery outcome unknown ${identity}.`,
      "The message may have arrived; sending it again could repeat it.");
  } else if (!result.delivered) {
    lines.push(`${context.kind} not delivered ${identity}.`);
  } else if (result.delivery === "queued") {
    lines.push(`${context.kind} queued ${identity}.`,
      "Recipient offline; queued for up to 24 hours while this broker remains running.");
  } else {
    lines.push(`${context.kind} sent ${identity}.`);
  }
  lines.push(`Message: ${messageName(result.id)}${result.delivered && result.delivery === "socket_delivered" ? " · endpoint accepted" : ""}`);
  if (result.reason) lines.push(`Reason: ${result.reason}`);
  if (result.code) lines.push(`Outcome code: ${result.code}`);
  if (result.code === "E_REPLY_TARGET") {
    lines.push("The broker cannot authorize this thread. Its relationship may have expired or been lost on restart; the local message can still be retained.");
  }
  if (result.peerCompaction) {
    lines.push(formatPeerCompactionNotice(context.target, result.peerCompaction, recipient?.id, context.sessionName));
  }
  return lines.join("\n");
}

export function formatCancellationResult(result: SendResult, messageRef: (id: string) => string = (id) => id): string {
  const message = messageRef(result.id);
  if (!result.outcomeKnown || result.delivery === "unknown") {
    return `Cancellation outcome unknown for ${message}. The request may still be actionable.${result.reason ? ` ${result.reason}` : ""}`;
  }
  if (!result.delivered) {
    return `Cancellation was not accepted for ${message}.${result.reason ? ` ${result.reason}` : ""}`;
  }
  if (result.cancellation === "removed_from_mailbox") {
    return `Cancelled ${message}: removed from the offline mailbox before delivery.`;
  }
  if (result.cancellation === "not_delivered") {
    return `Cancelled ${message}: the original message was not delivered.`;
  }
  return `Withdrawal requested for ${message}. Work may already have happened; this does not undo it.`;
}
