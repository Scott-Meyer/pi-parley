import test from "node:test";
import assert from "node:assert/strict";
import { stripVTControlCharacters } from "node:util";

import { visibleWidth } from "@mariozechner/pi-tui";
import { formatPeerCompactionNotice } from "../compaction-awareness.ts";
import { formatDeliveryResult } from "../message-results.ts";
import { InlineMessageComponent } from "../ui/inline-message.ts";
import type { Message, SessionInfo } from "../types.ts";

const theme = {
  fg(_name: string, text: string): string {
    return text;
  },
};

const from: SessionInfo = {
  id: "session-12345678",
  name: "sender",
  cwd: "/tmp/project",
  model: "model",
  pid: 1,
  startedAt: 0,
  lastActivity: 0,
};

const message: Message = {
  id: "message-1",
  timestamp: 0,
  content: {
    text: "This is a long message that should use the available terminal width instead of a narrow fixed card.",
  },
};

test("inline parley messages render at the available terminal width", () => {
  const component = new InlineMessageComponent(from, message, theme as any);

  const lines = component.render(120);

  assert.ok(lines.length > 0);
  for (const line of lines) assert.equal(visibleWidth(line), 120);
});

test("expanded inline parley messages show the full body without collapse controls", () => {
  const component = new InlineMessageComponent(from, message, theme as any, "parley({ action: \"reply\", message: \"...\" })");

  const rendered = component.render(100).join("\n");

  assert.match(rendered, /available terminal width/);
  assert.match(rendered, /narrow fixed/);
  assert.match(rendered, /card/);
  assert.match(rendered, /To reply: parley/);
  assert.doesNotMatch(rendered, /Ctrl\+O/);
});

test("compaction notices identify the actual peer after mailbox identity rebound", () => {
  const notice = formatPeerCompactionNotice("departed-worker", {
    peerSessionId: "replacement-session-id",
    peerName: "replacement-worker",
    requestedPeerSessionId: "departed-session-id",
    generation: 2,
    previousGeneration: 1,
    compactedAt: 1234,
  });
  assert.match(notice, /replacement-worker \[session replacement-session-id\]/);
  assert.match(notice, /message was requested for departed-worker \[session departed-session-id\]/);
  assert.doesNotMatch(notice, /Note: departed-worker has compacted/);
});

test("broker-authored rebound ID wins even when it matches the replacement name", () => {
  const notice = formatPeerCompactionNotice("orchestrator", {
    peerSessionId: "replacement-session-id",
    peerName: "orchestrator",
    requestedPeerSessionId: "orchestrator",
    generation: 2,
    previousGeneration: 1,
    compactedAt: 1234,
  }, "orchestrator");
  assert.match(notice, /orchestrator \[session replacement-session-id\]/);
  assert.match(notice, /message was requested for orchestrator \[session orchestrator\]/);
});

test("routing names are not mistaken for stable-ID rebound", () => {
  const notice = formatPeerCompactionNotice("planner", {
    peerSessionId: "planner-stable-session",
    peerName: "planner",
    generation: 2,
    previousGeneration: 1,
    compactedAt: 1234,
  }, "planner");
  assert.match(notice, /Note: planner has compacted/);
  assert.doesNotMatch(notice, /message was requested for/);
});

test("collapsed inline parley messages keep preview, reply hint, and expand key visible", () => {
  const component = new InlineMessageComponent(
    from,
    {
      ...message,
      content: {
        text: "Alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu. This tail should only appear when expanded because the collapsed preview is intentionally brief.",
        attachments: [{ type: "snippet", name: "note.txt", content: "important details" }],
      },
    },
    theme as any,
    "parley({ action: \"reply\", message: \"...\" })",
    undefined,
    true,
  );

  const lines = component.render(120);
  const rendered = lines.join("\n");

  assert.equal(lines.length, 4);
  for (const line of lines) assert.equal(visibleWidth(line), 120);
  assert.match(rendered, /Alpha beta gamma/);
  assert.doesNotMatch(rendered, /intentionally brief/);
  assert.match(rendered, /To reply: parley/);
  assert.match(rendered, /Ctrl\+O/);
  assert.match(rendered, /1 attachment/);
});

const roleCodes = {
  accent: 31,
  toolTitle: 32,
  text: 33,
  muted: 34,
  dim: 35,
} as const;

function styledTheme(calls: string[] = []) {
  return {
    fg(name: keyof typeof roleCodes, text: string): string {
      calls.push(name);
      return `\u001b[${roleCodes[name]}m${text}\u001b[0m`;
    },
  };
}

test("inline message colors follow the tool-title, text, muted-border, and dim-metadata hierarchy", () => {
  const calls: string[] = [];
  const component = new InlineMessageComponent(
    from,
    {
      ...message,
      replyTo: "parent-message-id",
      content: {
        text: "Body copy",
        attachments: [{ type: "snippet", name: "note.txt", content: "details" }],
      },
    },
    styledTheme(calls) as any,
    "parley reply",
  );

  const lines = component.render(72);
  const rendered = lines.join("\n");

  assert.match(lines[0], /^\u001b\[34m╭\u001b\[0m\u001b\[32m From:/);
  assert.match(rendered, /\u001b\[34m│\u001b\[0m\u001b\[33mBody copy\u001b\[0m/);
  assert.match(rendered, /\u001b\[35m To reply: parley reply\u001b\[0m/);
  assert.match(rendered, /\u001b\[35m Attachment: note\.txt\u001b\[0m/);
  assert.match(rendered, /\u001b\[35m Reply to parent-m\u001b\[0m/);
  assert.match(lines.at(-1)!, /^\u001b\[34m╰─+╯\u001b\[0m$/);
  assert.ok(calls.includes("toolTitle"));
  assert.ok(calls.includes("text"));
  assert.ok(calls.includes("muted"));
  assert.ok(calls.includes("dim"));
  assert.ok(!calls.includes("accent"));
});

test("collapsed inline messages preserve the same hierarchy without accent", () => {
  const calls: string[] = [];
  const component = new InlineMessageComponent(from, message, styledTheme(calls) as any, "parley reply", undefined, true);

  const rendered = component.render(72).join("\n");

  assert.match(rendered, /\u001b\[32m From:/);
  assert.match(rendered, /\u001b\[33mThis is a long message/);
  assert.match(rendered, /\u001b\[35m To reply:/);
  assert.ok(!calls.includes("accent"));
});

test("semantic styling remains ANSI- and Unicode-width safe", () => {
  const unicodeFrom = { ...from, name: "送信者🛰️", cwd: "/tmp/計画" };
  const unicodeMessage = {
    ...message,
    content: { text: "\u001b[36m色付き本文\u001b[0m with emoji 🧪 and a long Unicode tail 計画計画計画" },
  };
  const component = new InlineMessageComponent(unicodeFrom, unicodeMessage, styledTheme() as any, "返信");

  for (const width of [18, 31, 52]) {
    const lines = component.render(width);
    assert.ok(lines.length > 0);
    for (const line of lines) {
      assert.equal(visibleWidth(line), width, `${width}: ${stripVTControlCharacters(line)}`);
    }
  }
});

test("inline messages pick up mutable theme proxy changes on rerender", () => {
  const palette: Record<string, number> = { ...roleCodes };
  const mutableTheme = new Proxy(
    {},
    {
      get(_target, property) {
        if (property !== "fg") return undefined;
        return (name: string, text: string) => `\u001b[${palette[name]}m${text}\u001b[0m`;
      },
    },
  );
  const component = new InlineMessageComponent(from, message, mutableTheme as any);

  const before = component.render(72).join("\n");
  palette.toolTitle = 96;
  palette.text = 97;
  palette.muted = 90;
  const after = component.render(72).join("\n");

  assert.match(before, /\u001b\[32m From:/);
  assert.match(after, /\u001b\[96m From:/);
  assert.match(after, /\u001b\[97mThis is a long message/);
  assert.match(after, /\u001b\[90m╭/);
  assert.notEqual(before, after);
});

test("with typed naming, a rebound notice names each identity by its own reference", async () => {
  const { ReferenceBook } = await import("../references.ts");
  const book = new ReferenceBook();
  // builder names another session; Bob names the session the message was meant for.
  book.observeLive([{ id: "other-id", name: "builder" }, { id: "old-bob", name: "Bob" }], { complete: true });
  book.sessionRef("other-id");
  book.sessionRef("old-bob");
  book.observeLive([{ id: "builder", name: "Bob" }], {});
  const notice = formatPeerCompactionNotice("Bob~2", {
    peerSessionId: "builder",
    peerName: "Bob",
    requestedPeerSessionId: "old-bob",
    generation: 2,
    previousGeneration: 1,
    compactedAt: 1234,
  }, undefined, (id, name) => book.sessionRef(name ? { id, name } : id));
  assert.match(notice, /^Note: Bob~2 \(message was requested for Bob\) has compacted since you last talked/);
  assert.doesNotMatch(notice, /\[session|builder/, "no raw identity, and never another session's reference");
});

test("a delivery receipt carrying a rebound compaction notice names every identity by its own reference", async () => {
  const { ReferenceBook } = await import("../references.ts");
  const book = new ReferenceBook();
  book.observeLive([{ id: "other-id", name: "builder" }, { id: "old-bob", name: "Bob" }], { complete: true });
  book.sessionRef("other-id");
  book.sessionRef("old-bob");
  book.observeLive([{ id: "builder", name: "Bob" }], {});
  const recipient = { id: "builder", name: "Bob", cwd: "/tmp", model: "m", pid: 1, startedAt: 0, lastActivity: 0 };
  // The receipt path the actor uses: typed recipient, message, and compaction naming together.
  const receipt = formatDeliveryResult({
    id: "m-1", delivered: true, delivery: "socket_delivered", outcomeKnown: true, retryable: false, recipient,
    peerCompaction: { peerSessionId: "builder", peerName: "Bob", requestedPeerSessionId: "old-bob", generation: 2, previousGeneration: 1, compactedAt: 1234 },
  }, {
    kind: "Message", sender: "observer", target: "Bob",
    sessionRef: (session) => book.sessionRef(session),
    messageRef: (id) => book.messageRef(id),
    sessionName: (id, name) => book.sessionRef(name ? { id, name } : id),
  });
  assert.match(receipt, /^Message sent as observer to Bob~2\.\nMessage: #1/);
  assert.match(receipt, /Note: Bob~2 \(message was requested for Bob\) has compacted since you last talked/);
  assert.doesNotMatch(receipt, /\[session|builder/, "no raw identity, and never another session's reference");
});
