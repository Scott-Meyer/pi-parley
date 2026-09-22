import test from "node:test";
import assert from "node:assert/strict";
import { ReferenceBook, type ReferenceRecord } from "./references.ts";
import { encodeOriginQualifiedSessionIdentity } from "./broker/federation-protocol.ts";

// The book's promises to its consumers: readable, pinned, never recycled, never guessed.

test("a retained snapshot never moves a session's current name backwards", () => {
  const book = new ReferenceBook();
  book.observeLive([{ id: "s-1", name: "alice" }], { complete: true });
  assert.equal(book.sessionRef("s-1"), "alice");
  book.observeLive([{ id: "s-1", name: "bob" }], { complete: true });
  assert.equal(book.sessionRef("s-1"), "bob");
  // An old message's sender snapshot still says alice.
  assert.equal(book.sessionRef({ id: "s-1", name: "alice" }), "bob");
  assert.equal(book.resolveSession("alice")?.id, "s-1", "the old name stays reserved for the same identity");
});

test("losing the transport makes reachability unknown, so no successor is offered on stale evidence", () => {
  const book = new ReferenceBook();
  book.observeLive([{ id: "old", name: "builder" }], { complete: true });
  book.sessionRef("old");
  book.observeLive([{ id: "new", name: "builder" }], { complete: true });
  assert.equal(book.successorHint("old"), "builder~2");
  book.forgetReachability();
  assert.equal(book.isReachable("old"), undefined);
  assert.equal(book.successorHint("old"), undefined);
});

test("names that merely look like handles round-trip, while real handles become references", () => {
  const book = new ReferenceBook();
  book.observeLive([{ id: "s-1", name: "oqm1.ZGVtbw" }], { complete: true });
  const ref = book.sessionRef("s-1");
  assert.equal(ref, "oqm1.ZGVtbw");
  assert.equal(book.present(`sent to ${ref}`), `sent to ${ref}`, "an already generated reference is left alone");
  const real = encodeOriginQualifiedSessionIdentity({ originId: "host:a", remoteScopeAlias: "a", remoteStableSessionId: "peer" });
  assert.doesNotMatch(book.present(`from ${real}`), /oqs1\./);
});

test("peer-authored text stays verbatim even when it mentions a declared identity", () => {
  const book = new ReferenceBook();
  book.observeLive([{ id: "game:t226", name: "mistfall" }], { complete: true });
  const declared = book.learnFrom({ from: { id: "game:t226", name: "mistfall" } });
  const text = `From game:t226\n\n${ReferenceBook.verbatim("See the game:t226 notes")}`;
  assert.equal(book.present(text, declared), "From mistfall\n\nSee the game:t226 notes");
});

test("names containing replacement patterns are inserted literally", () => {
  const book = new ReferenceBook();
  book.observeLive([{ id: "cost-bot-id", name: "cost$&bot" }], { complete: true });
  const declared = book.learnFrom({ recipient: { id: "cost-bot-id", name: "cost$&bot" } });
  assert.equal(book.present("sent to cost-bot-id", declared), "sent to cost$&bot");
});

test("a reserved label cannot be claimed twice, and releasing it frees it", () => {
  const book = new ReferenceBook();
  const first = book.reserveLabel("release-approval");
  assert.equal(first.ok, true);
  const second = book.reserveLabel("release-approval");
  assert.equal(second.ok, false, "a concurrent claim fails while the first is pending");
  if (first.ok) first.release();
  const third = book.reserveLabel("release-approval");
  assert.equal(third.ok, true);
  if (third.ok) third.bind("m-1");
  assert.equal(book.resolveMessage("release-approval")?.kind, "message");
  assert.equal(book.messageRef("m-1"), "#1 · release-approval", "the number stays primary");
  assert.equal(book.labelMessage("m-2", "release-approval").ok, false, "a bound label names one message");
});

test("allocations persist and replay into the same references", () => {
  const records: ReferenceRecord[] = [];
  const book = new ReferenceBook((record) => records.push(record));
  book.observeLive([{ id: "s-1", name: "alice" }, { id: "s-2", name: "alice" }], { complete: true });
  book.sessionRef("s-1");
  book.sessionRef("s-2");
  book.messageRef("m-1");
  book.labelMessage("m-1", "plan");
  book.originRef({ originId: "install:1" });
  const restored = new ReferenceBook();
  restored.restore(records);
  assert.equal(restored.sessionRef("s-2"), "alice~2");
  assert.equal(restored.messageRef("m-1"), "#1 · plan");
  assert.equal(restored.messageNumber("m-2"), "#2", "numbering continues from the high-water mark");
  assert.equal(restored.originRef({ originId: "install:1" }), "remote-1");
  assert.equal(restored.originRef({ originId: "install:2" }), "remote-2");
});

test("unnamed sessions get identity-neutral references", () => {
  const book = new ReferenceBook();
  book.observeLive([
    { id: "01a0ca60-cff7-70b0-a731-e4f208b09ae9", name: "session-01a0ca60-cff7-70b0", runtimeFallbackAlias: true },
    { id: "01a0ca61-0000-70b0-a731-e4f208b09ae9" },
  ], { complete: true });
  assert.equal(book.sessionRef("01a0ca60-cff7-70b0-a731-e4f208b09ae9"), "unnamed");
  assert.equal(book.sessionRef("01a0ca61-0000-70b0-a731-e4f208b09ae9"), "unnamed~2");
});

test("verbatim text round-trips any Unicode, including the markers themselves", () => {
  const book = new ReferenceBook();
  const body = "private-use \uE000 \uE001 \uE002 \uE010 stays intact";
  assert.equal(book.present(`Body: ${ReferenceBook.verbatim(body)}`), `Body: ${body}`);
});

test("a generated reference containing a known identity is never rewritten", () => {
  const book = new ReferenceBook();
  const uuid = "01a0ca60-cff7-70b0-a731-e4f208b09ae9";
  book.observeLive([{ id: uuid, name: "builder" }, { id: "helper-id", name: `${uuid}-helper` }, { id: "spaced-id", name: `helper ${uuid}` }], { complete: true });
  assert.equal(book.sessionRef(uuid), "builder");
  const helper = book.sessionRef("helper-id");
  const spaced = book.sessionRef("spaced-id");
  assert.equal(helper, `${uuid}-helper`);
  assert.equal(book.present(`sent to ${helper}`), `sent to ${helper}`, "a hyphen continues an identity");
  assert.equal(book.present(`sent to ${spaced}`), `sent to ${spaced}`, "a generated reference is protected as a whole");
  assert.equal(book.present(`from ${uuid}`), "from builder", "the bare identity is still presented");
  const once = book.present(`from ${uuid} to ${helper}`);
  assert.equal(book.present(once), once, "presentation is idempotent");
});

test("authored text keeps a literal name (name) exactly", () => {
  const book = new ReferenceBook();
  const declared = book.learnFrom({ from: { id: "game:t226", name: "builder" } });
  assert.equal(book.present(`From game:t226 (game:t226)\n\n${ReferenceBook.verbatim("Keep builder (builder) exactly")}`, declared),
    "From builder\n\nKeep builder (builder) exactly");
});

test("labels reject control and formatting characters", () => {
  const book = new ReferenceBook();
  assert.equal(book.labelMessage("m1", "unsafe\u001b[2J").ok, false);
  assert.equal(book.labelMessage("m1", "rtl\u202eeval").ok, false);
  assert.equal(book.labelMessage("m1", "release-approval").ok, true);
});

test("names and labels made of private-use characters round-trip as ordinary text", () => {
  const book = new ReferenceBook();
  book.observeLive([{ id: "pua-alice", name: "\uE000alice\uE001" }, { id: "plain-alice", name: "alice" }], { complete: true });
  const ref = book.sessionRef("pua-alice");
  assert.equal(book.sessionRef("plain-alice"), "alice");
  assert.equal(book.present(`To ${ref}`), `To ${ref}`, "the shown reference is the reserved one");
  assert.equal(book.resolveSession(book.present(ref))?.id, "pua-alice", "and it resolves back to the same identity");
  const labelled = book.labelMessage("m1", "\uE000plan\uE001");
  assert.equal(labelled.ok, true);
  if (labelled.ok) assert.equal(book.resolveMessage(book.present(labelled.ref))?.kind, "message");
});

test("a declared identity is presented by its declared kind even when this book has never seen it", () => {
  const book = new ReferenceBook();
  const messageId = "abcdef01-2345-4abc-8123-456789abcdef";
  const sessionId = "01234567-89ab-4cde-8123-456789abcdef";
  const declared = { messages: new Set([messageId]), sessions: new Set([sessionId]) };
  assert.equal(book.present(`sent ${messageId} to ${sessionId}`, declared), "sent #1 to unnamed");
  assert.equal(book.present(`sent ${messageId}`), "sent #1", "and it is known afterwards");
});

test("a declared identity never redefines a reference or label already issued with the same spelling", () => {
  const book = new ReferenceBook();
  book.observeLive([{ id: "builder-session", name: "builder" }], { complete: true });
  assert.equal(book.sessionRef("builder-session"), "builder");
  // A message whose identifier happens to be spelled like that session's reference.
  const declared = book.learnFrom({ messageId: "builder" });
  assert.equal(book.present("From builder\nMessage: #1", declared), "From builder\nMessage: #1");
  book.labelMessage("m-2", "planner");
  const sessionNamedLikeLabel = book.learnFrom({ targetId: "planner" });
  assert.equal(book.present("Label planner stays", sessionNamedLikeLabel), "Label planner stays");
});
