# Changelog

## 1.4.0 - 2026-09-30

### Added

- Parley runs in Oh My Pi (OMP 18.2.4). Detached `pi.on` calls are bound to their host, and Bun clients skip the native startup lock that crashed them; the Node broker still holds the ownership lock. `scripts/omp-compat/run.sh` checks a two-session ask and reply in OMP.
- Messages typed in the `/parley` composer carry `session_person` provenance, and readers see that the person at the sending session wrote them, not its agent. It's negotiated as `person-provenance-v1`. Older local receivers get the message without it, and federated peers receive text only.
- README section on when to use Parley vs Threadroom. The skill notes that questions for a person, and records meant to outlast the sessions, belong elsewhere.

### Changed

- Parley no longer shares anything about anyone's context. List rows drop context usage, presence stops publishing it (and brokers drop it from older clients and peers), sessions no longer show `compacting`, and compaction notices are gone, along with the client and broker compaction-awareness feature.
- A peer link that has been up for 10 seconds is replaced by a newly authorized link to the same origin instead of refusing it. After a remote broker or link controller restarts, the old link can linger on this side with nothing behind it, and that used to strand the peer until someone restarted the broker. Simultaneous attaches still resolve by preferred direction.
- A name that is the last part of exactly one visible session's name (`June` for `MistFall Windows:June`) reaches that session, and the result says so. When a name matches none or several, the result points at sessions it probably meant: the last part of a session name (`June` for `MistFall Windows:June`) or the persona leading a description. A `label` on a multi-target send no longer blocks the send; the messages go out and the result says the label wasn't applied.
- An unnamed session's own receipts and self-profile say `unnamed` rather than its runtime `session-<id>` alias. `/parley-id` still inserts the alias, because it's the contact other sessions can route to.
- Tool calls from models that fill every optional field now work. Empty strings and lists count as not given, `targets` may be empty, and a `targets` list that only repeats `to` means `to`. A send naming both `to` and `targets` goes to everyone named, each with its own outcome. Other actions ignore `targets` and say so, so filler there can't block a reply or an ask. `profile.description: null` no longer clears a focus; send a new description instead.
- Sessions on another computer are named with that computer's label first (`build-vm:pi-parley:Ellis`), and sessions on this computer keep their bare names. A same-named session on a different computer no longer collides as `name~2`.
- A message shown again from earlier in the conversation no longer says it is "not ... active conversation". It says it is not a new delivery and that its status is current, so unanswered requests still read as owed.
- Routing scope comes only from `PI_PARLEY_SCOPE_ID`. The temporary FlightDeck launch-context scope bridge is removed; FlightDeck now sets `PI_PARLEY_SCOPE_ID` itself.
- Everything a model reads or types now uses human-readable references: session names (`name~2` when a different session later takes a name already seen) and per-session message numbers (`#12`). Canonical session and message IDs, including `oqs1.`/`oqm1.` federation handles, stay on the wire and in `details`; tool inputs still accept them from programmatic callers.
- References are pinned and persisted: renames, reconnects, reloads, and name reuse never recycle one. Parley routes a pinned reference only to its exact identity. Absent local targets use the new negotiated `exact-identity-send-v1` broker mode (`SendOptions.exactIdentity`), which never falls back to name or prefix lookup; brokers without it refuse before sending with `E_EXACT_IDENTITY_UNSUPPORTED`. The broker's existing offline-mail rule (a unique same-name, same-directory live session may receive queued mail) is unchanged and is now reported when it applies; a same-named session is otherwise offered as a choice.
- A new `label` action and `label` parameter on `send`/`ask` give messages session-local names alongside their numbers.
- `/parley-id` inserts a readable contact by default; `/parley-id --id` keeps the stable canonical target.
- `sessionIdPrefixes` remains exported but is deprecated; model-facing text no longer uses ID prefixes.
- Automatic conversation context mentions elapsed or unreachable requests once and then counts them; `pending` and `status` stay complete.
- Collapsed tool rows, inline messages, and the session picker show the same references instead of 8-character ID prefixes.

## 1.3.0 - 2026-09-21

### Added

- A compiled `pi-parley/federation` facade for inspecting and attaching exact brokers from plain Node hosts.
- An explicit, runtime-idempotent `pi-parley/extension` entrypoint for application-owned Pi actor wrappers.
- A synchronous `resolvePresenceName` embedding policy covering session identity, compatibility renames, reconnects, and explicit advertised identity without publishing an unqualified fallback.

### Changed

- Federation attachment now keeps broker endpoints, TCP credentials, framing, and handshake details behind opaque rooted host capabilities.

## 1.2.0

### Added

- Targeted messaging and explicit asks/replies between Pi sessions, with independent outcomes for group sends.
- Delivery receipts that distinguish acceptance, offline queueing, known failure, and uncertain outcomes.
- Conversation journals for pending requests, received messages, controls, and interrupted delivery.
- Canonical session naming, concise focus labels, and live presence.
- Scoped child-session visibility and an optional supervisor channel.
- Durable compaction awareness for direct collaborators.
- Extension channels with capability discovery, ownership, and revisioned state.
- Experimental federation for remote discovery and direct text messaging.
- A session picker and message composer through `/parley` or Alt+M.
- OS-managed broker lifetime ownership, with safe concurrent startup and automatic crash recovery.
- Correlated cancellation acknowledgements and required current conversation capabilities.
- Broker health identity with a startup package/source snapshot.
- Unknown SDK context usage clears old presence estimates; notices identify the last reported sample.
- Retained model-context snapshots are historical, preserve arrival timing/disposition, and reconcile late host persistence without reviving answered requests.
- Optional authenticated loopback TCP on all supported platforms.
- Packed fresh-install checks and cross-platform ownership validation before automated publishing.
