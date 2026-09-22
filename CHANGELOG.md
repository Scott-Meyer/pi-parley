# Changelog

## Unreleased

### Changed

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
- A temporary FlightDeck launch-context scope bridge remains while generic scope propagation and version-matched remote actors roll out.

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
