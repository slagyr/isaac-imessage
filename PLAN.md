# isaac-imessage Plan

## Goal

Bi-directional iMessage conversation support for Isaac on
`zanebot`. Inbound messages route to Isaac sessions; crew replies
go back through Messages.

## Architecture

The integration runs as an Isaac comm module, activated by config
under `:comms.imessage`. All wire-level interaction with macOS
Messages is delegated to the [`imsg`](https://github.com/openclaw/imsg)
CLI — Isaac talks JSON-RPC over a long-lived stdio subprocess.

### Pieces

- `isaac.comm.imessage` — the comm impl. Owns the `imsg` subprocess
  via `imsg-client`, normalizes inbound notifications, dispatches
  turns, and enqueues outbound replies onto the generic Isaac
  delivery worker.
- `isaac.comm.imessage.imsg-client` — JSON-RPC client. Spawns
  `imsg rpc` once at comm startup, sends requests, correlates
  responses by id, routes pushed notifications to a callback.
  Reuses message construction from `isaac.util.jsonrpc`.

### Inbound

1. Comm startup spawns `imsg rpc` and calls `watch.subscribe` so
   imsg starts pushing message events.
2. Each pushed notification (`{:method "message" :params {:message
   {…}}}`) goes through `notification->work-item`:
   - drop self-messages (`is_from_me`)
   - drop senders not in `:allow-from` (fail-closed when set)
   - drop notifications with no chat identity
   - else build an Isaac work-item keyed by `imessage:<chat-guid>`
3. The work-item is dispatched (`api/dispatch!`) with a trusted
   inbound_meta system block carrying provider, surface, chat_guid,
   handle, was_mentioned.
4. The LLM response is chunked per `:message-cap` and each chunk is
   enqueued for the Isaac delivery worker.

### Outbound

1. Delivery worker reads pending records keyed `:comm "imessage"`
   and calls `ImessageComm/send!`.
2. `send!` translates the record into an imsg `send` request and
   sends it over the JSON-RPC client.
3. Response classification:
   - `:ok` → record deleted from queue
   - permission / unknown-buddy patterns → `:transient? false`
     (dead-letter on first attempt thanks to `isaac-pu2x`)
   - everything else → `:transient? true` (retry per the worker's
     backoff schedule)

### Lifecycle

- `on-startup!` spawns the imsg subprocess (when `:db-path` is
  configured), subscribes to inbound, stores the client in state.
- `on-config-change!` tears down the client on slot removal;
  otherwise updates the live slice in place.

## zanebot Operational Requirements

Same as `imsg` itself:

- macOS Sonoma (14.0) or later for `imsg`
- Full Disk Access for whatever runs the isaac process (reads
  chat.db through imsg)
- Automation → Messages for the same binary (imsg invokes
  Messages.app for sends)
- A logged-in GUI session (Messages.app must be running)
- imsg installed (`brew install steipete/tap/imsg`) — universal
  binary so x86_64 and arm64 hosts both work

Config example lives in `README.md`.

## MVP Scope

Supported:

- one-to-one conversations (chat_guid `any;-;<handle>`)
- plain text inbound and outbound
- allow-from sender filtering
- per-comm message-cap (auto-chunk above)

Explicitly out of scope (for now):

- group chats (need group routing + allow-from semantics)
- attachments
- reactions / edits / unsend
- typing indicators
- IMCore-bridge features that require SIP disabled

## Implementation Status

Phase 1 — outbound via imsg — **done**, smoke-tested on zanebot.

Phase 2 — inbound via imsg `watch.subscribe` — **done**,
smoke-tested on zanebot (round-tripped a real iMessage through the
crew).

Phase 3 — operational polish — **in progress**:

- launchd plist for keeping isaac alive on zanebot
- live coverage of allow-from drops, chunking, imsg subprocess
  crash recovery

## Testing Strategy

### Unit Specs

`spec/isaac/comm/imessage_spec.clj` — send! translation,
notification->work-item filter cases, dispatch-request trusted
block, result→reply text, chunking.

`spec/isaac/comm/imessage/imsg_client_spec.clj` — JSON-RPC client
with a stubbed subprocess: request correlation, notification
dispatch, write-then-close cleanup.

### Feature Specs (gherclj)

- `comm/imessage/send.feature` — outbound via the delivery worker
- `comm/imessage/intake.feature` — inbound notifications →
  work-items
- `comm/imessage/routing.feature` — chat_guid → session-key
- `comm/imessage/intake_filtering.feature` — allow-from
- `comm/imessage/lifecycle.feature` — comm activation /
  deactivation / config change
- `comm/imessage/turn_context.feature` — trusted inbound_meta
  injection
- `comm/imessage/reply.feature` — end-to-end inbound → reply

## Main Risk

`imsg` is the load-bearing third-party dependency. Risks:

- pre-1.0 maturity — JSON-RPC method names may shift between
  releases
- single-maintainer bus factor
- IMCore-bridge features need SIP disabled; if upstream pivots
  toward SIP-off-only, we'd be locked out

Mitigations: pin the imsg version on each host; subscribe to
release notes; keep our wire-level adapter (imsg-client + small
notification->work-item) small enough that a fork is cheap.
