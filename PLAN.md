# isaac-imessage Plan

## Goal

Add bi-directional iMessage conversation support for Isaac on `zanebot`.

This is not just an outbound notifier. It needs both:

- outbound message delivery to Messages.app
- inbound message intake from iMessage threads

## Architecture

This should be a full channel/module integration, closer to Discord than to a
small one-off `Comm` adapter.

### Outbound

Implement a new `Comm` impl for iMessage delivery.

- likely namespace: `isaac.comm.imessage`
- implement `isaac.comm/Comm`
- `send!` uses `osascript` or JXA to send text through Messages.app
- support delivery worker integration for queued outbound sends

### Inbound

Implement a polling/watching intake service for Messages data.

- likely namespace: `isaac.comm.imessage.inbox`
- read from `~/Library/Messages/chat.db`
- detect unseen inbound messages
- convert them into Isaac turns
- persist cursor / last seen message state

### Routing

Map iMessage conversations to Isaac sessions.

Recommended MVP rule:

- one Isaac session per iMessage thread

Store metadata such as:

- chat guid
- handle id / phone / email
- session key
- crew / model defaults
- last seen message id or timestamp

## zanebot Constraints

This must run as the logged-in macOS user, not a headless daemon.

Operational requirements:

- Automation permission to control `Messages`
- Full Disk Access to read `~/Library/Messages/chat.db`
- a live GUI session

This integration is likely brittle across macOS updates.

## MVP Scope

Start narrow.

### Supported

- one-to-one conversations only
- plain text only
- inbound polling
- outbound text sending
- thread-to-session persistence

### Explicitly Out of Scope

- group chats
- attachments
- reactions / edits
- typing indicators
- rich formatting

## Suggested Repo Shape

Potential first-pass namespaces:

- `isaac.comm.imessage`
- `isaac.comm.imessage.apple-script`
- `isaac.comm.imessage.inbox`
- `isaac.comm.imessage.routing`
- `isaac.comm.imessage.state`

Potential responsibilities:

- `apple-script`: send via `osascript` / JXA
- `inbox`: poll Messages database for new inbound rows
- `routing`: map inbound thread -> Isaac session
- `state`: persist watermarks / mapping metadata
- `imessage`: public comm + lifecycle wiring

## Delivery Semantics

Outbound delivery should use the existing delivery-worker shape where possible.

Expected record fields:

- `:comm`
- `:target`
- `:content`
- retry metadata

`send!` should classify failures as:

- transient: retry
- permanent: dead-letter

## Inbound Polling Strategy

Start with polling, not file-watch magic.

Possible loop:

1. query `chat.db` for rows newer than stored watermark
2. ignore messages sent by self
3. normalize sender / thread identifiers
4. route to session
5. dispatch turn
6. advance watermark only after successful ingest

## Session Behavior

Recommended defaults:

- create session lazily on first inbound message
- reuse same session for same iMessage thread
- allow config override for crew / model selection

## Testing Strategy

### Unit Specs

- message normalization
- thread/session mapping
- watermark persistence
- outbound command construction
- failure classification

### Integration Specs

- fake inbound DB rows -> Isaac turn dispatch
- outbound send path through `send!`

### Acceptance Features

First scenarios should cover:

1. inbound text from a known thread reaches an Isaac session
2. new thread creates a session
3. Isaac reply is sent through Messages transport
4. duplicate polling does not reprocess the same message

## Recommended Implementation Order

1. outbound-only sender abstraction
2. delivery worker integration
3. persisted routing + watermark state
4. inbound poller against a test seam
5. end-to-end thread -> session -> reply flow
6. real `chat.db` adapter on zanebot

## Main Risk

Outbound via `osascript` is straightforward.

Inbound iMessage observation is the risky part because it depends on local
Messages database behavior and macOS permissions rather than a supported server
API.

## Recommendation

Build this as a dedicated module with a small, careful MVP focused on direct
message text threads only. Prove reliability on `zanebot` before expanding the
surface area.
