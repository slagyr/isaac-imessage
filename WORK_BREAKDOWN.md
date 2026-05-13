# Work Breakdown

## Slice 1: Outbound Sender Seam

Goal:

- construct a safe `osascript` invocation for Messages.app
- classify send failures as transient or permanent

Deliverables:

- `isaac.comm.imessage.apple-script/build-script`
- `isaac.comm.imessage.apple-script/send-message!`
- specs for escaping, script shape, and failure classification

## Slice 2: Persistent Integration State

Goal:

- define and persist the minimal state needed for inbound polling

Deliverables:

- watermark storage
- thread -> session mapping storage
- read/write helpers with default empty state

## Slice 3: Top-Level Composition

Goal:

- provide one namespace that ties sender + state together

Deliverables:

- small top-level API for delivery and state access
- no inbound poller yet

## Later Slices

- inbound database seam
- real `chat.db` poller
- duplicate suppression
- end-to-end thread routing
- operational setup for `zanebot`
