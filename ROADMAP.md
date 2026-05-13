# isaac-imessage Roadmap

## Phase 1

Establish outbound-only delivery.

Candidate beans:

- add `osascript` sender abstraction
- add outbound failure classification
- add `Comm/send!` implementation for iMessage
- add delivery-worker integration

## Phase 2

Persist routing and inbound cursor state.

Candidate beans:

- thread/session mapping store
- watermark persistence
- self-message suppression

## Phase 3

Implement inbound polling.

Candidate beans:

- test seam for inbound message source
- Messages `chat.db` poller
- normalization of inbound rows to Isaac turns

## Phase 4

Connect end-to-end conversation flow.

Candidate beans:

- lazy session creation per thread
- reuse existing session for known thread
- inbound text -> turn dispatch -> outbound reply

## MVP Acceptance Goals

1. inbound text from a known thread reaches an Isaac session
2. new thread creates a session
3. Isaac reply is sent through Messages
4. duplicate polling does not replay the same inbound message

## Stretch Goals

- configurable crew/model selection per thread
- richer logging and observability
- operational setup guide for `zanebot`
