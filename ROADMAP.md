# isaac-imessage Roadmap

## Phase 1 — Outbound (done)

- `imsg-client` JSON-RPC subprocess wrapper
- `ImessageComm/send!` routes records through imsg's `send` method
- Permission / unknown-buddy errors classify permanent and
  dead-letter on first attempt (thanks to isaac-pu2x)
- Live verified on zanebot (Intel host, universal imsg binary)

## Phase 2 — Inbound (done)

- `watch.subscribe` issued on comm startup; imsg pushes message
  notifications
- `notification->work-item` filters self / allow-from / no-chat,
  builds the Isaac work-item
- `on-imsg-notification!` dispatches the turn and enqueues reply
  chunks
- Live verified on zanebot (round-tripped a real iMessage through
  the crew)

## Phase 3 — Operational polish (in progress)

- launchd user agent so isaac survives zanebot reboots
- live exercise of:
  - allow-from drop on a disallowed sender
  - reply chunking on a long LLM response
  - imsg subprocess crash / restart recovery
  - macOS update / Messages.app restart resilience
- README hardened with troubleshooting (PATH, FDA, Automation)

## Stretch Goals

- per-chat crew/model overrides via config
- richer observability (latency histograms, drop counts, last-seen
  per chat)
- group chat support
- reaction / edit / unsend handling (probably needs SIP off; punt
  until the use case is real)

## Done — for reference

- isaac-imessage-5dbp: collapsed four legacy poll/dispatch/reply
  paths into a single inbound→dispatch→enqueue flow
- isaac-pu2x: delivery worker honors `:transient? false` from
  Comm/send!
