---
# isaac-imessage-5dbp
title: Collapse the four poll/dispatch/reply paths into one dispatch-and-enqueue flow
status: completed
type: task
priority: normal
created_at: 2026-05-19T23:52:48Z
updated_at: 2026-05-20T16:26:46Z
---

## Context

`src/isaac/comm/imessage.clj` currently exposes four independent
paths through poll → dispatch → outbound, with no single canonical
end-to-end flow:

- `dispatch-work-item!`           — dispatch only, no outbound
- `dispatch-and-reply-work-item!` — dispatch + chunk + direct `send!`
                                    (bypasses the delivery queue;
                                    no retry, no dead-letter)
- `drain-once!`                   — poll + dispatch (no outbound)
- `drain-once-and-reply!`         — poll + dispatch + direct send!
                                    (also bypasses the queue)

The Comm/delivery infrastructure (queue, worker, backoff,
dead-letter — covered by `features/delivery/queue.feature` in isaac
and `features/comm/imessage/send.feature` here) is unreachable
from the inbound→reply path today. Crew responses bypass it.

## Why this matters

- Failure handling on outbound is inconsistent: queued sends get
  retry + dead-letter; inline replies get neither.
- Two code paths to maintain, two ways to test, two places to
  bolt on chunking/escaping/logging.
- The "headline MVP acceptance" path
  (`features/comm/imessage/reply.feature`, @wip) requires the
  enqueue wire anyway — once that's in, the direct-send paths
  become legacy.

## Scope (proposed)

After `features/comm/imessage/reply.feature` lands and inbound→
enqueue→worker→outbound is proven:

1. Delete `dispatch-and-reply-work-item!` and
   `drain-once-and-reply!`.
2. Make `drain-once!` (or its successor) the only canonical
   inbound flow: poll → dispatch → enqueue.
3. Move chunking into the enqueue step (one delivery record per
   chunk) instead of the deprecated direct-send path.
4. Update `imessage_spec.clj` to drop the with-redefs blocks on
   the removed functions; replace with assertions that delivery
   records were enqueued.

## Definition of done

- Only one inbound→outbound code path remains in `imessage.clj`.
- All outbound iMessage sends flow through the delivery queue.
- `bb spec` and `bb features` green.
- No regressions in `send.feature` or `reply.feature`.

## Origin

Surfaced during planning of `reply.feature`. The scope was
deliberately drawn tight there ("(a) — add enqueue-after-dispatch
alongside existing functions") so the feature could land first;
this bean captures the cleanup.
