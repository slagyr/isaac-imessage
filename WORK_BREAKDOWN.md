# Work Breakdown

The original polling-era slices (outbound seam → state seam →
inbox seam → poller → real chat.db → end-to-end) are obsolete.
The imsg migration collapsed them into a single inbound flow
driven by `imsg watch.subscribe`. See `PLAN.md` for the current
architecture and `ROADMAP.md` for phase status.

## Remaining slices

### Operational hardening

- launchd user agent for zanebot
- live coverage of allow-from drops, chunking, imsg crash recovery
- README troubleshooting section (PATH, Full Disk Access,
  Automation grant)

### Stretch

- per-chat crew/model overrides
- group chats
- reactions / edits / unsend
