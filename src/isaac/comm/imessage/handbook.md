<!--
Lint convention (isaac.comm.imessage.handbook-chapter-spec, isaac-6nfg,
following isaac.foundation's own): a backtick `config:<dotted.path>`
reference (no angle-bracket placeholder inside the path) is checked against
the composed config schema, and the word right after `isaac ` in
`isaac <command>` is checked against the registered top-level CLI commands.
Keep both literal and real when you write one — the lint fails the build
once either drifts from what Isaac actually exposes. `<placeholder>` shapes
(e.g. `config:<dotted.path>` itself, or `<module-id>#<slug>`) are
intentionally skipped.
-->

# isaac.comm.imessage — iMessage (macOS)

You are a crew running inside Isaac. This chapter covers what
**isaac-imessage** owns: one `imessage`-type entry in the `comms` table, a
subprocess that reads and sends real iMessages on a macOS host, the inbound
gate that decides which messages start a turn, and outbound delivery with
chunking and retry classification. It uses "crew", "session", "comm",
"comm__send", and "delivery worker" the way `isaac.agent` defines them —
read that chapter first if you haven't; this one names them once and moves
on. Config mechanics (`handbook__configure`, hot reload, `${VAR}` secrets)
are `isaac.foundation`'s.

An `imessage` comm has no CLI commands, tools, or slash commands of its
own — everything here is a config path under that comm's entry in
`comms.<name>.*`.

**The whole module is macOS-only.** It reads Apple's Messages database
directly and sends through Apple's own Messages automation. There is no
Linux/Windows equivalent — the host running the subprocess (directly, or as
the far end of an `imessage/command` tunnel, see Transport, below) must be a
Mac signed into iMessage.

## The comm entry

**What it is.** An iMessage comm is one entry in the `comms` table. The impl
is selected by `type` when present, otherwise by the slot's own name — so
the conventional slot name `comms.imessage` needs no explicit `type` field,
but a differently-named slot does. Fields:

| Field | Type | Purpose |
|---|---|---|
| `imessage/db-path` | string | Absolute path to Apple's `chat.db` (typically `/Users/<you>/Library/Messages/chat.db`). Required to spawn the subprocess at all — omitting it leaves the comm inert (no client, no watch), which is deliberate so a test or a misconfigured host doesn't touch a real database. |
| `imessage/bin` | string | Path to the `imsg` binary when it's on a PATH the running process can see. Defaults to whatever `PATH` resolves; ignored when `imessage/command` is set. |
| `imessage/command` | seq of strings | Full argv prefix launched instead of a local `imsg` — see Transport, below, for the remote/tunnel case. Takes precedence over `imessage/bin`. |
| `imessage/service` | string | Which transport `imsg` should use for **every** send from this comm: `"auto"`, `"sms"`, or `"imessage"`. Omit it — `"auto"` is what Isaac does by default and is also the only transport confirmed to actually deliver; see Outbound, below. |
| `imessage/inbound?` | boolean | `false` makes this comm send-only: Isaac never opens the inbound watch. See Send-only comms, below. |
| `imessage/allow-from` | seq of strings | Inbound sender allowlist — see Inbound, below. |
| `imessage/message-cap` | int | Character count above which a reply is split into multiple sends. Default 2000. |
| `imessage/max-chunks` | int | Hard cap on how many chunks one reply produces. Default 3. |

**How to change it.** Set fields with `handbook__configure` (or `isaac config
set`), using placeholder values in place of a real path or handle:

```
config set comms.imessage.imessage/db-path /Users/example/Library/Messages/chat.db
config set comms.imessage.imessage/allow-from '["+15555550123", "friend@example.com"]'
config set comms.imessage.imessage/message-cap 1500
```

A second iMessage comm (a second Mac, or the same Mac reached a second way)
is a second `comms` entry with its own name and its own fields — there is no
shared or default instance.

**How to verify.** `config get comms.imessage` (or `config get
comms.<name>`) shows the resolved entry; `isaac config validate` reports an
unknown or mistyped field by name. Config changes take effect live — see
Config hot reload in `isaac.foundation`'s chapter — the running subprocess
is **not** restarted for a plain field change (message-cap, allow-from); see
Troubleshooting below for which changes do need a fresh spawn.

### Troubleshooting

- **A field doesn't seem to apply.** Confirm the slot's `type` (explicit or
  implied by its name) really is `imessage` — a typo'd or differently-typed
  slot never reaches this schema at all.
- **The comm never starts (no subprocess, nothing in the logs about it).**
  `imessage/db-path` is unset. That's by design — a comm with no db-path
  stays completely inert rather than defaulting to a guessed path.
- **A config change to `imessage/db-path`, `imessage/bin`, or
  `imessage/command` doesn't seem to take effect on the running comm.** Only
  fields read at spawn time (the subprocess launch arguments) require a
  fresh subprocess to pick up; other fields (`imessage/message-cap`,
  `imessage/allow-from`, `imessage/inbound?`, `imessage/service`) apply to
  the very next inbound/outbound event on the already-running subprocess.
  Unset and reset the comm slot (or restart the host process) to force a
  respawn if you changed how `imsg` itself is launched.

## Transport: the imsg subprocess and macOS permissions

**What it is.** Isaac never talks to Messages directly. It spawns and holds
open one long-lived `imsg rpc` subprocess (the `imsg` CLI project) and
exchanges newline-delimited JSON-RPC 2.0 over its stdin/stdout: `send` to
post a message, `watch.subscribe` to open the live inbound feed, plus
history/chat-list lookups `imsg` itself may need. `imsg` is what actually
reads `chat.db` and drives Apple's Messages automation — Isaac's own code
never touches SQLite or AppleScript.

Two ways to point at it:

- **Local** — `imessage/bin` (or bare `imsg` on `PATH`) plus `imessage/db-path`
  naming a file on the same host. Isaac checks the db file exists and is
  readable *before* spawning; a missing or unreadable path fails fast with a
  log entry rather than spawning a subprocess that would only fail later.
- **Wrapped/remote** — `imessage/command` names a full launch prefix (for
  example a `ssh` invocation ending in the remote `imsg` binary). Isaac
  appends `rpc --db <imessage/db-path>` to whatever prefix you give it and
  skips the local file-readability check entirely, since the path is
  meaningful on the far end, not on the host running Isaac. This is how a
  non-Mac (or a second Mac) host relays through one Mac's `chat.db` without
  copying it anywhere.

**Permissions the Mac running `imsg` needs.** `imsg` itself needs:

- **Full Disk Access**, to open `chat.db` under `~/Library/Messages/` — a
  TCC-protected path on modern macOS. Isaac's own error path surfaces this
  by name: a failed subscribe or send whose underlying error carries
  "Permission Error … grant Full Disk Access" is logged with that text
  intact (see `-imsg-error-message` — it prefers the actual permission
  detail over `imsg`'s generic "Internal error" wrapper).
- **Automation permission to control Messages**, for `imsg` to actually send
  through Apple's Messages automation. `[verify]` — Isaac's own code and
  specs confirm the Full Disk Access failure mode exactly (see above); the
  exact system-permission name and dialog for the send path is inferred
  from how `imsg` sends, not asserted anywhere in this repo, so treat the
  precise wording as a starting point rather than a guarantee.

Both are granted once, in the Mac's System Settings, to whatever process
actually runs `imsg` (a Terminal session, a LaunchAgent, or — for a
`imessage/command` wrapper — the remote login shell on the far end). Isaac
has no config path for either; they're OS-level grants outside Isaac
entirely.

**How to verify.** `isaac logs server` (or `cli`) shows `:imsg.client/db-path-unavailable`
(local path check failed before spawn), `:imsg.watch/subscribe-failed` /
`:imsg.watch/subscribe-timeout` (subprocess spawned but the watch call
itself failed), or `:imsg.client/start-failed` (the subprocess never came
up) — each carries the resolved db-path/bin/command for that slice so you
can see exactly what was attempted.

### Troubleshooting

- **Startup logs `:imsg.client/db-path-unavailable`.** The configured
  `imessage/db-path` doesn't exist or isn't readable *by the process
  running Isaac* — check the path itself, then Full Disk Access for that
  process. This check only runs for a local (non-`imessage/command`) setup.
- **A subscribe or send fails citing "Permission Error" / "Full Disk
  Access".** Grant Full Disk Access to the process actually running `imsg`
  (not necessarily the process running Isaac, if you're using
  `imessage/command`), then let Isaac's automatic reconnect pick the
  subprocess back up — see Delivery failures, below, for the reconnect
  schedule.
- **A remote (`imessage/command`) setup never reports a bad local path, but
  nothing ever arrives.** That's expected — the readiness check is
  local-only. Check permissions and `chat.db` health on the *remote* host
  instead, using the same log events, since they still flow back through
  the same JSON-RPC channel.

## Inbound: the allow-from gate and session routing

**What it is.** `imsg watch.subscribe` pushes one JSON-RPC `message`
notification per new inbound row, database-wide — not scoped to one
conversation. Each notification passes through a fixed filter, in order:

1. **Self.** A message flagged `is_from_me` is dropped before anything else
   — it never reaches the allow-from check and is never logged above debug.
2. **Send-only.** If this slot has `imessage/inbound? false`, every
   notification is dropped here — see Send-only comms, below.
3. **Allow-from.** `imessage/allow-from` is matched by **exact string**
   against the sender handle (a phone number or an email, whatever Apple
   reports) — no wildcard or domain form. Missing/`nil` allows everyone;
   an **empty** list is fail-closed and drops everyone. A drop here logs
   `:imessage.intake/drop-sender` at `:debug`, with the handle that didn't
   match, meant to be copied straight into the allowlist.

A message that survives the gate becomes a work item and dispatches a turn
immediately — there is no separate "heard but not answered" state the way a
mentions-gated comm has; every allowed, non-self inbound message is a turn.

**Session routing.** Each Apple chat (`chat_guid`) maps to exactly one
session, named `imessage:<chat-guid>` — stable across restarts, since `imsg`
reports the same `chat_guid` for every message in a thread. There is no
config for retargeting one chat's session, crew, or tags the way some other
comms allow per-conversation overrides; every chat on this comm slot routes
through the same crew resolution `isaac.agent` uses for the slot.

**Trusted turn context.** Every iMessage turn gets a small JSON block
(schema `isaac.inbound_meta.v1`) prepended to the soul, naming the provider,
the chat guid, the sender handle, and that it's a direct one-on-one surface
— framed explicitly as trusted metadata, distinct from the untrusted message
text. There's no config for this; it's unconditional for every turn this
comm dispatches.

**How to change it.**

```
config set comms.imessage.imessage/allow-from '["+15555550123"]'
config unset comms.imessage.imessage/allow-from
```

**How to verify.** `isaac logs server` shows `:imessage.intake/drop-sender`
(debug — reason logged at debug because self/allowlist misses are routine,
not incidents) for anything the gate dropped, with the offending
handle and chat guid; `isaac sessions list` shows the session a chat landed
on, named `imessage:<chat-guid>`.

### Troubleshooting

- **A sender that should be allowed is still dropped.** Compare the exact
  handle in the `:imessage.intake/drop-sender` log against
  `imessage/allow-from` character-for-character — this filter has no
  wildcard, so a phone number formatted differently than Apple reports it
  (with/without country code, spaces, dashes) won't match.
- **Nothing is ever answered, even from an allowed sender.** Check
  `imessage/allow-from` isn't an **empty** list — empty is fail-closed
  (drops everyone), which reads very differently from leaving the key unset
  entirely (allows everyone).
- **The same chat keeps landing on a *new* session instead of continuing
  the old one.** The session key is derived purely from `chat_guid` — if
  Apple assigns a new guid for what looks like "the same" conversation
  (for example, a merged SMS/iMessage thread), a new session is created for
  it; there is no manual remap.

## Send-only comms and a shared chat.db

**What it is.** `watch.subscribe` is database-wide: every host pointed at
one `chat.db` (whether directly, or through separate `imessage/command`
tunnels into the same Mac) sees every inbound row. Two comms watching the
same database both dispatch a turn for the same inbound message and both
reply — a duplicate, not a retry. `imessage/inbound? false` is the fix:
Isaac skips `watch.subscribe` entirely for that slot, so it neither watches
nor answers, while outbound sends from that slot still work normally.
`imessage/allow-from []` looks similar (it also drops every inbound message)
but still opens a subscription the host never uses — `imessage/inbound?` is
the real switch; `imessage/allow-from` is the sender filter, not an inbound
on/off toggle.

**How to change it.**

```
config set comms.imessage.imessage/inbound? false
```

**How to verify.** `isaac logs server` shows `:imsg.watch/send-only` at
startup for a slot configured this way (instead of `:imsg.watch/subscribed`),
naming the db-path so you can confirm which slot opted out.

### Troubleshooting

- **Two hosts (or two comm slots) are both replying to the same inbound
  message.** They're both pointed at the same underlying `chat.db` with
  `imessage/inbound?` unset (or `true`) on more than one. Set it `false` on
  every slot but the one that should actually watch and answer.
- **A send-only slot's outbound messages stopped working after you set
  `imessage/inbound? false`.** They shouldn't — sending is independent of
  the watch. If sends broke at the same time, look for an unrelated
  transport change (Transport, above) rather than assuming the two are
  linked.

## Outbound: chunking and transport

**What it is.** A crew's reply, or an agent-initiated message via the
shared `comm__send` tool (`isaac.agent`'s — see that chapter for the tool
itself), is split into chunks no longer than `imessage/message-cap` characters
(default 2000, preferring the last whitespace boundary within that limit),
each becoming its own iMessage send. If the reply would still produce more than
`imessage/max-chunks` chunks (default 3), everything past the cap is
dropped and replaced with one notice chunk naming how many were cut — this
bounds how badly a runaway reply can flood a phone with message bubbles.
`comm__send`'s schema for this comm accepts `imessage/target` (the
recipient handle) and an optional per-message `imessage/service` override.

**Transport service.** `imessage/service` (on the comm entry, or per-call on
`comm__send`) is passed to `imsg` as its `service` parameter, lowercased.
Leaving it unset sends no `service` field at all and lets `imsg` choose —
this is the confirmed-working default. Setting it to the literal value
`"imessage"` is **not** recommended: through `imsg`'s AppleScript transport
an explicit `imessage` send has been observed to report `-32001 "Delivery
outcome unknown"` and never actually arrive, while `"auto"` (or omitting
the field) delivers on the first attempt. `"sms"` forces the SMS/Continuity
path instead.

**Attachments.** This comm does not support attachments today —
`comm__send`'s schema for `imessage` carries no attachment field, and
nothing in the send path uploads or references a file. `[verify]` — worth
confirming with Micah whether that's an intentional MVP gap or a wanted
follow-up bean; nothing in the current code suggests it partially works.

**How to change it.**

```
config set comms.imessage.imessage/message-cap 1500
config set comms.imessage.imessage/max-chunks 5
config unset comms.imessage.imessage/service
```

**How to verify.** Trigger a long reply and check the number and length of
messages that actually arrive against `imessage/message-cap` /
`imessage/max-chunks`; a truncated reply's last chunk literally reads
`[reply truncated — N more chunk(s) dropped; keep replies shorter]`.

### Troubleshooting

- **An outbound message never arrives, and no error is logged.** Check
  `imessage/service` isn't explicitly set to `"imessage"` — that's the one
  documented-broken transport value; unset it or set `"auto"` instead.
- **A long reply arrives as far fewer messages than expected, with a
  truncation notice.** That's `imessage/max-chunks` working as intended —
  raise it (or raise `imessage/message-cap` so each chunk holds more) if
  the truncation itself is the problem.
- **`comm__send` refuses the call outright.** Confirm `imessage/target` is
  present — it's the only required field on this comm's send schema, and a
  blank target is refused before any send is attempted (logged
  `:imessage.send/no-target`).

## Delivery failures, retry classification, and reconnects

**What it is.** `imsg` answers a failed send with a structured error whose
`:data` states plainly whether retrying is safe — Isaac trusts that answer
ahead of guessing from the error text. `retry_safe false` (commonly paired
with `disposition "may_have_completed"`, meaning the send might already
have gone out) is treated as a **permanent** failure and dead-lettered
immediately, on the very first attempt — resending would risk saying the
same thing to a person twice. `retry_safe true` is **transient** and goes
through the generic delivery worker's retry/backoff (`isaac.agent`'s
chapter has that schedule). When `imsg`'s error carries no structured
`:data` at all, Isaac falls back to matching the error text: "not
authorized", "permission", "unknown buddy", "invalid handle", or "no such"
are treated as permanent; anything else is treated as transient.

**Subprocess reconnects** are a separate concern from a single send's
retry: if the `imsg` subprocess itself dies (crash, killed, Mac asleep),
Isaac schedules a respawn with exponential backoff starting at 1 second,
doubling up to a 10-minute ceiling, for up to 100 attempts before giving up
and logging that the retry loop itself stopped. A reconnect re-opens the
inbound watch automatically (unless the slot is send-only) once the new
subprocess is up — no config or manual action needed for an ordinary,
short outage.

**How to verify.** `isaac logs server` shows `:comm.delivery/dead-lettered`
(with `reason :permanent`) for a send that will never be retried, and
`:imsg.client/reconnected` once a died subprocess comes back. A dead
delivery record moves to the `failed` side of the delivery queue rather
than disappearing — `isaac.agent`'s chapter covers the generic queue/backoff
mechanics this comm's failures feed into.

### Troubleshooting

- **A send silently never retries, and the record is gone.** Check
  `isaac logs server` for `:comm.delivery/dead-lettered` — a "may have
  already sent" or explicitly-permanent classification is deliberate,
  by design, not a dropped retry.
- **The comm seems dead after the Mac slept or `imsg` crashed, then
  recovers on its own a while later.** That's the reconnect backoff working
  as intended — check `:imsg.client/reconnected` in the logs for when it
  actually came back; nothing needs to be configured to speed this up.
- **Reconnects seem to have stopped entirely after a long outage.** After
  100 failed attempts the retry loop itself stops and logs it (the
  scheduler's own exhaustion signal) — that's a genuine "needs a human"
  state, not something a config change fixes; the underlying `imsg`/Mac
  problem has to be resolved and the comm slot reloaded.
