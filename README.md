# isaac-imessage

Bi-directional iMessage support for Isaac.

Runs as an Isaac module on a macOS host that is logged into the
Messages app. Inbound messages are polled from
`~/Library/Messages/chat.db` and dispatched to the configured crew;
outbound replies are sent through Messages via `osascript`.

See:

- `PLAN.md` — architecture and MVP scope
- `ROADMAP.md` — phased delivery plan

## Setup

### 1. macOS permissions

iMessage support requires two privacy grants on the host running
Isaac. Both are configured under
**System Settings → Privacy & Security**:

- **Full Disk Access** — needed to read
  `~/Library/Messages/chat.db`. Grant to the binary that runs
  Isaac (your terminal, `clojure`, `bb`, or the Java executable
  the JVM resolves to).
- **Automation → Messages** — needed to send iMessages via
  `osascript`. Grant the same binary the ability to control
  Messages. The first time Isaac calls `osascript`, macOS may
  prompt; the prompt only appears in a GUI session.

The integration must run in a logged-in GUI session on macOS —
not a headless daemon — because Messages.app must be running for
sends and the chat.db is owned by the GUI user.

### 2. Isaac config

Add `comms.imessage` to your `~/.isaac/config/isaac.edn`:

```clojure
{:comms {:imessage {:imessage/service     "iMessage"
                    :imessage/db-path     "/Users/zane/Library/Messages/chat.db"
                    :imessage/bin         "/usr/local/bin/imsg"
                    :imessage/allow-from  ["+15551234567" "friend@icloud.com"]}}}
```

All slice keys live in the `:imessage/` keyword namespace so the comm
config doesn't collide with anything Isaac (or another module)
might inject into the same map.

- `:imessage/service` — Messages service name. Almost always
  `"iMessage"`; use `"SMS"` only if you specifically want SMS over
  a paired phone.
- `:imessage/db-path` — absolute path to the Messages chat database.
  Required to spawn the imsg subprocess; omitting it leaves the
  comm dormant (handy for non-Mac dev, and a guard so tests can't
  accidentally hit a real chat.db).
- `:imessage/bin` — path to the imsg binary. Defaults to whatever's
  on `PATH`; set explicitly when the process launching isaac
  doesn't see `/usr/local/bin` or `/opt/homebrew/bin` (common for
  headless processes like launchd jobs).
- `:imessage/allow-from` — phone numbers / emails (string allowlist).
  Notifications from senders not in this list are dropped at
  debug log level (`:imessage.intake/drop-sender`).
  **Fail-closed**: an empty list drops everything; omit
  `:imessage/allow-from` to skip filtering entirely.

Optional:

- `:imessage/message-cap` — split replies above this character count
  into multiple sends. Default 2000.
- `:imessage/max-chunks` — hard cap on how many chunks a single reply
  produces. Above the cap the tail is dropped with a truncation
  notice so a runaway LLM response can't flood Messages. Default
  3 (≈6000 chars total at the default `:imessage/message-cap`).

### 3. Verify the host

Run the smoke check before bringing Isaac up:

```
bb smoke
```

Verifies `osascript`, `sqlite3`, and a successful read of
`chat.db`. To also confirm Automation permission for Messages,
pass a handle + body:

```
bb smoke +15551234567 "test from isaac-imessage"
```

A real iMessage should appear in your Messages window. If the
send fails with `Not authorized to send Apple events`, you
haven't granted Automation. If the chat.db read prints
`exit=1 err=unable to open database`, you haven't granted Full
Disk Access.

### 4. Start Isaac

```
isaac server
```

On startup, Isaac discovers this module via classpath manifest,
registers the `imessage` Comm in the comm registry, and starts
the poller. Look for these log events to confirm it's wired:

- `:comm/activated` `comm=imessage`
- `:imessage.poller/started` `interval-ms=…`

Inbound iMessages from allow-listed senders should now route to
sessions named `imessage:<chat-guid>`, and crew responses should
land back in the Messages thread.

## Development

```
bb spec       # unit specs (66+ examples)
bb features   # gherclj feature suite
```

Both should be green before pushing. The pre-push hook (if
installed) will re-run them.

## License

MIT

Copyright (c) 2026 Micah Martin

See [`LICENSE`](LICENSE).
