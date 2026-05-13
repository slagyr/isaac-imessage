# AGENTS.md

Micah's AI assistant management tools for `isaac-imessage`.

## Toolbox

This project uses [toolbox](https://raw.githubusercontent.com/slagyr/toolbox/main/SKILL.md)
to manage agent components. If `.toolbox/` doesn't exist, fetch the toolbox
SKILL.md from the URL above and follow its instructions. In this repo, the
toolbox is expected to be checked in so commands and skills are available
immediately.

- **Project context:** See `PLAN.md`, `ROADMAP.md`, and `WORK_BREAKDOWN.md`.
- **Skills:** Load from `.toolbox/skills/{name}/SKILL.md` when their descriptions match the task at hand.
- **Commands:** When the user invokes a command by name (for example `/work`), read and follow `.toolbox/commands/{name}.md`.

### Commands

- `plan`
- `todo`
- `work`
- `plan-with-features`
- `verify`

## Working Style

- Prefer a small, test-driven MVP.
- Keep the current focus on the iMessage module itself; do not modify the main Isaac repo unless the user explicitly asks.
- Treat AppleScript / Messages automation as a boundary seam and keep it easy to stub in specs.
- Treat inbound message ingestion as a separate seam from outbound sending.

## Testing Discipline

- No production code without a failing unit test first.
- Run `bb spec` before considering a slice done.
- Use `bb lint <file>` after editing Clojure files when available in the parent project workflow; at minimum keep the repo green with `bb spec`.

## Scope Guidance

Prioritize these layers in order:

1. outbound send seam
2. persisted state and routing metadata
3. fakeable inbound polling seam
4. real Messages DB adapter
5. end-to-end conversation flow

## Repo Notes

- This repo is an Isaac module scaffold, not the main Isaac application.
- The local development dependency on Isaac is intentional so this module can align with Isaac's real abstractions during development.
