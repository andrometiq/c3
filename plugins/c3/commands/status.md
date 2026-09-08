---
description: C3 broker health, channel state, live route claims.
---

!c3-broker status

Display the output verbatim. If the broker is unreachable, tell the user to quit Claude Code (Ctrl-D or `/exit`) and relaunch with `claude --dangerously-load-development-channels=plugin:c3@c3` (append `--resume` to pick your session back up) — the next session's adapter will auto-spawn a fresh broker; a bare `claude` would leave inbound silently dead. (Don't bounce the broker from inside Claude Code; it recycles the MCP server.) For config-only changes, `/c3:reload-config` is non-disruptive. No other commentary.
