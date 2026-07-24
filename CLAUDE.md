# CLAUDE.md — C3

## Session Start (do this every time)

1. Read `README.md` — what C3 is and the architecture.
2. Read `ROADMAP.md` — future/unbuilt work (what's next after v1).
3. Read `TODO.md` — the v0.1 finish-line checklist.
4. Read `DECISIONS.md` — all decisions made so far.
5. Read `AGENTS.md` — build/test commands + conventions (shared with Codex and other agents).

## The Short Version

C3 is a Telegram multiplexer for multiple Claude Code and Codex CLI sessions. One daemon, many MCP adapters, topic-based routing. Written in Go. See README.md for the full architecture.

Discuss before building anything non-trivial. The maintainer makes final calls.

## Launch command (for the maintainer to type)

For the Telegram channel to actually surface inbound messages in this CLI,
Claude Code must be started with the development-channels flag:

```
claude --dangerously-load-development-channels plugin:c3@c3
```

(or the same with `--resume` / `--resume <id>` appended)

A plain `claude` leaves the c3 channel notifications enabled at the broker
but not rendered live in the session. As of v0.1 this no longer means silent
loss: the adapter detects a host that can't render and the broker **holds
those messages in the durable queue** (a held-notice fires in the topic; the
agent recovers them with `fetch_queue`) while the session keeps its claim for
outbound. The flag is still recommended so inbound renders live as
`<channel>` blocks. Both gates matter for live rendering:
`~/.claude/settings.json`'s `channelsEnabled` + `allowedChannelPlugins` (the
user-side allowlist) AND the dev-channels flag (additional gate for
local-directory marketplace plugins like c3 that haven't shipped through
Anthropic's marketplace yet).

Do NOT alias this — the maintainer prefers the full command in shell history.
