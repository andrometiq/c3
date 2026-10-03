# AGENTS.md — C3

Guidance for AI coding agents (and human contributors) working in this repository.
Start here, then read `README.md` for the full picture.

## What C3 is

C3 bridges chat channels (Telegram today) to coding-CLI sessions (Claude Code, Codex,
and more) through one broker daemon plus a set of per-CLI MCP adapters, with
topic-based routing so many sessions can share one chat. Written in Go. See `README.md`
and `docs/` for the architecture.

## Session start

1. `README.md` — what C3 is and the architecture.
2. `ROADMAP.md` — future and not-yet-built work.
3. `DECISIONS.md` — decisions taken and their rationale.
4. `WORKLOG.md` (local, gitignored) — `sed -n 1,12p WORKLOG.md; grep '^## ' WORKLOG.md | tail -5`,
   then read only the last entry. Never read the whole file.

## Build · test · check

- Build:   `go build ./...`   (or `make`)
- Test:    `go test ./...`    — hermetic; no network required
- Vet:     `go vet ./...`
- Format:  `gofmt -l .` should print nothing
- Full CI gate: `make ci`
- Install the repository pre-commit gate once per clone: `make hooks`

Keep the tree green: build + `go test ./...` must pass before a change is considered done.

## Where to read

- `docs/` — deeper design: `CHANNELS.md`, `PLUGINS.md`, `COMMANDS.md`, `DESKTOP.md`, `INSTALL.md`
- `TODO.md` — the v0.1 release record (closed)

## Launching Claude Code with C3

Inbound Telegram messages render live only when Claude Code starts with
`claude --dangerously-load-development-channels=plugin:c3@c3` (append `--resume` or
`--resume <id>` to resume). Why, and the settings it needs: `docs/INSTALL.md` Step 4.
Give the maintainer the full command; never suggest an alias for it.

## Conventions

- Discuss non-trivial changes before building; the maintainer makes the final call.
- Commit style: `c3: <imperative one-line summary>`, then a short body explaining *why*.
- This is a PUBLIC repository. Never commit secrets, credentials, personal names or home
  paths, or internal infrastructure hostnames/IPs. Keep committed docs generic.
- Prefer small, reviewable commits that keep the build and tests green at every step.
