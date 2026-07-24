# AGENTS.md — C3

Guidance for AI coding agents (and human contributors) working in this repository.
Start here, then read `README.md` for the full picture.

## What C3 is

C3 bridges chat channels (Telegram today) to coding-CLI sessions (Claude Code, Codex,
and more) through one broker daemon plus a set of per-CLI MCP adapters, with
topic-based routing so many sessions can share one chat. Written in Go. See `README.md`
and `docs/` for the architecture.

## Build · test · check

- Build:   `go build ./...`   (or `make`)
- Test:    `go test ./...`    — hermetic; no network required
- Vet:     `go vet ./...`
- Format:  `gofmt -l .` should print nothing

Keep the tree green: build + `go test ./...` must pass before a change is considered done.

## Where to read

- `README.md` — what C3 is + the architecture
- `docs/` — deeper design: `CHANNELS.md`, `PLUGINS.md`, `COMMANDS.md`, `DESKTOP.md`, `INSTALL.md`
- `DECISIONS.md` — decisions taken + rationale
- `ROADMAP.md` — future / not-yet-built work
- `TODO.md` — the current finish-line checklist

## Conventions

- Discuss non-trivial changes before building; the maintainer makes the final call.
- Commit style: `c3: <imperative one-line summary>`, then a short body explaining *why*.
- This is a PUBLIC repository. Never commit secrets, credentials, personal names or home
  paths, or internal infrastructure hostnames/IPs. Keep committed docs generic.
- Prefer small, reviewable commits that keep the build and tests green at every step.
