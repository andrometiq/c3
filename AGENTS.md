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

- Install with `make install` (or `/c3:build`): it stamps the build identity; a plain `go install`
  stamps `dev`.
- One broker serves every attached session, so restarting it bounces them all for a few seconds.
  Ask the maintainer before redeploying a broker that live sessions use.

## Releases

- A release is a pushed `v*` tag: the release workflow builds the GitHub Release and the updater
  reads the latest release. Pushing the default branch or redeploying a local broker is not a
  release. An untagged local build may report release version `dev`, and a local deploy alone
  never shows that anything was published.
- Check a release against what users receive (the latest GitHub Release), not the local deploy.
- Tags and pushes happen only on the maintainer's word.
- Before any push, besides the secrets scan, grep the push diff and its commit messages for the
  maintainer's keep-out terms (a local, gitignored list under `local-notes/`). Secret scanners
  have missed infrastructure strings here.

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
- Build features asked for by a downstream project as generic, adapter-agnostic C3 features. No
  downstream project names in code, docs, tests, fixtures or commit messages.
- Identity checks ("is this the same session / route / process?"): an unknown or empty identity
  never matches another unknown one, and identity is settled before anything that depends on it is
  answered. When a root cause points at a class of bug, sweep for the class in the same change or
  file it as owned work.
- Chat notices: if a notice would later need editing and that edit needs new durable state (for
  example a sent-message registry that survives restarts), don't send it; carry the information
  on a message that is sent anyway.
