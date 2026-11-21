# Operations notes

Practical knowledge from running and developing C3 day to day that the other docs don't hold.
`DEBUGGING.md` (root) covers log lines and diagnostic flows; `docs/INSTALL.md` covers setup;
`AGENTS.md` covers releases. Paths below assume the defaults (`$XDG_STATE_HOME` unset).

## Inbound silently missing

- **Allowlist first.** Broker, token and polling all healthy but nothing arrives:
  `grep 'GATE drop' ~/.local/state/c3/broker.log`. Every sender not in the `allowlist` of
  `~/.config/c3/mappings.json` is dropped (`internal/broker/pairing.go`, `Gate()`). The fix is an
  `"allowlist": {"users": [<owner user id>], "groups": [<group chat id>]}` entry, then a hot reload
  with SIGHUP (`/c3:reload-config`, or `pkill -HUP -x c3-broker`), but only when
  `$XDG_RUNTIME_DIR/c3-broker.caps` lists `sighup-reload` (older brokers treat SIGHUP as terminate).
  A bad edit is rejected and the old config kept.
- **`c3-broker reload-config` is not a subcommand,** though `cmd/c3-broker/main.go`'s header comment
  lists it. Reload is the slash command or SIGHUP.
- **"Missing" message ids are usually not drops.** Forum-topic message ids are chat-global (other
  topics and service messages use them up). Check `broker.log` (one `inbound update=<uid> msg=<id>
  thread=<t> kind=<k>` line per update; contiguous update ids mean nothing was missed) and the
  per-topic queue JSONL under `~/.local/state/c3/queue/` (`.trash/` is retention) before calling it a
  loss. Bursts arrive merged; the merge keeps each source id (`merged_message_ids`).
- **A Claude Code session started without the dev-channels flag** (forked or background sessions):
  check `tr '\0' ' ' </proc/<claude-pid>/cmdline`. Such a session can still receive live through the
  owning-session inbox when that inbox is eligible (`c3-broker status` shows `live: inbox, confirmed
  <age>`). Only when neither transport works does the route go `pull-only (<reason>)` with a held
  notice; recover with `fetch_queue`.
- **Background agents** (`--bg`, `claude agents`) never render channel blocks live: they don't
  inherit `--dangerously-load-development-channels`, and no settings key substitutes.
- **A broker that stops answering:** try `c3-broker restart` first. Only for a broker that no longer
  answers its socket, `pkill -x c3-broker` as a last resort; an adapter respawns it and queue files
  persist, but it skips the controlled drain (open Allow/Deny or ask prompts are dropped, not
  cancelled with a notice).

## Telegram offline while the internet works

Seen once as an IPv6 path-MTU blackhole on a home network: IPv6 connects to `api.telegram.org` but TLS
never completes, while IPv4 is fine. `c3-broker status` shows `DOWN … outbound send failing`; compare
`curl -4` and `curl -6 https://api.telegram.org/`. Fixed in v0.4.0 (before it, the outbound-down latch
also deadlocked held voice notes).

## Deploying a local build

- One broker serves every session; a restart bounces all attached sessions for a few seconds (they
  reconnect, the queue holds). Ask whoever is using it before bouncing it mid-work.
- Deploy with `make install && c3-broker restart` (what `/c3:build` runs). `make install` stamps
  `internal/buildidentity.ID`; a plain `go install` stamps `dev`. A local deploy never publishes
  anything (`AGENTS.md`, Releases).
- `c3-broker restart` is the controlled restart: it refuses new prompts and pushes, waits up to 60 s
  for open prompts, then cancels them with a notice on their own route (90 s watchdog). Don't deploy
  with `pkill`: that skips the drain. Verify with `c3-broker status` (build id) and `broker.log`.
- Adapters are per session. Current adapters re-exec onto a newer broker build; an older one gets a
  notice to reconnect. In Claude Code `/reload-plugins` does not restart MCP servers; `/mcp` →
  Reconnect does.
- Claude Desktop keeps its adapter for the app's whole life: fully quit the app (tray → Quit), then
  check with `ps -eo pid,lstart,args | grep c3-desktop-adapter` that it started after the new binary.
- Claude Code ≥ 2.1.245 exports the stable (resumed) session id as `CLAUDE_CODE_SESSION_ID`; older
  versions exported a per-spawn id. The handoff keys (instance, stable id, owner PID, `internal/hostid`)
  exist because of this.
- Broker log timestamps are UTC.

## Tests on a busy machine

- Keep the Go cache off a small tmpfs (`GOCACHE=$HOME/.cache/...`): a full `/tmp` makes every later
  command fail quietly.
- The pre-commit hook runs the full Go gate plus cross-compiles; it is not cheap. On a loaded machine
  run full suites with `go test -p 2 ./...`, one heavy job at a time.
- Known flaky tests and how to treat them: `docs/WORKPLAN.md`, "Known flaky tests".
- When an AI agent runs Codex lanes in this repository, launch them with
  `-c mcp_servers.c3_codex.enabled=false`: without it a headless lane loads C3 and can attach to a
  live topic.
- `pgrep -f` / `pkill -f` patterns match their own shell; use `[b]racket` patterns.

## The live matrix

`scripts/live-matrix/run.sh` (`docs/TESTING-LIVE-MATRIX.md`) runs for about three hours and spawns
real CLI sessions; run full passes from your own terminal, not from an agent (classifiers and memory
supervisors stop it). Single cells in the foreground are fine. Run 3 (2026-09-12, Claude Code
2.1.269): 91 pass, 32 fail, 3 not reached, zero C3 delivery defects; every failure was the harness
driving the host (resumed and active states). On failures: triage the failing cells' retained
evidence first (harness defect, product defect or infeasible setup), rule, fix, then re-run only the
failed cells with `--cell`.

## Speech to text

- The chain runs on bare `python3` plus ffmpeg (`plugins/c3/stt/stt-pkg/stt.py`), with keys loaded
  from an env file it names; no venv or PyPI packages. A broker under systemd needs
  `plugins.stt.handler_path` in `mappings.json` (there is no `$CLAUDE_PLUGIN_ROOT` there).
- LLM speech-to-text invents fluent transcripts from silent audio whatever the prompt says. The guard
  is the ffmpeg energy gate before any provider (`C3_STT_SILENCE_MAX_DB`, default -50 dB; silence is
  about -91 dB, speech -30 to -3 dB). Quiet but real speech can still be embellished.
- Default model `google/gemini-3.7-flash` since v0.2.1: no regression against 3.6-flash, better on
  proper names, half the price. 3.5-flash-lite was rejected (drops long audio). Some errors (a version
  number rewritten) are a coin flip on every model; never judge a model from one run. The bake-off
  harness is `plugins/c3/stt/bakeoff/run_bakeoff.py` (`C3_STT_GEMINI_MODEL`).

## Telegram rendering facts

`<blockquote expandable>` collapses only in normal messages (at most 4096 characters), not in rich
messages; Telegram's own "Show more" starts around 8000 characters; `<details>` collapses but jumps,
and its collapsed text is still searchable. Pipe tables can render as broken text on some clients;
prefer lists. A reaction on a message (C3's own `react` included) makes Telegram emit an
`edited_message` seconds later; C3 suppresses it (`DEBUGGING.md`).

## Before a push

- `make ci` green.
- A secrets scan over the push (for example `gitleaks git --log-opts=origin/master..HEAD`).
- Grep the diff and the commit messages (`git log -p origin/master..HEAD`) for personal names, home
  paths, hostnames and IPs, account ids and downstream project names: the repository is public, and
  secret scanners have missed infrastructure strings here. The maintainer keeps the exact term list.
- No session links or session ids in commit messages.
