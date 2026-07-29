# C3 — your coding agents, one Telegram inbox

C3 puts the coding-agent sessions already running on your machine behind a single Telegram
bot, with one forum topic per project. Send a message from your phone; the session attached
to that topic receives it, works, and answers in the same topic. If no session is attached,
the message waits on disk instead of disappearing.

It's a bridge to CLIs you already run — Claude Code, Codex, Claude Desktop, Grok Build, the
Antigravity CLI — not another agent runtime, not a hosted service. Your bot token, your queue,
your hardware. Go, MIT, Linux and macOS (Windows in beta).

```text
Telegram topic "api"  ⇄  c3-broker  ⇄  attached CLI session
                              │
                    no session attached?
                              │
                              ▼
                     durable inbound queue
```

## What it looks like

You send a voice note to the Telegram topic for a project:

> Run the tests, fix the flaky one, and tell me what changed.

Claude Code receives a native `<channel>` turn whose content is:

```text
[Transcribed voice]: Run the tests, fix the flaky one, and tell me what changed.
```

When a tool call needs approval, the topic shows the literal command and a real inline
keyboard:

```text
🔐 Permission: Bash

git push origin fix/flaky-test

[✅ Allow] [❌ Deny]
```

The agent's reply comes back to that same topic. If the session is down when you send the
next message, C3 stores it and tells you so:

```text
📨 Held — nothing lost.
1 message queued.


Send /status to check.
```

Attach a session to that topic and the queued message is waiting for it. You don't re-record
the voice note.

If C3 could not open its queue directory at startup it cannot make that promise, so it does
not make it — it says the opposite, in the same place:

```text
⚠️ NOT held — that message was dropped.
C3's durable queue is DISABLED for this run — it failed to open at startup, so inbound has no durable safety: anything not successfully handed to a live session is NOT saved and cannot be recovered.


Send /status to check.
```

It warns your DM at startup too, and `/status` keeps saying so until you fix it. Live delivery
to an attached session still works; it is the hold-while-you're-away guarantee that is off.
See *Degraded mode* in [`docs/USAGE.md`](docs/USAGE.md).

*(Those are the strings C3 actually renders, copied out of the code — not a mock-up.)*

## Adapters

An adapter is a small MCP server that connects one host CLI to the broker. Five ship:

| Adapter | Host | Inbound delivery | Tools | Notes |
|---|---|---|---|---|
| `c3-claude-adapter` | Claude Code | live push (native `<channel>` turns) | 12 | The reference adapter. Only one with `ask` and the permission relay. |
| `c3-codex-adapter` | Codex | live push through the Codex app-server | 12 | Adds `codex_forward`. No `ask`, no permission relay. Heavier install: launcher → app-server → adapter → TUI. |
| `c3-desktop-adapter` | Claude Desktop | pull only | 13 | Adds `observe` and `open_inbox`, plus an inbox panel. You ask Claude to check; it calls `fetch_queue`. |
| `c3-grok-adapter` | Grok Build | live push, needs leader mode | 11 | Requires `[cli] use_leader = true` in Grok's config. Without it, pull only. |
| `c3-agy-adapter` | Antigravity CLI | pull only | 11 | The host has no async push. Newest and least travelled; no dedicated doc yet. |

Claude Code is the adapter the others are measured against — everything else trades something
away, and the Inbound column is where you'll feel it. Install with `c3-broker
install-claude-shim`, `install-codex-shim`, `install-desktop`, `install-grok`, or
`install-agy`. Details in [`docs/ADAPTERS.md`](docs/ADAPTERS.md),
[`docs/DESKTOP.md`](docs/DESKTOP.md), and [`docs/GROK-INJECT.md`](docs/GROK-INJECT.md).

## Channels

One: Telegram.

| Channel | Status | Carries |
|---|---|---|
| Telegram | shipping | Markdown, quote-replies, six media kinds, edits, reactions, polls, inline buttons |

`internal/channel/channel.go` is the seam a second channel would sit behind, but nothing else
implements it — adding one is Go work today, not configuration. If your config has no
`channels.telegram.bot_token`, the broker starts with no transport at all rather than
failing. See [`docs/CHANNELS.md`](docs/CHANNELS.md).

## Bundled plugins

One, compiled into the broker: `stt`.

| Plugin | Does | Default | Needs |
|---|---|---|---|
| `stt` | Transcribes Telegram voice notes into `[Transcribed voice]: …` | on | `python3`, an API key for at least one provider, and `ffmpeg` for the pre-send silence gate (optional) |

It's a Go hook that shells out to a bundled Python chain. Four providers ship on disk and
run in order until one returns a transcript: OpenRouter Gemini Flash, Soniox Async v5,
ElevenLabs Scribe, then Sarvam Saaras. Providers without keys skip immediately;
`C3_STT_CHAIN` overrides the order. The handler resolves through an explicit config path,
Claude Code's plugin root, a source checkout, or the runtime bundle beside release
binaries; `install-desktop` records the resolved path for non-plugin hosts. Disable it with
`plugins.stt.enabled = false`.

External or loadable plugins are not implemented: `plugins.<name>` in the config is a
settings bag for built-ins, not a loader. See [`docs/PLUGINS.md`](docs/PLUGINS.md).

## Is C3 for you?

C3 fits when:

- you run coding agents across **several projects** and want one phone inbox without losing
  track of which project a message belongs to;
- you want to steer the **actual** CLI process on your machine — including setups behind
  Bedrock, Vertex, an API gateway, or a proxy;
- you want a **self-hosted** bridge whose bot token and message queue stay on your hardware.

It works fine with a single session. The architecture starts to matter once you have several.

## What ships today

- **One bot, many sessions.** A single broker owns the token. Every project gets a forum
  topic, and only the session holding that topic's claim receives its messages.
- **Topic routing and session resume.** Attach a session to a topic once; a resumed session
  silently re-attaches only to its own recorded topic. A fresh session asks before claiming
  anything.
- **Inbound survives sleeping sessions.** With no render-capable session on a topic, messages
  go to a durable on-disk queue for later readback instead of being lost.
- **Rich two-way Telegram** — markdown, quote-replies, attachments, edits, reactions, polls,
  and inline buttons.
- **Voice notes** — the bundled STT chain turns phone audio into `[Transcribed voice]: …`;
  the original attachment stays available for re-transcription.
- **Remote permission decisions** — Claude Code can relay its permission prompt as an
  Allow/Deny keyboard. Only a DM-paired operator's tap becomes a verdict, and command
  previews render literally rather than as markdown.
- **Your CLIs, your auth, your machine** — C3 is a local multiplexer. Each host keeps its
  normal models, credentials, tools, sandboxes, and project directories.
- **Local, inspectable state** — Go binaries, MIT licence, a mode-0600 config file, and a
  queue you can read on disk. No C3 cloud relay.

## Before you install — the trust model

Two things worth knowing up front, because they're easy to miss:

- **Anyone in the paired Telegram group can drive the agent.** C3 pipes chat messages into a
  coding CLI that holds tool access on your machine. Message content is trusted about as far
  as you trust the group. Approving a *tool call* is stricter — only a DM-paired operator's
  tap counts — but sending messages into the agent's context is group-wide. Pair a group
  you'd hand a shell to.
- **Your bot token is a password.** It lives in `~/.config/c3/mappings.json` (mode 0600) on
  your machine. Anyone holding it can read every message in that group and post as the bot.

## Install

Linux is the primary, fully supported platform. macOS has prebuilt binaries. Windows is
**beta**: prebuilt binaries are published, but inbound is poll-only and self-update is
refused while the `.exe` files may be live (quit C3 and re-extract the tarball instead).

In any Claude Code session, paste:

```text
follow https://github.com/Andrometiq/c3/blob/master/INSTALL.md to install c3
```

The playbook asks which host you're installing for and sets up the matching path. You provide
a Telegram bot token and send two short pairing codes; setup discovers your user id and the
group's chat id without an id hunt. Codex integration is a deliberate opt-in step, because
C3's launcher is *also* named `codex` and takes the real command's place on `PATH`. Grok and
Antigravity are set up after the base install with `c3-broker install-grok` / `install-agy`.

See [`INSTALL.md`](INSTALL.md) for the agent-driven playbook and
[`docs/INSTALL.md`](docs/INSTALL.md) for the human walkthrough.

### Why Claude Code needs a development-channels flag

Live inbound to Claude Code currently starts with:

```text
claude --dangerously-load-development-channels plugin:c3@c3
```

Claude Code applies that preview guardrail to every locally-installed channel plugin — it
isn't a C3 hack. Without the flag, C3 detects that the host can't render live channel turns
and keeps inbound in the queue for `fetch_queue` rather than dropping it; the installer can
also drop in a small `claude` shim so the flag is automatic. See Anthropic's
[Channels documentation](https://code.claude.com/docs/en/channels).

## Stability

C3 is pre-1.0. Interfaces can still move between minor versions; what's pinned is pinned
deliberately:

- **The on-disk queue format and the adapter IPC wire are pinned as of v0.1.0.** Every wire
  field carries an explicit JSON name, so renaming a Go field can't silently move the format
  under a queued message or a third-party adapter. The IPC handshake exchanges a
  `protocol_version` in both directions (currently 1; absent means 1). It is bumped only for a
  change a peer on the other version could misread — never for a new optional field or a new
  op, which stay compatible both ways. A mismatch is logged on both sides and the connection
  is kept, because a version skew is the normal state for a few seconds after an update.
- **`~/.config/c3/mappings.json` carries a `schema_version`.** The broker refuses a version it
  doesn't recognise rather than guessing at it.
- **Conversation, user, and message identifiers are Telegram-shaped 64-bit integers in v0.1.**
  They will become channel-scoped in a future minor version, when a second channel makes that
  necessary. That's a planned iteration, published here in advance — not a promise we intend
  to break quietly.

## Releases

Release tarballs are published on GitHub alongside a `SHA256SUMS` file. The updater
(`c3-broker update`, or `/c3:update` inside Claude Code) fetches over HTTPS and verifies the
SHA-256 digest before it replaces anything; a mismatch aborts with nothing installed, and a
release with no `SHA256SUMS` asset is refused outright.

Releases are **not** cryptographically signed yet. A checksum published next to the artifact
proves integrity of the download, not provenance of the build. Signing is planned, and from
the version that introduces it onward it will be mandatory — checksum-only verification will
stop being accepted at that point.

## How is this different from X?

Bridging a coding agent to Telegram is a well-populated genre. What each neighbour does, and
where C3 sits:

- **Anthropic's official Claude Code Channels** — the closest first-party option. It runs
  **one bot poller per open session** (no topic-per-project multiplexing), is Claude-Code-only,
  is blocked behind Bedrock/Vertex/gateways, and by its own docs delivers events *only while
  the session is open* — no offline queue. C3 is one bot → many sessions with a topic per
  project, cross-CLI, self-hosted behind any auth, and holds a durable backlog.
- **Happy** — polished native iOS/Android/web apps for Claude Code and Codex, with realtime
  voice and remote approvals. It's app-based: a session *list*, not one-bot topic-per-project
  routing, and no durable offline queue. C3 is chat-native (no app to install, group
  visibility) and queues what you send while a session is down.
- **cc-connect** — a Go daemon bridging many agents to many chat platforms, multi-project,
  with built-in STT/TTS. It has no Telegram forum-topic model, no documented durable offline
  queue, and handles permissions with a `/mode` toggle rather than relaying the CLI's own
  prompt. C3 trades breadth for the topic-per-project + durable-queue + prompt-relay cut.
- **OpenClaw** — a self-hosted 24/7 *assistant gateway* with its own agent runtime and a
  skills marketplace. Different product: it's an always-on assistant platform, not a
  multiplexer of the real CLI sessions you already run. C3 drives your actual CLIs.

No single trait here is unique — self-hosted, CLI-agnostic, one token multiplexed into
per-project topics, a durable inbound queue: each one has a rival. C3 is where all four
overlap. If you only need one of them, one of the above is probably the easier install.

## Architecture

```text
   Telegram Bot API
          │
   ┌──────┴───────┐
   │  c3-broker   │   one poller, routing + claims, durable queue,
   │  (Go)        │   plugin host, local IPC socket
   └──────┬───────┘
          │  local Unix socket
   ┌──────┼────────┬─────────┬─────────┐
   ▼      ▼        ▼         ▼         ▼
 claude  codex  desktop    grok       agy
 adapter adapter adapter  adapter   adapter
   │      │        │         │         │
   ▼      ▼        ▼         ▼         ▼
 CLI     TUI    Desktop     TUI       CLI
```

**Broker.** One long-running Go process owns the Telegram poller, per-route workers, topic
claims, durable queues, outbound rate limits, and plugin hooks. Adapters connect over a local
Unix socket. A singleton lock stops two brokers polling one token on the same machine.

**Adapters.** Thin MCP stdio servers connecting each host process to the broker. They expose
only the capabilities that host can actually support, and reconnect after a broker bounce.
Codex's live path adds its launcher and app-server because the app-server — not the TUI —
owns MCP startup.

**Channels.** Telegram, and the interface a second transport would implement.

**Plugins.** Built-in Go plugins subscribe to broker hooks. The shipped STT plugin drives a
bundled Python provider chain; external loadable plugins remain roadmap work.

**Config.** `~/.config/c3/mappings.json`, written mode 0600 with an atomic rewrite and one
backup. It contains the bot token — treat it like a password.

## Routing

- **Telegram topics** are the primary path. A topic maps to at most one live session claim; a
  fresh session asks before creating, claiming, or stealing.
- **Bot DMs** route to whichever CLI explicitly claims `dm`.
- **Groups without topics** can map a whole group to one CLI.

## Extending C3

- Add a built-in broker plugin under `internal/plugin/builtins/<name>/`; STT is the worked
  example.
- Implement a new channel behind `internal/channel/channel.go` and wire it into the broker.
- Build a new CLI adapter as an MCP + C3 IPC client with reconnect and capability reporting.

The real contracts and the honest effort involved are in
[`docs/PLUGINS.md`](docs/PLUGINS.md), [`docs/CHANNELS.md`](docs/CHANNELS.md), and
[`docs/ADAPTERS.md`](docs/ADAPTERS.md).

## Building and contributing

Go ≥1.25. `go build ./...`, `go test ./...` (hermetic — no network needed), `go vet ./...`,
and `gofmt -l .` should print nothing. [`AGENTS.md`](AGENTS.md) is the operating doc for
contributors and AI coding agents alike; [`DECISIONS.md`](DECISIONS.md) records why things are
the way they are.

## Why the name?

**C³** is the old military/NATO doctrine term — **Command, Control, and Communications**:

- **Command** — send intent to an agent.
- **Control** — supervise execution and answer its decisions.
- **Communications** — keep a reliable link between the phone and the local CLI.

## Roadmap

What's next lives in [`ROADMAP.md`](ROADMAP.md). Shipped work is in git history.

## License

MIT — see [`LICENSE`](LICENSE).
