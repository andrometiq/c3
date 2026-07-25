# C3 — your coding agents, one Telegram inbox

C3 turns one Telegram bot into a **topic-per-project remote control** for the Claude Code
and Codex sessions already running on your machine.

Send text, voice notes, files, or decisions from your phone. C3 routes each message to the
CLI attached to that project, sends the agent's replies back to the same topic, and holds
inbound messages on disk when no session is attached. It's a self-hosted bridge to your real
coding CLIs — not another agent runtime.

```text
Telegram topic "api"  ⇄  c3-broker  ⇄  attached Claude Code / Codex session
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

*(Those are the strings C3 actually renders. Claude Code owns the surrounding `<channel>`
envelope, so C3 doesn't invent or promise its host-controlled attributes.)*

## Is C3 for you?

C3 fits when:

- you run coding agents across **several projects** and want one phone inbox without losing
  track of which project a message belongs to;
- you want to steer the **actual** Claude Code or Codex process on your machine — including
  setups behind Bedrock, Vertex, an API gateway, or a proxy;
- you want a **self-hosted** bridge whose bot token and message queue stay on your hardware.

It works fine with a single session. The architecture starts to matter once you have
several.

## Why C3

1. **One bot, many sessions.** A single broker owns the Telegram bot token. Every project
   gets a forum topic, and only the CLI session holding that topic's claim receives its
   messages.
2. **Inbound survives sleeping sessions.** When no render-capable session owns a topic, C3
   keeps received messages in a durable on-disk queue for later readback instead of making
   you resend them.
3. **Your CLIs, your auth, your machine.** C3 is a local multiplexer, not a hosted agent
   service. Claude Code and Codex keep their normal models, credentials, tools, sandboxes,
   and project directories.

## What ships today

- **Topic routing and session resume** — attach a session to a topic once; a resumed session
  silently re-attaches only to its own recorded topic. A fresh session asks before claiming
  anything.
- **Rich two-way Telegram** — markdown, quote-replies, attachments, edits, reactions, polls,
  and inline buttons.
- **Voice notes** — a pluggable speech-to-text chain turns phone audio into
  `[Transcribed voice]: …`; the original attachment stays available for re-transcription.
- **Remote permission decisions** — Claude Code can relay its permission prompt as an
  Allow/Deny keyboard. Only an allowlisted operator's tap becomes a verdict, and command
  previews render literally rather than as markdown.
- **One broker across CLIs** — Claude Code and Codex coordinate claims through the same
  daemon, so two sessions never answer one topic.
- **Local, inspectable state** — Go binaries, MIT licence, a mode-0600 config file, and a
  durable queue on disk. No C3 cloud relay.
- **Release updates** — a status-line notice when a new release ships, plus a
  checksum-verified updater on Linux/macOS. Windows updates are manual.

**Codex parity, stated plainly.** Codex sessions get topic routing, the durable queue, `reply`, reactions, edits, polls, and attachments. They do **not** get `ask`, `detach`, or the permission relay — those are Claude Code-only today — and the Codex bridge is heavier (a 4-process launcher → app-server → adapter → TUI chain, with an NVM symlink step). See [`docs/ADAPTERS.md`](docs/ADAPTERS.md) and [`ROADMAP.md`](ROADMAP.md).

**Claude Desktop, stated plainly.** There's a `c3-desktop-adapter` for Claude Desktop, but it's a **pull bridge, not a push one**: Claude Desktop can't surface a Telegram message on its own, so inbound waits in the durable queue and you pull it by asking Claude to check (it calls `fetch_queue`); replies go out on request. No live `<channel>` rendering and no permission relay — those stay Claude Code-only. Install with `c3-broker install-desktop`; see [`docs/DESKTOP.md`](docs/DESKTOP.md).

The Desktop adapter also ships an inbox panel: watch a topic read-only, take it over
explicitly, hand queued messages to Claude, opt into auto-forwarding while the panel is
open, and reply to Telegram. Desktop stays pull-based — the panel doesn't turn it into push.

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
follow https://github.com/karthikeyan5/c3/blob/master/INSTALL.md to install c3
```

The playbook asks which host you want — Claude Code, Claude Desktop, or CoWork — then
installs the matching path. You provide a Telegram bot token and send two short pairing
codes; setup discovers your user id and the group's chat id without an id hunt. Codex
integration is a deliberate opt-in step, because C3's launcher is *also* named `codex` and
takes the real command's place on `PATH`.

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

## How is this different from X?

The Telegram-bridge idea is a genre; the differentiator is the architecture. Honest one-liners:

- **Anthropic's official Claude Code Channels** — the closest first-party option. It runs **one bot poller per open session** (no topic-per-project multiplexing), is Claude-Code-only, is blocked behind Bedrock/Vertex/gateways, and by its own docs delivers events *only while the session is open* — no offline queue. C3 is one bot → many sessions with a topic per project, cross-CLI, self-hosted behind any auth, and holds a durable backlog.
- **Happy** — polished native iOS/Android/web apps for Claude Code and Codex, with realtime voice and remote approvals. It's app-based: a session *list*, not one-bot topic-per-project routing, and no durable offline queue. C3 is chat-native (no app to install, group visibility) and queues what you send while a session is down.
- **cc-connect** — a Go daemon bridging many agents to many chat platforms, multi-project, with built-in STT/TTS. It has no Telegram forum-topic model, no documented durable offline queue, and handles permissions with a `/mode` toggle rather than relaying the CLI's own prompt. C3 trades breadth for the topic-per-project + durable-queue + prompt-relay cut.
- **OpenClaw** — a self-hosted 24/7 *assistant gateway* with its own agent runtime and a skills marketplace. Different product: it's an always-on assistant platform, not a multiplexer of the real Claude Code / Codex sessions you already run. C3 drives your actual CLIs.

The unduplicated cut: self-hosted single-binary + CLI-agnostic + one-token multiplexing into per-project topics + a durable inbound queue. Each axis has a rival; the intersection is C3's.

## Architecture

```text
   Telegram Bot API
          │
   ┌──────┴───────┐
   │  c3-broker   │   one poller, routing + claims, durable queue,
   │  (Go)        │   plugin host, local IPC socket
   └──────┬───────┘
          │
   ┌──────┼──────────────────┐
   │      │                  │
   ▼      ▼                  ▼
 Claude  Claude             Codex
 adapter adapter            adapter
   │      │                  │
   ▼      ▼                  ▼
 CLI-1   CLI-2              TUI
```

**Broker.** One long-running Go process owns the Telegram poller, per-route workers, topic
claims, durable queues, outbound rate limits, and plugin hooks. Adapters connect over a
local Unix socket. A singleton lock stops two brokers polling one token on the same machine.

**Adapters.** Thin MCP stdio servers connecting each host process to the broker. They expose
only the capabilities that host can actually support, and reconnect after a broker bounce.
Codex's live path adds its launcher and app-server because the app-server — not the TUI —
owns MCP startup.

**Channels.** v0.1 ships Telegram. The channel interface is the seam for future transports,
but adding one is Go work today, not a config-only plugin.

**Plugins.** Built-in Go plugins subscribe to broker hooks. The shipped STT plugin drives a
bundled Python provider chain; external loadable plugins remain roadmap work.

**Config.** `~/.config/c3/mappings.json`, written mode 0600 with an atomic rewrite and one
backup. It contains the bot token — treat it like a password.

## Routing

- **Telegram topics** are the primary path. A topic maps to at most one live session claim;
  a fresh session asks before creating, claiming, or stealing.
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
contributors and AI coding agents alike; [`DECISIONS.md`](DECISIONS.md) records why things
are the way they are.

## Why the name?

**C³** is the old military/NATO doctrine term — **Command, Control, and Communications**:

- **Command** — send intent to an agent.
- **Control** — supervise execution and answer its decisions.
- **Communications** — keep a reliable link between the phone and the local CLI.

The name describes the job. The product definition comes first.

## Roadmap

What's next lives in [`ROADMAP.md`](ROADMAP.md). Shipped work is in git history.

## License

MIT — see [`LICENSE`](LICENSE).
