# C3 — command every coding agent you run, from one chat

**C³** is the old military/NATO doctrine term **Command, Control, and Communications** — and the triad maps 1:1 to what this does:

- **Command** — issue intent: send work to any agent, text or voice, from anywhere.
- **Control** — supervise execution: Allow/Deny its tool calls, steer it mid-run.
- **Communications** — the link you own: one self-hosted broker multiplexing every session, with a durable queue so no message you send is ever lost.

C3 multiplexes every Claude Code and Codex session onto one Telegram bot — a thread per project, tap-to-approve, and no message you send is ever lost.

> One bot. Every project its own thread. Claude Code and Codex on one self-hosted broker — however you run your agents.

---

## The moment

You've got six agents going across six projects. Your phone buzzes. **Which one was that?**

C3 is a **multiplexer, not a wrapper**: one broker daemon owns a single Telegram bot token, and every CLI session — Claude Code or Codex — connects to it over a unix socket as a thin MCP adapter. One broker, one bot, N sessions, a Telegram **forum topic per project**. Each project gets its own thread on your phone.

Three things a per-session bridge structurally can't match:

1. **Many agents, one place.** Every session is its own topic on one bot. Not a poller per session, not another app — one thread per project, on the messenger you already use.
2. **A message you send to a sleeping session is never lost.** Fire a note (or a voice memo) at a project whose session is down; it's held in a durable on-disk queue and delivered the moment a session attaches there. *(The queue is inbound — messages you send in. See the honest scoping in "How is this different?" below.)*
3. **Works however you run your agents.** API key, Bedrock, Vertex, a gateway, a proxy — the setups a first-party phone bridge refuses. If your CLI runs, C3 reaches it.

Works great with one agent; scales to ten.

## What you get today

Differentiators first:

- **One bot, a topic per project** — multiplex any number of Claude Code and Codex sessions. Run `claude` (or `codex`) in a project, `attach` once to pick or create its topic, and every resume of that session silently re-attaches its own topic. Two sessions never fight over one topic.
- **Durable inbound queue** — once C3 has *received* a Telegram message it never drops it: messages that arrive while a session is down (or before you attach) are held on disk and delivered on attach, with a per-message backlog preview. You never re-forward a voice note.
- **Cross-CLI on one broker** — Claude Code and Codex on the same project coordinate through the broker, so only one holds the topic claim at a time; no double-replies.
- **Rich two-way Telegram** — markdown in both directions (bold/italic, lists, code blocks, tables, blockquotes), quote-replies with the quoted text in context, attachments, message edits, reactions, and polls, all surfaced to the CLI as structured channel events.
- **Self-hosted, one Go binary set, MIT** — the broker runs on your machine and holds your bot token in a mode-0600 config file. No vendor cloud relay.
- **Keeps itself current** — an update notice in your status line when a newer release ships, and a checksum-verified `/c3:update` one-command install (or fully automatic, opt-in) that atomically swaps the binaries and lets sessions reconnect on their own.

Delighters — the demo magic, not the headline:

- **Tap-to-approve** — when a Claude Code tool call needs permission, C3 relays *the CLI's own prompt* to the topic as an inline Allow/Deny keyboard, and only an allowlisted operator's tap authorizes it. (Claude Code only today.)
- **Voice notes → transcript** — record on your phone; a pluggable speech-to-text chain transcribes it and the CLI sees `[Transcribed voice]: …`, with the original audio kept so you can re-transcribe if a provider was flaky.

**Codex parity, stated plainly.** Codex sessions get topic routing, the durable queue, `reply`, reactions, edits, polls, and attachments. They do **not** get `ask`, `detach`, or the permission relay — those are Claude Code-only today — and the Codex bridge is heavier (a 4-process launcher → app-server → adapter → TUI chain, with an NVM symlink step). See [`docs/ADAPTERS.md`](docs/ADAPTERS.md) and [`ROADMAP.md`](ROADMAP.md).

**Claude Desktop (Windows), stated plainly.** There's a `c3-desktop-adapter` for Claude Desktop, but it's a **pull bridge, not a push one**: Claude Desktop can't surface a Telegram message on its own, so inbound waits in the durable queue and you pull it by asking Claude to check (it calls `fetch_queue`); replies go out on request. No live `<channel>` rendering and no permission relay — those stay Claude Code-only. Install with `c3-broker install-desktop`; see [`docs/DESKTOP.md`](docs/DESKTOP.md).

## Install

C3 installs as a Claude Code plugin (marketplace add straight from GitHub) with prebuilt binaries — no toolchain needed; a build-from-source path stays for contributors. In any Claude Code session, paste:

```
follow https://github.com/karthikeyan5/c3/blob/master/INSTALL.md to install c3
```

The agent runs the playbook end-to-end. You'll be asked for a Telegram bot token (from `@BotFather`) and two short pairing codes — one sent to the bot in a DM, one in your group — which discover your user id and the group's chat id automatically, no id hunting. About five minutes.

See [`INSTALL.md`](INSTALL.md) for the agent script and [`docs/INSTALL.md`](docs/INSTALL.md) for the human-readable walkthrough.

> **Platform support:** Linux is the primary, fully-supported platform. Windows (Claude Desktop + Claude Code) is **beta** — it works and the known Windows bugs are fixed, but inbound is poll-only there (`/c3:fetch-queue`) and it builds from source (no prebuilt Windows release yet).

### The `--dangerously-load-development-channels` flag

To surface inbound Telegram messages, you start Claude Code with a flag:

```
claude --dangerously-load-development-channels plugin:c3@c3
```

This isn't a C3 hack. Claude Code gates inbound channel notifications from **every** locally-installed channel plugin behind this development flag — it's [Anthropic's own documented preview guardrail](https://code.claude.com/docs/en/channels) for third-party channels that haven't shipped through their marketplace. Without it, a session can't render inbound live — but it isn't lost either: C3 detects a host that can't render and holds those messages in the durable queue, so they're recoverable once you relaunch with the flag. The install can also drop in a tiny `claude` shim so you never type the flag by hand.

## How is this different from X?

The Telegram-bridge idea is a genre; the differentiator is the architecture. Honest one-liners:

- **Anthropic's official Claude Code Channels** — the closest first-party option. It runs **one bot poller per open session** (no topic-per-project multiplexing), is Claude-Code-only, is blocked behind Bedrock/Vertex/gateways, and by its own docs delivers events *only while the session is open* — no offline queue. C3 is one bot → many sessions with a topic per project, cross-CLI, self-hosted behind any auth, and holds a durable backlog.
- **Happy** — polished native iOS/Android/web apps for Claude Code and Codex, with realtime voice and remote approvals. It's app-based: a session *list*, not one-bot topic-per-project routing, and no durable offline queue. C3 is chat-native (no app to install, group visibility) and queues what you send while a session is down.
- **cc-connect** — a Go daemon bridging many agents to many chat platforms, multi-project, with built-in STT/TTS. It has no Telegram forum-topic model, no documented durable offline queue, and handles permissions with a `/mode` toggle rather than relaying the CLI's own prompt. C3 trades breadth for the topic-per-project + durable-queue + prompt-relay cut.
- **OpenClaw** — a self-hosted 24/7 *assistant gateway* with its own agent runtime and a skills marketplace. Different product: it's an always-on assistant platform, not a multiplexer of the real Claude Code / Codex sessions you already run. C3 drives your actual CLIs.

The unduplicated cut: self-hosted single-binary + CLI-agnostic + one-token multiplexing into per-project topics + a durable inbound queue. Each axis has a rival; the intersection is C3's.

## Architecture

```
   Telegram Bot API
          │
   ┌──────┴───────┐
   │  c3-broker   │   single daemon, owns bot token, polls Telegram,
   │  (Go)        │   runs plugin host (STT), holds routing table,
   └──────┬───────┘   listens on $XDG_RUNTIME_DIR/c3.sock
          │
   ┌──────┼──────────────────┐
   │      │                  │
   ▼      ▼                  ▼
 adapter  adapter            adapter
 (Claude) (Claude)           (Codex)
   │      │                  │
   ▼      ▼                  ▼
  CLI-1  CLI-2              codex
```

**Broker.** Single long-running Go process. One Telegram poller (a Bot API constraint), N MCP adapters connected over a unix socket. A per-route serial executor (one goroutine per `RouteKey = {channel, chat_id, topic_id?}`) owns the inbound pipeline, outbound calls, typing indicator (relayed automatically by the route worker — not an agent tool), and debounce/merge. `flock` singleton with stale-pid recovery.

**Adapters.** Thin MCP stdio servers, one per CLI process. Each looks like a normal MCP server to its CLI and receives only messages routed to its attached topics. Tools: `attach`, `detach`, `topics`, `reply`, `react`, `edit_message`, `poll`, `stop_poll`, `download_attachment`, `fetch_queue`, `retranscribe`, plus `ask` on Claude Code. Codex's adapter omits `detach` and `ask` and adds an env-gated `codex_forward` debug tool — see [`docs/ADAPTERS.md`](docs/ADAPTERS.md). Survives a broker bounce via exponential-backoff reconnect plus replay of the last successful attach.

**Channels.** The transport layer. v0.1 ships Telegram only (`internal/channel/telegram`, cleanroom Go via `gotgbot/v2`) with resilience hardening: 401 circuit-breaker, 429 retry-after, 409 conflict detection, a persisted update-id watermark, outbound rate-limiting, and per-update semantic dedup. The `Channel` interface (`internal/channel/channel.go`) is the seam for a future Slack/web/voice transport — see [`docs/CHANNELS.md`](docs/CHANNELS.md).

**Plugins.** Compiled-in Go extensions that subscribe to broker hooks. The one shipped plugin is STT — a Go shim under `internal/plugin/builtins/stt/` driving a bundled Python provider-chain pipeline at `plugins/c3/stt/`. See [`docs/PLUGINS.md`](docs/PLUGINS.md).

**Config.** `~/.config/c3/mappings.json` (mode 0600, atomic-rewrite with one `.bak`). The bot token lives here; treat it like a password.

## Routing

- **Topic-based** (primary) — a Telegram supergroup with topics enabled; each topic binds to one CLI session. The natural start is `cd <project-dir> && claude`, then `attach`: a session that has attached before silently re-claims its own topic, and a first-time session gets a picker (seeded from the project dir) to choose or create one. C3 never binds a topic the session didn't choose.
- **DM-based** — your personal DMs with the bot route to whichever CLI claims `dm`. Works anywhere, no project binding.
- **Group-based** — a whole group without topics maps to one CLI. Useful for shared rooms.

## Extending C3

Honest about today's seams:

- **Plugins are compiled-in Go.** You add a package under `internal/plugin/builtins/<name>/`, register it in the `builtinPlugins` slice in `cmd/c3-broker/main.go`, and rebuild the broker. **STT is the worked example** — and the only plugin that uses a non-Go runtime, because a swappable provider chain is worth more there than language purity ([`plugins/c3/stt/stt-pkg/README.md`](plugins/c3/stt/stt-pkg/README.md) is the "add a provider" how-to).
- **A new channel or CLI adapter is real Go work**, not a drop-in: a channel implements the `Channel` interface and is hand-wired into the broker; an adapter is a from-scratch MCP + IPC + reconnect job (each existing adapter is 1.5–2.3k LOC). [`docs/CHANNELS.md`](docs/CHANNELS.md) and [`docs/ADAPTERS.md`](docs/ADAPTERS.md) document them as they actually are.

External (non-Go, loadable) plugins and more channels are on the [roadmap](ROADMAP.md), not shipped.

## Roadmap

What's next lives in [`ROADMAP.md`](ROADMAP.md). Shipped work is in git history.

## License

MIT — see [`LICENSE`](LICENSE).
