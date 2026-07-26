# Writing C3 Channels

A C3 channel is a transport — the thing that carries messages between users and the broker. Telegram is the v0.1 channel. The architecture admits more (web chat with magic-link sessions, voice mode, IRC, Slack, Matrix); each one is a Go package implementing a small interface.

Channels are not plugins. Plugins extend the broker with capabilities orthogonal to transport (transcription, summarization, OCR). Channels move bytes. If you're adding "Slack support" you want a channel; if you're adding "auto-translate every inbound" you want a plugin.

## Where channels live

```
internal/
├── channel/
│   ├── channel.go          # the Channel + Host interfaces
│   └── telegram/
│       ├── telegram.go     # implements Channel; New() constructor + getUpdates loop
│       ├── inbound.go      # raw update → normalized Inbound
│       ├── outbound.go     # reply / react / edit_message / download implementations
│       ├── poll.go         # long-poll dispatch + event surfacing
│       └── ...             # format, media, sendrich, resilience, offset_tracker, ...
```

There is no `registry.go`. A new channel adds a sibling package under `internal/channel/<name>/` with an exported `New()` constructor, and is **hand-wired into the broker**: `cmd/c3-broker/main.go` calls `br.RegisterChannel(telegram.New())` today — add a `br.RegisterChannel(<name>.New())` line beside it. The broker does not iterate a registry at boot.

## The Channel interface

```go
package channel

import (
	"context"

	"github.com/Andrometiq/c3/internal/c3types"
)

// Channel is the contract every transport implements. Methods are called by
// the broker on its own goroutine — implementations must be safe for
// concurrent use, except Start/Stop which are sequenced.
type Channel interface {
	// Name is the stable identifier ("telegram", "web", "voice"). Must match
	// the key under mappings.json:channels.<name>.
	Name() string

	// Start brings the channel up. The implementation reads its config from
	// host.Config(name, &cfg), opens its transport (long-poll, websocket,
	// whatever), and begins emitting inbound via host.Emit. Returns when the
	// channel is operational; long-running work goes in goroutines. host.Done()
	// returns a channel closed at shutdown; observe it and stop cleanly. Note
	// host is passed by interface value (Host, not *Host).
	Start(ctx context.Context, host Host) error

	// Stop tears down the transport. Called once, may be called concurrently
	// with in-flight Send* calls — finish the in-flight call first, then close.
	Stop() error

	// Capabilities returns this channel's static capability manifest (what the
	// broker advertises to adapters: rich text, chunking, media caps, etc.).
	Capabilities() c3types.Capabilities

	// Outbound primitives. The broker exposes these to adapters, which surface
	// them as MCP tools (reply, react, edit_message, download_attachment, …).
	SendReply(args c3types.ReplyArgs) (sentMessageID int64, err error)
	SendTyping(chatID int64, threadID *int64) error
	EditMessage(args c3types.EditArgs) (*c3types.EditResult, error)
	React(args c3types.ReactArgs) error
	DownloadAttachment(fileID string) (path string, err error)

	// StopPoll force-closes a bot-sent poll and returns its final aggregate
	// tally (Telegram-specific in v1; future channels may stub it).
	StopPoll(chatID, messageID int64) (*c3types.PollResult, error)

	// Topic management (Telegram-specific in v1; future channels may stub).
	CreateTopic(chatID int64, name string) (topicID int64, err error)
	ValidateTopic(chatID int64, threadID int64) error
}
```

`SendReply` returns the sent message id (`int64`), not a result struct. The outbound-arg types (`ReplyArgs`, `EditArgs`, `ReactArgs`) and results live in `internal/c3types`. The topic and poll methods are Telegram-shaped — channels not based on Telegram-style topics implement no-op stubs that return an unsupported error, or we refactor the interface as the second channel lands. The latter is preferred when it's clear the interface is generalizing too eagerly.

## Inbound events

A channel emits one normalized struct for every user-originated message:

```go
type Inbound struct {
	Channel    string             // your channel's name
	ChatID     int64
	TopicID    *int64             // nil for non-topic chats; 1 for Telegram General; >1 for custom
	MessageID  int64
	Sender     Sender
	Text       string             // plain text after channel-side preprocessing
	Attachments []Attachment
	ReplyTo    *ReplyContext      // present iff the user quote-replied
	Timestamp  time.Time
	Raw        map[string]any     // channel-specific fields the broker might pass through
}
```

Emit via `host.Emit(&Inbound{...})`, which returns `true` when the inbound was accepted onto the broker's per-route worker queue (and will be persisted) and `false` when it was dropped (queue full or stopped). On a `false` return the inbound never reaches durable storage, so a channel that staged any in-flight bookkeeping for it (e.g. an offset watermark) must resolve that itself. Before `Emit`, run the inbound through `host.GateInbound` (allowlist + pairing) and act on the decision. The broker handles the plugin pipeline, debounce, dedup, routing, and CLI delivery from there.

(Poll results, reactions, and callbacks are surfaced as *events* — an `Inbound` with a non-empty `Kind` and an `Event` payload — which the route worker flushes alone and keeps out of the text-debounce/STT path.)

For voice messages, set `Attachments[0].Kind = "voice"` and `.FileID = "..."`. The broker will fan the event through `OnVoiceReceived` plugins (STT) before substituting the returned transcript into `.Text`.

## Configuration

Channel config lives at `mappings.json:channels.<name>`. The Telegram channel uses:

```json
{
  "channels": {
    "telegram": {
      "bot_token": "...",
      "default_group": "main",
      "groups": {"main": {"chat_id": -100..., "title": "..."}},
      "dm_chat_id": ...,
      "topics": [...],
      "debounce_ms": 1500
    }
  }
}
```

Your channel defines its own subkey schema. Common fields by convention:

- `enabled: bool` — broker skips disabled channels at boot.
- Auth (token, key, oauth) — channel-specific names; document them.
- `topics` — only relevant if your transport has the topic concept. The broker reads/writes this; channels don't need to manage it.
- `debounce_ms` — defaults to 1500 if absent. Channels can override.

The broker doesn't introspect anything beyond `enabled`. Your channel reads what it needs via `host.Config(name, &cfg)`.

### Connectivity notifications

A top-level `notifications` block (sibling of `channels`, not per-channel) governs the *invasive* health-alert surfaces:

```json
{
  "notifications": { "invasive": true }
}
```

- Default is `true` (absent ⇒ enabled).
- `false` silences the desktop popup **and** the CLI turn-injection, but **keeps** the always-on ambient status-line indicator (`health.json`).
- It is SIGHUP-reloadable, like other mappings changes (`/c3:reload-config`).

#### `health.json` shape (the ambient status-line read source)

The broker writes the ambient connectivity state to `$XDG_STATE_HOME/c3/health.json` (fallback `$HOME/.local/state/c3/health.json`), resolved by `broker.HealthFilePath()`. It is written atomically (temp-in-same-dir + rename, no fsync — best-effort). The top level is a **wrapper that carries broker liveness**, with the per-channel snapshot nested under `channels`:

```json
{
  "broker_pid": 12345,
  "written_unix": 1718722725,
  "version": "v0.1.0",
  "update_available": true,
  "latest_version": "v0.2.0",
  "channels": {
    "telegram": {
      "state": "down",
      "since_unix": 1718722680,
      "since_hhmm": "14:38",
      "reason": "dial failures",
      "consec": 3
    }
  }
}
```

- `broker_pid` — `os.Getpid()` of the writing broker.
- `written_unix` — unix seconds, **refreshed on every write**: edge-driven writes (UP↔DOWN) *and* a slow 45-second refresh ticker that runs regardless of edges. So while the broker is alive, `written_unix` stays current.
- `version` — the running broker's build version (`"dev"` for an uninjected local build).
- `update_available` / `latest_version` — **omitted** while the broker is on the current stable release; set once the ~6h update check finds a newer release, so the status line can render `c3 update available — /c3:update`. Independent of the `auto_update` toggle (the notice always fires). See "Updating C3" in [`USAGE.md`](USAGE.md).
- `channels` — map of channel name → per-channel entry (`state` is `"up"`/`"down"`; `since_unix`/`since_hhmm`/`reason`/`consec` describe the current state). At boot it is `{}` (no outage asserted), so a crash never leaves a stale per-channel `down`.
  - For `telegram`, `state` is the **combined reachability** of both directions: the channel now tracks outbound send health (sends failing after retries) alongside inbound fetch health, and surfaces them on this **single** entry so one root cause (the wire is down ⇒ both fail) never produces two notifications. `reason` therefore reflects the failing direction(s): `"inbound unreachable"`, `"outbound send failing"`, or `"unreachable (inbound + outbound)"`. No new field and no status-line reader change — the reader still reads `.channels.telegram.state`/`.reason`.

**Why the wrapper exists (broker-dead detection):** previously the top level was a flat `{"<channel>": {...}}` map written only on health edges + startup. When the **broker process** died, the file froze at its last value (usually `up`), so a status line showed green while C3 was completely dead. A reader now treats **`broker_pid` not alive** (e.g. `kill(pid, 0)` fails) **OR** `now - written_unix > 90s` (2× the 45s refresh interval) as **broker-down/unknown**, regardless of the per-channel `state`. The bash status-line reader reads `.channels.telegram.state` and `.channels.telegram.since_hhmm`, and additionally checks `broker_pid`/`written_unix` for liveness.

## Channel lifecycle

```
boot:
  for each channel in mappings.json:channels:
    construct (just calls New<Name>())
    Start(ctx, host)        # runs in a goroutine
    block until ready or err

shutdown (broker SIGTERM/SIGINT):
  cancel ctx (each channel's Start should observe and unwind)
  Stop()                    # in parallel for all channels
  wait up to 5s for clean exit, then force kill
```

The host (`channel.Host`, in `channel.go`) gives the channel:

- `host.Config(name, &cfg)` — read your config from `mappings.json:channels.<name>`.
- `host.Emit(*Inbound) bool` — emit a normalized inbound message (see "Inbound events").
- `host.GateInbound(*Inbound)` — run the allowlist + pairing gate; call before `Emit`.
- `host.HandleCommand(*Inbound)` — hand a recognized bot command (e.g. `/status`) to the broker for direct handling.
- `host.NotifyHealth(HealthEvent)` — report a fetch-health UP/DOWN edge (out-of-band alerting).
- `host.Logf` — structured logging.
- `host.Done()` — channel closed on shutdown.

## Error handling

Transient transport errors (network blips, rate limits) → log, back off, keep going. Don't return from `Start`.

Fatal errors (bad credentials, unsupported API version) → return from `Start`. The broker logs and continues with other channels; your channel's tools become unusable until config is fixed and the broker is restarted.

For `Send*` calls, return errors verbatim — the broker forwards them back to the adapter, which surfaces them to the CLI/user. Don't swallow.

For rate limits specifically, **respect provider-supplied retry-after**: Telegram's `parameters.retry_after` (in `Bad Request` responses), Slack's `Retry-After` header, etc. Sleep for that long and retry once before giving up.

## Testing

A channel ships with Go tests under the package. The host exposes `channel.MockHost(t)` that captures emitted inbound events and lets your test drive synthetic transport responses.

For Telegram specifically, mock at the `gotgbot` boundary: don't hit the real Bot API in tests. Use `httptest.Server` for the HTTP layer and a fake getUpdates loop. The Telegram channel's existing tests demonstrate the pattern.

## Adding a new channel — checklist

- [ ] Package under `internal/channel/<name>/`
- [ ] Type implements the `Channel` interface (incl. `Capabilities()` + `StopPoll()`)
- [ ] `New()` constructor exported
- [ ] Hand-wired via `br.RegisterChannel(<name>.New())` in `cmd/c3-broker/main.go` (no `registry.go`)
- [ ] Config schema documented (under `mappings.json:channels.<name>`)
- [ ] Inbound emission tested (mock-host captures `Inbound{}` for known transport input)
- [ ] `GateInbound` called before `Emit`; a `false` `Emit` return resolves any staged bookkeeping
- [ ] All `Send*` methods return errors, don't swallow
- [ ] Rate limit handling honors provider conventions
- [ ] No `fmt.Println` — use `host.Logf`

## Telegram channel: what's there

The Telegram channel implementation lives at `internal/channel/telegram/`. Key things it does that future channels may want to reference:

- **Long-polling getUpdates loop** with `allowed_updates` opt-in for `message`, `edited_message`, `callback_query`, `message_reaction`. Service-message types (`forum_topic_created`/`forum_topic_edited`/etc) are received but ignored in v0.1 — plumbed for future use.
- **General topic id is `1`, not `0`.** A common confusion — topic_id 0 means "no topic" (DM, non-forum group); General is a real topic with id 1.
- **Bot API has no `getForumTopics`.** The local `topics` registry under `mappings.json:channels.telegram.topics` is the source of truth. Topics are added when a session attaches and creates one (or claims an explicit topic_id), never opportunistically from inbound traffic.
- **Reply threading**: when an inbound has `reply_to_message`, the channel populates `ReplyContext` with `MessageID`, `User`, `Text`. The Claude adapter renders this as `<channel reply_to_message_id="..." reply_to_text="...">` attributes.
- **Voice handling**: voice messages emit an inbound with `Attachments[0].Kind="voice"`, `FileID=...`, and empty `Text`. The STT plugin's `OnVoiceReceived` fills in `Text`. The voice attachment is preserved so a CLI can re-download the audio if a transcript is ambiguous.
- **Broker bot commands (`/status`, `/queue`, `/drain`)**: registered at startup via `setMyCommands`, so they autocomplete in Telegram's `/` menu. An inbound whose FIRST token is one of these commands (optionally `@<botname>`-suffixed on that token only, case-insensitive) is intercepted in the poll path **after the allowlist gate** (a stranger's command dies at the gate — silence, never a reply) — the broker answers it directly and the update is **never queued or routed to an agent** (its `update_id` is marked done so the offset can advance past it). A handled command with an EMPTY reply sends nothing (the operator-gate silent drop, or an async `/drain`/`/queue <q>` that posts its own reply from a broker goroutine). Messages carrying attachments are never intercepted (a command in a media caption would swallow the attachment). This is a distinct surface from the `/c3:status` CLI slash command. See `docs/COMMANDS.md` "Telegram bot commands" for the grammar and authorization matrix.
- **Persisted-offset advance**: the Telegram read offset advances only to the highest contiguous `update_id` whose message has been **durably persisted** to the inbound queue (`fsync`'d), or which was a no-op (gated, dropped, `/status`, or a non-message update). An update still mid-STT or not yet persisted does not advance the offset, so a crash there means Telegram redelivers it (within its 24h retention) — loss-free by construction. STT runs at flush time so the stored line already carries the transcript; storage is per-message (the debounce/merge is a delivery-presentation concern only and does not merge stored lines). See `docs/USAGE.md` "Durable inbound queue & backlog" for the user-facing view.

Use these as patterns, not copy-paste templates — your transport probably has its own quirks that rate higher than Telegram's idioms.
