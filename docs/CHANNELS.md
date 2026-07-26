# C3 Channels

A channel is a transport — the thing that carries messages between users and the broker. Plugins are the other extension seam: they add capabilities orthogonal to transport (transcription, summarization, OCR). Channels move bytes. "Slack support" is a channel; "auto-translate every inbound" is a plugin.

## Status: one channel, in-tree only

**C3 v0.1.0 ships exactly one channel — Telegram — and channels are in-tree only for this release. This is a deliberate choice, not an oversight.**

Concretely:

- Every package in this repo lives under `internal/`. Go's internal-package rule means an out-of-tree module **cannot** import `github.com/Andrometiq/c3/internal/channel`, `internal/c3types`, or anything else here. There is no public package to build a channel against, and no plugin/`.so` loading path either.
- `channel.Channel` has had exactly one implementation since it was written. It has never been compiled against a second one. Where the interface is transport-neutral and where it is Telegram-shaped has not been tested by anything except Telegram.

So the honest framing is: **adding a channel today means opening a pull request into this repo, and changing this interface as part of it.** It is not "implement an interface from outside." We would rather say that plainly than ship a how-to that quietly does not work.

## We want the second channel — please open the PR

This is an invitation, not a brush-off. A real second transport (Slack, Matrix, IRC, XMPP, web chat, SMS) is the single most useful contribution C3 can receive right now, because it is the only thing that can tell us which parts of this interface are actually general.

If you are considering it:

- **Open an issue first** and say which transport. Not for gatekeeping — so we can tell you which of the blockers below will bite you, and so interface changes land once rather than twice.
- **Expect to change `internal/channel/channel.go`, and that's fine.** A PR that only adds a package and works around the sharp edges is worse for everyone than one that fixes the edge. The identifier types and the `Emit` contract in particular are expected to move.
- **Expect the merge to be collaborative.** The maintainer will pair on the broker-side changes (routing, gating, config) rather than hand you the whole surface.
- Telegram must keep working. That constraint is real, and §"Known blockers" is largely a list of the places where a naive second channel silently breaks it.

The rest of this document is the map: what the interface really is today, and exactly where it will not fit you.

## Where channels live

```
internal/
├── channel/
│   ├── channel.go          # the Channel + Host interfaces
│   └── telegram/
│       ├── telegram.go     # implements Channel; New() constructor + lifecycle
│       ├── inbound.go      # raw update → normalized Inbound
│       ├── outbound.go     # reply / react / edit_message / download implementations
│       ├── poll.go         # long-poll dispatch + event surfacing
│       ├── offset_tracker.go, offset_store.go   # persisted-offset machinery
│       └── ...             # format, media, sendrich, resilience, readback, ...
```

There is no `registry.go`, and the broker does not iterate a registry at boot. A channel is **hand-wired**: `cmd/c3-broker/main.go` calls `br.RegisterChannel(telegram.New())`, and a second channel adds a sibling `br.RegisterChannel(<name>.New())` line beside it. That single line is also a guaranteed merge conflict for anyone maintaining a fork, which is one more reason the in-tree PR is the supported path.

## The Channel interface

Reproduced from `internal/channel/channel.go` — read the file for the full doc comments.

```go
type Channel interface {
	Name() string
	Start(ctx context.Context, host Host) error
	Stop() error

	Capabilities() c3types.Capabilities

	SendReply(args c3types.ReplyArgs) (sentMessageID int64, err error)
	SendTyping(chatID int64, threadID *int64) error
	EditMessage(args c3types.EditArgs) (*c3types.EditResult, error)
	React(args c3types.ReactArgs) error
	DownloadAttachment(fileID string) (path string, err error)

	StopPoll(chatID, messageID int64) (*c3types.PollResult, error)

	CreateTopic(chatID int64, name string) (topicID int64, err error)
	ValidateTopic(chatID int64, threadID int64) error
}
```

Notes that are true today:

- `Name()` must match the key under `mappings.json:channels.<name>`.
- `Start` reads config via `host.Config(name, &cfg)`, opens the transport, and returns when the channel is operational; long-running work goes in goroutines. `host` is passed by interface value (`Host`, not `*Host`).
- Methods are called from broker goroutines and must be safe for concurrent use, except `Start`/`Stop`, which are sequenced.
- `Capabilities()` takes no argument in v1 deliberately: a `RouteKey` argument would introduce a `channel` → `broker` import cycle.
- `SendReply` returns the sent message id as a bare `int64`, not a result struct.
- `StopPoll`, `CreateTopic`, and `ValidateTopic` are Telegram-shaped. A transport without forum topics or bot polls can stub them with an unsupported error — but see §"Known blockers", because the identifier types are the deeper problem, not these three methods.

## The Host interface

```go
type Host interface {
	Config(name string, target any) error
	Emit(in *c3types.Inbound) bool
	Logf(format string, args ...any)
	Done() <-chan struct{}
	NotifyHealth(ev c3types.HealthEvent)
	GateInbound(in *c3types.Inbound) GateInboundDecision
	HandleCommand(in *c3types.Inbound) (reply string, handled bool)
}
```

- `Config(name, &cfg)` — JSON round-trip of `mappings.json:channels.<name>` into your struct. See blocker 3: the round-trip goes through a closed Go struct, so it can only ever hand you keys that struct already declares.
- `Emit(*Inbound) bool` — submit an inbound to the per-route worker pool. **Read blocker 1 before you write the `false` branch.**
- `GateInbound(*Inbound)` — allowlist + pairing gate. Channels **MUST** call this before `Emit` and act on the decision: `GateInboundAllow` → forward to `Emit`; `GateInboundDrop` → discard silently (never log content); `GateInboundPairConsumed` → discard silently, the broker has already mutated allowlist + pairing state.
- `HandleCommand(*Inbound)` — hand a recognized broker command (`/status`, `/queue`, `/drain`) to the broker. On `handled=true` the channel sends the returned reply itself and MUST NOT gate, emit, queue, or route the message.
- `NotifyHealth` — report a reachability UP/DOWN **edge** (not per attempt), non-blocking. The broker fans it out to out-of-band sinks so a dead channel can still raise an alarm through another path.
- `Done()` — closed at shutdown.
- `Logf` — use this; no `fmt.Println`.

**Not on this interface but load-bearing:** `SetPersistedCallback` and `SetPersistFailedCallback`. See blocker 2.

## Inbound events

A channel emits one normalized struct per user-originated message. The real definition is `c3types.Inbound` in `internal/c3types/types.go`:

```go
type Inbound struct {
	Channel     string        `json:"Channel"`
	ChatID      int64         `json:"ChatID"`
	TopicID     *int64        `json:"TopicID"`   // nil = no topic; &1 = Telegram General; >1 = custom
	MessageID   int64         `json:"MessageID"`
	Sender      Sender        `json:"Sender"`
	Text        string        `json:"Text"`
	Attachments []Attachment  `json:"Attachments"`
	ReplyTo     *ReplyContext `json:"ReplyTo"`
	Timestamp   time.Time     `json:"Timestamp"`

	Kind        InboundKind   `json:"Kind,omitempty"`        // "" = ordinary message; non-empty = channel event
	Event       *InboundEvent `json:"Event,omitempty"`       // payload for a non-empty Kind
	DrainedFrom string        `json:"DrainedFrom,omitempty"` // drain provenance; set by the broker, not by you
	V           int           `json:"V,omitempty"`           // record format version; absent or 0 means 1
	ConvKind    string        `json:"ConvKind,omitempty"`    // "dm" | "group", stated by the channel
}
```

There is **no `Raw` field** and no channel-specific passthrough. The JSON tags are frozen on purpose — this struct is marshalled straight into the durable queue `.jsonl` and onto the IPC wire, so the tag names are the on-disk and wire format. Do not "tidy" one.

Set `ConvKind` if you add a channel. It exists precisely so the trust gate stops inferring DM-vs-group from a Telegram sign convention — see blocker 4. It currently has no readers; wiring one is part of the second-channel PR.

Poll results, reactions, and callbacks are surfaced as *events*: an `Inbound` with a non-empty `Kind` and an `Event` payload. The route worker flushes those alone and keeps them out of the text-debounce and STT paths.

For voice, set `Attachments[0].Kind = "voice"` and `.FileID`. The broker fans the event through `OnVoiceReceived` plugins (STT) before substituting the returned transcript into `.Text`.

## Known blockers — the roadmap for a second channel

These are code-verified, not hypothetical. They are listed in the order they will hurt.

### 1. A `false` from `Emit` assumes your transport redelivers what you never acknowledged

`Emit` returns `false` when the per-route worker is saturated (after a grace window) or stopped. On `false`, **the message has not been persisted anywhere** and the broker cannot recover it.

The correct handling is **not** to advance past it. It is:

> **Leave the message unacknowledged on your transport so the transport redelivers it. Do not clear the staged bookkeeping. Do not advance a cursor past it.**

That reverses what earlier revisions of this document and the godoc in `channel.go` said. The earlier contract ("resolve the staged bookkeeping yourself") described a capacity **drop** — silent, unrecoverable loss of a message the user sent — which the v0.1.0 release audit removed. `BrokerHost.Emit` in `internal/broker/host.go` now records the reversal, and the Telegram implementation (`internal/channel/telegram/poll.go`, in `dispatchMessage`) implements it: on `false` it deliberately does **not** mark the update done, removes just that update's staged seam entry, and forgets its dedup record so the redelivery genuinely re-dispatches.

**Why this is a blocker rather than a footnote:** that contract is only satisfiable on a *pull* transport with a server-side cursor. Telegram's `getUpdates` retains unacknowledged updates and redelivers them; withholding the acknowledgement is a real backpressure primitive. A *push* transport has nothing to hold. Slack's Events API is an HTTP POST needing a 200 within three seconds, with three retries and then the event is gone; Socket Mode has the same ceiling on the envelope ack; and unrelated events keep arriving meanwhile, so there is no contiguous prefix to hold in the first place. For those transports, `Emit → false` is permanent loss, and the interface offers no recovery primitive — no error return, no park, no blocking variant, no "wake me when the route drains."

The assumption is currently stated only inside the Telegram implementation and the broker host, never in the interface. Making it explicit — or replacing `Emit(*Inbound) bool` with something a push transport can satisfy — is part of the work.

### 2. The persisted-callback is a single broker-wide slot, and registering a second channel steals it

`Broker.persistedCB` is one `func` field. `SetPersistedCallback` overwrites it, last write wins; `notifyPersisted` fires it for **every** persisted inbound with no channel filter. `SetPersistFailedCallback` is the same shape.

Neither is on the `channel.Host` interface. The Telegram channel discovers them at `Start` by an anonymous interface type-assertion on its host (`internal/channel/telegram/telegram.go`), which is why reading `channel.go` alone will not tell you they exist.

Telegram binds `onPersisted` to drive its persisted-offset tracker: the read offset advances only to the highest contiguous `update_id` that has been durably persisted. `RegisterChannel` calls `ch.Start(ctx, host)` — so a second channel that also binds the callback **overwrites Telegram's**. Telegram's contiguous prefix then never advances, the committed offset freezes at boot, the in-flight set grows without bound, and every restart redelivers from the last saved offset. Symmetrically, your channel starts receiving persist notifications for Telegram's inbounds.

There is no registration order that avoids this. Whichever channel registers second wins; the other one wedges. The fix is a per-channel callback registry (or moving the notification onto `channel.Host` keyed by channel name), and it belongs in the second-channel PR.

### 3. `mappings.ChannelConfig` is a closed struct — it deletes your config keys on the next save

`mappings.ChannelConfig` in `internal/mappings/types.go` declares a fixed set of Telegram-shaped fields (`bot_token`, `default_group`, `groups`, `dm_chat_id`, `master_user_id`, `topics`, `debounce_ms`, `debounce_max_messages`, `fallback_cooldown_s`, `stt_prefix`, `api_base_url`, `api_base_urls`, `rich_inbound`). There is no `map[string]any` overflow and no `json.RawMessage` passthrough.

`mappings.Read` unmarshals into that struct, discarding unknown keys. `mappings.Write` re-marshals **from** the struct and atomically replaces the file. So any broker action that saves mappings — an attach, a detach — **permanently deletes** a third-party channel's config keys from `mappings.json`. The pre-write `.bak` is one generation, so the second save eats the backup too.

The user-visible failure is nasty because it is delayed: your channel works, the operator attaches a session to some unrelated topic, and your channel silently fails to start after the next restart. And `host.Config(name, &cfg)` round-trips through the same struct, so it can only ever hand you the subset of keys `ChannelConfig` declares, however the file was written.

A second channel needs either a passthrough field on `ChannelConfig` or per-channel config files. Not a workaround — a fix.

### 4. Identifiers are Telegram-shaped 64-bit integers, everywhere

Every identifier seam in the interface is `int64`:

- `Inbound.ChatID`, `.MessageID`, `.TopicID`; `Sender.UserID`; `ReplyContext.MessageID`; `ReactionEvent.MessageID`
- `SendReply → (int64, error)`, `SendTyping(chatID int64, threadID *int64)`, `EditArgs{ChatID, MessageID int64}`, `ReactArgs{…}`, `StopPoll(chatID, messageID int64)`, `CreateTopic(chatID int64, …)`, `ValidateTopic(chatID, threadID int64)`
- `MakeRouteKey(channel string, chatID int64, topicID *int64)` in `internal/broker/route.go`
- `mappings.Allowlist{Users []int64, Groups []int64}`, `ChannelConfig{DMChatID, MasterUserID int64}`

Transports with opaque string identifiers (Slack channel `C…`/user `U…` ids, message timestamps like `"1718722725.000300"`; Matrix event ids; XMPP JIDs) need a mapping, and it has to be **reversible**, because the adapter hands your `int64` back to you in `EditArgs`/`ReactArgs`/`ReplyArgs`. An in-memory intern table dies on restart — and `Inbound` is marshalled into the durable queue under a frozen key set, so after a broker restart every queued line holds integers that resolve to nothing. There is no `Raw` escape hatch to carry the real identifiers alongside.

Two consequences worth calling out separately, because they are trust-boundary issues rather than ergonomics:

- **DM-vs-group is decided by Telegram's chat-id sign rule.** `isPrivateChat` in `internal/broker/pairing.go` is `in.ChatID > 0`, and it feeds `Gate` → the allowlist. Any positive integer you synthesize for a public channel is classified as a DM and gated against `Allowlist.Users`, which holds Telegram user ids. `c3types.ConvKind` exists to fix exactly this and currently has no readers — teaching the gate to prefer it is prerequisite work.
- **The allowlist is not namespaced by channel.** `Allowlist{Users, Groups}` is a flat list of `int64`. Any integer you coin can collide with a real Telegram id and grant access across channels.

### 5. A failing `Start` aborts broker startup for everyone

`RegisterChannel` starts the channel first and records the registration only if `Start` returns nil; `cmd/c3-broker/main.go` propagates that error out of startup. So a bad credential in a second channel takes the whole broker down, Telegram included. There is no per-channel isolation and no "continue with the other channels" loop.

There is also no `enabled` flag: `ChannelConfig` has no such field and `RegisterChannel` checks nothing. Disabling a misbehaving channel today means a rebuild.

## Configuration

Channel config lives at `mappings.json:channels.<name>`. Telegram's block:

```json
{
  "channels": {
    "telegram": {
      "bot_token": "...",
      "default_group": "main",
      "groups": {"main": {"chat_id": -100, "title": "..."}},
      "dm_chat_id": 0,
      "topics": [],
      "debounce_ms": 1500
    }
  }
}
```

Read it with `host.Config(name, &cfg)`. Subject to blocker 3: the set of keys that survive a round-trip is exactly the set `mappings.ChannelConfig` declares, and unknown keys are destroyed on the next save. `debounce_ms` defaults to 1500 when absent.

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

The broker writes ambient connectivity state to `$XDG_STATE_HOME/c3/health.json` (fallback `$HOME/.local/state/c3/health.json`), resolved by `broker.HealthFilePath()`. It is written atomically (temp-in-same-dir + rename, no fsync — best-effort). The top level is a **wrapper carrying broker liveness**, with the per-channel snapshot nested under `channels`:

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
- `written_unix` — unix seconds, **refreshed on every write**: edge-driven writes (UP↔DOWN) *and* a slow 45-second refresh ticker that runs regardless of edges. While the broker is alive, `written_unix` stays current.
- `version` — the running broker's build version (`"dev"` for an uninjected local build).
- `update_available` / `latest_version` — **omitted** while the broker is on the current stable release; set once the periodic update check finds a newer release, so the status line can render an update notice. Independent of the `auto_update` toggle. See "Updating C3" in [`USAGE.md`](USAGE.md).
- `channels` — map of channel name → per-channel entry (`state` is `"up"`/`"down"`; `since_unix`/`since_hhmm`/`reason`/`consec` describe the current state). At boot it is `{}` (no outage asserted), so a crash never leaves a stale per-channel `down`.
  - For `telegram`, `state` is the **combined reachability** of both directions: the channel tracks outbound send health alongside inbound fetch health and surfaces both on one entry, so a single root cause never produces two notifications. `reason` reflects the failing direction(s): `"inbound unreachable"`, `"outbound send failing"`, or `"unreachable (inbound + outbound)"`.

**Why the wrapper exists (broker-dead detection):** the top level used to be a flat `{"<channel>": {...}}` map written only on health edges and at startup. When the **broker process** died, the file froze at its last value (usually `up`), so a status line showed green while C3 was entirely dead. A reader now treats **`broker_pid` not alive** (e.g. `kill(pid, 0)` fails) **OR** `now - written_unix > 90s` (2× the 45s refresh interval) as broker-down/unknown, regardless of the per-channel `state`.

A second channel gets a `channels.<name>` entry here for free by calling `host.NotifyHealth` on edges.

## Channel lifecycle

What the code actually does:

```
boot (cmd/c3-broker/main.go):
  br.RegisterChannel(telegram.New())
    → NewBrokerHost(broker, ch.Name())
    → ch.Start(ctx, host)        # SYNCHRONOUS, on the caller's goroutine
    → on error: startup aborts (see blocker 5)
    → on success: registration recorded

shutdown (broker.ShutdownWithin):
  1. drain the worker pool within the grace period
  2. ch.Stop() for each channel, SERIALLY under the channel mutex
  3. cancel the broker context
```

The order matters and is not the obvious one. Cancelling the context first, or stopping channels before draining workers, was the original bug: workers held channel references and called `SendReply` mid-flight while `Stop` tore down state, producing 20-second hangs on stale request timeouts. `broker.go` documents this at `ShutdownWithin`. Keep the order.

`Stop` may be called concurrently with an in-flight `Send*` — finish the in-flight call, then close.

## Error handling

- **Transient transport errors** (network blips, rate limits) → log, back off, keep going. Do not return from `Start`.
- **Fatal errors** (bad credentials, unsupported API version) → return from `Start`. Be aware this currently aborts broker startup entirely (blocker 5), so reserve it for genuinely unrecoverable configuration faults.
- **`Send*` errors** → return them verbatim. The broker forwards them to the adapter, which surfaces them to the CLI and the user. Do not swallow.
- **Rate limits** → respect provider-supplied retry-after (Telegram's `parameters.retry_after`, an HTTP `Retry-After` header, etc.). Sleep that long and retry once before giving up.

## Testing

There is **no `channel.MockHost`** — it does not exist in the code. Hand-roll a fake `channel.Host` in your package's tests; `fakeChannel` in `internal/broker/attach_test.go` is the closest existing pattern for the mirror-image case.

For Telegram specifically, mock at the `gotgbot` boundary: never hit the real Bot API in tests. The existing tests under `internal/channel/telegram/` use `httptest.Server` for the HTTP layer and a synthetic `getUpdates` loop; there are a lot of them and they are the best available reference for what a channel's test suite should cover.

## When this seam reopens

**The interface generalises when a real second channel lands in-tree and teaches us the right shape — not before.**

That is the whole condition, and the reasoning is short: designing a transport-neutral interface against exactly one implementer is guessing. `channel.go` already carries the evidence — `Capabilities()` dropped an argument to avoid an import cycle, `StopPoll` and the topic methods are frankly labelled Telegram-specific, `ConvKind` was added for a second channel that has not arrived and consequently has no readers, and the `Emit` contract was reversed only when the release audit looked hard at what a `false` really meant. Every one of those is a thing we got right or wrong because of pressure from real code, not from imagining a hypothetical Slack.

So the sequence is: second channel lands as a PR, it changes this interface where the interface is wrong, and *then* we know enough to say what is stable. Freezing a "general" channel API before that would freeze a guess — and a frozen guess is harder to fix than an honestly-scoped internal interface.

Until then: channels are in-tree, the PR is welcome, and this document is the list of what you will have to fix on the way in.

## Adding a channel — checklist

- [ ] Issue opened naming the transport, before the code
- [ ] Package under `internal/channel/<name>/` with an exported `New()`
- [ ] Type implements `channel.Channel` (including `Capabilities()`; stub `StopPoll`/`CreateTopic`/`ValidateTopic` with an unsupported error if the transport has no equivalent)
- [ ] Hand-wired via `br.RegisterChannel(<name>.New())` in `cmd/c3-broker/main.go` (no `registry.go`)
- [ ] `GateInbound` called before every `Emit`, and its decision honored
- [ ] `ConvKind` set on every emitted `Inbound`
- [ ] `Emit`-false handling does **not** advance past the message (blocker 1) — and if your transport cannot redeliver, say so in the PR rather than working around it
- [ ] Config keys survive a mappings save (blocker 3) — needs a broker-side fix, not a channel-side workaround
- [ ] Persisted-callback conflict resolved (blocker 2) — Telegram's offset tracker must still work with your channel registered
- [ ] Identifier mapping is reversible across a broker restart (blocker 4)
- [ ] All `Send*` methods return errors, don't swallow
- [ ] Rate-limit handling honors provider conventions
- [ ] No `fmt.Println` — use `host.Logf`
- [ ] Tests with a hand-rolled host fake; transport mocked at its client boundary
- [ ] Telegram still passes its full test suite with your channel registered

## Telegram channel: what's there

`internal/channel/telegram/` is the reference implementation. Things it does that a second channel may want to borrow:

- **Long-polling `getUpdates`** with `allowed_updates` opt-in for `message`, `edited_message`, `callback_query`, `message_reaction`. Forum service-message types are received but ignored in v0.1 — plumbed for future use.
- **General topic id is `1`, not `0`.** Topic id 0 means "no topic" (DM, non-forum group); General is a real topic with id 1.
- **The Bot API has no `getForumTopics`.** The local `topics` registry under `mappings.json:channels.telegram.topics` is the source of truth. Topics are added when a session attaches and creates or claims one, never opportunistically from inbound traffic.
- **Reply threading**: an inbound with `reply_to_message` populates `ReplyContext` with `MessageID`, `User`, `Text`; the Claude adapter renders it as `reply_to_message_id` / `reply_to_text` attributes on the `<channel>` block.
- **Voice handling**: voice messages emit an inbound with `Attachments[0].Kind="voice"`, a `FileID`, and empty `Text`. The STT plugin's `OnVoiceReceived` fills in `Text`. The attachment is preserved so a CLI can re-download the audio if a transcript is ambiguous.
- **Broker bot commands (`/status`, `/queue`, `/drain`)**: registered at startup via `setMyCommands` so they autocomplete. An inbound whose first token is one of these (optionally `@<botname>`-suffixed on that token only, case-insensitive) is intercepted in the poll path **after the allowlist gate** — a stranger's command dies at the gate in silence, never a reply. The broker answers directly and the update is never queued or routed to an agent; its `update_id` is marked done so the offset advances. A handled command with an empty reply sends nothing. Messages carrying attachments are never intercepted, so a command in a media caption cannot swallow the attachment. See `docs/COMMANDS.md` for the grammar and authorization matrix.
- **Persisted-offset advance**: the read offset advances only to the highest contiguous `update_id` whose message has been durably persisted (`fsync`'d) to the inbound queue, or which was a no-op (gated, `/status`, or a non-message update). An update still mid-STT or not yet persisted does not advance the offset, so a crash there means Telegram redelivers it within its retention window — loss-free by construction. STT runs at flush time so the stored line already carries the transcript; storage is per-message, and the debounce/merge is a delivery-presentation concern that does not merge stored lines. See `docs/USAGE.md` "Durable inbound queue & backlog".

Treat these as patterns, not templates. Your transport has its own quirks, and where they conflict with a Telegram idiom, the quirk usually wins — that conflict is exactly the information this repo is missing.
