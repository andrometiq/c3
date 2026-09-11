# Writing C3 CLI Adapters

A C3 adapter is the bridge between the broker and a specific CLI's MCP-server expectations. The built-in adapters are `c3-claude-adapter` (Claude Code), `c3-codex-adapter` (Codex), `c3-grok-adapter` (Grok Build), `c3-desktop-adapter` (Claude Desktop, poll-only — see [`DESKTOP.md`](DESKTOP.md)), `c3-agy-adapter` (Antigravity, poll-only), `c3-cursor-adapter` (Cursor Agent CLI, poll-only), and `c3-dcode-adapter` (dcode, live-push via the external-event socket). If you want to integrate C3 with a CLI we don't yet support — Aider, plain shell, your own thing — write an adapter.

Adapters are not channels. Channels move bytes between users and the broker over the network (Telegram, web, voice). Adapters move messages between the broker and a single CLI process over MCP stdio. The two never see each other directly — both talk to the broker.

## What an adapter does

1. **Speak the host CLI's MCP protocol over stdio.** Claude Code and Codex both use a JSON-RPC 2.0 dialect very close to the MCP standard, with extensions for unsolicited notifications. A new CLI may differ in small ways; understand its dialect before starting.
2. **Maintain a connection to the broker over the C3 unix socket** and translate MCP tool calls into broker IPC ops.
3. **Translate inbound messages from the broker into whatever the host CLI can render** — and acknowledge each one, or the user's durable queue silently fills up forever.

---

## Read this first: the seam is the wire, not a Go package

**Every package in this repository lives under `internal/`.** There is no public Go package, and there is no adapter-client library to import. Concretely:

- A third-party **Go** module **cannot** import `github.com/Andrometiq/c3/internal/ipc` or `github.com/Andrometiq/c3/internal/broker`. Go's internal-import rule forbids it. If you want to reuse those types you must **fork this repository** and build your adapter inside the tree at `cmd/<cli>-adapter/`.
- An adapter in **any other language** (Rust, TypeScript, Python…) reimplements the wire from this document. That is a fully supported path — the socket is a plain newline-JSON unix socket with no Go-specific framing.

`internal/ipc/messages.go` and `internal/ipc/ops.go` are the **reference** for the shapes below, not a dependency you can take. This document is written to be sufficient on its own: you should not have to read Go source to ship a correct adapter. If you do, that is a bug in this document — please report it.

---

## Stability: Frozen vs Provisional

Every op below is labelled.

- **Frozen** — the shape is part of the release contract. Fields will not be renamed, retyped, removed, or have their meaning changed without a protocol-version bump (see below). New **optional** fields may still be added; ignore what you don't recognise.
- **Provisional** — real, implemented, and usable today, but **may change in a minor version**. If you depend on a Provisional op, pin your adapter to a C3 version and re-read this document before upgrading. We would rather label an op honestly than freeze a shape we are not confident in.

The Frozen core is deliberately small: handshake, ownership, message delivery with its acknowledgement, tool forwarding, durable-queue recovery, and errors.

**Contract pin (D039):** duplicates after expiry, restart, or degraded mode are allowed; silent loss is not. [Inbound delivery](#inbound-delivery) defines the broker-owned lifecycle, confirmed bilateral negotiation and unchanged legacy projection.

**"Frozen" is a promise about shape, not a requirement to implement.** The two are easy to conflate and this document used to. Every Frozen op's shape is part of the release contract — but of the 15, **12 are required** and 3 are frozen conveniences you may skip: `bye` (an optional graceful close; closing the socket is equivalent, and no built-in sends it) and the `list_topics` → `topics_list` pair (discovery only — an adapter that attaches explicitly, and surfaces the broker's picker response when it does not, is complete without them). Implementing them is a choice; their shape not changing under you is not.

One boundary inside a Frozen op, stated explicitly because it is genuinely mixed: `attach`'s **envelope** is frozen, while the optional **proposal payload** it can carry is still Provisional. The actions today are exactly five — `create`, `use_existing_other_group`, `disambiguate_dm`, `force_steal`, `pick_topic` — and two of them vary by *field*, not by name: `use_existing_other_group` may carry an `alternative` proposal, and `force_steal` renders differently when the holder is a desktop session. Treat the five as a snapshot rather than a closed set: **handle an unrecognised proposal action the way you handle an unknown op**, surfacing it to the user rather than failing.

### Mandatory rule: unknown ops must be logged and skipped, never fatal

> **Your frame reader MUST have a `default` arm that logs the unknown op and continues.**
> Do not error out, do not drop the connection, and do not silently discard the frame with no record.

The broker can be **newer than your adapter**. Updating C3 replaces the binaries and restarts the broker, while adapter processes belonging to already-running CLI sessions reconnect to the new broker with their old code. Mixed versions on one host are a normal, expected state — not an anomaly. Adding a new op is an *additive* change by contract, so an old adapter meeting a new broker will see ops it has never heard of.

A strict parser is the natural implementation here and it is **wrong**. A tagged-enum deserializer that rejects unknown variants (e.g. Rust `serde` with `#[serde(tag = "op")]` and no catch-all) turns a routine version skew into a dropped connection and a dead inbound path. A silently-ignoring reader is worse still: permission taps and answers vanish with no log line. Log it, skip it, keep reading. Every built-in adapter does exactly this.

### Protocol version

`hello` and `hello_ack` both carry an optional `protocol_version` integer. The current version is **1**. An absent or zero value means version 1 — the first C3 releases shipped no version field at all.

**C3 keeps a mismatched connection open, but it does not let an unknown dialect mutate ownership or durable state.** Both sides log a warning naming the disagreement. The broker has an explicit state-change compatibility window, currently **v1 through v1**. Outside it, safe/read-only and additive operations remain usable, including `fetch_queue` with `ack:false`, while these operations fail closed:

- ownership: `attach`, `release`, `recover_session`
- destructive delivery: `fetch_queue` with `ack:true`, `inbound_delivered`

The broker answers each refusal in that operation's response shape where one exists (`attached`, `fetch_queue_result`, `recover_session_result`), or with `error` for normally one-way operations. Those refusal errors are the version-mismatch exception to the usual no-response rule for `release` and `inbound_delivered`. Do not close the connection: surface the error and restart the CLI so its adapter and broker come from the same C3 build.

The bump rule, so you know what a bump means: the version increments only on a change a peer speaking the other version could **misinterpret** — renaming or removing a field, changing a field's type/units/meaning, changing what an existing op does, or making previously-optional behaviour mandatory. It is **not** bumped for a new optional field or a brand-new op.

---

## Transport

### Socket path resolution

The broker's socket is `c3.sock` inside a per-user runtime directory. Resolve that directory **in this exact order** — getting it wrong is the single hardest failure to diagnose, because a mismatched adapter will spawn a broker that immediately exits and then retry a socket that will never appear.

**On Windows:**
1. `%LOCALAPPDATA%\c3` — if `LOCALAPPDATA` is unset, `<user home>\AppData\Local\c3`; if the home directory is unresolvable, the system temp directory + `\c3`.
2. Create it with owner-only permissions if absent.

**On Unix:**
1. `$XDG_RUNTIME_DIR` — **but only if it exists and is a directory.** Set-but-nonexistent must fall through, not be used.
2. `/run/user/$UID` — an **unconditional probe, independent of the environment**. If it exists and is a directory, use it. Do not skip this step.
3. `/tmp/c3-$UID/` — last resort; create it with mode `0700`, then `lstat`
   it and fail closed unless it is a real directory (not a symlink), owned by
   `$UID`, with mode exactly `0700`. Never swallow the create/stat failure or
   continue through a pre-planted path.

Step 2 is not optional and is the step naive implementations miss. Whenever `XDG_RUNTIME_DIR` is unset but `/run/user/$UID` exists — a systemd unit, a cron job, `su -`, a non-login shell, any CLI not spawned from a graphical session — an adapter that jumps straight to `/tmp` will look in the wrong place while the broker listens elsewhere.

This resolution must be **deterministic across every process on the host, regardless of the calling process's environment**. It is written this way because of a real incident: two brokers were spawned with different `XDG_RUNTIME_DIR` values, producing two listen sockets, two pollers both conflicting against Telegram, and adapters scattered between them depending on each adapter's own environment. Messages were delivered to the wrong broker. The environment-first rule was the bug; the unconditional `/run/user/$UID` probe is the fix.

The same directory also holds `c3-broker.pid` (the broker's singleton flock file) and `c3-broker.caps`.

### Spawning the broker

If the connect fails because the broker isn't running, spawn `c3-broker` in a **detached process group** and retry the connect with a short backoff for up to ~10s. Singleton enforcement is broker-side via a flock on the pid file in the runtime directory, so a spawn race is safe — the loser exits with "broker already running".

Note the coupling: the pid file lives in the **same** runtime directory as the socket. If your socket-path resolution disagrees with the broker's, your spawned broker locks a different pid file than the running one, starts, finds the socket path already taken or the Telegram poller conflicting, and you get no diagnostic. Resolve the path correctly and this cannot happen.

### Framing

Newline-delimited JSON, one message per line, UTF-8. Specifics you must implement:

- **4 MiB hard frame cap.** A peer that streams more than 4 MiB without a newline is not respecting the framing. Stop reading and **close the connection** — do not attempt to resynchronise, because the remaining bytes cannot be attributed to any frame boundary. Do not pre-allocate the cap per connection; grow as an actual frame requires.
- **`\r\n` is tolerated on read.** Strip a trailing `\n` and an optional preceding `\r`. Write plain `\n`.
- **A clean EOF still delivers a trailing unterminated frame.** If the peer closes with bytes buffered and no final newline, treat those bytes as one last complete frame; only an empty buffer at EOF is a plain end-of-stream.
- **Writes must be serialised.** One frame reaches the wire at a time. Both sides guard the writer with a mutex; a partially interleaved frame is unrecoverable.

### The connection is duplex — you need a demux reader

The socket carries request/response traffic **and** unsolicited broker pushes on the same connection, written from different goroutines on the broker side at any moment. A "write then read the next frame" request/response loop is broken: the very next frame may be an `inbound` push, not your reply.

The correct shape is **one dedicated reader task** that dispatches every frame by op, plus a pending-request map that wakes the right caller:

- Ops that carry a correlation `id` — `tool_call`/`tool_result`, `fetch_queue`/`fetch_queue_result`, `retranscribe`/`retranscribe_result`, `observe`/`observe_result` — are matched on that id. Generate it yourself; the broker echoes it verbatim.
- `ask_register`/`ask_registered`/`ask_result` are matched on `ask_id`; `permission_request`/`permission_verdict`/`permission_settled` on `request_id`. Claude Code derives that five-letter permission id from the corresponding transcript `tool_use_id`; other hosts may mint it differently.
- **`attach`/`attached`, `list_topics`/`topics_list`, and every op in the CLI-client section carry no correlation id** and are matched **by op alone**. Keep at most one of each in flight per connection. (An optional `id` may be added additively in a future version; until then, serialise them.)
- On a compatible dialect, `release`, `inbound_delivered`, `permission_request`, `permission_settled`, and `bye` get **no reply on their normal path**. Do not await one — you will deadlock. An incompatible `release` or `inbound_delivered` is refused with `error`; your demux reader handles that unsolicited frame.

On a broker drop, wake every pending request with an error so the host CLI's tool calls don't hang.

### JSON key casing — the silent-corruption trap

**C3 uses two different casings on the same wire, on purpose.**

- **IPC envelope fields** (everything defined in this document's op tables — `op`, `conn_id`, `chat_id`, `update_id`, `ask_id`, `stable_session_id`, …) are **`snake_case`**.
- **Nested payload objects** — the `inbound` value, the `messages` array, `capabilities` — are **`PascalCase`, byte-identical to the Go field names**: `Channel`, `ChatID`, `TopicID`, `MessageID`, `Sender`, `Text`, `Attachments`, `ReplyTo`, `Timestamp`, `Kind`, `Event`, `RichText`, `MaxMessageRunes`, and so on.

This is frozen deliberately. Those Go field names *were* the on-disk queue format and the IPC wire format before explicit tags existed; the tags now pin the keys so the Go identifiers can change without moving the format. There is a one-directional golden test in the tree whose literals are the contract. **The keys will not be "tidied" to snake_case** — doing so would orphan every queued message on every user's disk.

New queue lines also carry one reserved, additive top-level key:
`_c3_queue_id`. It is a broker-private durable-line identity used to make a
tokened live-delivery ack remove the exact original/edit occurrence it rendered.
It is not part of `c3types.Inbound`, never appears inside an IPC `inbound`, and
adapters must neither emit nor interpret it. Older queue readers safely ignore
the unknown key; legacy lines without it remain readable and are never guessed
at by a destructive ack.

The failure this causes is silent and total: a blanket `rename_all = "snake_case"` (or equivalent) across your whole deserializer will parse an `inbound` frame into an **all-zero-value** message with no error. No exception, no log line, no clue. Scope your casing rules per type.

Frozen envelopes tolerate unknown JSON fields. The optional versioned delivery
offer and negotiated messages instead use their strict schema; a malformed offer
fails back to legacy as specified under Inbound delivery.

---

## Op reference

43 ops exist. This section documents all of them: **15 Frozen**, **12 Provisional**, **4 Provisional-negotiated**, **10 belonging to the bundled CLI rather than to adapters**, and **2 that are not implemented and must not be sent**.

Field names below are the literal JSON keys. `?` marks an optional field (omitted when empty/zero).

### Frozen core — 15 ops (12 required, 3 optional)

An adapter that implements only these is correct and complete for a CLI with no interactive-question, permission-relay, voice, or session-resume needs. Three of the fifteen — `bye`, `list_topics`, `topics_list` — are frozen in shape but **optional to implement**; see *Stability* above.

#### `hello` → `hello_ack` — the handshake

**`hello`** (adapter → broker) **MUST be the first frame on every connection**, including after a reconnect. Any other first frame gets `{"op":"error","err":"expected hello first"}` and the connection is closed. A malformed hello gets `{"op":"error","err":"malformed hello"}` and the same.

```json
{"op":"hello","cli":"rust","pid":12345,"cwd":"/absolute/path",
 "capabilities":["..."],"cannot_render_channels":false,"protocol_version":1}
```

| field | type | notes |
|---|---|---|
| `cli` | string | your adapter's CLI name. Appears in claim listings and logs. Avoid `c3-broker-cli` — that name is reserved for the bundled status client and is filtered out of session listings. |
| `pid` | int | your adapter's pid. The broker keeps a claim alive as long as this pid lives, so it must be a real, live process id. A future version may additionally bind this pid to its process start time, so that a *recycled* pid is not treated as the same session; an honest pid is unaffected. |
| `cwd` | string | resolved-absolute path. Seeds the attach picker's "current project" suggestion and the cwd→mapping lookup. |
| `capabilities`? | []string | free-form tags. **Currently recorded on the wire but not read by the broker** — informational only. |
| `cannot_render_channels`? | bool | Legacy only: true for `queue_only`, `probing`, and `cross_session`; absent preserves the old default. |
| `render_state`? | string | Legacy only: `capable`, `probing`, `cross_session`, or `queue_only`; never a negotiated receipt declaration. |
| `render_reason`? | string | Short generic explanation, without personal paths or identifiers. |
| `protocol_version`? | int | absent ⇒ 1. |
| `delivery`? | object | Provisional-negotiated offer; see [Inbound delivery](#inbound-delivery). Absent, malformed, or unknown-version offers retain legacy delivery. |

**The identity rule: `cli`, `pid` and `cwd` together are your session identity, and identity is what buys persistence.** A hello with an empty `cli` or `pid ≤ 0` is **accepted** — it is not a protocol error and your connection works normally — but it is **anonymous**, and the broker will never match it to any other connection. Concretely, an anonymous adapter gets no reconnect claim transfer (its claims are released when the connection drops, since a pid of `0` is never live), and no cross-connection continuation: a permission verdict arriving after a reconnect, or a "held by you" report, is refused rather than guessed at. Everything within one connection — attach, tools, inbound, permissions — works exactly as documented.

The broker rejects only *malformed JSON*. It does not reject an incomplete identity; it declines to treat one as an identity. The reason is a rule this project holds without exception: **two unknown identities must never compare equal.** Two adapters that both omit these fields are not the same session, and the broker will not hand one's topic to the other on the strength of them matching in their emptiness.

**Fail closed when host delivery is uncertain.** Until delivery negotiation is
confirmed, report the legacy `cannot_render_channels` flag accurately. Its legacy
render-state extension may admit one probe; negotiated eligibility and receipt
milestones instead come from the explicit delivery contract. Outbound claims
remain usable in either case.

If your CLI has no unsolicited-notification path at all — the exact case this document tells you to expect — and you leave this field absent, the broker reads your host as renderable, pushes to it, acks, and the user loses every message. The poll-only built-ins (`c3-desktop-adapter`, `c3-agy-adapter`, `c3-cursor-adapter`) set it `true` unconditionally.

**`hello_ack`** (broker → adapter):

```json
{"op":"hello_ack","conn_id":18,"no_mapping":true,
 "capabilities":{"Channel":"telegram","RichText":true,"MaxMessageRunes":4096,"…":"…"},
 "protocol_version":1}
```

| field | type | notes |
|---|---|---|
| `conn_id` | uint64 | this connection's broker-side id. Useful in logs. |
| `no_config`? | bool | the broker has no config file. Tell the agent to run setup. |
| `no_mapping`? | bool | config exists, but this `cwd` has no saved mapping. The agent has to call `attach`. |
| `capabilities`? | object\|null | the resolvable channel's capability manifest — **PascalCase keys**. May be `null` (older broker, or no channel resolvable). Fall back to an all-false default; never fabricate a capability. |
| `protocol_version`? | int | absent ⇒ 1. |

There are exactly **two** cases to branch on: `no_config`, and `no_mapping`. Neither set means config exists and a mapping is on file — it does **not** mean you are attached. **Nothing is claimed at hello.**

> **Deprecated fields — always absent or false. Do not branch on them.**
> `auto_attached` (always `false`), `mapping` (always `null`), `claim_holder` (always `null`).
> The broker no longer auto-attaches at hello and never populates these. A previous version of this document described a four-case auto-attach state machine built on them; that machine was removed from the code and the description was wrong. The stable, resumable session id arrives from a host hook roughly two seconds *after* the adapter spawns, so recovery cannot happen during the handshake — it runs later via **`recover_session`** (below). The fields remain on the wire only so old adapters keep parsing.

#### `attach` → `attached` — ownership

The only way a session claims a route. See the attach parser and proposal flow in [`COMMANDS.md`](COMMANDS.md).

**`attach`** (adapter → broker):

| field | type | notes |
|---|---|---|
| `cwd`? | string | resolved-absolute. |
| `expr`? | string | freeform: the raw user-supplied string, parsed broker-side. `""` = bare attach (own-session resume / picker — never a silent cwd claim); `"dm"` = the DM; `"<int>"` = topic id; `"create <name>"` or `"-y <name>"` = create; anything else = a name. Lets a slash-command wrapper be a one-liner. |
| `name`? / `target`? / `topic_id`? / `group`? / `channel`? | string / string / int64 / string / string | structured alternative to `expr`. `target` is `"dm"`. |
| `create`? | bool | confirm a creation proposal. |
| `steal`? | bool | evict a live holder. Only ever set after the user confirms a `force_steal` proposal — never silently. |
| `replay`? | bool | set true when re-sending after a reconnect. Suppresses the on-attach welcome message so a broker bounce doesn't look like a user action. |
| `chat_id`? | int64 | optional cross-check for an id-addressed replay: the broker refuses if the named group resolves to a different chat. Only meaningful alongside `topic_id`. Zero = no check. |
| `confirm`? | object | prior proposal, echoed back. Plumbed for forward-compat. |
| `policy_rejected`? | bool | hint set by the agent on a re-invoke after the host CLI's policy layer rejected a prior attach. The broker short-circuits to `status: "policy_rejected"`. The broker never infers this. |

**`attached`** (broker → adapter):

| field | type | notes |
|---|---|---|
| `ok` | bool | |
| `status`? | string | `"ok"` \| `"no_topics_configured"` \| `"policy_rejected"` \| `"cwd_default_collision"`. Absent ⇒ interpret `ok`/`err`/`proposal` as before. |
| `channel`? / `chat_id`? / `topic_id`? / `name`? / `group`? | string / int64 / int64 / string / string | the resolved route identity. |
| `needs_confirmation`? | bool | with `proposal` — surface it to the user; claim nothing. |
| `proposal`? | object | **Provisional sub-structure.** `action` is one of `create`, `use_existing_other_group`, `disambiguate_dm`, `force_steal`, `pick_topic`; plus `channel`, `group`, `name`, `existing?`, `alternative?` (recursive), `holder?`, and for `pick_topic` a ranked `suggestions[]` + `project` + `has_more`. |
| `capabilities`? | object | the just-attached channel's manifest, PascalCase. Refresh your agent-facing guidance from this. |
| `queued_count`? / `queued_summary`? | int / array | held backlog on the claimed route. Summary rows are `{message_id, sender?, kind?, unix?, preview?}` — previews are truncated, never full bodies. Render them and tell the agent to drain the rest with `fetch_queue`. |
| `cwd`? / `holder`? | string / object | set only on `cwd_default_collision`. |
| `err`? | string | |

**No correlation id.** Match by op; one attach in flight at a time.

#### `release` — drop the claim

```json
{"op":"release"}
```

**No response.** Drops the claim and *tombstones* the session attachment, so a later resume of the same session deliberately stays unattached. A process exit is not a release — do not send this on shutdown unless the user actually asked to detach; the broker handles conn-drop separately and preserving that distinction is what makes resume work.

#### `list_topics` → `topics_list`

```json
{"op":"list_topics"}
{"op":"topics_list","topics":[{"channel":"telegram","chat_id":-100…,"topic_id":42,
  "name":"…","group":"…","claimed_by":{"cli":"claude","pid":123,"cwd":"/…"}}]}
```

`claimed_by` is absent when unclaimed. No correlation id — match by op, one in flight.

#### `tool_call` → `tool_result`

```json
{"op":"tool_call","id":"<unique>","name":"reply","args":{"text":"…"}}
{"op":"tool_result","id":"<same>","result":{"content":[{"type":"text","text":"sent (id: 9)"}]}}
{"op":"tool_result","id":"<same>","error":{"code":-32000,"message":"…"}}
```

`id` is yours to generate and is echoed verbatim. `result` is the MCP content shape.

**Only seven tool names route through `tool_call`:** `reply`, `react`, `edit_message`, `send_typing`, `poll`, `stop_poll`, `download_attachment`. Anything else returns `error.message = "unknown tool \"<name>\""`. Everything else your CLI exposes is adapter-local and hits a dedicated op — see [Adapter-local tools](#adapter-local-tools-vs-forwarded-tools).

A `tool_call` before any attach returns `error.message = "tool_call before attach: no route claimed"`. A stalled route worker returns a clean timeout error rather than wedging your read loop.

**`args` never carries a destination.** Every tool call goes to the route your session claimed with `attach`, and only there. A `chat_id` or `topic_id` inside `args` is **refused**, not honoured and not ignored — the call fails with `"<field> is not a tool argument"`. Do not add either to a tool schema you expose. They were once honoured, which meant a compromised or prompt-injected agent could address any chat and thread the `topics` tool showed it; the destination is now structurally the claimed route. Note the refusal is on **presence**, not on disagreement: sending `chat_id` equal to your own route's id is still an error, because a value-comparing check would accept `chat_id: null`.

#### `inbound` — unsolicited push

```json
{"op":"inbound","inbound":{ /* PascalCase message object */ },"pending":2,"covered":3,
 "delivery_token":"<opaque broker token>"}
```

| field | type | notes |
|---|---|---|
| `inbound` | object | the normalised message. **PascalCase keys** — see the payload shape below. |
| `pending`? | int | messages **still queued after** the lines this push covered, i.e. backlog this push did *not* deliver. Surface it (e.g. "(N pending — call fetch_queue)") so a stuck item is visible on this push, not only at the next re-attach. |
| `covered`? | int | how many durable queue lines this (possibly **merged**) push covers. A debounced batch of N stored lines arrives as **one** notification with `covered: N`. Defaults to 1 when absent. **You must echo this back.** |
| `delivery_token`? | string | opaque broker-minted identity for this exact pushed route and durable record set. Echo it unchanged in `inbound_delivered`. Empty means a legacy broker; never invent one. |

The `inbound` object's fields (PascalCase, exactly as written):

`Channel` (string), `ChatID` (int64), `TopicID` (int64\|null — null = DM/no topic, `1` = the reserved root topic, `>1` = a custom topic), `MessageID` (int64), `Sender` (`{UserID, Username}`), `Text` (string), `Attachments` (array of `{Kind, FileID, Size, MIME, Name}` — `Kind` is one of `voice`, `audio`, `video`, `video_note`, `document`, `photo`, `sticker`), `ReplyTo` (`{MessageID, User, Text}`\|null), `Timestamp` (RFC3339), and:

- `Kind`? — **empty string means an ordinary message.** A non-empty value marks a *synthesized channel event*: `poll_result`, `reaction`, `callback`, or `system` (a broker-originated advisory such as a channel-health alert, carrying no user content).
- `Event`? — the event payload when `Kind` is non-empty. Exactly one of `PollResult`, `Reaction`, `Callback`, `System` is set.
- `DrainedFrom`? — provenance when the line was moved in by a drain; empty for organic messages.
- `V`? — record-format version. **Absent or 0 means version 1.** Direct marshaling and legacy queue rewrites preserve an absent key; a **new** durable queue append stamps `V:1` on a copy of the record. That stamp is additive and old-reader-compatible, but it means a newly appended record is not promised to remain byte-identical to its unstamped input. **Readers MUST NOT reject a higher value** — a newer writer sharing a socket or queue directory with an older reader is a normal partially-updated install, and hard-failing turns cosmetic skew into lost messages. Best-effort decode.
- `ConvKind`? — `"dm"` or `"group"` as stated by the channel. Empty means the channel didn't say.
- `Edited`? — set **only by the channel** when this is a new version of an already-delivered message. An edit reuses `MessageID`, has `Edited: true`, and carries new `Text`; it is not a duplicate. Never deduplicate inbound records on `MessageID`: the broker preserves occurrences for one id in FIFO order so the original and its correction can both be delivered.
- `MediaGroupID`? — the channel's album/media-group id. It is captured and persisted for future album-aware consumers but is not rendered today.
- `ForwardOrigin`? — flattened forward provenance as `{Kind,Name?}`. `Kind` is `user`, `hidden_user`, `chat`, or `channel`; `Name` is the best available display name.
- `Merged`? — ordered delivery structure for a debounced push, as `[{MessageID,Sender,Text?,ForwardOrigin?},…]`. It is presentation-only and is **never persisted**; durable `fetch_queue` rows remain discrete messages.
- `SourceMessageID`? — appears inside an `Attachments` entry only on a merged delivery and pairs that attachment with its source entry in `Merged`. It is presentation-only and is **never persisted**.

These four provenance keys follow the same Go-name-identical spelling rule as the existing wire fields. They are additive: readers must ignore unknown fields, and an older adapter may render less structure but must not reject or lose the inbound. `Merged` and `SourceMessageID` exist only on a multi-message live delivery; a single-message push retains its prior wire shape.

`ChatID` sign convention follows Telegram's: positive = user/DM, negative = group, `-100…` = supergroup.

#### `inbound_delivered` — the acknowledgement you must not skip

```json
{"op":"inbound_delivered","update_id":<Inbound.MessageID>,"ok":true,
 "count":<covered>,"delivery_token":"<same token from inbound>"}
```

**No response on the normal compatible path.** An incompatible dialect gets the
uncorrelated `error` described under Protocol version. This is the legacy
acknowledgement; negotiated deliveries use `attempt_result`. See
[Inbound delivery](#inbound-delivery).

#### `fetch_queue` → `fetch_queue_result`

The durable-queue drain. Every adapter exposes this as a tool; for a CLI with no push path it is the *only* way the user sees inbound.

```json
{"op":"fetch_queue","id":"<unique>","limit":3,"all":false,"ack":true}
{"op":"fetch_queue_result","id":"<same>","messages":[ /* Inbound objects */ ],"remaining":7}
```

| field | type | notes |
|---|---|---|
| `id` | string | caller-generated correlation id, echoed in the response. It is limited to **1 KiB UTF-8 bytes** because it shares the response frame with the messages; an over-limit id is refused without consuming anything. |
| `limit`? | int | oldest-first batch cap. The built-ins default to 3 and cap at 50. Every finite limit is still subject to the one-frame response budget; keep calling until `remaining` is zero. |
| `all`? | bool | overrides `limit`, still subject to the same one-frame response budget. |
| `ack` | bool | `true` consumes on the legacy/consume path, or reserves with `lease:true` on accepted receipt fetch; `false` peeks without mutation. Send explicitly; see Inbound delivery. |
| `messages`? | array | oldest first. Normal records carry their stored content. A record that can never fit in an IPC frame is moved aside and replaced **in position** by a broker-authored notice with the original identity and no original content; do not treat that notice as a user message. |
| `remaining` | int | still queued after this batch. |
| `err`? | string | set (and `messages` nil) on failure, e.g. no route claimed. |

#### `bye`

```json
{"op":"bye"}
```

No response; the broker returns from the connection handler and closes. Optional — closing the socket is equivalent, and no built-in adapter sends it.

#### `error`

```json
{"op":"error","err":"…"}
```

Sent by either side. **Not correlated to any request** — you cannot match it to the call that caused it. Log it and keep the connection; it is not fatal. In particular, if you are blocked waiting for a typed reply op and an `error` arrives instead, you will wait forever unless your reader treats `error` as a wake-up.

---

### Provisional-negotiated — 4 ops

`delivery_report`, `deliver`, `attempt_result` and `fetch_confirm` are defined in
[Inbound delivery](#inbound-delivery), including their negotiation gates.

### Provisional — 12 ops

Implemented and shipping, but the shapes are not frozen for v0.1.0. Each entry names why.

#### `recover_session` → `recover_session_result` — session resume

*Provisional: the session-identity model is under active review.*

**This is the real re-attach path.** There is no auto-attach at hello. If you skip this op, every restart of the user's CLI looks like a brand-new session to the broker and the user loses their topic binding.

Semantics that are not obvious:

- It is sent **after** `hello`, on the existing connection — not during the handshake. The stable session id is delivered by a host `SessionStart` hook that fires roughly two seconds after the adapter spawns.
- The id you send is the **stable, resumable transcript id**, which on Claude Code is **not** the value of the `CLAUDE_CODE_SESSION_ID` environment variable. That variable is an *ephemeral per-MCP-spawn* id; the built-in adapter uses it only to locate its own hook handoff file, which carries the real stable id. If your host has no equivalent, skip recovery — fail closed rather than guess.
- Stable ids are scoped by the `cli` family from `hello`. The same opaque id may validly name separate Claude, Codex, Grok, or other host sessions, and the broker stores those records separately. Old unqualified records are migrated under the first requesting CLI family and persisted before they become identity evidence; one legacy id can never identify a second family.
- Send it once per **broker connection**, guarded against races between your hook watcher and any first-activity recheck. Re-send it after every reconnect; otherwise a broker restart loses the session's recovery identity.

```json
{"op":"recover_session","stable_session_id":"<stable id>","cwd":"/absolute/path"}
{"op":"recover_session_result","recovered":true,"channel":"telegram","chat_id":-100…,
 "topic_id":42,"name":"…","group":"…","queued_count":3,"queued_summary":[…]}
```

The broker takes one of these branches, silently:

- **Stub already attached** → it records the current route under the stable id for a future resume. No re-claim; `recovered` stays `false`.
- **Explicit detach/tombstone, auto-attach disabled, expired or missing record, invalid route, or route held by another live session** → no claim and `recovered:false`.
- **Eligible remembered route is free (or already belongs to this logical session)** → claim it and return `recovered:true`.

An empty `err` with `recovered:false` is therefore an ordinary non-recovery outcome, not a transport failure.

`err` is set only on a malformed request or an empty id. `queued_summary` rows have the same `{message_id, sender?, kind?, unix?, preview?}` shape as `attached`.

On a successful recover, surface it to the agent along with the held backlog count. Note the broker *also* posts a one-shot confirmation to the recovered topic, because a CLI-side notice emitted in the resume idle gap can be dropped by the host.

#### `ask_register` → `ask_registered`, then `ask_result` (unsolicited)

*Provisional: `multi`, `allow_other`, `allow_skip`, and `free_text` are accepted on the wire but only partly honoured; the answer taxonomy is still being extended.*

A blocking, correlated question with buttons.

```json
{"op":"ask_register","ask_id":"<8-char id, adapter-generated>","question":"…",
 "options":["a","b"],"multi":false,"allow_other":false,"allow_skip":false,"free_text":false}
{"op":"ask_registered","ask_id":"<same>","ok":true,"message_id":901}
{"op":"ask_result","ask_id":"<same>","answer":{"selected":["a"]}}
```

Carries **no route** — the broker derives it from your current claim.

`ask_registered` is the **synchronous** ack: `ok: true` once the question and keyboard were sent, or `ok: false` + `err` on a fast failure (ask before attach, empty options, oversized keyboard, channel without inline-keyboard support, send error) so your tool call returns immediately instead of blocking the full answer timeout. **Handle it, or every failure costs the user a ten-minute hang.**

`ask_result` is an **unsolicited push**, delivered like `inbound`, correlated by `ask_id`. `answer` is `{selected?: []string, text?: string, skipped?: bool, timed_out?: bool}`.

#### `permission_request` → `permission_verdict` (unsolicited)

*Provisional: this is a trust boundary under active hardening. Verdicts bind to the prompt message and requesting session; settle reports bind to the requesting session and cannot clear another session's prompt.*

Relays a host tool-use permission prompt to the operator as an Allow/Deny keyboard.

```json
{"op":"permission_request","request_id":"<host-minted id>","tool_name":"Bash","preview":"…"}
{"op":"permission_verdict","request_id":"<same>","behavior":"allow"}
```

Carries **no route** — derived from your current claim. `preview` must be a short, **already-truncated** input snippet, never a secret body.

**`permission_request` is fire-and-forget: the broker sends no reply on any path.** A nil route, a channel lookup failure, a channel without inline keyboards, and a send failure are all logged broker-side and dropped. If you await an ack here you will deadlock.

`permission_verdict` is an **unsolicited push**; `behavior` is the string `"allow"` or `"deny"`. If you do not handle it, the operator taps Allow on their phone and their CLI waits forever with no indication why. This is the most user-visible consequence of an incomplete op switch.

##### `permission_settled` (adapter → broker, no reply)

Reports that the host resolved a relayed prompt outside C3, so the broker can remove the stale keyboard.

```json
{"op":"permission_settled","request_id":"<same>","outcome":"allow"}
```

`outcome` is `"allow"` when the host can prove the tool result was not an error, and `"unknown"` otherwise. The broker renders “Allowed in the CLI” only for `allow`; every other value renders “Settled in the CLI” and never invents a denial. The report is owner-bound to the stub that sent `permission_request` (or a reconnect with the same `cli`/`pid`/`cwd`). An unknown id is a no-op; a non-owner report is refused and leaves the prompt live.

**A settle only removes the pending entry and clears the keyboard. It never emits `permission_verdict`.** If the keyboard send is still in flight, the broker records the settle and clears the message as soon as its message id arrives.

The Claude adapter observes complete `tool_result` records appended after the permission request to the main session transcript or its subagent transcript subtree. The pending observation set is process-local: restarting the adapter loses it because the host does not re-send old permission requests; those keyboards fall back to the broker's 30-minute reaper. An absent `transcript_path` keeps the previous relay behavior. An old broker treats this additive op as unknown and returns `error`; the adapter logs that frame and stays connected.

#### `retranscribe` → `retranscribe_result`

*Provisional: coupled to the bundled speech-to-text plugin's provider chain.*

```json
{"op":"retranscribe","id":"<unique>","file_id":"…","message_id":123}
{"op":"retranscribe_result","id":"<same>","text":"…","err":""}
```

Re-runs speech-to-text over a cached voice attachment. `message_id` is optional: when the matching message is still queued, its stored text is refreshed in place. `err` set (and `text` empty) when the provider chain still fails.

#### `observe` → `observe_result`

*Provisional: newest op; added for the Desktop inbox panel and shaped by it.*

A **read-only peek** at any topic's durable queue. Resolves the topic by `name` / `target` / `topic_id` (+`group`, `channel`) exactly like attach, but **claims nothing and consumes nothing** — safe to call on a timer, and safe to call on a topic another session holds.

```json
{"op":"observe","id":"<unique>","name":"…","target":"","topic_id":42,"group":"…",
 "channel":"","limit":10,"all":false}
{"op":"observe_result","id":"<same>","ok":true,"status":"ok","channel":"telegram",
 "chat_id":-100…,"topic_id":42,"name":"…","group":"…",
 "holder":{"cli":"claude","pid":123,"cwd":"/…"},"held_by_you":false,
 "messages":[…],"remaining":4}
```

`status` is `"ok"` \| `"not_found"` \| `"ambiguous"` \| `"dm_unconfigured"` \| `"no_channel"`. `holder` is absent when unclaimed (or held only by a dead session); `held_by_you` is true when the calling connection is the live holder. `err` carries a transient peek failure without changing the resolved identity.

---

### Broker-CLI ops — 10 ops, not part of the adapter contract

*All Provisional.* These are spoken by the bundled `c3-broker` status/utility client (which introduces itself with `cli: "c3-broker-cli"` and is filtered out of session listings), not by adapters. They exist on the same socket, so you *can* send them, but no built-in adapter does and you do not need any of them for a correct adapter. They are listed so you can recognise them and so the op table is complete.

| request | response | purpose |
|---|---|---|
| `list_claims` | `claims_list` | snapshot of every live route claim: `{channel, chat_id, has_topic, topic_id?, topic_name?, group_name?, holder_cli, holder_pid, holder_cwd?, conn_id, connected}`. Dead holders are reaped and omitted. |
| `list_health` | `health_list` | last cached fetch-health per channel: top-level `{health:[…], queue_degraded?}` where `queue_degraded:true` means durable queue startup failed; each health row is `{channel, state:"up"\|"down", since_unix?, consec?, reason?, down_for_sec?}`. |
| `list_sessions` | `list_sessions_reply` | every live adapter the broker tracks: `{cli, pid, cwd, conn_id, attached_to?, is_this_session?}`, newest first. Request carries optional `pid`/`cwd` hints for the "you are here" marker. |
| `ping_this_session` | `ping_this_session_reply` | sends a one-shot "this is me" message to the route held by the calling user's session. Request: `{pid?, cwd}`. Response: `{ok, channel?, topic?, sent_text?, err?}`. |
| `pair_mode_start` | `pair_mode_reply` | arms a pairing window. Request: `{target:"dm"\|"group", chat_id?}` (`chat_id` required for `group`). Response: `{ok, code?, target?, chat_id?, ttl_sec?, err?}`. |

---

### Not implemented — do not send

**`server_info` and `tools_list` are dead constants.** They exist in `internal/ipc/ops.go` and nowhere else: no handler, no sender, no payload struct, no test. The broker's dispatch falls through to its default arm and replies:

```json
{"op":"error","err":"op not implemented yet: server_info"}
```

Note the reply op is **`error`**, not a typed result. An adapter that follows the old advice to "fetch `server_info` and `tools_list` at startup" and blocks waiting for a matching reply op **hangs forever**, never answers the host's `initialize`, and is killed on the host's handshake timeout — presenting to the user as "MCP server disconnected" with nothing pointing at the cause.

A previous version of this document instructed exactly that, in two places. It was wrong. **`serverInfo`, `instructions`, and the tool list are adapter-owned** — see [The adapter owns its MCP surface](#the-adapter-owns-its-mcp-surface). These two constants will be removed.

---

## Adapter responsibilities, in order

On startup:

1. **Resolve the socket path** per the three-step (Unix) / one-step (Windows) rule above.
2. **Connect.** If the broker isn't running, spawn `c3-broker` detached and retry with backoff for up to ~10s.
3. **Send `hello`** — `cli`, `pid`, `cwd`, and `cannot_render_channels: true` if your host cannot render unsolicited pushes.
4. **Read `hello_ack`.** Branch on `no_config` and `no_mapping` only. Keep `capabilities` — it drives what you tell the agent it can do.
5. **Start your demux reader task** before anything else can generate traffic.
6. **Build your own `serverInfo`, `instructions`, and tool list.** Do not ask the broker for them.
7. **Run the MCP stdio loop.** Read JSON-RPC requests from stdin; respond on stdout.
8. **If your host exposes a stable session id** (via a hook or equivalent), send `recover_session` once it arrives.

Do not answer an `attach` while that identity is still settling. A canceled caller must return with **no attach frame written**. At the bounded settle budget, refuse a bare attach with `identity still resolving; retry attach` rather than choosing between own-session recovery and the picker. An explicit target may keep waiting for the bounded recovery attempt, but its attach frame must never overtake that recovery. Every terminal recovery path — success, no record, broker refusal, disconnect, or timeout — must settle the gate so the adapter cannot wedge.

While running:

- **`initialize`** → respond with your own `serverInfo`, your `capabilities`, your assembled `instructions`, and the right `protocolVersion`.
- **`tools/list`** → return your tool list.
- **`tools/call`** → either forward via `tool_call` and await `tool_result` on the correlation id, or run it inline against a dedicated op (see the table below).
- **`inbound` push** → legacy render/ack or untracked event; **`deliver`** → execute the negotiated transport and report evidence. See Inbound delivery.
- **`ask_result` / `permission_verdict` pushes** → route to the waiting caller / emit into the host; when the host can observe an out-of-band permission resolution, send `permission_settled` without awaiting a reply.
- **`ping`** → respond `{}`.

On the broker dropping the connection:

- **Reconnect with backoff** — the built-ins loop with exponential backoff (0.5s → 30s cap) rather than giving up, and surface a one-shot "broker unreachable" advisory after ~30s so the user learns inbound is down instead of assuming it works.
- **Re-handshake with `hello`** — *not* `server_info`. The broker recognises the same `(cli, pid, cwd)` triple — **with `cli` non-empty and `pid > 0`** — as a reconnect and transfers your existing claims to the fresh connection, so your claim survives a bounce. An *anonymous* hello (see the identity rule above) gets no transfer: use the attach replay in the next bullet, which restores your claim precisely because dropping an anonymous connection releases it.
- **Replay your last successful attach** with `replay: true` so the claim is restored without the welcome message firing again. Address a remembered topic by `topic_id` + `group` (+ `chat_id` cross-check), **not** by name: a DM recovers as `name: "dm"`, and replaying `attach(name: "dm")` can silently bind a topic literally named `dm`.
- **Re-fire `recover_session`** if you had one.
- **Wake every pending request** with a "broker reconnect" error so the host's tool calls don't hang.

---

## Inbound delivery

This is the authoritative inbound contract (D039), implemented through D034–D038.
The broker owns delivery; adapters execute the requested transport and report
host evidence. The legacy projection below preserves unnegotiated protocol-v1
behavior.

### Row, Attempt and receipt milestones

A **Row** is one durable queued message occurrence, including its content revision.
Its lifecycle is `queued` → `attempting` → `retired`; only the broker changes it.
Retired means removed from the live queue after confirmation, operator drain
(copy before remove), documented eviction, or oversize set-aside. A negotiated
oversize replacement must be confirmed before its original retires. Queue limits
at this HEAD are 1,000 messages / 90 days per route; trash retention is bounded
separately. Events are never queued. Append precedes Telegram offset advancement;
with no queue, offsets freeze even after best-effort live forwarding. Telegram's
24-hour retention and roughly 100 waiting-update limit then bound replay.

An **Attempt** is broker-minted and memory-only: token, route, holder (connection,
claim generation and optional stable session id), exact row ids/revisions,
transport, reservation deadline and outcome. There is **at most one active attempt
per row**, with one live batch per route. Transports are `channel` (the host's
native live input), `inbox` (the owning host's validated peer endpoint), and
`fetch` (an explicit tool pull). Outcomes are `confirmed`, `failed`, `expired` or
`released`. Non-confirmation returns only surviving unchanged members to queued;
removed rows are never recreated. A new voice revision invalidates only its old
membership and wakes scheduling. Duplicates after expiry, restart or degraded
mode are allowed; silent loss is not.

The model has three declared **live receipt milestones**, never inferred from
render state:

| Milestone | Evidence and retirement promise |
|---|---|
| `transcript` | A known host intake record contains this attempt's token under the strict framing/provenance predicate. It proves host intake, not task completion. |
| `accept` | The host positively accepted a submission; accepted by the host, not proven displayed. Recovery payload copies remain best-effort, memory-only and bounded. |
| `none` | No live receipt; pull-only. |

**Implementation boundary:** negotiated live modes currently require `transcript`.
The parser rejects an `accept` offer to legacy; Codex, Grok and dcode currently
use legacy host-acceptance acknowledgements. `none` is accepted with no eligible
live transport. Fetch is declared separately as `receipt` (successful matching
host tool result) or `consume` (removed when returned, not proven displayed).
Nothing weaker than the active connection's declared contract retires a row.
Legacy recovery copies are separate from durable queue state and are never a
second durability promise.

### Capability offer, acceptance and confirmation (protocol v1)

This optional protocol-v1 extension has one delivery owner: the broker. The
Claude adapter offers it when channel OR inbox is eligible, or when fetch receipts
are observable (a readable transcript after host readiness). Both require host
readiness: `notifications/initialized` received AND notify transport present.
The startup hello carries no offer and reports a queue-only legacy route;
item-F re-hello offers delivery once the host is ready. Channel requires a
positively detected host, a C3 channel flag, and a readable transcript. Inbox
requires a readable transcript and a validated owning-session socket: private
runtime directory, the inherited owning inbox path, user ownership, and kernel
peer PID/UID matching the owning host. The capability probe sends no credentials. A channel-eligible hello defers the
optional inbox probe until acceptance, then reports the verified inbox fact;
this preserves legacy channel connections' socket behavior.
Flagless and background sessions with a valid inbox negotiate inbox delivery;
a ready session with neither live transport eligible can negotiate fetch receipts.

```json
{"op":"hello","cli":"claude","pid":123,"cwd":"/work/project",
 "capabilities":["claude/channel"],"delivery":{"version":1,
 "live":{"channel":{"eligible":true},
         "inbox":{"eligible":false,"reason":"no owning session socket"}},
 "receipts":"transcript","fetch":"receipt"}}
{"op":"hello_ack","conn_id":1,
 "delivery":{"version":1,"modes":["channel","inbox","fetch_receipt"]}}
{"op":"delivery_report","accepted":["channel","inbox","fetch_receipt"]}
```

`Capabilities` remains `[]string`. The current version-1 parser accepts `transcript` or `none` for
live receipts and a separate `fetch:"receipt"` or `fetch:"consume"` milestone.
A pull-only peer can declare `receipts:"none"` with both live transports ineligible;
`fetch:"consume"` then confirms an empty mode set and retains consume-on-return.
The broker can accept channel, inbox and, for `fetch:"receipt"`, `fetch_receipt`.
A degraded broker (`Queue == nil`) never accepts. Malformed optional delivery
data does not invalidate an otherwise valid hello.

**Adapter confirmation:** negotiation completes only when the adapter
sends `delivery_report{accepted:[...]}` immediately after `hello_ack`, listing the
supported modes it accepts from that acknowledgement. Until then the broker
treats the connection as LEGACY: legacy pushes continue and no negotiated attempt
is minted. The Claude adapter accepts an acknowledgement with at least one mode it offered;
it ignores unknown modes and never confirms them. Confirming a mode absent from
`hello_ack.delivery.modes` is a protocol error and leaves the connection legacy.
The confirmed subset is frozen for that connection. Reports may subsequently
change eligibility facts, but cannot expand or replace accepted modes.
Changing declared milestones or confirmed modes on reconnect releases old attempts
before their evidence could be interpreted under the new contract. Refreshed
eligibility facts alone do not prevent live adoption. Fetch attempts always release
on reconnect; their tool-result evidence belongs to the original connection.

When an un-negotiated connection gains eligible facts (for example, its first
transcript or owning socket appears), the adapter reconnects and re-offers:
at most once per fact change and once per 10 seconds, waiting for any legacy
push to finish its ack or expire before changing connections. The existing
same-process reconnect path transfers claims; modes remain frozen per connection.

The extension is **Provisional-negotiated**. The acceptance confirmation completes
negotiation; subsequent delivery ops are sent/honoured only on confirmed connections:

```json
{"op":"deliver","token":"opaque-token","transport":"channel",
 "deadline_ms":14990,"inbound":{"Channel":"telegram","ChatID":-100,
 "MessageID":7,"Text":"hello"}}
{"op":"attempt_result","token":"opaque-token","outcome":"confirmed","reason":""}
{"op":"delivery_report","live":{"channel":{"eligible":false,
 "reason":"session transcript unavailable"},"inbox":{"eligible":false}}}
```

`inbound` is the existing PascalCase payload. `deadline_ms` is the remaining
observation budget measured after write admission, immediately before writing.
The broker's monotonic deadline starts at reservation, lasts 15 seconds, and is
never extended by a receipt. `attempt_result.outcome` is `confirmed` or `failed`;
there is no response. The connection must be negotiated and protocol-compatible;
the token resolves the route and exact row revisions. The route worker checks
the exact confirmed holder, captured claim generation, connection, open attempt,
and deadline together. A miss is one logged no-op, including a copied token
presented by a new holder. Tokens never appear in status or ordinary notices.

For `transport:"channel"` the adapter notifies its host using
`c3_delivery_id=token` and `c3_attempt="channel:N"`. For `transport:"inbox"` it
writes the existing auth and peer user frames with `c3_delivery_id=token` and
`c3_attempt="inbox:N"`. The inbox token is a NEW broker attempt token, never the
expired channel token. Socket validation and `validateCrossSessionPeer` run
before any credential write. The inbox write, including connect and clean
close, is bounded to 2 seconds inside its own fresh 15-second attempt deadline.
A bounded adapter writer keeps socket work off the broker reader; there is no
goroutine per attempt.

Inbox observation uses `deliveryReceipt(cross=true)`. The Claude 2.1.263 user
receipt requires `isMeta:true`, `origin.kind:"peer"`, `origin.from:"c3"`, and the
exact host prefix line `Another Claude session sent a message:\n` immediately
before the C3 channel block. The strict opening tag must carry
`source="plugin:c3:c3"`, this token, and this attempt. Claude 2.1.266
peer intake is VERIFIED in `testdata/claude-2.1.266-peer-intake.jsonl`:
`queue-operation/enqueue.content` and `attachment/queued_command.prompt` start
at the bare C3 tag, with no host prefix; attachments additionally require nested
`isMeta:true` and `origin.kind:"peer"` / `origin.from:"c3"`. Enqueue has no origin
fields: its host intake envelope plus exact source/token/attempt bind the receipt.
Prefixed intake variants were inferred and are rejected. Existing channel intake
recognition remains unchanged. Writes
alone never confirm delivery. Notify/inbox errors and unavailable transcripts
report `failed` with a generic reason immediately, independently of receipt
polling, including a missing notify transport or a failed notify write. The adapter never retries, falls back, or changes delivery state on this
path. `delivery_report` reports eligibility facts only when they change
semantically, including when the owning socket appears or disappears; it cannot
change accepted modes or receipt milestones.

Each route has one selected FIFO batch, bounded against the complete IPC frame.
A cycle tries channel then inbox, each eligible transport at most once over
that selected batch's surviving revisions. New arrivals wait for the next batch.
Every fallback reserves a new token and deadline. A late channel receipt cannot
retire inbox members or rearm anything. An unproven session admits one live
attempt across all its routes; the cycle holds its slot across fallback. Releasing
that slot admits the longest-waiting eligible route. A confirmation proves the
session and schedules the next batch. Exhaustion pauses the route until a
semantic capability report, reconnect/hello, explicit attach, a live confirmation
on this session, or inbound arriving at least 60 seconds after exhaustion.
Earlier inbound neither retries nor resets that clock. Pending voice rows are
excluded only from live scheduling. Enrichment invalidates the old member
revision immediately. Drain/eviction remove only affected members; an empty
attempt releases admission and proves nothing. Drain snapshots precede new
reservations. Imported rows persist `origin:"drain"` and stay nudge-only.
Confirmed retirement makes at most three storage removal attempts, retaining
evidence until success or release; release can produce a duplicate.

Process death releases attempts immediately. A socket reconnect by the same
living CLI/PID/CWD with an uninterrupted claim adopts unexpired live attempts,
transfers their claim generation and preserves route history. The adapter keeps
its observers; delivery is never resent to restore observation.

### Fetch and receipt groups

**Compatibility boundary:** until `fetch_receipt` is confirmed, fetch retains
this baseline's consume-on-return (`ack:true`) and nonmutating peek (`ack:false`)
semantics, response shape and multi-route selection. `lease` is refused on
presence only for negotiated `fetch:"consume"` peers, without mutation. Legacy
peers and receipt declarations without accepted `fetch_receipt` retain the
baseline unknown-field behavior: `lease` is ignored. The legacy baseline has no
`fetch_confirm` implementation. `fetch:"consume"` means **removed when returned,
not proven displayed**; it does not assert a host tool-result receipt.

For a confirmed `fetch_receipt` connection:

* `fetch_queue{ack:false}` peeks without reserving or backfilling durable ids.
* `fetch_queue{ack:true,lease:true}` reserves a receipt group: one table attempt
  per selected route, all using the group token returned as `lease_token`.
  Every attempt has a 60-second deadline from its reservation. This does not
  take the session's live admission slot. Pending voice placeholders may be fetched.
* `ack:true` without `lease:true` is refused without mutation. If host correlation
  or transcript observation is unavailable, the adapter refuses destructive fetch
  and suggests a peek; it never silently downgrades a receipt fetch to consume.
* `messages` keeps the frozen PascalCase inbound array. New `members` is a parallel
  array of `{record_id,revision}`, one pair per returned message, in message order.
  `record_id` is the durable queue identity; `revision` is an opaque lowercase
  64-character SHA-256 digest of that tracked stored row. Adapters echo it; they
  never compute it. `receipt_trailer` contains the broker-authored trailer below.
  Empty batches have no group, members or trailer.
* Rows in open attempts of any transport are invisible to fetch, live scheduling,
  Held and attach backlog. `remaining` counts fetchable rows after this batch.
  Attach text explicitly says how many messages are fetchable now.
* The drain snapshot excludes attempting rows and precedes any new reservation.
  Drain removal, eviction and oversize set-aside reconcile individual memberships;
  empty attempts close without proof or rearm. An oversized row stays durable
  until the replacement notice receives confirmation. Only surviving unchanged
  revisions retire. Voice enrichment invalidates its old membership immediately.

The rendered tool response ends with this **fixed trailer grammar**, with LF
separators, exactly one ASCII space between fields, no escaping, and no final
newline or other text after the closing delimiter:

```text
[C3_FETCH_RECEIPT_V1]
group <lease_token>
member <record_id> <revision>
member <record_id> <revision>
[/C3_FETCH_RECEIPT_V1]
```

The delimiters and `group`/`member` words are literal. Angle-bracket fields above
are placeholders. Tokens and record ids are nonempty `[A-Za-z0-9_-]+`; revisions
are `[0-9a-f]{64}`. There is one member line per expected row, at least one.
The opening delimiter begins a line. Bodies can render freely before it; the
adapter appends `receipt_trailer` verbatim as the final content. Completeness
is set-based: member order may vary, but duplicate ids, missing or unexpected
members, wrong revisions, wrong tokens, truncation and trailing text all fail.

Claude confirmation requires a complete `type:"user"` transcript record with a
successful `tool_result` for the exact host tool-call id supplied in MCP
`_meta["claudecode/toolUseId"]`. `is_error:true` or an invalid error value never
confirms; an omitted flag has the host's normal false default. That matching
block's string content (or text blocks joined with LF) must carry the complete
matching trailer. An assistant quote, a successful write or a result for another
call is not confirmation.

The shared observer sends `attempt_result{token:<group>,outcome:"confirmed"}`.
`fetch_confirm{lease_token:<group>}` is an alias during the protocol-v1 migration
window, with the same original-connection binding and multi-route meaning.
Removing the alias is deferred to protocol v2; this redesign does not remove it. Each
route worker independently checks its exact holder, claim generation, deadline
and open surviving membership under `withConfirmedHolder`. Partial route success
is preserved: a changed holder releases its members without consuming them.
Late confirmations are no-ops. Expiry makes surviving rows fetchable again;
duplicates are allowed. A successful nonempty group rearms once, without changing
live transport proof or confirmation age.

### Per-route display and notices

The broker derives each negotiated route's display: `waiting`,
`live: channel, confirmed <age>`, `live: inbox, confirmed <age>`, or
`pull-only (<reason>)`. Either eligible transport allows waiting before the first
confirmation on that route or while awaiting proof on an eligible replacement transport. Exhaustion says
`pull-only (no receipt on channel or inbox; retries on reconnect, attach or new messages after 60 s)`
and preserves the prior confirmation as history. Ineligible routes show pull-only
immediately. Restart resets proof; fetch never changes it. The existing
`render_state` sender carries broker-derived route updates to the adapter, with
optional `channel`, `chat_id`, `topic_id`, `confirmed_at` and `confirmed_transport`; the adapter does not
send policy updates back. `attached.delivery_route` carries the same display
snapshot. Claims/session status also includes optional `confirmed_at` and `confirmed_transport`
so inbox history stays accurate after exhaustion.
Held notices recount at send time and exclude attempting rows. Scheduling
precedes Held evaluation after enqueue, attempt termination, ownership or
capability changes, enrichment and drain import. Route changes use a
60-second stable-state timer comparing only state and reason,
never age or queued count. Returning to the last announced state cancels the line.
Held has a separate 10-second per-route cooldown; its one route line also records
that state as announced. One sender holds the pending reservation through send
completion, then checks the latest state. Legacy sessions retain their four-state
wording (`channel`, `cross-session`, `probing`, `queue-only`).

| Negotiated route state | Meaning and history |
|---|---|
| `live: channel, confirmed <age>` | This route has a native live receipt. |
| `live: inbox, confirmed <age>` | This route has an inbox receipt. |
| `waiting` | An eligible live transport awaits this route's first confirmation, or a live attempt is still open. Prior proof can appear as `was <transport>, confirmed <age>`. |
| `pull-only (<reason>)` | No eligible live transport or the cycle exhausted; prior proof stays as `was <transport>, confirmed <age>`. Never shown while a live attempt is open. |

For `receipts:accept`, replace `confirmed` with `accepted by <host>` and retain
the age; acceptance is not proof of display. Fetch does not establish live proof.
Claims include `holder_build` and optional `accepted_by`; route display updates
also carry optional `accepted_by`. Status and Held exclude exact surviving
members of open attempts/fetch groups and rows with observed receipt evidence,
including when retirement storage retries fail. Receipt identity/revision markers
survive attempt-history eviction and worker replacement for the broker lifetime;
retirement/removal or a changed content revision releases the marker. Restart
replay remains possible. A changed content revision is queued again. Tokens appear in operator logs, never in status or ordinary notices.

### Legacy protocol-v1 projection

A hello **without `delivery` runs the legacy path unchanged**, including its
boolean/render-state decoding and acknowledgement semantics. Missing broker
acceptance, malformed or unknown-version offers, missing adapter confirmation,
and degraded storage also retain legacy delivery. Never infer a negotiated
receipt milestone from `render_state`. The compatibility translator alone keeps
the old field-presence distinction for legacy receipt/recovery bookkeeping.

Legacy Claude states remain `capable`, `probing`, `cross_session`, `queue_only`.
Only in this compatibility path does the adapter own probe/fallback policy:
channel first, one owning-inbox retry with the same delivery token and a new
`cross-session:N` marker, then queue-only until attach/reconnect. The broker's
legacy probe admission and recovery ledger still have callers. No such adapter
policy runs after confirmed negotiation. Legacy fetch consumes on return; an
`ack:false` peek never reserves. The baseline ignores an unknown `lease` field
and has no unnegotiated `fetch_confirm` handler.

For a legacy `inbound`, preserve each message occurrence (including edits), render
it, and acknowledge only the host's supported milestone:

```json
{"op":"inbound_delivered","update_id":<inbound.MessageID>,"ok":true,
 "count":<covered>,"delivery_token":"<inbound.delivery_token>"}
```

Echo the token and covered count exactly; `count < 1` is a no-op. A nonempty token
binds the original pushed route and exact durable ids, not the current output
route. Empty-token compatibility requires a unique outstanding occurrence for
that holder/message id; ambiguity consumes nothing. Unknown, repeated and
unmatched tokens consume nothing. A confirmed claim is required for destructive
fetch and acknowledgements. The outstanding legacy push-routing map is bounded
to 64 pushes; separate recovery payload copies are bounded to 32.

Never acknowledge synthesized events (nonempty `Kind`, `covered:0`); they have
no durable rows. A transport failure withholds the ack (or reports `ok:false`).
Legacy receipt-confirming acknowledgements clear recovery copies after durable
retirement. Other legacy acknowledgements retain recovery copies until cleanup;
fetch/drain and holder changes prevent intentionally removed rows from returning.
The phase-1 divergence guard continues checking exact legacy retire sets.

### Claude host transports and versioned fixtures

Channel eligibility uses fail-closed nearest-host detection, the C3 channel flag,
host readiness and transcript readability. Linux reads `/proc`; macOS reads
`kern.procargs2` / `kern.proc.pid`. An outer host's flag never qualifies an inner
flagless host. Native channel notification uses string-valued metadata; inbox
sends auth and peer-user JSON frames, then half-closes and requires clean EOF.
The peer user content is the same complete C3 channel block. Both outbound frames
are capped at 4 MiB before authentication. A clean write/EOF alone never confirms.
Inbox does not provide Telegram permission relay or native question answering;
peer text cannot authorize permissions. C3's own `ask` and `reply` remain usable.

The adapter delivers only to its owning session's inbox. It captures its own
`CLAUDE_CODE_MESSAGING_SOCKET` and `CLAUDE_CODE_MESSAGING_TOKEN` at startup,
never discovers other sessions, and never reads their environments. The endpoint
must resolve to `<runtime>/cc-socks/<hostpid>.sock`: `hostpid` is the nearest
Claude host in the adapter's direct ancestor chain, using the existing argv and
parent readers. With no identified Claude ancestor, only the immediate parent
is eligible. Unreadable, cyclic, or truncated ancestry fails closed.

The runtime directory must be user-owned and mode 0700; the endpoint must be a
user-owned Unix socket, with no symlink escape. Path checks run at startup,
re-probe, and each send. Before writing **any authentication bytes**, C3 checks
the connected descriptor with `fstat` and verifies the kernel-reported peer PID
and UID: Linux uses `SO_PEERCRED`; macOS uses `LOCAL_PEERPID` plus
`LOCAL_PEERCRED`. The peer PID must equal `hostpid`, and its UID must equal C3's
own UID. `fstat` checks the connected endpoint's type/owner; on Linux its inode
is not the listener's pathname inode. Peer credentials bind the connection even
if the pathname was substituted. Platforms without peer PID verification fail
closed; Windows remains queue-only. The user frame includes `session_id` when
C3 knows this session's stable UUID from the SessionStart handoff or registered
identity, allowing the host to reject a session mismatch.

Missing credentials, uncertain ownership, and unsafe paths yield generic errors;
credentials and raw socket errors are never logged. Re-probe revalidates the
captured path; changed environment credentials require restarting the adapter.

The inbox protocol, flagless user-turn delivery, and peer transcript record shape
are **VERIFIED on Claude Code 2.1.263**. The sanitized captured record is
`cmd/c3-claude-adapter/testdata/claude-2.1.263-peer.jsonl`. Its provenance is
`type:user`, `message.role:user`, `isMeta:true`, and an `origin` object with
`kind:peer`, `from:c3`, `verifiedPeerPid` (integer), and `verifiedPeerProcStart`
(string). It also has `promptSource:system` and `userType:external`. The PID,
process-start, prompt-source, and user-type fields are observed metadata, not
receipt requirements. Format drift or host refusal retains the durable row;
it never licenses a blind ack.

Channel intake during a tool call is **VERIFIED on Claude Code 2.1.266** in
`cmd/c3-claude-adapter/testdata/claude-2.1.266-intake.jsonl`: the host immediately
writes `type:queue-operation`, `operation:enqueue`, with a string `content`, then
writes `type:attachment`, `attachment.type:queued_command`, with a string
`attachment.prompt` and `attachment.origin.kind:channel` when the tool finishes.
Both designated fields use the same strict channel-tag receipt parser; enqueue
confirms host acceptance within the existing window, and `operation:remove`
never confirms. Peer intake is also **VERIFIED on Claude Code 2.1.266** in
`cmd/c3-claude-adapter/testdata/claude-2.1.266-peer-intake.jsonl`: both designated
fields contain bare channel blocks, and the attachment has nested
`isMeta:true`, `origin.kind:peer`, `origin.from:c3`. These shapes use the strict
source/token/attempt and closing-tag checks described above; the previously
inferred prefixed intake variants are rejected.

### Complete-record observation

Live, inbox and fetch receipts use the visitor reader `scanTranscriptRecords`
in `transcript_reader.go`, also used by permission readback (D020).
`receiptObservation.scan` wraps the unchanged live predicate and records diagnostic
progress; it is not a second scanner. It reads only complete
JSONL records after the pre-delivery offset, with 32 MiB snapshot budgets,
16 MiB line limits and persistent oversized-record discard across polls;
truncation resets the offset/discard state. Live observer admission is bounded
to 64. Cancellation generations and token-to-observer bookkeeping remain in the
adapter; delivery policy remains in the broker.

Live matching requires the exact `c3_delivery_id` and `c3_attempt` opening-tag
attributes. Duplicate attributes, malformed framing and wrong markers fail
closed. Channel user turns may use a self-closing tag; intake and inbox shapes
require `</channel>`. Inbox user turns additionally require the exact verified
host prefix and peer provenance; bare 2.1.266 intake has the distinct predicates
above. The counter is adapter-lifetime: `channel:N` or `inbox:N` on negotiated
attempts, `cross-session:N` on legacy fallback. An old broker's empty ack token
stays empty; a fresh local marker provides readback correlation only.

On a session's first confirmed delivery, the adapter logs the confirming record
type and the host version from MCP `initialize.clientInfo.version` (or `unknown`).
Three consecutive live attempts expiring with no recognized matching receipt
while the transcript grew trigger a one-time shape-drift hint. A failed write,
no growth or a recognized live receipt breaks that streak; fetch expiry does not
participate. The alarm changes no deadline, receipt predicate or row state. The
optional `receipt_shape_drift` host-version field travels in hello and diagnostic
reports: `delivery_report` only after confirmed negotiation, otherwise the existing
legacy `render_state` with no state (ignored by older brokers). The diagnostic
never resets legacy probe admission. Separate diagnostic reports let older strict
parsers ignore the field without losing eligibility changes. `list_sessions_reply`
includes it so `c3-broker status` shows the collection hint. Self-exec and socket
reconnect retain session diagnostics; a new stable session resets them.

Host shapes are versioned fixtures, not guesses. Adding a shape requires a
captured fixture and a predicate test without weakening framing. Readback proves
intake, never completion of the requested work. See the release acceptance
checklist in [TESTING-LIVE-MATRIX.md](TESTING-LIVE-MATRIX.md#acceptance-for-a-release).

---

## The adapter owns its MCP surface

`serverInfo`, `instructions`, and the tool list are **yours**. The broker does not supply them and has no op that does.

- **`serverInfo.name` MUST equal the key your CLI registers the server under** — the key in `.mcp.json` (or `mcp_servers.<key>`, or your host's equivalent) — **not your binary name.** Every reference MCP implementation does this, and using the binary name is a real, previously-shipped bug: the broker delivered, the notification frame went out correctly, and the host never injected it as a channel event. Delivery looks fine end to end and nothing renders. Budget a day if you get this wrong and don't know to look here.
- **`instructions`** is where you fold the channel `capabilities` from `hello_ack` into agent-facing guidance (rich text? message-length limit? polls? reactions? media kinds?). With `capabilities` null, render honest all-NO guidance — never fabricate a capability.
- **The tool list is adapter-owned**, and it is the natural place for host-specific wording. The reference Claude Code adapter registers **12** tools: `attach`, `detach`, `topics`, `reply`, `react`, `edit_message`, `poll`, `ask`, `stop_poll`, `download_attachment`, `fetch_queue`, `retranscribe`. Note `send_typing` is deliberately **not** an agent tool — the typing indicator is relayed programmatically by the broker's route worker, never by a model tool call, though the broker still dispatches the op for in-flight callers.

---

## Adapter-local tools vs forwarded tools

Half the reference tool set never touches `tool_call`. Get this wrong and you get `unknown tool` errors from the broker.

| tool | how it reaches the broker |
|---|---|
| `reply`, `react`, `edit_message`, `poll`, `stop_poll`, `download_attachment` | forwarded via **`tool_call`** |
| `attach` | direct **`attach`** op |
| `detach` | direct **`release`** op |
| `topics` | direct **`list_topics`** op |
| `ask` | direct **`ask_register`** op |
| `fetch_queue` | direct **`fetch_queue`** op |
| `retranscribe` | direct **`retranscribe`** op |

The dedicated-op tools are adapter-local because the *wording* of what the user sees differs per CLI — Claude Code natively renders `<channel>` blocks, Codex sees `notifications/message` log entries — and because several of them (attach proposals, the ask round-trip) need adapter-side state.

Tool names are **unprefixed** across all adapters: the MCP server name provides the namespace, so per-tool prefixing is redundant.

---

## Translating inbound messages

The broker emits a normalised message; converting it to what the host can ingest is **real work, not a pass-through**. The reference Claude Code implementation is roughly 200 lines: it string-coerces the metadata map, branches to a separate event-frame builder for poll/reaction/callback kinds, formats timestamps, and decorates the content with the pending-backlog nudge. Budget accordingly.

**Claude Code** uses `notifications/claude/channel` with string-valued `meta` attributes that render as `<channel source="…" chat_id="…" message_id="…" user="…" reply_to_message_id="…" reply_to_text="…">`. Attachments use the unsuffixed `attachment_kind`, `attachment_file_id`, `attachment_size`, `attachment_mime`, and `attachment_name` keys for the first item; multiple attachments add `attachment_count` and repeat those keys with `_2`, `_3`, and so on. A merged delivery additionally carries `merged_count`, comma-joined `merged_message_ids`, and `attachment_message_id` / `attachment_message_id_N` pairing keys. A discrete forwarded message carries `forwarded_from`.

**Codex** uses `thread/queue/add` through the session's app-server when the
launcher enables forwarding. Busy-session input is queued; C3 never automatically
steers or interrupts. An unsupported queue method yields pull-only delivery and a
`fetch_queue` notice, with no legacy `turn/start` resubmission. Native CLI queue
delivery is an explicit opt-in alternative. Queue acceptance permits an exact
broker-token ack; it is not a conversation receipt. A one-time MCP log notification announces the switch to pull-only; the broker
then holds new messages and coalesces Held notices; held messages are recovered
with `fetch_queue`. Codex reports `render_state: "queue_only"` with a generic
`render_reason` in hello and through the existing `render_state` update op when
the queue is unsupported, a destructive fetch outcome is uncertain, or the
conversation identity is unpinned. A successful pin or queue acceptance after
restart reports `capable` again.

**Grok Build** has no channel-notification dialect. Live inject **requires leader mode** (`[cli] use_leader = true`). The Grok adapter registers as a client on the leader socket and issues ACP `session/prompt` against the TUI session id (see [`GROK-INJECT.md`](GROK-INJECT.md)). Without a leader socket, inbound stays in the durable queue for `fetch_queue`.

**Claude Desktop** has no way for an MCP server to push into a chat at all, so `c3-desktop-adapter` is **pull-only**: inbound never surfaces on its own — it stays in the durable queue and the user drains it by asking Claude to call `fetch_queue`. See [`DESKTOP.md`](DESKTOP.md). **Antigravity** (`c3-agy-adapter`) and **Cursor Agent CLI** (`c3-cursor-adapter`) are pull-only for the same class of reason: the host has no channel push that starts a turn (and Cursor additionally has no idle-wake API into the stock interactive TUI).

**Headless runs.** Cursor loads user MCP servers during non-interactive review and tool runs too. The Cursor adapter therefore refuses explicit attach and session-recovery auto-attach when a `cursor-agent` or `cursor` ancestor has a headless output flag, preventing a background run from claiming an operator's topic. Set `C3_ALLOW_HEADLESS_ATTACH=1` in the adapter environment only when that claim was explicitly intended; read-only tools and `detach` remain available without it.

**dcode** (`c3-dcode-adapter`) has a real out-of-band push: when the TUI is launched with `DEEPAGENTS_CODE_EXTERNAL_EVENT_SOCKET=1`, it listens on `<runtime>/deepagents/events-<tui-pid>.sock` and accepts newline-JSON events; a `{"kind":"prompt"}` event enters the conversation as literal user text (mode "normal" — never parsed as a slash/shell command), and the `{"ok":true}` reply line is the landing confirmation. The adapter binds the socket by walking `/proc` ancestors (dcode spawns MCP servers with a sanitized env, so `XDG_RUNTIME_DIR` is not inherited) and acks the broker only after that confirmation, with `count=covered` and the `delivery_token` echoed. Without the flag the adapter reports `cannot_render_channels: true` and falls back to `fetch_queue`. `recover_session` is skipped: dcode exposes no stable session id to MCP children, and the rule is fail-closed rather than guess.

Held messages live in the broker's durable per-route queue. Negotiated scheduling offers eligible backlog automatically; the agent can also pull fetchable rows with `fetch_queue`.

For a new CLI, look at what unsolicited-notification capability it has. If none: set `cannot_render_channels: true` in `hello`, and make `fetch_queue` your delivery path. If there is a push API (websocket, HTTP, IPC), use it; offer eligible transports and let the broker select attempts after negotiation.

---

## Codex adapter specifics

Codex's `--remote` mode forces the bridge to span four processes:

```
codex (launcher binary)
  -> Codex app-server   (background process, holds MCP servers)
  -> c3-codex-adapter   (spawned by app-server as MCP stdio server)
  -> Codex TUI          (spawned by launcher with --remote)
```

The visible TUI talks to the app-server over WebSocket; the app-server runs MCP servers; one of those is the C3 adapter; the adapter talks to the broker. **The app-server, not the TUI, owns MCP server startup.** This is why the launcher injects MCP config args into the **app-server's** invocation — the same flags get duplicated into the TUI invocation, but the app-server-side copy is the load-bearing one.

If you write a new adapter, check how your CLI handles MCP servers under any equivalent "remote/embedded" mode before assuming the TUI is where MCP lives. Get this wrong and you'll have an adapter that runs in the foreground but has none of the environment it needs.

Recovery pins the thread once. If startup resolution fails, explicit attach is
allowed with a pull-only notice; live delivery refuses to rediscover a recipient.
A restart with `C3_CODEX_THREAD_ID`, or successful resolution on broker reconnect,
can establish the pin. Per-route origin tags are captured when input is enqueued.

Detach forgets released routes in both the visible route set and reconnect
replay. A targeted detach preserves the remaining routes and output and updates
the terminal title. Late recovery and replay responses cannot restore the
released snapshot. Buffered delivery
attempts for released routes are invalidated, and late inbound frames for those
routes are ignored. A submission already sent to Codex cannot be recalled, but
its obsolete completion cannot acknowledge or emit a failure notice. Releasing
the last route rearms the shared recovery notice. The conversation pin,
unsupported-queue detection, and unknown-fetch safety pause remain session-wide;
changing topics does not reset them.

Destructive fetch and in-flight submission are serialized. Inbound IPC includes
`record_ids`, the broker's durable source identities; destructive fetch response
messages carry `ConsumedRecordID`. The worker includes this field before frame
sizing and supplies it only for consumed records. Multi-route dispatch preserves
each route's identities under its confirmed-holder gate. Matching buffered
submissions are excluded regardless of frame order; a partially consumed merged
batch leaves its surviving sources held for pull. Events retain their complete
payload and carry no durable acknowledgement. A later successful token can be
acked even after an earlier delivery failed.

The explicit `c3-codex` alias targets a separate C3-owned executable. Re-run
`install-codex-shim` after a C3 update to refresh it. See
[`CODEX-NATIVE-QUEUE.md`](CODEX-NATIVE-QUEUE.md) for setup, unsupported-server
behavior, uncertain-response duplicates, and recovery limits.

## Distribution

A built-in adapter binary lives at `cmd/<cli>-adapter/main.go` and is installed alongside the broker. The Claude Code plugin's `.mcp.json` points at `c3-claude-adapter` by name; Codex's `mcp_servers.c3_codex.command` points at `c3-codex-adapter`.

If your target CLI has a plugin marketplace, ship the adapter as a thin manifest referencing the binary. If it doesn't, document the manual MCP server registration steps in your adapter's `SETUP.md`.

**Budget for real work.** The built-in adapters are, whole-package and excluding tests: Claude Code ~3.0k LOC, Codex ~2.4k, Grok Build ~3.1k, Claude Desktop ~2.4k, Antigravity ~1.4k, Cursor ~1.7k, dcode ~1.6k — each reimplementing the handshake, attach, tool forwarding, reconnect, delivery acknowledgement, and host-specific inbound translation. This is the hardest of C3's three extension seams.

## Adding a new adapter — checklist

- [ ] Socket path resolved with all three probes (`XDG_RUNTIME_DIR` **with existence check** → `/run/user/$UID` → `/tmp/c3-$UID/`), plus the Windows branch if you target it
- [ ] Newline-JSON framing with the 4 MiB cap, `\r\n` tolerance, trailing-frame-on-EOF, and serialised writes
- [ ] A dedicated demux reader task; correlation by `id` where it exists, by op elsewhere (one in flight)
- [ ] **A `default` arm that logs unknown ops and continues** — never fatal, never silent
- [ ] Protocol mismatch keeps the connection live but handles state-change refusals: `attach`, `release`, `fetch_queue(ack=true)`, `inbound_delivered`, and `recover_session`; safe peeks remain usable
- [ ] Per-type JSON casing: `snake_case` envelope, **PascalCase** nested payloads
- [ ] `hello` first on every connection, with `cannot_render_channels` set correctly for your host
- [ ] `serverInfo.name` equals the MCP server key your CLI registers, **not** the binary name
- [ ] Adapter-owned `serverInfo` / `instructions` / tool list; `instructions` folds in `hello_ack.capabilities`
- [ ] `attach` and `topics` implemented adapter-locally with the right user-facing wording; `detach`/`ask`/`fetch_queue`/`retranscribe` on their dedicated ops, not `tool_call`
- [ ] Inbound translation matches what the host renders
- [ ] **Legacy `inbound_delivered` sent after the supported receipt milestone** — echo `delivery_token` unchanged when present, `update_id` = `MessageID`, `count` = `covered`, never for a non-empty `Kind`, never on render failure; preserve same-`MessageID` occurrences in delivery order
- [ ] If offering `delivery`, wait for broker acceptance, confirm the supported set, then execute only broker-selected transports and report `attempt_result`
- [ ] Reconnect with backoff → `hello` → replay last attach with `replay: true` → re-fire `recover_session`; wake pending calls with an error
- [ ] `recover_session` sent once per **connection**, post-hello, with the **stable** session id — then re-sent after every reconnect, or resume deliberately skipped
- [ ] `ask_result` and `permission_verdict` handled if you expose `ask` / permission relay; `permission_settled` sent when that host exposes a reliable local-resolution signal
- [ ] Marketplace manifest authored if the CLI has one; `SETUP.md` if it doesn't
- [ ] Tests (see below)

## Testing

**There is no mock broker.** A previous version of this document promised a `broker.MockServer(t)` helper; it has never existed. Do not look for it.

What actually works:

The in-process broker fixtures inject installed-adapter lookup before serving
connections and pin PATH to OS utilities. This avoids reading a machine-installed
adapter or introducing unrelated upgrade hints/notices. Test-owned sockets in
private temporary directories are allowed; ambient host endpoints are not.
Corpus sidecars verify receipt predicates directly. Poll offsets, truncation and
oversize discard are verified separately by the shared-reader regression tests.

- **Hand-roll a fake broker.** Listen on a temporary unix socket, accept one connection, assert on the `hello` frame, and write scripted responses. That is the whole harness — the protocol is newline-JSON and the handshake is one round trip. Drive your adapter through handshake → attach → tool call → inbound → ack → reconnect against it.
- **Mock the host-CLI side** with a stdin/stdout pipe pair and assert on the JSON-RPC traffic. The Claude and Codex adapters both have tests demonstrating this pattern.
- **Test against a real broker** for anything involving the durable queue. Queue consumption, `covered`/`pending` arithmetic, and the never-ack backlog behaviour are the things a fake broker will not catch, and they are exactly the things that corrupt user data when wrong.
- **Write at least one asymmetric wire test.** A marshal-then-unmarshal round trip is invariant under a key rename by construction and will bless the exact change that breaks compatibility. Assert against key names written as literals, and against a byte-for-byte captured frame. This repo does that for its payload types and it is the only kind of test that can fail on a rename.

## Seamless Claude adapter upgrade (Provisional)

`hello.build` and `hello_ack.build` are optional strings identifying the running
executables, separate from release versions and IPC protocol versions. Builds
inject `git describe --always --dirty` through ldflags, falling back to `dev`.
The broker resolves `c3-claude-adapter` through PATH, exactly as the plugin command
does, and reads Go build settings from that file. It never infers identity from
mtime or executes the candidate for inspection. An unreadable identity yields
no hint. A differing compatible binary yields
`hello_ack.upgrade = {"path":"…","build":"…"}`.

Claude additionally sends `resume_contract` (the pinned MCP contract hash) and
`upgrade_disabled` (unsupported platform or exhausted/failed upgrade). These
additive fields are Provisional. A changed contract requires reconnect because
the host retains capabilities, instructions, and tools. Changing semantics or
cached instructions also requires a new contract epoch even with unchanged
schemas. The SDK tools/list contract test pins names, schemas, and descriptions.

On a hint the adapter reports unavailable delivery facts (legacy queue-only),
waits for complete request responses and permission relays, legacy ack/expiry,
and negotiated live attempt completion/expiry. Fetch reservations release on
reconnect, retaining their durable rows. It also tracks consumed stdin bytes
and stdout writes. After draining it self-execs with `--mcp-resume`, preserving
PID and fds. Private environment state transfers the original SDK initialization
parameters and local attachment identity; it is removed from the new process's
environment immediately. `ServerSessionOptions.State` restores the SDK session.
No instructions are resent and no startup watchdog runs in resume mode. Normal
readiness/re-hello and same-PID/CWD claims transfer remain authoritative.

Fallback uses the existing broker system event, with the exact text:
`C3 was updated to <build>. This session still runs the previous adapter: run /mcp and reconnect c3 (or restart the session) to switch.`
The adapter preamble requires verbatim relay. Deduplication is by logical
connection identity and target build, persisted in the broker state directory
as `upgrade-notices.json` so ordinary broker restarts do not repeat it. Pre-feature adapters always receive it, using the broker build when the installed
identity is unreadable (including the `dev` fallback for uninjected builds).

The preceding broker bounce still follows the reconnect cancellation contract
above; lossless broker-side in-flight work across shutdown is not implemented.
Concurrent installers replacing the checked path before exec are also outside
this provisional handoff's atomicity boundary.

Retry reconnects close push and MCP admission under the same locks as the socket
close. Host input remains unread until broker recovery finishes, then resumes on
the existing MCP connection. In resume mode, a repeated
`notifications/initialized` is accepted locally without reinitializing the SDK.
If installed-binary inspection fails, disabled and pre-feature adapters still
receive the fallback event using the broker build; inspection failure never
produces an exec hint.

## Broker restart and interactive requests

C3-controlled restarts (`c3-broker restart`, update/upgrade bounces, and
configuration bounces) pause new questions, permission relays and live pushes.
The running broker keeps IPC and channels available for up to 60 seconds so
already-open prompts can be answered. Ordinary inbound remains queued under its
existing storage contract, and existing delivery receipts still settle.
At the cap, C3 removes unanswered prompts from its local registries, clears their
keyboards and attempts a plain cancellation notice on each original route.
Questions receive the existing `ask_result.err`; permission relays send no
fabricated Allow/Deny verdict. C3 cannot cancel the host's underlying permission
request: it may still be waiting at the laptop, where it must be cancelled before
asking again.

Cancellation notifications share a five-second budget, with at most sixteen
workers. An edit failure gets one plain reply attempt on the original route.
Delivery can fail or wedge; local cancellation remains authoritative even if no
notice arrives. There is no persistence, resumption or later retry. An already
issued answer/verdict is not labelled cancelled; a successful socket write does
not prove host acceptance. The controlled shutdown watchdog forces exit after
90 seconds from the first restart intent, armed by the callback even if channel
startup is blocked; duplicate intent does not extend it.
The administrative client allows two seconds for its entire exchange and waits
up to 91 seconds from initiation for the old process to exit, deliberately
outliving the broker’s 90-second watchdog. A failed exchange
or exit timeout prevents it from starting a successor. Configuration restart
failures also fail `setup finish`; it does not print “Setup complete”.

An older running broker that does not support controlled restart is left running
and the command fails visibly. Manual restart is needed to activate the installed
build in that case, and pending prompts have no new protection during that manual
restart. SIGTERM/SIGINT preempt an ongoing controlled drain without cancelling
its pending prompts, then take immediate IPC teardown and the 15-second watchdog;
reboot, crash, OOM and external process termination receive no new prompt
protection. SIGHUP remains a configuration reload.

Ordinary broker calls can still be interrupted. Durable inbound, attachment
recovery, adapter self-exec and `upgrade-notices.json` retain their existing
contracts. Ordinary stale-button and native-ID-reuse gaps also remain unchanged.

The administrative exchange is `{"op":"broker_restart"}` followed by
`{"op":"broker_restart_reply","ok":true}` (or `ok:false` and `err`). Only a
compatible `c3-broker-cli` connection may request it. No interactive wire field,
capability negotiation or acknowledgement was added. Existing tokenless
`ask_result.err` carries `C3 is restarting; this request was cancelled — ask again.`
