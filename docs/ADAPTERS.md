# Writing C3 CLI Adapters

A C3 adapter is the bridge between the broker and a specific CLI's MCP-server expectations. The built-in adapters are `c3-claude-adapter` (Claude Code), `c3-codex-adapter` (Codex), `c3-grok-adapter` (Grok Build), `c3-desktop-adapter` (Claude Desktop, poll-only — see [`DESKTOP.md`](DESKTOP.md)), and `c3-agy-adapter` (Antigravity, poll-only). If you want to integrate C3 with a CLI we don't yet support — Cursor, Aider, plain shell, your own thing — write an adapter.

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

The Frozen core is deliberately small: it is exactly the set an adapter cannot be correct without — handshake, ownership, message delivery with its acknowledgement, tool forwarding, durable-queue recovery, and errors.

### Mandatory rule: unknown ops must be logged and skipped, never fatal

> **Your frame reader MUST have a `default` arm that logs the unknown op and continues.**
> Do not error out, do not drop the connection, and do not silently discard the frame with no record.

The broker can be **newer than your adapter**. Updating C3 replaces the binaries and restarts the broker, while adapter processes belonging to already-running CLI sessions reconnect to the new broker with their old code. Mixed versions on one host are a normal, expected state — not an anomaly. Adding a new op is an *additive* change by contract, so an old adapter meeting a new broker will see ops it has never heard of.

A strict parser is the natural implementation here and it is **wrong**. A tagged-enum deserializer that rejects unknown variants (e.g. Rust `serde` with `#[serde(tag = "op")]` and no catch-all) turns a routine version skew into a dropped connection and a dead inbound path. A silently-ignoring reader is worse still: permission taps and answers vanish with no log line. Log it, skip it, keep reading. Every built-in adapter does exactly this.

### Protocol version

`hello` and `hello_ack` both carry an optional `protocol_version` integer. The current version is **1**. An absent or zero value means version 1 — the first C3 releases shipped no version field at all.

**C3 never refuses a connection over a version mismatch.** Both sides log a warning naming the disagreement and carry on. Your adapter should do the same: compare, log if different, proceed.

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
3. `/tmp/c3-$UID/` — last resort; create it with mode `0700`.

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
- `ask_register`/`ask_registered`/`ask_result` are matched on `ask_id`; `permission_request`/`permission_verdict` on `request_id`.
- **`attach`/`attached`, `list_topics`/`topics_list`, and every op in the CLI-client section carry no correlation id** and are matched **by op alone**. Keep at most one of each in flight per connection. (An optional `id` may be added additively in a future version; until then, serialise them.)
- `release`, `inbound_delivered`, `permission_request`, and `bye` get **no reply on any path**. Do not await one — you will deadlock.

On a broker drop, wake every pending request with an error so the host CLI's tool calls don't hang.

### JSON key casing — the silent-corruption trap

**C3 uses two different casings on the same wire, on purpose.**

- **IPC envelope fields** (everything defined in this document's op tables — `op`, `conn_id`, `chat_id`, `update_id`, `ask_id`, `stable_session_id`, …) are **`snake_case`**.
- **Nested payload objects** — the `inbound` value, the `messages` array, `capabilities` — are **`PascalCase`, byte-identical to the Go field names**: `Channel`, `ChatID`, `TopicID`, `MessageID`, `Sender`, `Text`, `Attachments`, `ReplyTo`, `Timestamp`, `Kind`, `Event`, `RichText`, `MaxMessageRunes`, and so on.

This is frozen deliberately. Those Go field names *were* the on-disk queue format and the IPC wire format before explicit tags existed; the tags now pin the keys so the Go identifiers can change without moving the format. There is a one-directional golden test in the tree whose literals are the contract. **The keys will not be "tidied" to snake_case** — doing so would orphan every queued message on every user's disk.

The failure this causes is silent and total: a blanket `rename_all = "snake_case"` (or equivalent) across your whole deserializer will parse an `inbound` frame into an **all-zero-value** message with no error. No exception, no log line, no clue. Scope your casing rules per type.

The broker never rejects unknown JSON fields, and neither should you.

---

## Op reference

38 ops exist. This section documents all of them: **15 Frozen**, **11 Provisional**, **10 belonging to the bundled CLI rather than to adapters**, and **2 that are not implemented and must not be sent**.

Field names below are the literal JSON keys. `?` marks an optional field (omitted when empty/zero).

### Frozen core — 15 ops

An adapter that implements only these is correct and complete for a CLI with no interactive-question, permission-relay, voice, or session-resume needs.

#### `hello` → `hello_ack` — the handshake

**`hello`** (adapter → broker) **MUST be the first frame on every connection**, including after a reconnect. Any other first frame gets `{"op":"error","err":"expected hello first"}` and the connection is closed. A malformed hello gets `{"op":"error","err":"malformed hello"}` and the same.

```json
{"op":"hello","cli":"rust","pid":12345,"cwd":"/absolute/path",
 "capabilities":["..."],"cannot_render_channels":false,"protocol_version":1}
```

| field | type | notes |
|---|---|---|
| `cli` | string | your adapter's CLI name. Appears in claim listings and logs. Avoid `c3-broker-cli` — that name is reserved for the bundled status client and is filtered out of session listings. |
| `pid` | int | your adapter's pid. The broker keeps a claim alive as long as this pid lives, so it must be a real, live process id. |
| `cwd` | string | resolved-absolute path. Seeds the attach picker's "current project" suggestion and the cwd→mapping lookup. |
| `capabilities`? | []string | free-form tags. **Currently recorded on the wire but not read by the broker** — informational only. |
| `cannot_render_channels`? | bool | **inverted sense, and load-bearing — read the next paragraph.** |
| `protocol_version`? | int | absent ⇒ 1. |

**`cannot_render_channels` is the one field a naive adapter gets wrong with data-loss consequences.** Absent or `false` means *"my host can render unsolicited channel pushes."* Set it to `true` only when you are confident your host **cannot** display a push. When true, the broker never marks that session's inbound as delivered: durable human messages fall through to the queue plus a held-notice (recoverable via `fetch_queue`, which is a tool *result* and therefore always renders), while the session keeps its claim for outbound.

If your CLI has no unsolicited-notification path at all — the exact case this document tells you to expect — and you leave this field absent, the broker reads your host as renderable, pushes to it, acks, and the user loses every message. The two poll-only built-ins (`c3-desktop-adapter`, `c3-agy-adapter`) set it `true` unconditionally.

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
{"op":"inbound","inbound":{ /* PascalCase message object */ },"pending":2,"covered":3}
```

| field | type | notes |
|---|---|---|
| `inbound` | object | the normalised message. **PascalCase keys** — see the payload shape below. |
| `pending`? | int | messages **still queued after** the lines this push covered, i.e. backlog this push did *not* deliver. Surface it (e.g. "(N pending — call fetch_queue)") so a stuck item is visible on this push, not only at the next re-attach. |
| `covered`? | int | how many durable queue lines this (possibly **merged**) push covers. A debounced batch of N stored lines arrives as **one** notification with `covered: N`. Defaults to 1 when absent. **You must echo this back.** |

The `inbound` object's fields (PascalCase, exactly as written):

`Channel` (string), `ChatID` (int64), `TopicID` (int64\|null — null = DM/no topic, `1` = the reserved root topic, `>1` = a custom topic), `MessageID` (int64), `Sender` (`{UserID, Username}`), `Text` (string), `Attachments` (array of `{Kind, FileID, Size, MIME, Name}` — `Kind` is one of `voice`, `audio`, `video`, `video_note`, `document`, `photo`, `sticker`), `ReplyTo` (`{MessageID, User, Text}`\|null), `Timestamp` (RFC3339), and:

- `Kind`? — **empty string means an ordinary message.** A non-empty value marks a *synthesized channel event*: `poll_result`, `reaction`, `callback`, or `system` (a broker-originated advisory such as a channel-health alert, carrying no user content).
- `Event`? — the event payload when `Kind` is non-empty. Exactly one of `PollResult`, `Reaction`, `Callback`, `System` is set.
- `DrainedFrom`? — provenance when the line was moved in by a drain; empty for organic messages.
- `V`? — record-format version. **Absent or 0 means version 1.** **Readers MUST NOT reject a higher value** — a newer writer sharing a socket or queue directory with an older reader is a normal partially-updated install, and hard-failing turns cosmetic skew into lost messages. Best-effort decode.
- `ConvKind`? — `"dm"` or `"group"` as stated by the channel. Empty means the channel didn't say.

`ChatID` sign convention follows Telegram's: positive = user/DM, negative = group, `-100…` = supergroup.

#### `inbound_delivered` — the acknowledgement you must not skip

```json
{"op":"inbound_delivered","update_id":<Inbound.MessageID>,"ok":true,"count":<covered>}
```

**No response.** See [The inbound delivery contract](#the-inbound-delivery-contract) below — this is the single highest-consequence thing in the document and it gets its own section.

#### `fetch_queue` → `fetch_queue_result`

The durable-queue drain. Every adapter exposes this as a tool; for a CLI with no push path it is the *only* way the user sees inbound.

```json
{"op":"fetch_queue","id":"<unique>","limit":3,"all":false,"ack":true}
{"op":"fetch_queue_result","id":"<same>","messages":[ /* Inbound objects */ ],"remaining":7}
```

| field | type | notes |
|---|---|---|
| `limit`? | int | oldest-first batch cap. The built-ins default to 3 and cap at 50. |
| `all`? | bool | overrides `limit`; drains everything. |
| `ack` | bool | `true` **consumes** (advances the cursor, deletes files when drained); `false` **peeks**. Not optional — send it explicitly. |
| `messages`? | array | full content, oldest first. |
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

### Provisional — 11 ops

Implemented and shipping, but the shapes are not frozen for v0.1.0. Each entry names why.

#### `recover_session` → `recover_session_result` — session resume

*Provisional: the session-identity model is under active review.*

**This is the real re-attach path.** There is no auto-attach at hello. If you skip this op, every restart of the user's CLI looks like a brand-new session to the broker and the user loses their topic binding.

Semantics that are not obvious:

- It is sent **after** `hello`, on the existing connection — not during the handshake. The stable session id is delivered by a host `SessionStart` hook that fires roughly two seconds after the adapter spawns.
- The id you send is the **stable, resumable transcript id**, which on Claude Code is **not** the value of the `CLAUDE_CODE_SESSION_ID` environment variable. That variable is an *ephemeral per-MCP-spawn* id; the built-in adapter uses it only to locate its own hook handoff file, which carries the real stable id. If your host has no equivalent, skip recovery — fail closed rather than guess.
- Send it **exactly once per session**, guarded against races between your hook watcher and any first-activity recheck.

```json
{"op":"recover_session","stable_session_id":"<stable id>","cwd":"/absolute/path"}
{"op":"recover_session_result","recovered":true,"channel":"telegram","chat_id":-100…,
 "topic_id":42,"name":"…","group":"…","queued_count":3,"queued_summary":[…]}
```

The broker takes one of two branches, silently:
- **Stub already attached** → it *records* the current route under the stable id so a future resume can recover it. No re-claim. `recovered` stays `false`.
- **Stub not attached** → it attempts to re-claim the route that stable id was last attached to. `recovered: false` is also returned for an expired/tombstoned attachment or a route now held by another live session. A `false` is not an error and should be silent.

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

*Provisional: this is a trust boundary under active hardening — verdict-to-prompt binding in particular.*

Relays a host tool-use permission prompt to the operator as an Allow/Deny keyboard.

```json
{"op":"permission_request","request_id":"<host-minted id>","tool_name":"Bash","preview":"…"}
{"op":"permission_verdict","request_id":"<same>","behavior":"allow"}
```

Carries **no route** — derived from your current claim. `preview` must be a short, **already-truncated** input snippet, never a secret body.

**`permission_request` is fire-and-forget: the broker sends no reply on any path.** A nil route, a channel lookup failure, a channel without inline keyboards, and a send failure are all logged broker-side and dropped. If you await an ack here you will deadlock.

`permission_verdict` is an **unsolicited push**; `behavior` is the string `"allow"` or `"deny"`. If you do not handle it, the operator taps Allow on their phone and their CLI waits forever with no indication why. This is the most user-visible consequence of an incomplete op switch.

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
| `list_health` | `health_list` | last cached fetch-health per channel: `{channel, state:"up"\|"down", since_unix?, consec?, reason?, down_for_sec?}`. |
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

While running:

- **`initialize`** → respond with your own `serverInfo`, your `capabilities`, your assembled `instructions`, and the right `protocolVersion`.
- **`tools/list`** → return your tool list.
- **`tools/call`** → either forward via `tool_call` and await `tool_result` on the correlation id, or run it inline against a dedicated op (see the table below).
- **`inbound` push** → render, **then acknowledge**. See the next section.
- **`ask_result` / `permission_verdict` pushes** → route to the waiting caller / emit into the host.
- **`ping`** → respond `{}`.

On the broker dropping the connection:

- **Reconnect with backoff** — the built-ins loop with exponential backoff (0.5s → 30s cap) rather than giving up, and surface a one-shot "broker unreachable" advisory after ~30s so the user learns inbound is down instead of assuming it works.
- **Re-handshake with `hello`** — *not* `server_info`. The broker recognises the same `(cli, pid, cwd)` triple as a reconnect and transfers your existing claims to the fresh connection, so your claim survives a bounce.
- **Replay your last successful attach** with `replay: true` so the claim is restored without the welcome message firing again. Address a remembered topic by `topic_id` + `group` (+ `chat_id` cross-check), **not** by name: a DM recovers as `name: "dm"`, and replaying `attach(name: "dm")` can silently bind a topic literally named `dm`.
- **Re-fire `recover_session`** if you had one.
- **Wake every pending request** with a "broker reconnect" error so the host's tool calls don't hang.

---

## The inbound delivery contract

This is the part a doc-conformant adapter previously got wrong in a way that works perfectly in a demo and then quietly corrupts the user's queue.

**A delivered message stays in the durable queue until you acknowledge it.** The broker writes the push to your socket and then waits. It does not consider the message done.

The full loop:

1. Receive `inbound` with `inbound`, `pending`, `covered`.
2. Render it into your host's dialect.
3. **On success**, send:
   ```json
   {"op":"inbound_delivered","update_id":<inbound.MessageID>,"ok":true,"count":<covered>}
   ```
4. **On render failure, do not ack.** Return without writing anything (or send `ok: false`, which the broker logs as a NACK). The message stays queued as backlog and surfaces in the next push's `pending` count and in `fetch_queue`. Also log the full content locally — the message is otherwise invisible.

Four rules, each with a concrete failure behind it:

- **`update_id` carries `inbound.MessageID`.** The field name is a leftover from an earlier design; the `inbound` object has no `update_id` of its own. Sending anything else is unmatched.
- **`count` must echo `covered`.** A merged debounced batch covers N stored lines and must consume N. Acking `count: 1` for a 5-line batch orphans four lines as phantom backlog that nothing will ever consume. `count < 1` is dropped by the broker as a no-op.
- **Never ack a synthesized event.** If `inbound.Kind` is non-empty (`poll_result`, `reaction`, `callback`, `system`), the message was never queued — it covers zero lines. Acking one consumes a real queued backlog message the event never delivered, silently dropping it. The broker also stamps `covered: 0` for events, but check `Kind` yourself.
- **You must be genuinely attached.** The broker drops a consume from a connection whose route was never confirmed by an explicit claim. This is a deliberate fail-closed tripwire, not a bug — a legitimate holder always has a confirmed route.

**If you never ack at all**, everything appears to work: messages render, the user reads them. Meanwhile every delivered message stays queued forever, `pending` climbs monotonically on every push, and `fetch_queue` re-delivers messages the user has already seen — permanently, because nothing ever consumes the head.

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

The six dedicated-op tools are adapter-local because the *wording* of what the user sees differs per CLI — Claude Code natively renders `<channel>` blocks, Codex sees `notifications/message` log entries — and because several of them (attach proposals, the ask round-trip) need adapter-side state.

Tool names are **unprefixed** across all adapters: the MCP server name provides the namespace, so per-tool prefixing is redundant.

---

## Translating inbound messages

The broker emits a normalised message; converting it to what the host can ingest is **real work, not a pass-through**. The reference Claude Code implementation is roughly 200 lines: it string-coerces the metadata map, branches to a separate event-frame builder for poll/reaction/callback kinds, formats timestamps, and decorates the content with the pending-backlog nudge. Budget accordingly.

**Claude Code** uses `notifications/claude/channel` with `meta` attributes that render as `<channel source="…" chat_id="…" message_id="…" user="…" reply_to_message_id="…" reply_to_text="…">`.

**Codex** doesn't render unsolicited MCP notifications in the TUI today (upstream issues #18056, #17543, #15299). The Codex adapter therefore does two things in parallel:

1. Emit a `notifications/message` log notification (cheap; future-proofs for when Codex surfaces unsolicited notifications).
2. **If `C3_CODEX_REMOTE_BRIDGE=1`** is set (the C3 launcher sets it), forward the inbound as a real `turn/start` to the running Codex app-server over WebSocket. This is the path that makes a Telegram message appear as a normal turn in the user's TUI.

**Grok Build** has no channel-notification dialect. Live inject **requires leader mode** (`[cli] use_leader = true`). The Grok adapter registers as a client on the leader socket and issues ACP `session/prompt` against the TUI session id (see [`GROK-INJECT.md`](GROK-INJECT.md)). Without a leader socket, inbound stays in the durable queue for `fetch_queue`.

**Claude Desktop** has no way for an MCP server to push into a chat at all, so `c3-desktop-adapter` is **pull-only**: inbound never surfaces on its own — it stays in the durable queue and the user drains it by asking Claude to call `fetch_queue`. See [`DESKTOP.md`](DESKTOP.md). **Antigravity** (`c3-agy-adapter`) is pull-only for the same reason.

Held messages — anything that arrived while no session was attached — are **not** buffered in the adapter. They live in the broker's durable per-route queue and the agent drains them with `fetch_queue`.

For a new CLI, look at what unsolicited-notification capability it has. If none: set `cannot_render_channels: true` in `hello`, and make `fetch_queue` your delivery path. If there is a push API (websocket, HTTP, IPC), use it; an adapter can have several delivery paths in parallel.

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

## Distribution

A built-in adapter binary lives at `cmd/<cli>-adapter/main.go` and is installed alongside the broker. The Claude Code plugin's `.mcp.json` points at `c3-claude-adapter` by name; Codex's `mcp_servers.c3_codex.command` points at `c3-codex-adapter`.

If your target CLI has a plugin marketplace, ship the adapter as a thin manifest referencing the binary. If it doesn't, document the manual MCP server registration steps in your adapter's `SETUP.md`.

**Budget for real work.** The five built-in adapters are, whole-package and excluding tests: Claude Code ~3.0k LOC, Codex ~1.9k, Grok Build ~3.1k, Claude Desktop ~2.4k, Antigravity ~1.4k — each reimplementing the handshake, attach, tool forwarding, reconnect, delivery acknowledgement, and host-specific inbound translation. This is the hardest of C3's three extension seams.

## Adding a new adapter — checklist

- [ ] Socket path resolved with all three probes (`XDG_RUNTIME_DIR` **with existence check** → `/run/user/$UID` → `/tmp/c3-$UID/`), plus the Windows branch if you target it
- [ ] Newline-JSON framing with the 4 MiB cap, `\r\n` tolerance, trailing-frame-on-EOF, and serialised writes
- [ ] A dedicated demux reader task; correlation by `id` where it exists, by op elsewhere (one in flight)
- [ ] **A `default` arm that logs unknown ops and continues** — never fatal, never silent
- [ ] Per-type JSON casing: `snake_case` envelope, **PascalCase** nested payloads
- [ ] `hello` first on every connection, with `cannot_render_channels` set correctly for your host
- [ ] `serverInfo.name` equals the MCP server key your CLI registers, **not** the binary name
- [ ] Adapter-owned `serverInfo` / `instructions` / tool list; `instructions` folds in `hello_ack.capabilities`
- [ ] `attach` and `topics` implemented adapter-locally with the right user-facing wording; `detach`/`ask`/`fetch_queue`/`retranscribe` on their dedicated ops, not `tool_call`
- [ ] Inbound translation matches what the host renders
- [ ] **`inbound_delivered` sent on every successful render** — `update_id` = `MessageID`, `count` = `covered`, never for a non-empty `Kind`, never on render failure
- [ ] Reconnect with backoff → `hello` → replay last attach with `replay: true` → re-fire `recover_session`; wake pending calls with an error
- [ ] `recover_session` sent once per session, post-hello, with the **stable** session id — or resume deliberately skipped
- [ ] `ask_result` and `permission_verdict` handled if you expose `ask` / permission relay
- [ ] Marketplace manifest authored if the CLI has one; `SETUP.md` if it doesn't
- [ ] Tests (see below)

## Testing

**There is no mock broker.** A previous version of this document promised a `broker.MockServer(t)` helper; it has never existed. Do not look for it.

What actually works:

- **Hand-roll a fake broker.** Listen on a temporary unix socket, accept one connection, assert on the `hello` frame, and write scripted responses. That is the whole harness — the protocol is newline-JSON and the handshake is one round trip. Drive your adapter through handshake → attach → tool call → inbound → ack → reconnect against it.
- **Mock the host-CLI side** with a stdin/stdout pipe pair and assert on the JSON-RPC traffic. The Claude and Codex adapters both have tests demonstrating this pattern.
- **Test against a real broker** for anything involving the durable queue. Queue consumption, `covered`/`pending` arithmetic, and the never-ack backlog behaviour are the things a fake broker will not catch, and they are exactly the things that corrupt user data when wrong.
- **Write at least one asymmetric wire test.** A marshal-then-unmarshal round trip is invariant under a key rename by construction and will bless the exact change that breaks compatibility. Assert against key names written as literals, and against a byte-for-byte captured frame. This repo does that for its payload types and it is the only kind of test that can fail on a rename.
