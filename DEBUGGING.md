# Debugging C3

> Companion to [`docs/COMMANDS.md`](docs/COMMANDS.md), the cross-CLI
> source of truth for verb behavior. If you're adding/changing a
> command, read that first.


When something looks wrong (message didn't arrive, attach is rejected, the
broker won't start), this is where to look first. **Read this before adding
print statements or `pkill`-ing the broker.**

## TL;DR — first thing to do

```bash
c3-broker status              # broker pid, socket, mappings, log path
tail -F ~/.local/state/c3/broker.log
```

Then reproduce the bug. The broker emits one log line per inbound delivery
(success or failure). If nothing appears in the log when you send a
Telegram message, the bug is upstream of the broker (polling, network, or
Telegram API).

## Logs

| Source | Path | Notes |
|---|---|---|
| Broker | `$XDG_STATE_HOME/c3/broker.log` (default `~/.local/state/c3/broker.log`) | Tee'd to stderr while a tty exists. Persistent. Override with `C3_LOG_FILE=...`. |
| Adapter (`c3-claude-adapter`) | The CLI's stderr — captured by Claude Code's plugin mechanism | Not persistent; restart the session to clear. The broker log is the durable signal. |

The broker log is **always read first.** Adapter stderr is opportunistic.

## Log line shape

All lines use stdlib `log` format: `2026/05/09 06:45:01.123456 ` prefix.
Field order is stable so you can grep / awk.

```
telegram: inbound update=12345 msg=914 chat=-1001234567890 thread=914 kind=text edited=false
telegram: suppress phantom edit update=12346 msg=914 chat=-1001... thread=914 kind=voice — deliverable content unchanged (reaction-triggered edited_message); not re-dispatched
emit SATURATED chan=telegram chat=-1001... topic=914 msg=914: worker queue full after 2s — HOLDING the offset so Telegram redelivers (not dropped)
delivered chan=telegram chat=-1001... topic=914 msg=914 to cli=claude pid=1234 conn=2
deliver FAIL chan=telegram chat=-1001... topic=914 msg=914 to cli=claude pid=1234: write: broken pipe
drop chan=telegram chat=-1001... topic=- msg=915: no claim, fallback in cooldown
fallback chan=telegram chat=-1001... topic=- msg=916: no claim, sent fallback reply
fallback FAIL ... : send: telegram: 400 Bad Request: chat not found
```

Field meanings:

- `chan` — channel name (always `telegram` in v1).
- `chat` — `chat_id` (negative for groups, positive for DMs).
- `topic` / `thread` — `message_thread_id`. `-` means no topic (DM or
  non-forum group).
- `msg` — `message_id`.
- `cli` / `pid` / `conn` — which adapter received the inbound. `conn` is
  the broker's monotonic adapter conn id (bumped on each (re)connect).

## Content policy

The rule (set 2026-05-09): **never log content on a successful delivery,
always log content on a failure.** A message that reached a CLI doesn't
need to be in our log — it's already in the receiving session's
context. A message that didn't reach anyone is at risk of being lost,
and the log is the last place to find it.

| Path | Content logged? |
|---|---|
| `delivered chan=… to cli=…` | **No** — message reached a CLI, content lives there. |
| `deliver HELD … cannot render` | **No** — the session can't render inbound (e.g. launched without the dev-channels flag); the message is held in the durable queue, recover with `fetch_queue`. Not a loss. |
| `deliver FAIL …` (write error to adapter) | **Yes** — `from=@user(uid=N) text="…" attach=kind/size`. |
| `drop … no claim, fallback in cooldown` | **Yes** — bot didn't even reply, message is fully lost. |
| `fallback … sent fallback reply` | **Yes** — user got a boilerplate, but no CLI processed the original. |
| `fallback FAIL …` | **Yes** — couldn't even send the boilerplate. |
| `telegram: skip update=… (unsupported service)` | **No** — these are forum_topic_created / new_chat_members type events with no useful content. |
| `perm settled …` / `perm settled DEFERRED …` / `perm settled REFUSED …` | **No** — a CLI-local resolution cleared the keyboard, is waiting for the in-flight message id, or was rejected as a non-owner. These lines carry ids and session metadata, never the prompt preview. |

Specifics:

- Text is truncated at 200 chars (with `…` suffix) and quote-escaped.
- Sender as `@username(uid=N)` if both present, else just one or the
  other. No user content beyond the text itself.
- Attachments as `kind/size` only (e.g. `attach=voice/12345`). Never the
  file content; `file_id` is a Telegram-side opaque token, not content.
- See `fallbackSummary` in `internal/broker/worker.go`.

## Common diagnostic flows

### "I sent a message in Telegram and the CLI never saw it"

```bash
tail -F ~/.local/state/c3/broker.log &
# now send the message in Telegram
```

What to look for, in order:

1. **No `telegram: inbound …` line.** Polling didn't see the message, or
   `convertInbound` returned nil. Possible causes: bot token revoked,
   network timeout, message is a service event (forum_topic_created etc.),
   or the bot isn't a member of the chat. Check
   `pgrep -af c3-broker` is alive; check `ss -tnp | grep c3-broker` for
   the TLS connection to `149.154.166.x:443`.
2. **`telegram: inbound …` but no `delivered` / `drop` / `fallback` line
   within ~2 seconds.** Worker pool is wedged, or the route worker's
   debounce isn't flushing. Worker's `defaultDebounceWindow` is 1.5s.
3. **`drop … no claim, fallback in cooldown`.** No adapter has claimed
   the route, AND a fallback was sent in the last 5 minutes. The
   adapter that should own this route either crashed, or never called
   `attach`, or attached to a *different* `chat_id`/`topic_id`. Run
   `c3-broker status` and compare `chat_id`/`topic_id` from the log line
   against the mappings file's `topics` and `dm_chat_id`.
4. **`fallback … sent fallback reply`.** Same as above but the cooldown
   was clear, so Telegram now shows a "no agent attached" message in the
   chat. Same fix path.
5. **`delivered chan=… to cli=claude pid=…`.** Broker did its job. Bug is
   adapter-side or Claude Code side. Move to the next section.

### "Broker says delivered but Claude Code doesn't show the message"

Read the **Live route** line in `attach`, `c3-broker status` / `/c3:status`,
or Telegram `/status`:

- `channel`: live pushes are eligible; each durable message still needs a
  transcript receipt before it is consumed.
- `probing (channels flag present, awaiting confirmation)`: the host has
  `--channels plugin:c3@c3`; C3 is waiting for its first channel receipt.
- `probing (cross-session awaiting confirmation; …)`: the channel is unavailable;
  C3 is testing the session's inherited messaging inbox with its first human push.
- `cross-session (reason; permission relay unavailable)`: a peer user turn was
  receipt-confirmed through that inbox. Telegram Allow/Deny and native
  `AskUserQuestion` answers are unavailable; slash commands arrive as text.
  C3's own `ask` and `reply` tools still work.
- `queue-only (reason)`: inbound remains on disk for `fetch_queue`. Outbound
  tools still work. This is a route status, not a request to resend messages.

The MCP preamble reports `Permission relay: available/unavailable` separately.
A transcript being unavailable need not disable an already registered permission
channel. An unproven host channel cannot be assumed to relay permissions.

Previously, the detector used `/proc` everywhere and treated uncertainty as
capable. macOS has no `/proc`, so even flagless hosts were reported capable.
The adapter acknowledged successful stdout writes while Claude silently dropped
unregistered channel notifications, consuming the only durable copy. Detection
now fails closed and macOS reads native process ancestry. Only the nearest
Claude host's flags count; an outer flagged session cannot qualify a nested one.

Host identification accepts `claude`, the npm `@anthropic-ai/claude-code/cli.js`
script, and native argv[0] paths directly under `claude/versions/`. This includes
resolved versioned binaries launched by the C3 shim or a background supervisor.
A bare version number is not a host. The native layout is assumed to be the same
on macOS (offline installer documentation was unavailable).
`no Claude Code host identified in the process tree` means the walk completed
without a match; `process tree truncated` and `process tree unreadable` mean it
could not complete. An identified versioned binary without a qualifying flag
reports `no dev-channels flag on host`.

`live push not confirmed` means no matching complete user channel record was
observed within 15 seconds. No acknowledgement is sent; the durable copy stays
available. If the inherited inbox is eligible, C3 first attempts one cross-session
fallback with the same delivery token and a new `c3_attempt="cross-session:<n>"`
marker. Native attempts carry `c3_attempt="channel:<n>"`; receipts cannot cross
those attempt boundaries. `cross-session push not confirmed` means
that attempt failed its socket exchange or transcript confirmation; a coalesced
held notice reports it, and subsequent messages are held without further pushes.
Explicit attach or reconnect retries channel eligibility first, then fallback.
`session transcript unavailable`
means the SessionStart handoff did not resolve a readable regular transcript.

Readback requires `type=user`, `message.role=user`, and channel content bearing
`c3_delivery_id`; an enqueue-only `queue-operation` is insufficient. Partial
JSONL writes are retried. If Claude changes that record shape, C3 falls back to
queue-only. A record arriving after the window may produce a duplicate on fetch;
check the existing conversation before repeating work. The broker's `delivered`
log records its socket write, not host confirmation; durable consumption is
controlled by the later receipt.

The cross-session endpoint and token are captured from the adapter's own
environment at startup. Do not dump tokens, process environments, or socket
frames into logs. Owning-session delivery requires the exact resolved path
`<runtime>/cc-socks/<hostpid>.sock`, a user-owned 0700 runtime directory, and a
user-owned socket. The existing argv/parent readers identify the nearest Claude
ancestor; if none is identified, only the immediate parent is eligible.
Unreadable or uncertain ancestry disables fallback.

Before auth is written, the **connected fd** is checked with `fstat`, and kernel
peer credentials must report that same host PID and C3's own UID. Linux uses
`SO_PEERCRED`; macOS uses `LOCAL_PEERPID` and `LOCAL_PEERCRED`. This prevents a
substituted pathname from redirecting credentials. No peer-PID facility means
no fallback. The user frame also supplies `session_id` when the owning stable
UUID is known from the SessionStart handoff/registered identity. Generic errors
distinguish path/ownership/peer failures without exposing credentials. Attach
revalidates the captured endpoint; changed credentials need an adapter restart.
Outbound JSON frames (newline included) over the 4 MiB IPC cap are refused
before auth, logged as `cross-session outbound frame exceeds IPC cap`, and held
in the durable queue. C3 does not alter host accept/hold/refuse policy.

Peer transcript shape is **VERIFIED on Claude Code 2.1.263**; the sanitized
capture is `cmd/c3-claude-adapter/testdata/claude-2.1.263-peer.jsonl`. A receipt
requires a complete `type:user`, `message.role:user`, `isMeta:true` record and an
`origin` object with both `kind:peer` and `from:c3`. String content or its first
text block must start with the exact host line
`Another Claude session sent a message:\n`, immediately followed by our complete
`<channel source="plugin:c3:c3" …>` block carrying the exact `c3_delivery_id` and
`c3_attempt`. Trailing host guidance after `</channel>` is allowed. The captured
record includes `origin.verifiedPeerPid`, `origin.verifiedPeerProcStart`,
`promptSource:system`, and `userType:external`; these are not required for a
receipt. Bare blocks, guessed prefixes, quoted text, XML comments, attributes,
multiple wrappers, and later content blocks are rejected.

Set `C3_DEBUG=1` in the environment inherited by the adapter at startup (restart
the adapter to apply it). This enables the existing Go `slog` debug bridge into
the adapter's normal logger. Look for `cross-session receipt candidate rejected`
and `prefix_redacted` in `$XDG_STATE_HOME/c3/adapter.log` (default
`~/.local/state/c3/adapter.log`); the same output goes to adapter stderr.
The preview is capped at 120 characters, with words, values, delivery tokens,
and attempts masked and only framing punctuation/spacing retained. Tokens and
raw transcript prose are never logged. Use that framing preview to diagnose
allowlist drift during an authorized live test. Socket EOF alone never confirms
delivery; failed receipts leave rows available in `fetch_queue`, with possible
duplicates after a late injection.

Messages are held durably until delivered or fetched, within the queue's documented per-route limits (1,000 messages / 14 days); retention cleanup may permanently remove evicted records.

### "Broker won't start"

```bash
c3-broker 2>&1 | head -20      # run in foreground
```

Common causes: stale pid file with a live foreign process at that pid
(unlikely but possible after reboots reusing pid numbers); mappings.json
parse error (`c3-broker validate`); socket dir not writable.

### "Two brokers fighting" (409 Conflict)

Telegram gives `409 Conflict` when two `getUpdates` calls hit the same bot
token. **The poll loop now self-heals from this** — it no longer exits. On a
409 it logs `409 CONFLICT (consec=N) … backing off … retrying`, backs off with
an escalating delay (5s → 60s), and keeps retrying; the first successful poll
recovers automatically with no kill/restart.

Most 409s are transient: after a client-side long-poll timeout (flaky network/
proxy, e.g. a laptop waking up) the next `getUpdates` races Telegram's still-open
prior poll. These clear within seconds on their own — `consec` stays low and no
`FETCH DOWN` alert fires.

A *persistent* 409 (rising `consec`, a `FETCH DOWN` alert) means a genuine
second poller is really holding the token — another machine, or a leftover
Python POC broker. The Go broker keeps retrying + alerting until it goes away.
To end it now, find and kill the other poller:

```bash
pgrep -af 'c3-broker|broker.py'
```

If the Python POC broker is still running, kill it. The Go broker will resume
on its next poll automatically — you do NOT need to restart `c3-broker`.
See `INSTALL.md` step 2. (The broker singleton flock prevents a second *local*
`c3-broker`, so a persistent conflict is almost always an off-box poller.)

## Conventions for future log lines

- One line per **delivery outcome**, not per stage. Stages are debug noise.
- Lead with the verb: `delivered`, `drop`, `deliver FAIL`, `fallback`,
  `fallback FAIL`, `emit SATURATED`. Greppable.
- Always include `chan=… chat=… topic=… msg=…` in that order.
- Failures get a trailing `: <reason>`. Successes don't.
- Never log message content. Never log usernames. See content policy above.

## Things that should be logged but aren't yet

- Outbound tool calls (`reply` / `react` / `edit_message` / `send_typing` /
  `download_attachment`) — currently silent. Add when an outbound bug
  surfaces and you actually need them. The log line should include the
  same `chan/chat/topic` fields plus `tool=reply` and the resulting
  Telegram `message_id` on success / error string on failure.
- Plugin-pipeline drops — when an `OnInbound` hook returns nil and the
  message is intentionally dropped, log `pipeline DROP chan=… msg=…
  by=<plugin>`. Currently silent (intentional drops look identical to
  bugs).
- Adapter (re)connect events with conn id and prior conn id — useful when
  a session loses its claim and you want to know why.

## Telegram resilience parity (prior-art reference)

A prior TypeScript Telegram bot (grammy-based, `extensions/telegram/`) had
more production hardening than we currently do — explicit 429 `retry_after`
honoring, 401 circuit-breaker, error classification (transient vs
permanent), persisted update-id watermark for crash safety, per-method
timeout policy, etc.

The actionable punch list (researched 2026-05-09) lives in
[`TODO.md`](TODO.md) under **"Telegram resilience — prior-art parity"**.
Not done yet; pick from there in priority order when adding more
hardening to `internal/channel/telegram/`.

## The channel-allowlist gate (THE 2026-05-09 silent-drop bug)

Even when the broker delivers, the adapter writes the correct frame to
stdout, AND the wire bytes match the official telegram + fakechat plugins,
**Claude Code will silently drop `notifications/claude/channel` from any
plugin not on the user's allowlist.**

The allowlist lives in `~/.claude/settings.json`:

```json
"channelsEnabled": true,
"allowedChannelPlugins": [
  { "marketplace": "c3", "plugin": "c3" }
]
```

`channelsEnabled` is the global on/off. `allowedChannelPlugins` is a
per-plugin opt-in keyed by `(marketplace, plugin)` names exactly as
declared in the plugin's marketplace.json + plugin.json.

If our plugin emits `notifications/claude/channel` but isn't listed,
Claude Code drops the frame with no log surfaced to the user. The broker
log will say `delivered`, the adapter will say `notified`, and the CLI
will see nothing.

**How to verify** (search the Claude Code binary):

```bash
strings ~/.local/share/claude/versions/*/  | grep -E 'allowedChannelPlugins|--channels|channel_enable'
```

You'll see strings like
"Managed-org allowlist of channel plugins. When set, …" and
"is not plugin-sourced; channel_enable requires a marketplace plugin".

**How to fix** (one of):

1. Edit `~/.claude/settings.json` to include
   `{ "marketplace": "c3", "plugin": "c3" }` in `allowedChannelPlugins`.
2. Or invoke Claude Code with `--channels c3` per session (per-run flag).
3. Or rely on Claude Code's interactive elicitation — when a new
   channel-capable plugin loads, CC may prompt the user to opt in.

Naming gotcha: the entry uses **plugin name from plugin.json**, not the
.mcp.json server key. They happen to be the same in our case (`c3`), but
in general they're different concepts.

## STT failure modes

The STT plugin shells out to a Python handler that runs the
gemini-3-flash-openrouter → soniox-stt-async-v5 → elevenlabs-scribe-v2 →
sarvam-saaras-v3 chain (providers without configured API keys skip at runtime).
The broker logs explicit failure lines now (no more silent empty transcripts).

| Log line shape                                                 | Meaning                                                                                |
|-----------------------------------------------------------------|----------------------------------------------------------------------------------------|
| `stt: msg=N transcribed in 22s (chars=730)`                    | Success.                                                                               |
| `stt: msg=N timeout after 5m0s (timeout=5m0s, file_size=...)`  | Hit the broker's file-size-scaled subprocess deadline: base 300s, scaling up to a 720s cap. Long voice notes + slow downloads are the usual culprit. |
| `stt: msg=N error after Ns (...): exit status 1 \| stderr-tail=...` | Python handler errored. stderr-tail (last 240 chars) shows the cause.                 |
| `stt: msg=N empty transcript after Ns (no provider returned text)` | All providers returned empty. Token expired? Provider down?                        |
| `stt: token read failed for msg=N: ...`                         | mappings.json missing or `bot_token` empty.                                            |
| `stt: msg=N handler missing at <path> (...)`                    | Handler script went missing between broker start and this message. Marker = `handler_missing`. Restoring the script makes the NEXT voice message transcribe — no broker restart needed. |
| `stt: handler <path> missing at startup (...); voice messages will surface [STT FAILED: handler_missing] ...` | Startup-time notice that the script is absent. The plugin still registers; per-message check inside the callback decides each time. |

When transcription fails, the inbound text becomes `[STT FAILED: <reason>]`
instead of the silent `(voice message)` placeholder — the receiver
knows to ask the user to resend. Two safety nets layer here:

1. **Plugin-level marker** — STT plugin returns `[STT FAILED: <reason>]` on
   any failure mode it can name (handler_missing, token_unavailable,
   timeout, killed, error, empty).
2. **Broker-side defense-in-depth** — if the OnVoiceReceived chain produces
   no transcript AT ALL (e.g. STT plugin disabled, no plugin registered,
   future channel/plugin layout where voice arrives unrouted), the worker
   substitutes `[STT FAILED: no_transcript_plugin]` before forwarding to
   the adapter. The silent `(voice message)` placeholder is no longer
   reachable for voice attachments going through the broker pipeline.

Tunables in mappings.json:
- `plugins.stt.timeout_seconds` — broker's base hard deadline (default 300s), scaling with file size up to a 720s cap.
- `plugins.stt.handler_path` — override the Python script path.
- `plugins.stt.enabled` — set false to disable transcription entirely.

## DM disambiguation

If a topic literally named `dm` (case-insensitive) exists in a channel,
`attach target="dm"` is ambiguous — the user could mean the actual
Telegram DM or that topic. The broker returns
`needs_confirmation` with proposal_action=`disambiguate_dm`; the LLM
asks the user which they meant. To bypass disambiguation (e.g. after
the user explicitly confirmed the actual DM), pass `steal=true` along
with `target="dm"`.

Topic creation with name "dm" is **not** refused. The design choice:
don't refuse creating DM or anything; whenever there's an attach and
there's an ambiguity, just show it as a question. The disambiguation
happens at attach time, not creation time.

## Multi-session: alive-but-abandoned tabs

**The case.** A `claude --resume <id>` session left open in another
terminal still holds the route claim on a topic. A new session in a
third tab tries `/c3:attach <same topic>` and the broker rejects with
a `force_steal` proposal because the prior holder's PID is still
alive. The user is confused — they thought they'd closed it.

**Why the broker doesn't auto-detect.** The broker doesn't
periodically ping each adapter asking "is a human still driving
you?" — intentional (out of scope).
Adding heartbeats for an edge case adds plumbing and false-positive
risk that outweighs the papercut. The PID-liveness check
(`stubs.go::isPIDAlive`) only releases when the OS process is
actually gone; an idle-but-alive tab keeps its claim by design.

**Workaround.**

1. In the new session, run `/c3:attach <topic>`; when the broker
   surfaces the `force_steal` proposal naming the prior holder, accept
   it. The broker calls `ForceReleaseKey` on the abandoned route
   before granting the new claim — see
   `internal/broker/attach.go::tryClaim` (`steal=true` branch).
2. Alternatively kill the abandoned `claude` PID from another shell.
   The next inbound to that route triggers fallback / re-route after
   the broker's PID-liveness defense-in-depth check
   (`internal/broker/handler.go` conn-drop defer + worker dispatch
   check).
3. To identify which terminal currently owns the topic **before
   evicting**, run `/c3:ping` in each candidate tab. The session that
   actually holds the claim posts a one-shot identification message
   (cwd / cli / pid / timestamp) to the Telegram topic; tabs that
   aren't holders get `not attached` and stay silent. See
   `plugins/c3/commands/ping.md` for the slash command and
   `internal/broker/handler.go::handlePingThisSession` for wire
   semantics.

**Related verifications** (covered as tests so the behaviour can't
regress silently):

- `internal/broker/handler_test.go::TestConnDrop_ReleasesClaimWhenPIDDead`
  — closing a conn whose stub's PID is already dead releases the
  claim.
- `cmd/c3-claude-adapter/lifecycle_test.go::TestReplayLastAttach_ResendsLastAttachWithReplayFlag`
  — `--resume` re-attach replays the last `AttachReq` with
  `Replay=true` so the broker re-grants the claim silently (no spam
  welcome).

## Things we've found and fixed (history)

- **2026-05-09 — getUpdates always timing out.** gotgbot's
  `DefaultTimeout` is 5s; our long-poll asked Telegram to hold 25s.
  Client cancelled every cycle with `context deadline exceeded`. Zero
  inbounds reached the broker. Fixed in `internal/channel/telegram/poll.go`
  by passing `RequestOpts.Timeout` per call.

- **2026-05-09 — 10s margin still too tight for long-poll.** Even with
  `25s + 10s = 35s`, occasional `context deadline exceeded` showed up
  under transit-latency spikes. Generalized into `timeoutFor(method, longPollSeconds)` in
  `internal/channel/telegram/resilience.go`: getUpdates gets `25s + 30s`,
  control calls (`getMe`, `setMyCommands`) get 10s, sends/edits get 20s.

- **2026-05-09 — adapter dies permanently on broker restart.** The old
  reconnect-once policy meant any broker bounce killed every connected
  CLI session. Replaced with `recoverBroker` (exponential backoff, no
  give-up) plus `replayLastAttach` so the route claim is restored
  automatically. See `cmd/c3-claude-adapter/main.go`.

## Codex policy layer rejected attach

**Failure mode (from an install pilot).** Codex was
configured with `approvals_reviewer = "auto_review"` (or
`"guardian_subagent"`) in `~/.codex/config.toml`. A fresh `attach`
tool call from a Codex session was silently classified as an
"unacceptable risk" (data-export class — bot tokens, chat ids in the
response) and rejected by Codex's policy layer **before reaching the
spawned `c3-codex-adapter`**. The agent observed the rejection in the
Codex UI but didn't relay an actionable next-step to the user. Manual
retry succeeded only after the tenant admin approved the Telegram
destination tenant-side.

Note: `C3_CODEX_REMOTE_BRIDGE` and `C3_CODEX_ALLOW_MANUAL_FORWARD`
are **not** involved. Those env vars gate the WebSocket forwarder to
the codex-app-server; the policy layer sits well upstream of any C3
wire.

**How c3 surfaces it (2026-05-19).** The `attach` MCP tool accepts an
explicit `policy_rejected=true` argument. When the agent observes
the host's rejection (e.g. tool call comes back with "unacceptable
risk" / "tool call rejected" in the Codex UI), the agent re-invokes
`attach(policy_rejected=true, …)`. The broker short-circuits with
`AttachedMsg.Status = "policy_rejected"` and the adapter formats:

> "Attach rejected by your CLI host's policy layer. The Telegram
> destination needs tenant-admin approval before this CLI can
> attach. Ask the tenant admin to approve the destination, then
> retry attach."

This replaces the prior silent-fail / generic-error mode where the
user couldn't distinguish "broker isn't configured" from "tenant
policy blocked the call" from "broker succeeded but delivery
dropped."

**Why the adapter can't detect it itself.**
`approvals_reviewer` and `mcp_servers.<name>.tools.<tool>.approval_mode`
live in `~/.codex/config.toml` — host-owned, not exposed via env or
the MCP initialize handshake to spawned MCP servers. Any per-request
decision happens upstream of the spawned MCP server; when Codex
rejects, our adapter never receives the tool call. Only the agent
(LLM) sees the rejection in its turn output, so the agent is the
right vector to surface the structured hint.

**How the user resolves:**

1. Tenant admin reviews the request and approves the Telegram
   destination (chat id + bot token) for the specific Codex tenant /
   project.
2. After approval, the agent retries `attach` (this time **without**
   the `policy_rejected` hint — that hint is only for surfacing the
   prior failure, not for the retry itself).
3. If retry still rejects: confirm `approvals_reviewer` in
   `~/.codex/config.toml` and the per-tool `approval_mode` for
   `mcp_servers.c3_codex.tools.attach` aren't set to a stricter
   policy than the approved destination warrants.

**Distinguishing from "no topics configured".** That's a separate
state (`AttachedMsg.Status = "no_topics_configured"`) — the broker
has zero channels or destinations registered yet. Fix is
`c3-broker setup`, not tenant approval. The adapter formatter
renders both cases with distinguishable user-facing prose so the
agent can give the user the right next-step.

## Persistent state files

Everything C3 reads or writes lives in one of these paths. There is **no** pre-c3 / legacy path that's still active; if you see files under `~/.claude/channels/telegram/` they are leftovers from the Python POC and not used by the Go broker.

| File | Purpose | Owner |
|---|---|---|
| `~/.config/c3/mappings.json` | Bot token, channel config, cwd→topic mappings, plugin config | broker (config) |
| `~/.config/c3/mappings.json.bak` | One-generation backup, written before each rewrite | broker |
| `~/.claude/stt.env` | API keys for STT providers (`OPENROUTER_API_KEY`, `SONIOX_API_KEY`, `ELEVENLABS_API_KEY`, `SARVAM_API_KEY`); read by the bundled handler. Optional — skip if STT is disabled or you've pointed `plugins.stt.handler_path` at a custom script that loads keys differently. | user (manual, one-time setup) |
| `$XDG_RUNTIME_DIR/c3-broker.pid` | Singleton flock + pid | broker |
| `$XDG_RUNTIME_DIR/c3.sock` | Adapter ↔ broker socket | broker |
| `$XDG_STATE_HOME/c3/broker.log` | Broker log (this file) | broker |
| `$XDG_STATE_HOME/c3/telegram-offset.json` | Persisted highest update_id | broker (telegram channel) |
| `$XDG_CACHE_HOME/c3/telegram/attachments/` | Downloaded media | broker (telegram channel) |

`$XDG_RUNTIME_DIR` defaults to `/run/user/$UID`; `$XDG_STATE_HOME` defaults to `~/.local/state`; `$XDG_CACHE_HOME` defaults to `~/.cache`.

## STT handler path resolution

Resolution order and fallbacks:

1. If `mappings.json:plugins.stt.handler_path` is set → use that. (User override.)
2. Else if `$CLAUDE_PLUGIN_ROOT` is in the broker's env → use `$CLAUDE_PLUGIN_ROOT/stt/stt-handler.py`. Claude Code sets this env when it launches the c3 adapter; the adapter inherits it when spawning the broker.
3. Else if `$C3_SRC_DIR` is set → use `$C3_SRC_DIR/plugins/c3/stt/stt-handler.py`.
4. Else use `plugins/c3/stt/stt-handler.py` beside the resolved broker executable, if the release bundle is valid.
5. Else use `~/.local/share/c3/plugins/c3/stt/stt-handler.py`, if the release bundle is valid.
6. Else use `~/src/c3/plugins/c3/stt/stt-handler.py`, if it exists. If no candidate resolves, voice messages surface `[STT FAILED: handler_missing]` per call.

If you run `c3-broker` outside Claude Code (manual daemon, systemd unit, debugging), `$CLAUDE_PLUGIN_ROOT` won't be set; the remaining automatic fallbacks still apply, and `plugins.stt.handler_path` remains the explicit user override.
