# Decisions

Entries are newest first. This is the public architecture record: it records
rulings and rationale, never private operational details.

## D035: Inbox as a broker-owned transport (phase 3)

**Date:** 2026-09-09

**Decision:** This supersedes the delivery parts of D031 for negotiated
connections. D031's owning-session endpoint validation, peer credentials,
framing and transcript provenance remain unchanged. Legacy connections retain
their adapter-owned fallback and retry behavior.

Claude offers delivery when channel or inbox is eligible. Inbox requires a
readable transcript and the validated owning-session socket, including peer
PID/UID verification without disclosing credentials. The broker accepts
`["channel","inbox"]`. It schedules channel then inbox over one selected batch;
fallback keeps the unproven admission slot and reserves a new token and a fresh
15-second deadline. Each transport is tried once per batch. New arrivals and
changed revisions wait for a later batch. Late evidence never retires another
attempt's members or rearms a route. Exhaustion retains confirmation history
and uses the existing rearm events.

The adapter executes exactly the requested transport through a bounded writer
and shared observation loop, with no goroutine per attempt. Inbox writes keep
the 2-second bound and validate the connected peer before authentication.
Receipts carry `c3_attempt="inbox:N"` and the new token. VERIFIED real transcripts
show prefixed peer user turns on 2.1.263 and bare `enqueue.content` /
`queued_command.prompt` blocks on 2.1.266 (versioned peer fixtures); user turns
retain the exact host prefix and peer provenance, while attachments require
nested `isMeta:true` and `origin.kind:peer/from:c3`. Enqueue has no origin fields;
its host intake envelope and exact C3 source/token/attempt bind the receipt.
Prefixed intake variants were inferred and are rejected; all shapes retain
strict tag framing. Capability reports
include socket availability changes. Per-route display names channel or inbox;
fetch cannot change that history.

**Maintainer ruling:** The phase-2 binary was never released or run by user
sessions, so it carries no compatibility obligation. Acceptance requires an
intersection with an offered mode and ignores unknown modes. If initial facts
are ineligible, newly eligible facts trigger a fresh hello on a new connection,
at most once per fact change and once per 10 seconds, after any legacy push's
ack or expiry. This preserves frozen modes without stranding fresh sessions
whose transcript did not exist at startup.

**Host readiness (item G):** Both live transports remain ineligible until
`notifications/initialized` has arrived and the notify transport exists. The
startup hello cannot offer delivery; the bounded re-hello offers after readiness.
A missing notify transport or failed write reports a failed attempt immediately,
independently of receipt polling, so startup backlog cannot silently spend an
attempt deadline before the host is listening.

**Why:** The broker owns durable row delivery, including fallback identity,
admission and deadlines. Keeping transport execution and receipt parsing in the
adapter preserves host-specific validation without creating a second scheduler.

## D034: Negotiated channel delivery (phase 2)

**Date:** 2026-09-09

**Decision:** Protocol v1 gains an optional bilateral delivery negotiation. A
valid hello offers channel eligibility and transcript receipts; `hello_ack`
accepts only channel, frozen per connection. Degraded brokers and ineligible
Claude hosts stay legacy. Legacy wire semantics, recovery ledgers and adapter
fallback policy remain unchanged.

For accepted connections the broker owns reservation, deadlines, cycles,
admission, retirement and per-route display. `deliver`, `attempt_result` and
`delivery_report` are Provisional-negotiated. The attempt table supplies exact
row identities and content revisions; confirmed-holder, connection and claim
generation checks bound receipt authority. The 15-second monotonic deadline
includes write admission. A cycle tries channel once, then waits for a specified
rearm event; ordinary inbound before 60 seconds cannot retry or reset exhaustion.
Unproven sessions admit one live attempt, handing the slot to the longest-waiting
eligible route. Active attempts keep their worker alive; bounded writes run
outside receipt processing, with no goroutine per attempt.

Only surviving revisions return to queued. Enrichment, eviction, drain and
set-aside reconcile individual members; empty attempts close without proof.
Drain snapshots precede reservation and imports persist nudge-only provenance.
Storage failures retain confirmation evidence for three removal attempts before
release. Socket reconnects of the same living process adopt unexpired attempts
under the uninterrupted claim; death or changed negotiation releases them.

Fetch keeps its baseline consume/peek semantics. No fetch receipt mode or lease
is accepted yet. Open live-attempt rows are invisible to fetch, backlog and Held.
The broker renders waiting, confirmed channel, or pull-only per route, preserving
confirmation history on exhaustion. Scheduling precedes Held evaluation, and
notices recount at send time. The general flap timer remains deferred.

The Claude adapter executes channel notification and observes the existing
strict transcript receipt predicate, including the three known host intake
shapes. It reports evidence or definitive transport failure and eligibility
changes. Its retry, fallback and route-policy machine is bypassed on accepted
connections; observation bookkeeping survives same-process socket reconnects.

**Why:** Delivery must have one owner. A transport write cannot establish host
receipt, and a copied token cannot establish ownership. Atomic negotiated
cutover preserves older sessions while moving these decisions together.
Duplicates after expiry, restart or a failed retirement are possible; durable
rows are never silently discarded by failed attempts.

## D033: Inbound delivery contract pins

Phase 1 lands a shadow attempt table; no behaviour change.

**Date:** 2026-09-08

**Decision:** Pin three receipt milestones, declared by the adapter at hello,
never inferred: `transcript` means the host transcript records the attempt token;
`accept` means the host positively accepted the submission, not proven displayed;
`none` means pull-only. Nothing weaker than the session's declared milestone
retires a durable row.

Rows survive until confirmed or the documented limits (1,000 messages / 90 days).

**2026-09-09 operator ruling:** Queue age limit raised to 90 days so held messages survive a long absence; age is a safety valve, not a feature.

A queue-disabled broker never advances Telegram offsets: inbound stays at
Telegram for replay after restart with a working queue. Holding the earliest
unpersisted update holds the global frontier across topics. Live delivery remains
best-effort; anything delivered live in the meantime will arrive again. The hold
is bounded by the provider: Telegram retains an update for about 24 hours, and
about 100 waiting updates block newer ones until restart.
Duplicates after expiry, restart, or degraded mode are allowed and documented;
silent loss is not.

Capability negotiation will use an explicit optional hello field. A hello without
it runs the existing protocol-v1 path unchanged. Later phases implement negotiation
and receipt enforcement; this phase implements only the queue-disabled offset hold.

**Why:** A live handoff is not durable storage. Explicit receipt milestones keep
retirement tied to the evidence each host can provide, while compatibility stays
opt-in.

## D032: Codex queues busy-session input and keeps the user's TUI

**Date:** 2026-09-08

**Decision:** Use `thread/queue/add` by default, including while Codex is busy.
Never automatically steer or interrupt. App-servers without the queue method
are pull-only with an explicit recovery notice; do not resubmit via `turn/start`.
Keep bridging the user's own TUI. C3-owned session runtime work is separate.

**Why:** Queued delivery preserves deliberate scheduling without an uncertain
response licensing a second submission. A clear compatibility floor is easier
to verify than several automatic transport contracts. This increment acknowledges
queue acceptance, not transcript receipt, and makes no exactly-once guarantee.
Broker record identities and confirmed per-route fetch boundaries coordinate
manual recovery with live submission; later exact-token acknowledgements do not
consume earlier failed messages.

## D031: Cross-session messaging is a receipt-gated fallback only

**Date:** 2026-09-08

**Decision:** Prefer the native channel route, then transcript-confirmed delivery
through the owning session's inherited messaging inbox, then durable queue-only
delivery. Never use the inbox while the channel is capable. Every inbox push
carries the same channel block and broker delivery token, plus its own
`c3_attempt="<route>:<n>"` marker (`channel` or `cross-session`, with an increasing
adapter-lifetime counter). Both a clean socket exchange and a receipt for that
exact attempt are required before acknowledgement; receipts cannot cross routes. Failed
confirmation retains the row, disables further fallback pushes until explicit
attach/reconnect, and reports a coalesced held notice. Re-probe starts with channel
eligibility. Credentials are captured once from this adapter's environment; the
endpoint must resolve to `<runtime>/cc-socks/<hostpid>.sock` inside the user's
0700 runtime directory. Existing argv/parent readers select the nearest Claude
ancestor, or only the immediate parent if no Claude ancestor is identified;
unreadable or uncertain ancestry fails closed. Before any auth bytes, `fstat`
validates the connected socket's type/owner and kernel peer credentials must
match that PID and our UID (`SO_PEERCRED` on Linux; `LOCAL_PEERPID` plus
`LOCAL_PEERCRED` on macOS). Path substitution cannot redirect the authenticated
connection; platforms without a peer PID facility have no fallback. A known
stable SessionStart/registered session UUID is included as `session_id` so the
host can reject a mismatch. Each complete outbound JSON frame is capped at the
existing 4 MiB inbound IPC limit before auth; oversize leaves the row queued.

The peer record shape is **VERIFIED on Claude Code 2.1.263**, captured in
`cmd/c3-claude-adapter/testdata/claude-2.1.263-peer.jsonl`. Cross-session user receipts
require `type:user`, `message.role:user`, `isMeta:true`, `origin.kind:peer`, and
`origin.from:c3`. The verified record also carries `verifiedPeerPid` and
`verifiedPeerProcStart` inside `origin`, plus `promptSource:system` and
`userType:external`; these fields are not receipt requirements.
Content starts with the exact host line `Another Claude session sent a message:\n`,
immediately followed by our complete `<channel source="plugin:c3:c3" …>` block.
Both delivery and attempt markers must match. The host appends two newlines and
a fixed paragraph beginning `This came from another Claude session —` after
`</channel>`; trailing host text is allowed. Bare blocks and guessed prefixes
are rejected, as are quoted text, comments, attributes, and later content blocks.
`C3_DEBUG=1` enables a debug preview of rejected candidates' first 120 characters
with words/values/tokens/attempts masked, exposing only framing.

Mid-turn channel intake is **VERIFIED on Claude Code 2.1.266** from a real
transcript: `queue-operation` / `enqueue` stores the block in string `content`
immediately, and `attachment` / `queued_command` later stores it in string
`attachment.prompt` with `attachment.origin.kind:channel`. Both are receipts
through the existing strict opening-tag parser, exact delivery/attempt markers,
and closing `</channel>`; queue `remove` records never confirm. This recognizes
host acceptance during long tool calls before the unchanged 15-second window
expires, preventing unnecessary fallback and a leftover fetchable row. Peer
variants of these new types are inferred, not verified, and fail closed unless
the designated field starts with `Another Claude session sent a message:\n`
followed immediately by the C3 channel block with `source="plugin:c3:c3"`.
The verified user peer provenance rule, fetch receipts, fallback order, and
route state machine are unchanged.

**Why:** A flagless host can accept peer user turns even when it drops channel
notifications. This recovers an inbound route without confusing transport success
with delivery or silently replacing the richer native channel. The active route
and its reason must remain visible in attach, MCP instructions, status and Telegram.
Peer input cannot grant permission approval: Telegram Allow/Deny relay and native
`AskUserQuestion` answering are unavailable, and slash commands arrive as text.
C3's own `ask` and `reply` tools remain usable. The socket protocol and peer
transcript shape were live-verified. A delivered peer turn initially went
unrecognised because the receipt prefix was wrong;
format drift still fails toward held messages and possible duplicates rather
than loss.

## D030: Drive feedback is centralized and deliberate cancellation preserves long audio

**Date:** 2026-08-30

**Decision:** Every driver-invisible state cue is defined in one local notice
table and dispatched through one sound, vibration, and fixed-speech sink. A
local Haptics preference gates only vibration. Unlocked recording cancellation
requires a down-dominant 96 px drag followed by release inside the visible
cancel zone; locked recording can be cancelled only by holding its separate
target for 700 ms. A committed cancel keeps audio of at least five seconds in a
single IndexedDB draft slot. The same slot is used when page hiding interrupts
an unreleased push-to-talk capture, and the next load offers resend or discard
without auto-sending. One unsent-work predicate drives the browser leave prompt
and logout confirmation.

**Why:** Push-to-talk otherwise has no audible progress in Drive, and a small
sideways or downward wobble could discard the only copy of a recording during
movement. A learned set of short multimodal cues keeps state legible without a
glance; release-to-commit and a dedicated locked-cancel target make destructive
intent explicit. IndexedDB is the native browser store suitable for a binary
blob that may exceed local-storage limits, while a single replaceable slot
keeps recovery bounded. The page-hide save remains best effort because mobile
browsers may terminate recorder or database work before completion.

## D029: Multi-channel attach — route set + output route

**Date:** 2026-08-30

**Decision:** A session holds a set of channel routes and exactly one output
route. A bare explicit attach keeps switch semantics; `+X` or `add=true` adds X
to the held set and makes it output. The `output` tool moves the output role
without changing the held set. Message tools may name a held route with the
symbolic `channel` selector; the broker resolves it against that session's held
set, refuses unheld names, and continues to reject raw destination ids. When a
session holds more than one route, inbound is tagged with its channel and, for
Telegram, its held topic name.

**Why:** The operator needs to drive a session from a phone without releasing
the live Telegram topic that still supplies context and input. Separating
membership from the outbound default makes that state explicit, while a
broker-enforced held-route selector fails closed under ambiguous or injected
destinations and still permits deliberate one-message cross-route replies.

## D028: Agent HTML opens in a sandboxed in-page viewer

**Date:** 2026-08-30

**Decision:** The web channel accepts one path-based `.html` or `.htm` file per
outbound part, copies at most 5 MiB into a private newest-200 store, and
publishes a document attachment on the ordinary persisted message event. The
authenticated page renders a DOM-built card and opens its random-token
`/files/` response in a full-screen iframe with exactly `allow-scripts`. The
response independently applies the same CSP sandbox plus a default-deny content
policy, and the viewer treats a pruned file as expired.

**Why:** A self-contained document is a useful high-bandwidth agent reply, but
its author is an untrusted, potentially prompt-injected model. A download was
rejected because it loses the contained in-page reading flow and does not
preserve response policy after the file leaves C3. `srcdoc` was rejected because
it would move untrusted bytes into the trusted parent page and complicate
authentication, replay, and range serving. `allow-same-origin` was rejected
because it would remove the opaque-origin boundary that keeps document script
away from the session cookie, parent DOM, same-origin endpoints, and persistent
origin storage. A cookie-gated response with both iframe and response sandboxing
keeps in-page viewing fail closed. Opening in a new tab was rejected because a
top-level document is outside the parent page's CSP box, while its response
sandbox does not stop it from navigating itself to an attacker page.

## D027: Web presence follows broker route ownership

**Date:** 2026-08-30

**Decision:** The broker publishes every route claim, transfer, and release to
an optional channel presence interface after updating its route table, and
replays current holders when a channel registers. Notification runs outside the
claim path and contains channel panics. Web keeps its operator-route holder only
in memory, sends it as a live-only SSE event on changes and stream open, and
makes the CLI plus shortened working directory the primary Drive and Chat
connection label. Full holder details remain available in a tap-open sheet.

**Why:** The broker is the authority for which CLI session can receive and act
on a route. Showing generic transport connectivity can look healthy while the
page is unclaimed or driven by a different session; presenting broker ownership
directly makes the agent-driving boundary visible without creating a second
source of truth.

## D026: Drive is the web landing screen; its circle only talks

**Date:** 2026-08-30

**Decision:** The signed-in web page lands on a full-screen Drive lamp, with the
complete Chat surface behind an explicit corner target, remembered panel choice,
edge-only swipe, or arrow key. The Drive circle has one operation: talk; it has
no idle tap or multi-tap command. Sliding a held push-to-talk upward locks it,
after which one tap finishes that same recording. Live remains a deliberate
long-press target.
Mute cuts local playback and its queue immediately and disables server-side
synthesis for the session; speaking-state circle press is Stop through barge-in,
not resumable pause. Drive is only a presentation and gesture layer over the
existing recorder, VAD, upload, playback, MediaSession, wake-lock, and earcon
state.

**Why:** A driver must be able to identify and operate the current audio turn
with one glance and one large, unambiguous target. Keeping rich conversation
controls intact but behind a deliberate transition avoids accidental Chat
interaction, while reusing the audio state machine prevents Drive and Chat from
disagreeing about recording, synthesis, interruption, or Live state. Turning
synthesis off at Mute avoids both surprise playback and needless provider work.

## D025: Headless Cursor runs cannot claim routes by default

**Date:** 2026-08-30

**Decision:** Background Cursor review and tool runs must not be able to claim
an operator's topic. The Cursor adapter refuses explicit attach and session
recovery auto-attach when a `cursor-agent` or `cursor` ancestor has a headless
output flag, unless `C3_ALLOW_HEADLESS_ATTACH=1` explicitly opts the run in.

**Why:** Cursor loads every configured MCP server for non-interactive runs, so a
background agent can discover and call `attach` even though it cannot render
channel pushes. The guard belongs in the adapter because the broker sees only a
Cursor connection and cannot distinguish the interactive TUI from a headless
process.


## D024: Hands-free uses local energy VAD; the web app caches nothing

**Date:** 2026-08-30

**Decision:** The foreground hands-free web mode uses the browser's Web Audio
analyser for adaptive, speech-band energy VAD instead of a WASM voice model.
Spoken replies use the media element's direct output; they are not captured
through the analyser's AudioContext, so playback-floor calibration observes
only loudspeaker sound returned through the microphone and is best-effort.
The installable web app's service worker performs install and activation only;
it has no fetch handler and caches no responses.

**Why:** The page CSP forbids external and WASM code, while the v1 environment
is explicitly a screen-on phone in a cradle or on loudspeaker, where a tuned
energy detector and echo-aware barge-in are sufficient. Authentication pages,
session-bound responses, synthesized audio, and the live SSE stream must always
reach the broker; service-worker interception would add stale-auth and broken-
stream failure modes without supplying background audio on iOS. Some WebKit/iOS
versions silence a media element after `createMediaElementSource`, so direct
playback is more important than tighter analyzer/playback coupling.

## D023: Web voice transcodes at the edge; synthesized audio stays transient

**Date:** 2026-08-30

**Decision:** Browser voice notes are converted on receipt from the recorder's
WebM/Opus or MP4/AAC shape into 48 kHz mono OGG/Opus. The STT shim resolves the
web channel's retained local file and the Python handler copies it into its
existing OGG inbox, so providers keep one audio contract. Spoken-reply MP3 is
held only in a bounded 15-minute memory cache; audio SSE events are live-only
and are never added to replay or session persistence.

**Why:** Browser recorder formats vary by platform while every shipped STT
provider already assumes OGG, and duplicating format handling throughout that
chain would multiply failure modes. Synthesized speech is a paid, ephemeral
rendering of text already stored in history; persisting or replaying it would
add stale URLs, disk retention, and unexpected playback after reconnect.

## D022: On-the-go mode uses attach plus shared protocol text

**Date:** 2026-08-30

**Decision:** On-the-go mode has no activation tool or new IPC operation. Only
the unambiguous imperatives “start on-the-go mode” and “switch to the web chat,”
plus the slash command, call the existing attach tool for the web route and
switch the agent's output mode. The generic English fragment “on-the-go” is not
a trigger. The end phrases later re-attach the topic the agent held before.
Spoken-reply guidance is rendered only from the channel's `SpokenReplies`
manifest flag, and the same shared protocol text is delivered to Claude, Codex,
and Codex's managed `AGENTS.md` block.

**Why:** `attach web` already claims the route and delivers the login link, so a
second activation mechanism would duplicate state transitions. Treating the
trigger phrase as the explicit output-mode request preserves the no-inference
mode contract, while manifest-driven guidance cannot drift from whether a
channel can actually read replies aloud.

## D021: Web channel phase 1 and TTS phase 3a contracts

**Date:** 2026-08-30

**Decision:** Freeze the first web-chat phase around these rulings:

1. Web is a real `Channel`; its route is `(web, operator user id, no topic)`.
2. A session still owns one route at a time; switching channels releases the old claim.
3. `Emit=true` means worker-queue acceptance, not durability; web retries only `false` responses.
4. Persisted and persist-failed callbacks are per channel, never broker-global.
5. Web config is typed (`enabled`, `listen`, `public_url`) and operator identity stays in Telegram config.
6. Channels register Telegram-first in a log-and-continue loop; web depends on live Telegram for login-link delivery.
7. Attach parses selectors before request-aware resolution; Telegram remains primary for topic-oriented surfaces.
8. Authentication is a short-lived, single-use magic link delivered through the allowlisted operator's bot DM; private reach is the default.
9. Phase 1 is drive-only: keyboard-less routes show a laptop-permission notice and create no remote verdict path.
10. Browser transport is SSE out and POST in, with no external page resources or HTML injection.
11. Web advertises bounded rich text and native GFM tables, rendered client-side with safe DOM construction; it still advertises no keyboards, media, reactions, or polls.

**Why:** These rulings close the cross-channel loss, ambiguity, startup, and
trust-boundary holes before the HTTP transport lands. The accepted windows are
explicit: worker-queue acceptance can be lost in a crash before append, browser
sessions and reply replay are in memory for phase 1, and tool permissions/`ask`
remain laptop-only.

**Phase-2 reach ruling:** C3 terminates HTTPS on its tailnet listener with a
broker-managed private CA, installed once on each operator phone. The CA is
stable and fail-closed; leaf certificates are short-lived and reissued when
their derived SAN set changes. WireGuard/Tailscale remains the private reach
layer and no listener is public by default.

Tailscale Serve, `tailscale cert`, and tsnet were preferred when the tailnet
control server issues certificates, but cannot establish HTTPS on tailnets
without that issuance. A self-signed leaf plus a browser interstitial does not
produce the trusted secure context required by media capture, Wake Lock, and
service workers. Public tunnels violate the private-first boundary, while ACME
DNS-01 introduces DNS credentials and renewal machinery. A stable local CA won
because it keeps reach private, needs one explicit trust installation per
phone, and lets the broker reissue address-correct leaves without changing the
trusted root.

**TTS phase 3a:** Live provider probes on 2026-08-30 made Sarvam Bulbul v3 and
ElevenLabs Flash v2.5 the first two defaults: both round-tripped mixed Tamil and
English through the STT chain word-for-word. Three Gemini preview probes instead
returned 1.2–2.1-second clips containing only “Hello”, while a plain-English
probe repeated its first sentence three times and omitted the second; OpenRouter
also accepted only 24 kHz mono PCM, requiring local MP3 transcoding. Gemini stays
as the last fallback, and `C3_TTS_CHAIN` remains the explicit order override.

**Phase 2 addendum:** Persist browser sessions by the SHA-256 hash of the cookie,
never the credential itself, and persist the sequenced 200-event replay ring so
cookies, history, and reply ids survive broker restarts. Web now advertises rich
text and rich tables because its self-contained page renders the supported
Markdown client-side with safe DOM construction, a scrollable table wrapper,
and a link-scheme allowlist. The Telegram
`mdToTelegramHTML` converter was not reused: it is package-private, emits
Telegram-specific markup, and deliberately does not validate browser URL
schemes.

## D020: Observe Claude transcripts to settle locally-resolved permission prompts

**Date:** 2026-08-30

**Decision:** While a Claude permission relay is pending, observe complete
`tool_result` records appended to the session or subagent transcript and send
the additive `permission_settled` op. The broker accepts it only from the
requesting session (including the same logical session after reconnect), clears
the keyboard, and records “Allowed in the CLI” or “Settled in the CLI”. A settle
never produces a permission verdict. This adds no protocol-version bump.

**Why:** Claude emits no channel notification when the terminal resolves a
prompt, while every allow, deny, cancel, and allowed-tool failure eventually
lands in the transcript. Hooks were rejected: pre-tool hooks run before the
decision, post-tool hooks cover only allowed tools after completion, and no
hook uniformly covers local denial or cancellation. Transcript observation is
idle while no prompt is pending and fails back to the existing reaper when the
path or process-local pending set is unavailable.

**Residuals (accepted):** Candidate matching inherits the host's five-letter
id space, so an unrelated `tool_result` written during a pending window can
derive to a live prompt's id (roughly 1 in 1,000,000 per result per prompt,
across eleven candidates). That can wrongly clear a keyboard and make a later
tap be refused; it is the same class as the host's own residual. The subagent
transcript layout is undocumented, so layout drift degrades to the reaper and
is surfaced by the adapter's expiry log. Finally, between a local allow and the
tool finishing, a Telegram tap can still render as effective even though the
host drops it; only an upstream `permission_resolved` notification can close
that window.

**2026-09-08 inbound extension:** Reuse the complete-record reader and session
transcript resolver to confirm Claude channel injection before acknowledging
live inbound. Match a per-push `c3_delivery_id` in a user message's channel tag,
not merely a `queue-operation` enqueue record. A 15-second unconfirmed push
keeps its durable copy and changes the route to queue-only until attach or
reconnect. Additive render-state hello fields and updates keep protocol v1.
Messages are held durably until delivered or fetched, within the queue's documented limits (1,000 messages / 90 days).
Transcript format drift may cause duplicate recovery; it must never authorize
consumption on a successful stdout write alone.

## D019: dcode adapter — live push via the external-event socket; slash commands as user skills

**Date:** 2026-08-16

**Decision:** Ship `c3-dcode-adapter` with live inbound over dcode's
external-event Unix socket (`{"kind":"prompt"}` events; the `{"ok":true}` ack
is the landing confirmation, and the broker ack follows only after it), plus
`c3-broker install-dcode` merging `~/.deepagents/.mcp.json`. dcode's plugin
system has no `commands` component, but its user skills ARE slash commands
(`/skill:<name> [args]` with autocomplete), so `install-dcode` also installs
`c3-attach`, `c3-fetch`, and `c3-topics` skills into
`~/.deepagents/agent/skills/` — the three highest-value verbs for a host whose
MCP tools are otherwise agent-invoked only. `recover_session` is skipped:
dcode exposes no stable session id to MCP children; fail closed rather than
guess.

**Why:** dcode (deepagents-code) is a TUI-first agent harness whose only
out-of-band ingress is the experimental event socket, gated behind
`DEEPAGENTS_CODE_EXTERNAL_EVENT_SOCKET=1`. Prompt-kind events enter the
conversation as literal user text (never parsed as a slash command), which
matches C3's injection-safety requirement. Without the flag the adapter is
honest about being pull-only (`cannot_render_channels: true`), like Cursor and
Antigravity.

## D018: Cursor Agent CLI is poll-only (stock TUI over ACP inject)

**Date:** 2026-07-31

**Decision:** Ship `c3-cursor-adapter` as a poll-only MCP adapter (agy-shaped:
`CannotRenderChannels: true`, `fetch_queue` for inbound) plus
`c3-broker install-cursor` merging `~/.cursor/mcp.json`. Do **not** drive Cursor
via ACP for C3 inbound, and do **not** reuse `c3-claude-adapter` under Cursor.

**Why:** Cursor's interactive `agent` TUI has no idle-wake / inject API and no
Claude-style channel renderer. ACP `session/prompt` can start turns but replaces
the stock terminal client. The maintainer required zero TUI degradation, so
poll-only + durable queue is the honest contract. Pointing Cursor at
`c3-claude-adapter` is unsafe: render detection defaults capable and can
black-hole Telegram inbound.

## D017: Final release discipline — independent review, then a direct v0.1.0 release

**Date:** 2026-07-29

**Decision:** Code authors and reviewers must be from different model families;
a finding remains open until a review explicitly passes it. After the remaining
release blockers and final audits, v0.1.0 is released directly rather than via a
second release candidate.

**Why:** Independent review found real release blockers that the implementing
lane had missed. A further candidate is not a substitute for closing those
findings and running the final release checks on the exact tree.

## D016: Ask the bot server; do not hardcode a voice-download limit

**Date:** 2026-07-29

**Decision:** C3 asks the configured Bot API server whether it will serve an
attachment and reports that answer. It does not impose a baked-in size ceiling;
server refusals are shown transparently and notices do not suggest re-recording.

**Why:** A deployment's server is authoritative and may have different limits.
A local threshold can reject a file the configured server would serve, while a
generic transcription failure hides the actual, actionable cause.

## D015: Degraded durability uses posture B — continue loudly

**Date:** 2026-07-27

**Decision:** If the durable queue cannot start, keep the broker running but
make the loss mode explicit through startup, status, hold-notice, and log
surfaces. A fail-fast alternative was considered; the loud-degrade ruling
stands.

**Why:** Refusing to advance an unpersisted source update can wedge all inbound.
Continuing has a real loss cost, so it is acceptable only when every affected
surface makes that cost impossible to mistake for normal durable delivery.

## D014: Frozen describes wire shape, not an implementation requirement

**Date:** 2026-07-27

**Decision:** A frozen operation promises stable fields and meanings; it does
not automatically require every adapter to implement that operation. Incomplete
session identity is accepted for a live connection but must never match another
connection or receive persistence privileges.

**Why:** Optional frozen conveniences remain useful without becoming a false
compatibility burden. Likewise, rejecting an incomplete hello would not make an
identity safe; accepting it while refusing cross-connection matching preserves
ordinary use without guessing ownership.

## D013: Preserve oversize records before making the live queue usable

**Date:** 2026-07-27

**Decision:** When trash retention is available, a record that cannot fit in a
response frame is retained outside the live queue and represented to the
session by an identity-preserving notice. At append time, an over-bound record
is retained first when possible, then truncated with an in-band marker rather
than rejected. Without retention, the marker or notice says no copy was kept.

**Why:** Leaving an impossible head record blocks every later message; silently
truncating misrepresents user content; rejecting it causes the source to replay
the same unwriteable record forever. Retain-first plus an explicit marker keeps
the route moving without concealing the loss boundary.

## D012: A Codex launcher never adopts another app-server

**Date:** 2026-07-26

**Decision:** A launcher starts its own app-server rather than adopting a
reachable existing one. A busy or lost port costs a retry, not a session merge.

**Why:** Launch context is a description, not a unique session identity. Sharing
an app-server based on matching launch attributes can cross-deliver two
independent conversations.

## D011: Codex bridge implemented in Go

**Date:** 2026-05-09

**Decision:** The Codex bridge is implemented in Go. The active path is the
`codex` launcher, `c3-codex-adapter` MCP adapter, and the broker's installation
support for that launcher.

**Why:** It keeps the single-broker architecture while supporting Codex as an
adapter front-end.

## D010: Superseded by D011

**Date:** 2026-05-09

**Decision:** Retired.

**Why:** D011 is the active Codex-bridge decision.

## D009: Go implementation landed

**Date:** 2026-05-09

**Decision:** The full v3 Go rearchitecture is the active C3 codebase. It
honors D006 and D008, reactivates D007, and formalizes the plugin extension
system.

**Structural choices baked in:**

- One Go module and eleven release binaries: `c3-broker`, seven CLI adapters,
  `codex`, `claude-shim`, and `migrate-legacy`.
- Telegram channel implementation in Go; typed IPC structs and operations; and
  a value-typed route key.
- One serial executor per route for inbound, outbound, and presentation work.
- One atomic, user-scoped mappings file with a recovery copy.
- Multi-group attach proposals, cooldown-aware routing, and manual JSON-RPC
  framing for CLI notifications.
- Four declared plugin hook callbacks, two invoked in v0.1.0; STT is the
  shipped built-in plugin.

## D008: Use Official Go MCP SDK

**Date:** 2026-04-15

**Decision:** Use `github.com/modelcontextprotocol/go-sdk` for MCP stub
implementation.

**Why:** It supports stdio transport, tool registration, and the custom
notifications C3 needs while retaining broad compatibility.

## D007: Pluggable transport layer

**Date:** 2026-04-15

**Decision:** Design the daemon with a pluggable transport interface from the
start. Telegram is first; other transports remain future work.

**Why:** A transport boundary avoids rewriting the broker when other chat
surfaces are added.

## D006: Go for daemon and MCP stubs

**Date:** 2026-04-15

**Decision:** Write the C3 system in Go.

**Why:** It keeps the long-running broker efficient, deployable as a single
binary, and suitable for the concurrency model.

## D005: Project name — C3

**Date:** 2026-04-15

**Decision:** The project is C3, pronounced “C-cubed”: Command, Control,
Communications.

**Why:** The name describes the multiplexer’s three responsibilities.

## D004: Use the predecessor bot's message tool as a reference

**Date:** 2026-04-15

**Decision:** Use a predecessor bot's messaging features as a reference spec,
not as source code.

**Why:** Reuse proven concepts while keeping C3's Telegram-centric model and
implementation independent.

## D003: STT built into the daemon

**Date:** 2026-04-15

**Decision:** Speech-to-text runs in the daemon rather than being patched into
each MCP stub.

**Why:** It centralizes transcription and gives every adapter text-first inbound
messages.

## D002: Telegram topics as primary routing

**Date:** 2026-04-15

**Decision:** Use Telegram group topics as the primary routing mechanism: one
topic per CLI instance.

**Why:** Topics give people a visible, lightweight separation between sessions.

## D001: Architecture — daemon plus MCP stubs

**Date:** 2026-04-15

**Decision:** Use one daemon that owns the bot connection and thin per-CLI MCP
stubs that connect to it over a local socket.

**Why:** The daemon centralizes channel polling while the stubs distribute
messages to concurrent CLI sessions.

## D037: Seamless adapter upgrade by self-exec with MCP resume

Claude Code does not automatically restart an exited stdio MCP server and does
not initialize again after an in-place exec. Replacing a binary at an unchanged
plugin command also does not force `/reload-plugins` to reconnect it.

Use broker hello upgrade hints based on embedded build identity. On supported
platforms, drain adapter requests and delivery observations, then exec the
installed adapter with locally restored Go SDK session state. Keep stdin framing
and stdout completion inside the exec gate. Pin the cached tool contract and
require reconnect on incompatible releases. Preserve PID, descriptors, claims,
and the existing readiness/re-hello path. Legacy receipt bookkeeping runs after
releasing the route ownership lock, preventing a nested-read-lock deadlock when
claim transfer starts immediately after ack. Updates end with a broker bounce to
trigger discovery; older adapters get an explicit system notice.

This is provisional: broker shutdown still cancels broker-side calls and loses
in-memory ask/permission state. A full lossless update requires a separately
specified broker drain/handoff protocol; the adapter exec gate alone cannot
provide that guarantee.
