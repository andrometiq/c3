# Decisions

Entries are newest first. This is the public architecture record: it records
rulings and rationale, never private operational details.

## D022: Web voice transcodes at the edge; synthesized audio stays transient

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
