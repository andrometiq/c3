# Decisions

Entries are newest first. This is the public architecture record: it records
rulings and rationale, never private operational details.

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

- One Go module and ten release binaries: `c3-broker`, six CLI adapters,
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
