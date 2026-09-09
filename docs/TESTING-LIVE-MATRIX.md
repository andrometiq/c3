# Live delivery matrix

Written before implementation, from the 2026-09-09 inbound ruling. Run only
outside the coding sandbox. Unit tests cannot establish a new host record shape.

The Cartesian product is **180 cells**: transport × arrival state × session ×
kind × burst. `scripts/live-matrix/run.sh --list` enumerates every cell; the
report retains infeasible cells as N/A instead of silently dropping them.

| Axis | Values |
| --- | --- |
| Transport | channel (development channel flag); inbox (flagless, verified owning-session socket); fetch (agent pulls, live input disabled by the harness MCP proxy) |
| Arrival state | idle; foreground Python sleep; background Python sleep/waiting; startup before notifications/initialized; immediately after MCP reconnect |
| Session | fresh, no transcript at arrival; resumed with --continue |
| Kind | text; voice placeholder followed by transcript revision; photo attachment metadata |
| Burst | single; two messages within the configured debounce window |

The word **fresh** means literally no transcript at arrival. An untouched prompt may have no transcript. Reaching a tool call
requires a transcript; starting a conversation and calling it fresh would hide
the startup bug. New sessions after their first turn are covered by the resumed
state mechanics but are not relabelled as transcript-free.

Each entry below expands across all three kinds and both bursts (six cells).

| Arrival state | channel fresh | inbox fresh | fetch fresh | channel resumed | inbox resumed | fetch resumed |
| --- | --- | --- | --- | --- | --- | --- |
| idle at prompt | feasible if transcript absent; warm up after injection | same | same | feasible | feasible if owning socket exists | feasible |
| foreground tool | N/A: tool call requires transcript | same | same | feasible | feasible if owning socket exists | feasible; pull after tool completes |
| background tool/wait | N/A: tool call requires transcript | same | same | feasible | feasible if owning socket exists | feasible; pull while background task waits |
| startup, before initialized | feasible; enqueue before attach/readiness | feasible; enqueue before attach/readiness | feasible; queue until explicit pull | feasible; recover owning route after readiness | feasible if owning socket exists | feasible; queue until explicit pull |
| immediately after /mcp reconnect | N/A: reconnect requires an established session | same | same | feasible; gate initialized on replacement adapter | feasible if owning socket exists | feasible; queue until explicit pull |

Thus **126 feasible cells**, **54 structurally N/A**. An unavailable owning socket,
unrecognized host UI, missing readiness signal, authentication failure, or failed
state setup is a FAIL with evidence, never a PASS or a fabricated record. Photo
cells exercise attachment metadata and delivery, not Telegram download or vision.
Voice uses deterministic local transcription through the real scheduler, durable
placeholder and resolve path; it does not test an STT provider.

For every feasible channel/inbox cell: each injected source is received exactly
once, the host intake receipt is recognized within the 15-second attempt window,
there is no fallback attempt and no false Held notice, and all final revisions
retire. A text/photo burst normally has one debounced attempt with two members.
Voice resolutions may schedule separate attempts: each final revision still has
exactly one attempt. No live attempt may include a pending voice placeholder.

For every feasible fetch cell: rows remain durable until an explicit fetch and
its successful tool-result receipt. A returned result alone is not receipt
evidence. The observation window begins at the fetch, not at inbound arrival.
The harness pauses the host tool-result on the way back to Claude and checks
that the queue is still populated before releasing it. Pending voice placeholders
are allowed on fetch; this matrix pulls after revision, so each source is received
once. Unit tests cover confirmation of an obsolete voice revision.

Startup and reconnect must show no live reservation before initialized is
released. A queued startup row can legitimately be Held before a session is ready;
it must not be counted as Held while attempting. The harness records these phases
separately. The foreground sleep exceeds the live window, so accepting only the
eventual turn record cannot accidentally pass. Background setup must be evidenced
by a Bash tool call with run_in_background and a running Python process.

Intake records and rendered turns are distinct evidence: enqueue + queued_command
for one delivery are not two deliveries. Count user turns or queued_command
attachments carrying each token/source, deduplicating repeated record UUIDs;
also retain enqueue evidence for the receipt deadline. Never count assistant
quotes or queue-operation/remove as another delivery.

## Fixture provenance and gaps

* 2.1.263: captured channel user turn (predates token metadata) and peer user turn.
  The tokenless channel record is a negative receipt fixture.
* 2.1.266: captured channel enqueue, remove (negative), and queued_command
  attachment. The bare cross-session enqueue lacks peer provenance and is negative.
* TODO: capture the actual 2.1.266 inbox mid-turn record from the harness. The
  incident says the shape differs; it does not specify a verified JSON shape.
* TODO: collect version-specific inbox background/startup/reconnect, channel
  background/startup/reconnect, and fetch tool-result records. No inferred peer
  intake records are promoted to verified fixtures.

`--collect-only` records shapes and setup failures without asserting delivery
success. Its report says COLLECTED, never PASS. `--fixtures` exports sanitized
records plus per-record expectations for review; unknown shapes remain TODO and
must be classified from actual evidence before entering positive unit coverage.
Do not run collection as a coding-lane acceptance check.

## In-process compatibility coverage

The injection tests also cover legacy transcript-confirming adapters. A delayed
wire write/receipt keeps durable rows out of Held notices. A receipt for a known
expired attempt leaves them available for exactly one fetch, and duplicate or
post-fetch receipts cannot retire them again. Legacy hosts that declare only
submission acceptance keep their historical timing; the transcript deadline is
not applied to their potentially longer host submission. If bounded legacy
attempt history is absent, the existing exact covered-row correlation remains
authoritative. No new legacy fallback ladder is introduced.

The current baseline consumes fetch rows when returning them and has no fetch
receipt token. The live harness intentionally reports FAIL for that behavior
against the stronger tool-result receipt contract above. That is an uncovered
implementation phase, not a missing JSON shape that a fixture may invent.
