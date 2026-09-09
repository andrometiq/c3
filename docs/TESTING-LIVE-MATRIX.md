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
  attachment. The bare cross-session enqueue is also verified peer intake: its opening C3
  source, token and attempt bind the receipt without user-turn provenance.
* 2.1.266 peer intake: the captured bare enqueue and queued_command with nested
  peer origin/isMeta are verified positives; its queue removal is negative.
  These records are folded into the version directory and collector classifier.
* TODO: collect version-specific inbox background/startup/reconnect, channel
  background/startup/reconnect, and fetch tool-result records. No inferred peer
  intake records are promoted to verified fixtures.

`--collect-only` records shapes and setup failures without asserting delivery
success. Its report says COLLECTED, never PASS. `--fixtures` exports sanitized
records plus per-record expectations for review; unknown shapes remain TODO and
must be classified from actual evidence before entering positive unit coverage.
Do not run collection as a coding-lane acceptance check.

## In-process compatibility coverage

The injection tests also cover legacy transcript-confirming adapters. Legacy
acks retain master's exact identity/holder retirement semantics even after the
broker's observational deadline expires. In particular, channel timeout followed
by the adapter's inbox fallback reuses the same token with a fresh adapter window;
that valid receipt retires the row once. Only negotiated attempts have broker-owned
expiry rejection. Duplicate and post-fetch receipts cannot retire a row twice.
Held notices exclude surviving attempting identities. A queue read error is logged
and uses the cached count, or explicitly reports that the count is unavailable.

Phase 4 implements `fetch_receipt`. The collector takes the expected group and
record/revision set from the held MCP response, then requires that exact complete
trailer in the successful matching host tool result. `records.expect.json` now
carries `transport:"fetch"`, `tool_use_id`, `token`, `members` and an explicit
accept/reject expectation per captured record. Sanitization preserves distinct
member identities as ROW1, ROW2, etc. No inferred host records are checked in.

Fetch cells require fetch attempts (no live transport), group confirmation and
retirement inside 60 seconds, durable rows while the result is held, a complete
matching final trailer, and one occurrence of each injected source. Missing,
truncated, wrong-token/member or trailing-text trailers fail. The corpus test
executes these sidecars through the same visitor-backed fetch receipt predicate.
The coding lane runs collector unit tests only; a maintainer must run this matrix
and review/export actual host fetch fixtures before claiming live acceptance.
