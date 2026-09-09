# Debugging

## Attempt shadow (phase 1)

For legacy connections the broker keeps a memory-only shadow of tracked delivery
attempts; that shadow does not decide delivery, retirement, status, or adapter
behaviour. Negotiated entries in the same table have authority, as described below. A disagreement
between an attempt's surviving member IDs and an authorised retirement logs
`attempt shadow DIVERGED token=… route=… expected=[…] removed=[…] reason=…` once
per retained token. The table keeps at most 128 entries per route and 4,096 globally,
evicting oldest terminal entries first and skipping new observations if only active
entries fill a cap. Channel observations expire lazily after 15 seconds with reason
`unobserved`; inbox fallback cannot be distinguished on this baseline. Legacy fetch
opens and immediately confirms a `fetch` attempt labelled `legacy_consume`; fetch
receipts use negotiated group entries in phase 4. The legacy shadow itself adds no wire or display behavior.


## Negotiated channel and inbox delivery (phase 3)

A valid `delivery` offer, `hello_ack.delivery` acknowledgement and the adapter
confirmation `delivery_report{accepted:[...]}` select the broker-owned attempt
path. Missing confirmation means legacy, including degraded brokers. Ready
Claude sessions with readable transcripts can use fetch receipts when neither
live transport is eligible. Use `attach`, `/status`, or
`c3-broker status` to inspect broker-derived route state; the adapter preamble
shows the same state.

- `waiting`: channel or inbox is eligible, with no confirmation on this route yet.
- `live: channel, confirmed <age>` or `live: inbox, confirmed <age>`: the host
  transcript confirmed the named transport.
- `pull-only (<reason>)`: the confirmed transport is ineligible or a cycle exhausted. Exhaustion
  says `no receipt on channel or inbox; retries on reconnect, attach or new messages after 60 s`.
  Previous transport confirmation remains visible as history.

A fetch never proves live transport. Held and attach backlog counts omit rows
inside open attempts. After channel expires, a new inbox attempt covers only the surviving batch
with a new token and fresh 15-second budget. After exhaustion, rows become visible again. Inbound
at +30 seconds does not retry; inbound at or after +60 seconds rearms a cycle.
Reconnect, explicit attach, changed capability facts, and a confirmation on a
sibling route also rearm. There is no phase-5 flap timer yet.

Diagnostic lines are deliberately generic:

- `attempt result ignored: authority, deadline or open membership mismatch`:
  one no-op for stale, forged, late or already-closed evidence; no row is consumed.
- `attempt reserved transport=inbox members=N budget_ms=15000`: a fresh inbox attempt.
- `attempt finished transport=inbox outcome=confirmed` (or `failed` / `expired`):
  serialized terminal outcome; the inbox write is bounded to 2 seconds.
- `attempt exhausted: no receipt on channel or inbox; route paused`: all eligible transports failed or
  their authoritative reservation deadlines expired; rows remain available for fetch.
- `attempt retirement retry: storage write failed; evidence retained`: receipt
  arrived in time, but removal failed. Three bounded removal tries preserve evidence.
- `attempt retirement released: storage retry limit reached`: no retirement was
  reported; surviving rows remain queued and a later delivery can duplicate them.
- `protocol gate REFUSED op=attempt_result`: incompatible protocol version.

A socket reconnect of the same living process adopts unexpired live attempts
without resending; fetch attempts release, and process death releases them. Removing the final member through drain,
eviction or revision reconciliation releases the slot without proving transport.
Drain imports retain durable `origin:"drain"` provenance and are never live pushed.
The legacy suite continues to assert `attempt shadow suite divergences=0`.

`delivery eligibility available: reconnecting to offer delivery` means a legacy
connection gained an eligible transcript or inbox. This automatic re-hello is
limited to once per fact change and once per 10 seconds, after any active legacy
push's ack or expiry; the existing reconnect path transfers its claims.

Host readiness is a capability fact: `notifications/initialized` must arrive
and the notify transport must exist before either live transport is eligible.
The startup hello carries no delivery offer even if a transcript already exists;
the bounded eligibility re-hello makes the offer after the MCP handshake.
Definite notify failures immediately send `attempt_result` with `outcome:"failed"`
and a generic reason (`notify transport unavailable` or `channel notify write failed`),
without waiting for receipt polling or the 15-second deadline.

## Live matrix

The test injection hook is available only in a deliberately opted-in scratch
broker. Normal daemon startup never enables it, even if the test environment
variable is present. The `test_inject` socket operation refuses otherwise.

```sh
# Choose a NEW directory beneath an existing private scratch parent.
bin/c3-broker test-serve --allow-test-inject --state "$PWD/scratch-broker"
# In another terminal; socket selection is mandatory and never auto-spawns:
bin/c3-broker inject --socket "$PWD/scratch-broker/c3.sock" --topic 42 --text 'matrix sample'
bin/c3-broker inject --socket "$PWD/scratch-broker/c3.sock" --topic 42 --text 'matrix voice sample' --voice --voice-delay-ms 1000 --count 2
```

`C3_ALLOW_TEST_INJECT=1` is equivalent to the flag on `test-serve` only. The
scratch command refuses an existing state directory and creates private config,
queue, log, and socket paths. It loads no production mappings, Telegram channel,
credential, update checker, or STT provider. Attach with `channel=test-inject`,
`topic_id=42` (the synthetic group's chat id is -1).

Injection runs the shared channel gate and `BrokerHost.Emit` path. Its JSON
response says **accepted**, meaning worker admission; durability follows through
the ordinary append and can fail independently. `--count 2` submits the whole
burst within debounce; `--photo` supplies attachment metadata, and `--voice`
uses a deterministic local transcription hook through the real voice scheduler.
There is no media download. All synthetic rows have `TestInjected=true`, channel
frames have `c3_test_injected="true"`, and the channel namespace is `test-inject`.
Replies, Held/route notices, edits, and voice echoes go to `TEST SINK` log lines.
No injected route resolves to Telegram. Treat log contents as local test data.

The full specification is [TESTING-LIVE-MATRIX.md](TESTING-LIVE-MATRIX.md).
The maintainer runs `scripts/live-matrix/run.sh` outside the coding sandbox;
see `scripts/live-matrix/README.md` for collection, fixtures, isolation and timing.

With injection enabled, any same-UID process with access to the private socket
can invoke the hook, matching the broker socket's existing trust model. The flag
is not an additional caller-authentication boundary. The harness isolates its
configuration and paths; it does not confine Claude's filesystem access.

For a bounded channel/inbox text sample, run:

```sh
scripts/live-matrix/run.sh --cell '[ci]*-[ifs]*-*-text-single'
```

The driver reports 12 matched cells, 10 feasible, 2 N/A and **16 Claude sessions**
(including the six resume seeds). `--list` applies the same filter without launches.

### Fetch receipt attempts (phase 4, D036)

Negotiation starts after `delivery_report{accepted:[...]}`, immediately following
`hello_ack`. Until it arrives, legacy pushes continue. An unaccepted mode logs
`delivery negotiation protocol error: unaccepted mode; legacy connection`.
Mixed builds without confirmation therefore remain usable through legacy delivery.

For accepted `fetch_receipt`, returning the MCP response reserves rows; it does
not consume them. The final `[C3_FETCH_RECEIPT_V1]` trailer contains the group
and all record/revision pairs. A complete successful matching host tool result
must retain this trailer with nothing after its closing delimiter. Missing host
`claudecode/toolUseId`, unavailable transcripts, incomplete trailers and error
results leave rows durable. They become fetchable again at the 60-second deadline.
The group result fans out to route workers; changed holders and revised voice rows
cannot consume another member's current content. `fetch_confirm` is the migration
alias for the same group evidence, bound to the original connection.

Attach reports messages fetchable now; Held and backlog exclude open fetch
attempts. Fetch confirmation never updates live transport proof or age. The
shared visitor reader discards oversized records across bounded polls. The store
now reports `aged` and `overCount` separately, while eviction notice wording is
still the phase-5 follow-up.

In-process fixture coverage includes `TestFetchGroupLifecycleIndependentRoutes`,
`TestFetchGroupRevisionEvictionDrainAndExpiry`, `TestFetchReceiptMultiRouteIPCConfirm`,
`TestFetchTrailerGrammarAndToolResult`, and `TestFetchReceiptBrokerAdapterToolResult`.
These tests isolate PATH and inject installed-adapter discovery when constructing
brokers. They use synthetic transcripts and, where needed, test-owned Unix sockets
in private temporary directories; they do not verify a machine-installed adapter
or a live host. Setup-coverage tests guard both broker and adapter constructors.
See `TESTING-LIVE-MATRIX.md` for maintainer-run verification; fixture absence is
not evidence of host success.

## Adapter upgrades
The broker bounce at the end of `/c3:update` and `/c3:build` triggers a new hello.
`c3-broker status` lists session builds and marks builds differing from the
readable installed Claude adapter as `(stale)`. An unreadable binary never
produces a guessed upgrade hint.
Look for these metadata-only lines:
- `upgrade hint sent conn=… from=<build> to=<build>`
- `adapter upgrade: exec <path> (build …)`
- `adapter resumed after upgrade (build …)`
- `upgrade postponed: <reason>`
- `upgrade fallback notice sent conn=…`
A postponed upgrade leaves the process serving its existing MCP connection.
After three drain windows, or on an incompatible contract or unsupported
platform, reconnect c3 through `/mcp`. The new process restores SDK initialization
locally; no initialize exchange or instructions resend is expected. Resume
bypasses the 60-second startup watchdog, including when the host remains idle.
The gate covers self-exec only; broker shutdown still cancels broker-side calls.
