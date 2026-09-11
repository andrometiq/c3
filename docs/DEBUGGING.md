# Debugging

## Legacy attempt diagnostics

For legacy connections the broker keeps a memory-only shadow of tracked delivery
attempts; that shadow does not decide delivery, retirement or adapter
behaviour. Phase 5 uses its identities to exclude in-flight rows from notices and status. Negotiated entries in the same table have authority, as described below. A disagreement
between an attempt's surviving member IDs and an authorised retirement logs
`attempt shadow DIVERGED token=… route=… expected=[…] removed=[…] reason=…` once
per retained token. The table keeps at most 128 entries per route and 4,096 globally,
evicting oldest terminal entries first and skipping new observations if only active
entries fill a cap. Channel observations expire lazily after 15 seconds with reason
`unobserved`; this is not a legacy host termination. Notice counts keep an
unobserved live push excluded while its holder still reports a live/probing
route, including a legacy inbox fallback on the same token. A terminal queue-only
report, receipt, release or identity reconciliation resolves that uncertainty. Legacy fetch
opens and immediately confirms a `fetch` attempt labelled `legacy_consume`; fetch
receipts use negotiated group entries in phase 4. The legacy shadow adds no wire behavior.


## Delivery notices and operator logs

A negotiated delivery logs these metadata-only lines (no message content):

```text
delivered chan=telegram topic=281 msg=42 to cli=claude conn=7 transport=channel token=TOKEN
attempt confirmed token=TOKEN route=-100/281 ms=100
attempt retired n=1 token=TOKEN
```

`transport` is `channel`, `inbox` or `fetch`. `delivered` means the frame was
written to the adapter, not that the host recorded it; `attempt confirmed` records
the validated receipt, and `attempt retired` follows successful durable removal.
Existing `attempt reserved transport=… members=… budget_ms=…`,
`attempt finished transport=… outcome=…` and `attempt exhausted: …` lines remain.
A fetch group uses the same token on each route; its confirmation and retirement
lines are per route. Storage failures log retries without claiming retirement. After exhausted storage
retries the route says `receipt recorded; queue retirement failed`, rather than
claiming no receipt.

Held counts only queued rows after scheduling, excluding open attempts, fetch
groups and observed receipts. A channel timeout → inbox fallback → confirmation
should produce no Held and one calm route line after 60 seconds of stability.
State and reason changes restart that timer; confirmation age does not.
Age eviction logs `queue eviction chan=… chat=… topic=…: expired=N over_count=M`;
only count overflow says `queue full`. Both notices state the remaining held count.
Use `/status` or `c3-broker status` to inspect route history and the session build.

After `c3:update`/`c3:build`, check the session build: supported sessions self-upgrade.
If a session still reports the old build, reconnect c3 with `/mcp` in that open
session or restart it. `/reload-plugins` does not restart its MCP server process.

## Inbound delivery

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
- `pull-only (<reason>)`: no live transport is eligible or a cycle exhausted. Exhaustion
  says `no receipt on channel or inbox; retries on reconnect, attach or new messages after 60 s`.
  Previous transport confirmation remains visible as history.

A fetch never proves live transport. Held and attach backlog counts omit rows
inside open attempts. After channel expires, a new inbox attempt covers only the surviving batch
with a new token and fresh 15-second budget. After exhaustion, rows become visible again. Inbound
at +30 seconds does not retry; inbound at or after +60 seconds rearms a cycle.
Reconnect, explicit attach, changed capability facts, and a confirmation on a
sibling route also rearm. Route notices use the 60-second stable-state timer described above.

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

## Host receipt shape drift

The first confirmed delivery in a session logs, for example:

```text
first delivery confirmed by record=queue-operation/enqueue (host 2.1.266)
```

Other recognized types are `user`, `attachment/queued_command` and
`user/tool_result` for fetch. Host version comes from MCP initialization; an
unavailable or invalid version is `unknown`. The adapter logs this hint once
when three consecutive live attempts expire with zero recognized matching
records while their transcript grew:

```text
receipt shapes may have changed (host 2.1.266): run scripts/live-matrix/run.sh --collect-only --fixtures
```

`c3-broker status` includes the same hint for the affected session. It is a
possible host-format change, not proof of one. A recognized live receipt, no
transcript growth or a definitive failure breaks the expiry streak; fetch
expiries do not count. The alarm does not change delivery or loosen receipt
validation. Reconnect and compatible self-exec preserve diagnostics. Follow the
[release acceptance checklist](TESTING-LIVE-MATRIX.md#acceptance-for-a-release)
and review captured fixtures before extending receipt shapes; collection is not
an acceptance PASS.

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

The full specification and [Acceptance for a release](TESTING-LIVE-MATRIX.md#acceptance-for-a-release)
checklist are in TESTING-LIVE-MATRIX.md. The authoritative protocol contract is
[Inbound delivery](ADAPTERS.md#inbound-delivery).
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

### Fetch receipt attempts

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
shared visitor reader discards oversized records across bounded polls. The store reports `aged` and `overCount` separately; eviction notices distinguish
age expiry from count overflow.

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

### Controlled restart waiting on prompts

A C3-controlled restart can wait 60 seconds for open questions and permission
relays, then spend up to five seconds attempting cancellation notices. It holds
the singleton until exit; the broker watchdog is 90 seconds, armed at first
intent even during blocked channel startup. The client exit wait is deliberately
91 seconds, measured from its own request rather than from the broker's later
intent, so it normally outlives that watchdog. Repeated restart
requests do not reset the clock. Notification failures log the kind, existing ID
and original route, without prompt bodies. A missing notice does not mean the
relay remains answerable: cancellation is local and there is no later retry.
Check the requesting session, especially for a permission still waiting at the
laptop. Socket write success alone does not establish host acceptance.

The forced-exit log is:

`c3-broker: controlled restart exceeded 90s; forcing exit. Some request results or cancellation notices may not have been delivered. Check the requesting session.`

An unsupported old broker is left running and the restart command reports that a
manual restart is needed; it does not fall back to SIGTERM. An exchange failure
or old-process exit timeout prevents successor startup. External SIGTERM/SIGINT
preempt an ongoing controlled drain and take the immediate shutdown path with
its 15-second watchdog. Configuration restart failure also makes `setup finish`
fail without printing “Setup complete”. Post-cap permission taps report cancellation
only for relays actually cancelled at the cap; already-resolved and unknown IDs
retain the ordinary inactive feedback. New relay refusal says the request
“may still be waiting at the laptop”; C3 cannot establish its current host state. See
[Updating C3](USAGE.md#updating-c3) for the scope and limitations.
