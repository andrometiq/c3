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
should produce neither Held nor an automatic route diagnostic. Telegram emits
one quoted Held per backlog episode; pending voice work uses its quoted readback
with a short held-count suffix when needed. Drained rows are never quoted.
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
sibling route also rearm. The former 60-second stable-state timer for chat route
notices has been removed; local status and the MCP preamble retain route diagnostics.

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
Replies, Held notices, edits, and voice echoes go to `TEST SINK` log lines.
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

## Auto-mode approval

The broker logs every request's lifecycle as metadata only: request id, route, tool name, input hash, the tapping user's id and a cause. **Tool input is never logged**, whether the request succeeds or fails. That is stricter than the content policy in the top-level [`DEBUGGING.md`](../DEBUGGING.md#content-policy), and for this feature it wins.

```text
auto-approval created id=ID route=-100/281 tool="Bash" hash=HASH actor=0 detail=""
auto-approval card-sent id=ID route=-100/281 tool="Bash" hash=HASH actor=0 detail="msg=42 state=pending"
auto-approval tap-allow id=ID route=-100/281 tool="Bash" hash=HASH actor=USER detail=""
auto-approval consumed id=ID route=-100/281 tool="Bash" hash=HASH actor=0 detail="tool_use_id=TOOL_USE_ID"
auto-approval no-card session="SESSION" tool="Bash" hash=HASH classifier_reason="[…]" cause="unknown reason"
```

- **Lifecycle events:** `created`, `attachment-sent`, `card-sent`, `tap-allow` / `tap-deny`, then the state reached: `approved`, `armed`, `consumed`, `expired`, `denied`, `timed_out`, `cancelled`, `vetoed-after-allow` or `allow-unconfirmed`.
- **`tap-<verb>-refused`** means the tap changed nothing. Its `detail` gives the reason: `not an operator`, `not active`, `already decided`, `card not recorded yet`, or `wrong chat, topic or message`.
- **`cancelled`** details: `hook gone`, `session detached`, `adapter disconnected`, `session no longer holds the route`, `feature disabled`, `card not sent`, `no hook could be told` and `broker stopping`. `no hook could be told` (sometimes followed by the last write error) means the grant was armed but no waiting hook received `armed` and none was left to receive it; one hook's failure never cancels a grant another hook was told about.
- **`card-failed`** carries the channel error. A `send aborted, delivery unknown` failure may still have reached Telegram. **`attachment-orphaned msg=N`** means an overflow file was posted but the request ended before its card was sent: the input is in the topic with no card, and you may want to delete it.
- **`no-card`** is logged when a denial gets no card while the feature is on. Its `classifier_reason` is the denial's reason, not tool input, and is logged so an unrecognised denial form can be seen. The causes are `unknown reason`, `no single live adapter for the session`, `no unique confirmed Telegram route`, `max_pending reached on route …`, `grant already armed for this call`, `vetoed after allow`, `hook budget exhausted`, `malformed frame`, `missing or mismatched call context` and `tool_input is not strict JSON`. With the feature off there's no line.
- **`vetoed-after-allow`** means Claude Code denied the very call (same `tool_use_id`) that a grant allowed, after the `PreToolUse` hook confirmed with a `grant_delivered` frame that it had printed the `allow`. Claude Code no longer lets a hook `allow` override the auto-mode classifier: turn the feature off. No new card is posted.
- **`allow-unconfirmed`** means the broker consumed a grant but never received `grant_delivered` for that call within one second, and the same call was then denied again. C3 can't tell whether the allow reached Claude Code (usually it didn't: the hook ran out of its 300 ms budget), so it is not reported as a veto. The old card says so, and the new denial gets a fresh card, so the operator can approve again. A denial that arrives while the confirmation wait is still open waits for it (up to one second) before being classified. A hook binary older than the `grant_delivered` frame never confirms, so with one, a re-denial always gets a fresh card rather than a veto label.

**No log line at all after a denial** means the hooks never reached the broker. Check, in order:

1. `c3-broker --help` lists `pretooluse-hook` and `permission-denied-hook`. If it doesn't, the binary predates the feature: update it.
2. The plugin is current (`/plugin`), so `hooks/hooks.json` registers `PreToolUse` and `PermissionDenied`.
3. The running broker is the installed build (`c3-broker status`). A broker older than the hooks answers their opening frame with `expected hello first` and registers nothing, so restart it.
4. The platform isn't Windows, and the broker runs as the same user as Claude Code. The hooks refuse to talk to a broker running as anyone else.

### Hook and binary versions (maintainers)

`plugins/c3/hooks/hooks.json` registers both hooks behind a shell guard:

```json
"PreToolUse":       [{"matcher": "*", "hooks": [{"type": "command", "command": "c3-broker pretooluse-hook || exit 0", "timeout": 5}]}],
"PermissionDenied": [{"matcher": "*", "hooks": [{"type": "command", "command": "c3-broker permission-denied-hook || exit 0", "timeout": 330}]}]
```

Plugin files reach users through the Claude Code marketplace; binaries reach them through `c3-broker update` or a tarball. Either can be newer than the other.

- **Why the guard exists:** exit code 2 from a `PreToolUse` hook blocks the tool call. A `c3-broker` that lacks these subcommands exits 2 (unknown subcommand), which would block every tool call. The `|| exit 0` guard turns that, a missing binary, or any other failure into a silent exit 0. Binaries that have these subcommands also exit 0 silently for any unknown `*-hook` subcommand, so a hook added later is tolerated even without the guard.
- The guard needs a POSIX shell: Windows PowerShell 5.1 can't parse `||`. The feature is unsupported on Windows.
- Hook connections open with a `hook_hello` frame instead of `hello`. A running broker that predates it answers `expected hello first` and registers nothing, so the hooks stay silent until the broker restarts on the new binary.
- **Release order:** ship the binaries with, or before, the `hooks.json` change. The guard makes hooks-first safe (the hooks stay silent), but it's a safety net, not the release plan. Bump the version in `plugins/c3/.claude-plugin/plugin.json` in the release commit so marketplace clients pick up the new hooks.
- `cmd/c3-broker/hooks_contract_test.go` checks three things: every registered hook names a subcommand the binary handles; every `PreToolUse` hook carries the guard; and the guarded commands exit 0 with no output when `c3-broker` fails with exit 2 or is missing. Keep it green when adding a hook.
