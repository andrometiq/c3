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
receipts join the table in phase 4. The legacy shadow itself adds no wire or display behavior.


## Negotiated channel and inbox delivery (phase 3)

A valid `delivery` offer and `hello_ack.delivery` acceptance select the broker-owned channel/inbox
attempt path. A missing acceptance means legacy, including degraded brokers
and Claude sessions with neither eligible transport. Use `attach`, `/status`, or
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

A socket reconnect of the same living process adopts unexpired attempts without
resending; process death releases them. Removing the final member through drain,
eviction or revision reconciliation releases the slot without proving transport.
Drain imports retain durable `origin:"drain"` provenance and are never live pushed.
The legacy suite continues to assert `attempt shadow suite divergences=0`.

`delivery eligibility available: reconnecting to offer delivery` means a legacy
connection gained an eligible transcript or inbox. This automatic re-hello is
limited to once per fact change and once per 10 seconds, after any active legacy
push's ack or expiry; the existing reconnect path transfers its claims.
