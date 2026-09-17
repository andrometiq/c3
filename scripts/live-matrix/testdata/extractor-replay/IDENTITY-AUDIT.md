# Phase 2B occurrence identity audit

The common join is `Assembler.identities()` in `capture_context.py`. It accepts
supplemental and observed identity dictionaries, compares overlapping fields,
and returns the observed values on disagreement. Member pairs and source sets
use unordered equality. Disagreement emits `delivery evidence identity mismatch:
<field> correlation conflict` at the artifact's physical line/block and records
a failed correlation. `finish()` cannot restore that event's completeness, and
the top-level observation becomes incomplete. A rejected extractor candidate
still produces no successful host milestone.

The table lists every site that supplies canonical identity from an observer
table or correlates it with an occurrence. Regression names below omit the
`occurrence-` prefix unless stated otherwise.

| Assembly site | Occurrence authority and supplemental limits | Frozen contradictions |
| --- | --- | --- |
| `scope()` | An occurrence's run, route, host session, broker session, connection epoch and claim generation override the artifact owner only with a failed-correlation diagnostic when different. A subsequent principal transcript/session or route claim takes precedence over either observer value. | `scope-run_id`, `scope-route_id`, `scope-host_session_id`, `scope-session_id`, `scope-connection_epoch_id`, `scope-claim_generation` |
| `prepare()` / `members()` / `assemble_rows_injection()` persistence | The raw row ID and exact revision select the supplemental source mapping; lookup never substitutes a different row/revision. If a persistence occurrence itself carries source IDs, they survive conflicting earlier observations of that same pair. Unknown pairs retain their row/revision with unavailable source membership. | `persisted-source`, `held-row`, `held-revision`, `recorded-row`, `recorded-revision`; existing `stale-retired-revision`, `wrong-retired-row` |
| `event()` | Supplemental attempt/group/member bindings are reconciled with observed fields, including receipt observations even though they do not require an extra binding. `classify()`'s actual attempt label becomes `attempt_id`; its token and transport remain raw. Group is supplied only where the occurrence has no group ID. Fetch member sets are compared against the occurrence binding, independently of host-versus-held classification. | `channel-attempt`, `inbox-attempt`, `host-token`, `host-transport`, `receipt-attempt_id`, `receipt-group_id`, held/recorded row and revision cases |
| `assemble_driver()` | `host_session_ref` is an observed session-selection identity and cannot be replaced by ownership scope. Proof still checks the original disagreement and observed transcript extent. `resume`/`new` is an operation enum, not a delivery operation ID. | `selected-session` |
| `assemble_rows_injection()` admission | Injection source IDs and their exact broker admission lines must agree with `TEST INJECT` source IDs; broker topic must agree with occurrence route ownership. Neither source counts nor sample text supply identities. | `broker-source`; existing duplicate-source and missing/malformed broker cases |
| `assemble_fetch()` request | The recorded host tool-use UUID, call ID, session and arguments must agree with the driver association. Raw UUID/call disagreements get a diagnostic at the host record even when the request is rejected. The driver RPC frame ID remains a separate domain value associated with that call, never equated to it. | `call-uuid`, `call-operation`; host session and scope cases exercise the shared session join |
| `assemble_fetch()` held response | The real `fetch_trailer()` provides token and exact row/revision pairs. The response RPC ID must agree with the driver request before supplying its associated canonical operation ID. Binding rows cannot override trailer members, and a group binding cannot override the reservation token. | `held-request`, `held-row`, `held-revision`; existing `wrong-held-response-token` |
| `assemble_fetch()` release / `bound_operation()` | Release artifact, RPC request and host operation identities must agree with the actual held response and recorded call. Broker records may borrow the operation only through matching attempt/group association because `TEST ATTEMPT` has no operation field. | `release-artifact_id`, `release-request_id`, `release-operation_id`, `receipt-operation_id` |
| `assemble_broker()` | `attempt_events()` supplies token, transport and topic. Topic replaces only the topic component of the supplemental route; disagreement fails. Attempt/group, row/revision membership and retired membership are absent from this grammar and remain independently observed facts, with counts checked only as counts. | `broker-token`, `broker-transport`, `broker-topic`; existing terminal-token and retirement cases |
| `assemble_host()` live delivery | Real `classify()` supplies attempt/token/transport. Transcript `sessionId` (or `session_id`) takes precedence; conflicting aliases fail. Raw message/merged-message IDs replace supplemental source claims. Present chat/topic attributes replace their respective route components. | `channel-attempt`, `inbox-attempt`, `host-token`, `host-transport`, `host-session`, `host-session-alias`, `host-source`, `host-chat`, `host-topic` |
| `assemble_host()` fetch result | The real parsed trailer and raw tool-use ID are compared before accepting a milestone. A parseable but rejected candidate retains its raw contradiction and locator. Accepted raw member order is normalized only using independent row observation order. | `recorded-row`, `recorded-revision`, `recorded-token`, `recorded-operation`; existing duplicate/stale trailer cases; healthy member permutation |
| `assemble_receipts()` | The receipt occurrence directly carries attempt/group/token/transport/operation and row/revision pairs. `event()` receives these observed values rather than assigning supplemental attempt/group values afterward. Reservation comparison preserves any conflicting observed claim. | `receipt-attempt_id`, `receipt-group_id`, `receipt-operation_id`; existing wrong broker session and reused-PID epoch cases |
| `assemble_samples()` | Barrier IDs, stream cutoffs, snapshot IDs, raw queue rows and contract barrier references come from their physical observer records. Ownership scope goes through the same join; row/revision pairs go through `members()`. Notice names derive from the explicit event reference, not text or a source count. | All scope/row domains above; existing `false-held`, `premature-fetch-consumption`; cut regressions below |
| `finish()` | A uniquely associated reservation is an independent identity check, never a source for replacing an event's attempt/group/token/transport/operation/member/scope claims. Failed correlations remain failed after checked-read completeness is known. | All `occurrence-*` cases assert collection incomplete and the precise conflicting domain before verdict assertions |

Observer-only identities need no invented principal field. `TEST ATTEMPT` does
not carry broker session, connection epoch or generation; their mutation
captures change the occurrence-specific observation in `ownership.json`, leaving
the artifact owner unchanged. PID and peer process-start observations are not
connection epochs. The closed broker/table schemas are unchanged.

Identity domains also remain separate: `c3_delivery_id` and the trailer's
`group` field are receipt **tokens**, not canonical delivery IDs or observer
group IDs. Transcript UUID is the raw `host_record_id`; observer event/delivery
IDs, stream/clock IDs, snapshot IDs and artifact inventory IDs name different
entities and are supplied only by their respective observer records. UUID
re-observation/conflict checks and the unchanged evaluator's occurrence-integrity
checks still distinguish two host deliveries; existing repeated/duplicate UUID
and explicit collapse regressions remain active.

Cut validation is separate from identity correlation. Every driver cut must
name owned streams and fit checked stream extents; a final cut must cover the
full extent. `short-final-cut`, `short-injection-cut`, `short-held-cut` and
`early-final-cut` assert their raw cutoff, actual host extent, exact driver-line
diagnostic, barrier incompleteness and top-level incompleteness. The harmless
classifier-diagnostic exception cannot hide a failed barrier.

For the original 50 captures, reviewed observation changes are restricted to
the corrected attempt IDs, artifact digests, correlation/completeness flags and
stream diagnostics. All original classifications and mandatory mutation reasons
are retained. Three rejected fetch mutations add `evidence collection incomplete`
for their newly diagnosed occurrence conflicts. Every frozen capture still runs
raw replay, canonical export and fresh-process sanitized raw replay.
