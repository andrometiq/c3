# Frozen extractor replay, context version 1

All identifiers, timestamps, process observations and responses here are
invented. `provenance.kind` is `raw_replay`, with synthetic build metadata.
These files test evidence interpretation, not broker delivery execution or a
particular host version. Nothing in this corpus establishes new live observer
coverage. The live collector cannot accept an authored context through its
API or aggregate evidence dictionary.

Run from the repository root, without a network, host CLI, or broker:

```sh
python3 -m unittest discover -s scripts/live-matrix -p 'test_*.py'
go build ./...
```

Python discovery is a separate required gate: `make ci` does not run it.
Run normal Go tests in an environment that permits their local sockets.

## Raw grammars and observer boundary

| Artifact | Grammar / provenance |
| --- | --- |
| `broker.log` | `internal/broker/test_inject.go`: `testInject`, `logTestAttempt`, test sink reply/edit logging. Standard log timestamp prefixes are optional. Attempt counts are diagnostic, never membership. |
| `adapter.log` | Opaque ordinary adapter diagnostics; this control records initialization only. It supplies no receipt by itself. |
| `records.jsonl` | Reviewed Claude channel and peer envelopes represented by `collect.classify`, and tool-use/result records consumed by `classify_fetch`; corresponding receipt shapes live in `cmd/c3-claude-adapter/testdata/`. |
| `held-fetch/response-1.json` | Raw MCP JSON-RPC `result.content`, as captured by `proxy.py`. The strict final trailer grammar is the one parsed by `fetch_trailer`, also emitted by the adapter's fetch handler. RPC request IDs and host call IDs are deliberately different. |
| `context/injection.json` | Injection result `message_ids` plus physical broker acceptance-line references. The IDs come from this result, never `MATRIX_SAMPLE`. |
| Other `context/` files | **Synthetic observer format defined below.** These are not current broker log fields or existing live instrumentation. |

`load_capture()` checks every inventory file and calls the same production
`capture_observation()` seam that `collect()` uses. The seam's principal read
arguments stay authoritative; supplemental context cannot replace them. The
result then goes through `build_verdict_inputs()` and the unchanged evaluator.
No replay path calls the historical witness author or patches an extractor.

`capture.json` has closed fields: `schema_version: 1`, `context_version: 1`,
requested `cell`, intended identity `evidence`, `provenance`, and `artifacts`.
Each inventory entry names an `artifact_id`, contained relative `file`, raw
`table`, `format` (`json`, `jsonl`, `log`), and `seal`. A seal records `bytes`,
`sha256`, physical `lines` (one logical record for a JSON object), and
`read_state`. Inventories cannot substitute canonical verdict inputs. Canonical
`events`, complete observations, verdicts, assertion flags and count-only
membership records are rejected by the closed schemas. Unsupported versions,
missing tables and ambiguous inventories fail incomplete.

Observer tables contain raw scalar fields and raw row/revision pairs; there are
no canonical Scope, Position, Revision, Member, Event, proof or snapshot objects.
The loader constructs those objects:

- `ownership.json`: versioned `owners` and `occurrences`. Owners associate an
  artifact and stream role with run, route, both sessions, connection lifetime,
  claim generation and PID. A PID does not name a connection lifetime. Multiple
  roles may observe the same artifact (the driver journal also records a fetch
  request). Occurrences name an artifact, physical `line`, zero-based `block`,
  observer-assigned `event_id`, optional `delivery_id`, stream and comparable
  observer clock/time. Scope fields may record an occurrence-specific owner.
  These IDs name raw occurrences, not successful deliveries or canonical events.
  A transcript's own session claim takes precedence, with a conflict diagnostic.
- `rows.jsonl`: observed durable `row_id`, scalar exact `revision`, and explicit
  `source_ids`. Revisions are observations, not hashes recomputed after redaction.
  Repeated row observations retain their physical positions. Later persisted
  revisions do not silently replace an older retirement claim.
- `attempts.jsonl`: artifact/line/block references, independent `attempt_id` and
  `group_id`, raw `rows` and `retired_rows` pairs. Tokens and transport come from
  the referenced raw occurrence. Counts in logs are checked against these lists,
  and never expand into invented members. Live-delivery attempt IDs use the
  observed `c3_attempt` label verbatim; bindings must agree with that label.
  Opaque fetch attempt IDs and independent group IDs remain observer facts.
  An unobserved deadline remains null.
- `receipts.jsonl`: explicit accepted/rejected broker observations, observed
  token/attempt/group/transport/operation, row pairs, and elapsed milliseconds.
  Each record's own physical line is its source occurrence. Acceptance and
  terminal retirement remain separate events.
- `queue-samples.jsonl`: a barrier reference, snapshot identity, `read_state`,
  and the actual raw row list, including a genuinely observed empty list.
  A notice sample also references its notice `event_id`.
- `contracts.jsonl`: observed negotiation, channel/inbox eligibility, receipt
  type, fetch policy and accepted modes at a named cut. Values do not come from
  the expected contract. Missing/invalid samples do not select weaker assertions.
- `driver.jsonl`: observed state and session-selection samples, named cuts of
  stream sequence numbers, the actual forwarded fetch request frame and its
  recorded host call association, response release, and a final observation
  window sample. No `proven` flags are accepted. Session proof is constructed
  from the observed new/resume operation, selected session and prior transcript
  extent. Cut and window evidence are independently necessary for completeness.

Rows, receipts, samples and journal entries cite their own checked artifact and
physical line; ownership occurrences and attempt rows explicitly cite their
observed target. Every resulting event retains those locators. Missing joins
produce diagnostics and incomplete evidence. Unknown host shapes and rejected
fetch trailers produce no successful host milestone. This version supports one
independently correlated fetch operation per capture; ambiguous associations
fail incomplete rather than picking one.

`Assembler.identities()` enforces occurrence authority at every identity join.
It preserves observed values and reports the conflicting field at the source
line/block. A failed correlation keeps the event incomplete through finalization;
checked bytes and seals cannot restore its completeness. See
[`IDENTITY-AUDIT.md`](IDENTITY-AUDIT.md) for every assembly site, the domains
actually present in each grammar, and the individual contradiction regressions.
Cutoffs must name known streams and fall within their checked extents; final
cuts cover the full extent. An incomplete barrier makes the observation
incomplete, including when classifier rejection diagnostics are otherwise harmless.

Checked final reads distinguish missing, unreadable, malformed, partial and
truncated data. They reject invalid UTF-8, JSON constants/duplicate fields,
non-object JSONL, invalid recognized records and unterminated tails. Diagnostics
retain physical line or byte positions without copying input secrets. Ordinary
diagnostic log lines may remain opaque. The permissive `host.read_jsonl()`
polling behavior is unchanged.

## Reviewed controls and expected observations

Five passing controls are frozen. All PASS results are exactly
`{"status":"PASS","reasons":[],"not_evaluated":[]}`.

| Control | Independently reviewed facts |
| --- | --- |
| `healthy-channel-double` | Sources 41001 and 41002, distinct rows and tokens, one accepted UUID per source; reservations at 200/300 ms, host records at 250/350, broker acceptance at 270/370, retirement at 280/380. |
| `healthy-inbox-single` | One source and row, peer origin from c3, `isMeta`, exact user-turn prefix, and owning transcript session. |
| `healthy-fetch-double` | One recorded call at 180 ms, reservation 200, produced response 220, held cut 230 with both rows, release 240, recording 250, acceptance 270, retirement 280. The held RPC ID and host tool-use ID are associated by the recorded request journal. |
| `healthy-channel-repeated` | An identical second observation of the first UUID creates no additional canonical event or delivery. |
| `healthy-fetch-permuted` | The host trailer reverses member order; classification and canonical membership remain equal to the original fetch control. |

All controls observe an empty pre-injection queue, the selected resumed session
and idle state before the injection cut, independently recorded contract samples
at injection and final, an empty final queue, and a final clock observation at
70000 ms. Every required stream has an explicit final cut. Canonical member order
uses the independent row-observation order, not pseudonym lexical ordering.

`expected-observation.json`, `expected-result.json`, `expected-diagnostics.json`
and `expected-classifications.json` are frozen review artifacts. Initial
extraction candidates were inspected separately: source/row/token/session joins,
physical locators, event roles and times, held/release ordering, cuts, queue
survivors, and every named negative discriminator were checked before copying
expectations into this corpus. Healthy timelines and negative identities are
also asserted explicitly, independently of the full JSON comparisons. No test
writes or updates these files, and no helper derives expected results.

The 84 mutation directories each retain a `mutation.json` with the passing base,
exact changed artifact files, before/after byte digests, and independently
specified mandatory reasons. Tests verify the bytes changed, the requested
scenario did not change, setup succeeded, and the full frozen reason order and
`not_evaluated` list. Logical changes have resealed artifacts. The two truncation
cases deliberately retain the original seal. Count/sample-text totals remain
unchanged in the duplicate-source substitution case.

The original 45 mutations retain all their mandatory reasons. The additions are
36 `occurrence-*` contradictions covering every audited identity domain, plus
invalid injection/held cuts and an early final cut. Each contradiction records
an independently authored raw bad value and required correlation field in its
manifest. Tests assert those values before evaluation, and assert the retained
canonical value or absent successful milestone. `short-final-cut` additionally
requires the driver line 6 diagnostic and both completeness flags to be false.

The mutations cover every Phase 2 §6.5 row: token/session/epoch/row/revision
conflicts, PID reuse, distinct-UUID duplicates, duplicate source substitution,
duplicate trailer rows, every required artifact missing independently, truncated
lines and complete suffixes, malformed numeric/required broker fields, five
host-read corruptions, conflicting UUID contents, missing/short final cuts,
premature fetch consumption and terminal retirement, both rejected peer
provenance forms, false Held, and an automatic route line. The additional wrong
recorded fetch row isolates the row-collapse regression. Identity tests assert
the observed bad canonical claim before evaluating the named failure; rejected
fetch candidates instead assert their retained raw bad trailer, rejection
locator, and absent successful recording.

To author new *raw* candidates explicitly, use a fresh destination:

```sh
python3 scripts/live-matrix/replay_fixture_support.py --write-raw /tmp/new-replay-candidates
```

This helper imports no evaluator, derivation, extractor or historical witness
module. It refuses an existing destination and never creates expectations.
Expectation review/freezing is a separate deliberate operation. Ordinary test
discovery only reads this frozen corpus.

## Three paths and the semantic comparison

Every control and mutation runs these paths:

1. Checked raw files → real classifiers/context assembly → expected observation
   and complete result comparison.
2. Shared Phase 2A identity context → sanitize the complete scenario, contract,
   observation and summary → serialize/reload → evaluate.
3. The same context sanitizes every raw artifact → recompute representation
   digests/seals → discard the maps → a fresh Python process reads the exported
   files and repeats production extraction.

Path 3 must match path 2 field-for-field, except
`observation.artifacts[*].content_digest`, which describes different serialized
bytes. **Nothing else is omitted:** identities, ownership, members, time, causal
edges, stream extents, barriers, snapshots, completeness, diagnostics and
classification decisions are compared. Derived source/occurrence projections
and the complete verdict must also agree. Original failed read states and
extent inequalities remain failed in the exported representation; unsafe raw
bytes use invalid stubs that retain their failure class and locator.

Notice barrier *names* have a core-defined derived grammar, `notice/<event-id>`.
`replay_export.py` renders that derived name using the mapped event ID while
retaining the independently mapped barrier and snapshot IDs. Raw notice samples
carry an explicit event reference so the loader reconstructs the same name.
This is a schema rendering operation; it neither repairs observed identities
nor changes `redaction.py` or the Phase 2A live exporter.

Extracted attempt labels also have a fixed grammar: `channel:N`, `inbox:N`, or
`cross-session:N`. The replay exporter presents these canonical/observer
`attempt_id` values through the sanitizer's existing literal `attempt` alias,
then restores the field name. Thus `channel:1` and a contradictory `channel:999`
remain distinct in both bindings and host text. Opaque fetch attempt IDs still
receive attempt-domain pseudonyms. No numeric suffix is interpreted as an
alias for an opaque observer ID, and no identity map is inferred from host text.

Six focused collapse tests additionally prove inequality and repeated-identity
joins for tokens, different UUIDs, held versus recorded rows, expected versus
observed sessions, exact 64-hex revisions, and sources across injection JSON,
channel attributes, members and summary dictionary keys. They require the
appropriate replay failure after sanitization. The old diagnostic source-count
and shared-sample weaknesses remain characterized in the existing suites; they
cannot authorize canonical delivery.
