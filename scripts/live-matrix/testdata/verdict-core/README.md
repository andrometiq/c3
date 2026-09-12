# Verdict core synthetic fixtures

These are authored captures, not saved live runs. Each JSON file retains its old
aggregate evidence, explicit canonical witness, frozen historical judgment, and
separately authored core expectation. The old source digest is recorded in
`../../test_verdict_legacy_oracle.py`.

`../../generate_verdict_goldens.py` is a manual authoring tool. It obtains historical
results only from that frozen oracle. Core expectations use the historical result
plus explicit, reviewed correction lists; the evaluator never generates expected
results. Tests only read the checked-in JSON. Changes to these goldens require
review of both the input witness and the expected judgment.

`../../verdict_fixture_support.py` is test-only. It authors hypothetical identities
and checks their legacy projections, including member cardinalities, terminal
elapsed time, source occurrences, and notices. Production never imports it.

The fixtures cover the historical common, negotiated-live, and receipt-fetch
branches, boundary timings, empty token sets, separate and merged delivery,
readiness epoch exclusions, Held accounting, setup/run precedence, and
collect-only reporting. Correctness fixtures add identity and revision failures,
missing evidence, exact held-row retention, legacy milestones, and fault classes.
The test module also covers contract axes, consume-on-fetch, duplicate ordering,
identity renaming, token/revision collapse, collection manifests, and immutability.

Intentional corrections retain the original result alongside the new result.
Missing aggregate identities fail closed. Invalid host occurrences do not count
as eligible delivery; their identity mismatch still fails the verdict. Missing
final disposition can add a new reason after historical delivery failures.
Setup outcomes keep only the first setup error and list selected skipped checks,
including the new correctness checks.

Known defects:

| ID | Historical baseline | Correction ownership |
| --- | --- | --- |
| `shared-sample-fetch-oracle` | One source repeats shared sample text twice; old text count claims double delivery. The raw result includes the independently expected receipt trailer. | Phase 2 raw replay; Phase 5 identity-based fetch counting. |
| `unaccepted-live-intake-counted` | A token-bearing user record is counted although the existing classifier rejects it. | Phase 2 raw replay; Phase 5 acceptance correction. |
| `explicit-attach-masks-resume` | Delivery-only resume passes after explicit attach; the dedicated automatic-recovery scenario fails. | Phase 5 automatic attachment recovery scenario. |

Known-defect annotations are provenance, not assertion bypasses. Their witnesses
represent the old extractor's claims; they are not faithful normalizations of the
raw defective record.

The existing live instrumentation lacks complete ownership, row/source mapping,
and observation-window boundaries. The production collector therefore exports an
incomplete observation and fails closed. Report field names remain compatible;
missing-evidence judgments intentionally change. Sanitized observation exports
are not identity-complete replay inputs; export pseudonyms belong to Phase 2.

The accept-on-absence correction requires positive delivery bindings, pinned and
ordered barriers, and reconciliation of every scoped queue row. Eight previously
failing characterizations are now intentional corrections under the maintainer's
ruling: channel/fetch extra confirmations and unequal token sets, fetch held-row
counts 0/1/3, and live reserved count 2. Their frozen reports and legacy facts are
unchanged; each carries the `accept-on-absence` correction ID and a note. No stored
expected status changed. The synthetic visible-backlog witness now retains that
row through every snapshot, preserving its historical PASS and injected counts.

The harness helper validates authored raw delivery claims before passing negative
traces to the evaluator. An unbound raw record can exist while its accepted count
is zero. The characterization adapter retains its stricter accepted-projection
assertions; neither helper rewrites evidence or supplies expected core verdicts.

Binding completeness now uses one required-binding manifest and validation loop.
It covers both source/admission directions, evidence roles and artifacts, pinned
cuts and chronology, contract and proof checkpoints, event/reservation/token and
member/revision joins, retirement authorization, all queue rows, and recovery.
Role streams can span epochs and claim generations: event scopes and barrier
cutoffs determine epoch attribution. The predecessor-offer characterization stays
unchanged. Tests exercise the unassigned-admission, orphan-final-artifact, and
host-delivery-in-state-stream traces through both the core and collector adapter.

Five already-failing fetch-trailer corrections now also fail collection
completeness when the claimed token/member binding contradicts its counterpart.
They carry the `accept-on-absence` defect ID; their historical reports, legacy
facts, and statuses are unchanged. The synthetic degraded-recovery witness cites
the recovery role stream. All characterization fixtures remain unchanged.
