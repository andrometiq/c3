# Frozen Claude driver conformance

These 43 cases are **authored `raw_replay`**, not recordings that certify an
installed Claude version. The version selects the frozen 2.1.267 control recipe.
No CLI, plugin installation, credentials, broker process, or network is needed.

Run the suite with:

```sh
python3 -m unittest discover -s scripts/live-matrix -p 'test_hostdriver_con*.py'
```

Each case contains a pinned scenario/contract in `profile.json`, native raw
artifacts in `raw/`, and read-only expected driver rows, classifications,
diagnostics, canonical observation, and ordered verdict. A missing
`expected-producer-diagnostics.json` means an empty producer diagnostic list.
`terminal.json` contains the frozen terminal result for submission controls.

The test stages only `raw/` in temporary scratch. It explicitly refuses a
`capture.json`, `context/driver.jsonl`, or any `expected*` file in that input.
The real `ClaudeHostDriver` is prepared in replay mode with a deterministic
clock and a backend factory that raises on execution. Its real `observe()`
selects the SessionStart transcript, reads the proxy's held response, consumes
control history, and generates/seals the snapshot. Driver rows and diagnostics
are checked before assembly. The production `checked_context()` and
`assemble_driver_capture()` then call the actual context-backed assembler and
unchanged evaluator. Full canonical observations, including byte digests, and
ordered verdicts are compared to frozen expectations.

The earlier broker/queue/ownership/contract observers are independent authored
inputs, as in the extractor replay corpus. `control/operations.jsonl` uses the
production control journal's `facts.observer_rows` grammar for saved samples and
shared cuts. It is input history, not a copy of the expected output file. Normal
completed cases omit the final driver rows: the observation-window operation
causes the real `CaptureStore.finalize()` to append the final cut and window end.
Missing and deliberately short-cut cases retain their incomplete captured
history instead of silently repairing it.

Raw proxy/pane/filesystem controls do not independently prove delivery. Replay
preserves them; the live control methods interpret them. The companion
`test_hostdriver_controls.py` exercises those real methods against the same pane
grammar, fake process/terminal I/O, and temporary filesystem dependencies.
In particular, a recognized submitted composer does not become a transcript
receipt, and an unknown pane cannot erase independently recorded host evidence.
The submission snapshots deliberately keep delivery artifacts constant to check
this separation; control tests prove submission uncertainty and absence of proof.

## Expectation authoring

The raw transport records and initial expectations reuse the committed
`testdata/extractor-replay` healthy controls and named mutations. `profile.json`
records that reference. They are copied here so each case is self-contained.
The submission panes reuse the rule-delimited examples in `test_harness.py`.

Native-path expectation edits were explicit: profile/provenance values, artifact
inventory order, representation digests, and final-cut-before-window-end order.
Additional authored variants change specific raw facts and the corresponding
canonical fields: state/session proofs, contract axes/epochs, selected transcript
paths, readiness cuts and queue samples. No test writes or regenerates a golden,
uses expected rows as driver input, or substitutes a classifier, observation,
or verdict. There is no golden regeneration command.

Several failures have consequences beyond their immediate symptom. A
predecessor-only contract sample cannot authorize current-epoch receipts, so its
ordered failure includes source accounting, identity, disposition, and
completeness. Missing state proof also fails core completeness. An escaped
transcript is unreadable at the producer; snapshot validation records the absent
bytes and their mismatch, and canonical collection remains incomplete. These
are characterized outcomes, not production fixes.

## Coverage map

| Area | Frozen conformance tests | Control interpretation |
| --- | --- | --- |
| Native delivery | `test_native_delivery`: channel and inbox receipts; rejected peer origin/prefix never authorize intake | Existing peer grammar tests remain unchanged |
| Fetch | `test_fetch`: held rows, distinct RPC/tool IDs, complete matching result, permutation, wrong-token rejection | Existing `test_fetch_barrier_holds_actual_response` |
| Occurrence identity | `test_occurrence_identity`: repeated UUID deduplication, distinct duplicate deliveries, conflicting UUID corruption | Native checked reader remains real |
| Source identity | `test_source_identity`: two admissions, unchanged sample totals cannot hide duplicate-source substitution; wrong token survives | No counting-based witness |
| Ownership | `test_ownership`: transcript/owner disagreement and reused PID with conflicting receipt epoch | Existing resume/reconnect handle checks |
| Submission | `test_submission_artifacts_are_not_delivery_witnesses`: retains unsent/wrapped/submitted/unknown/exited panes with independently fixed delivery | `test_submission_frozen_panes_and_bounded_retries`, `test_wrapped_prompt_requires_observed_transition` |
| State | `test_state`: foreground/background invocation and marker agreement; missing/stale marker supplies no state proof | `test_real_workload_reader_and_injection_revalidation`, `test_stale_marker_and_old_invocation_cannot_establish_state` |
| Session | `test_session`: empty-at-injection fresh proof, eager transcript, correct resume, conflicting resume sample, wrong selected file despite a healthy decoy | New fresh-arrival test plus existing correct/wrong-target resume test |
| Barriers | `test_barriers`: held queue before startup/reconnect release, successor cuts, late predecessor notification | New stale-release/idempotence and menu tests; existing predecessor/reused-PID checks |
| Collection | `test_collection_checked_reads`, `test_collection_boundaries`, `test_truncation_keeps_prior_bytes_and_invalidates_delivery`: missing, JSON/UTF-8 corruption, partial tail, retained prior bytes, absent/short final cut | Existing partial-tail completion and finalization tests |
| Contract | `test_contract`: missing negotiation, downgrade kept distinct from pinned policy, predecessor-only sample | `test_unknown_recipe_is_setup_failure_in_real_orchestrator` |
| Isolation | `test_isolation_before_open`: outside-scratch and symlink reads raise if the sentinel is opened; no private bytes exported | Existing live transcript-selection containment test |
| Export | `test_export_fresh_interpreter`: real driver reads sanitized raw export in a fresh interpreter; full semantics and source/identity distinctions survive | No redaction map crosses processes |

Export pseudonymizes opaque contract/profile IDs. The fresh interpreter rebinds
those two labels to the declared recipe only after verifying equality of every
pinned policy field. It never chooses policy from observed negotiation. Only
artifact content digests are omitted from the post-serialization comparison;
they describe the new byte representation. Primary goldens compare them exactly.

The existing control suites additionally cover literal launch arguments and
resume ordering, owned cleanup after partial preparation, session/cursor handle
validation, deterministic selection, final capture before teardown, and arbitrary
driver identifiers with identical orchestration. Fresh-process import guards, an actual-orchestrator unknown-recipe setup
failure, and `test_arbitrary_identifiers_preserve_policy_and_order` extend those
checks here. The latter compares the real selected scenario, contract, case,
workload, and timeouts as well as the complete call sequence.

CI discovery was already present in `scripts/ci.sh`, guarded by `CI=true` or
`RUN_PY_TESTS=1`; this step does not change it or the pre-commit hook.

Hermetic coverage does not establish live broker instrumentation or certify a
current tmux/Claude interaction. The maintainer's step-7 smoke and broader live
qualification remain separate acceptance work.
