# Captured Claude receipt corpus

The `claude-<version>/` directories contain sanitized host records, with a
`.expect.json` sidecar for every `.jsonl`. The sidecar has one entry per record:
`transport`, `token`, `attempt`, `accept` and a provenance/rejection `reason`.
The existing top-level 2.1.263 and 2.1.266 captures are the sources of these seeds;
no host envelopes were synthesized. Tokenless channel input and queue removal
are negative fixtures. The bare cross-session enqueue is verified positive on 2.1.266.

The captured 2.1.266 peer-intake fixture supplies verified bare enqueue and nested
peer queued_command positives, plus a negative queue removal. Provenance/prefix
negatives follow each envelope: top-level peer metadata and literal prefix on
user turns, nested peer metadata on attachments, and exact C3 source binding on
bare enqueues. The receipt predicate is unchanged from master.

TODO: capture background, startup, reconnect and receipt-based fetch on each
version. A startup readiness failure is an ordering failure, not a new transcript
shape; it has no invented positive JSON fixture.

Harness exports may contain `accept: null` with a `TODO:` reason. The unit test
makes those gaps explicit with skips; review an actual record before classifying
it as positive or negative. Fetch receipt mode is not present in this baseline.
Run the harness outside the sandbox, never from a coding lane. See
`scripts/live-matrix/README.md` and `docs/TESTING-LIVE-MATRIX.md`.
