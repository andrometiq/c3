# Captured Claude receipt corpus

The `claude-<version>/` directories contain sanitized host records, with a
`.expect.json` sidecar for every `.jsonl`. The sidecar has one entry per record:
`transport`, `token`, `attempt`, `accept` and a provenance/rejection `reason`.
The existing top-level 2.1.263 and 2.1.266 captures are the sources of these seeds;
no host envelopes were synthesized. Tokenless channel input and queue removal
are negative fixtures. The bare cross-session enqueue is also negative.

TODO: the 2026-09-09 inbox mid-turn incident reports a different envelope but
provides no verified JSON shape here. Capture it with the live harness. Also
capture background, startup, reconnect and receipt-based fetch on each version.
A startup readiness failure is an ordering failure, not a new transcript shape;
it has no invented positive JSON fixture.

Harness exports may contain `accept: null` with a `TODO:` reason. The unit test
makes those gaps explicit with skips; review an actual record before classifying
it as positive or negative. Fetch receipt mode is not present in this baseline.
Run the harness outside the sandbox, never from a coding lane. See
`scripts/live-matrix/README.md` and `docs/TESTING-LIVE-MATRIX.md`.
