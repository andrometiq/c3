# TODO — C3 v0.1 release

The v0.1 finish line is closed. Shipped history is in git; future and unbuilt
work lives in [`ROADMAP.md`](ROADMAP.md).

## Release gates

- [x] **Full release-readiness audit — AFTER the maintainer's final go-ahead, BEFORE the
      v0.1.0 tag.** The pre-rc1 audit no longer holds: the code has changed substantially
      since (loss-path fixes, identity fixes, degraded mode, port race, recovery ordering).
      Deliberately NOT run for release candidates — final tag only. Expect findings; fix
      only what is genuinely required, then push and tag. (Maintainer's instruction,
      2026-07-27.) **PASSED 2026-07-30** — final-tree build, vet, tests, race tests,
      formatting, shell syntax, all release cross-builds, archive inspection, launcher
      signal tests, adversarial cross-review (GO; no SEV1/SEV2), and exact-tree PII audit
      all passed before `v0.1.0`.

Live human checks:

- [x] `ask` live-verify: button tap → choice returns to Claude — **PASSED 2026-07-29**
      (post-restart broker at 32f5c7d; tapped choice returned as the tool result)
- [x] Permission-relay live-verify: a real Claude Code permission prompt → approve/deny over
      Telegram — **PASSED 2026-07-30**. A manual-mode Claude session registered Bash request
      `jkzfo`; the maintainer tapped Allow in `perm-test`; the broker logged the matching
      resolution; Claude resumed without local approval and created the requested probe file.
- [x] Smoke-test visual tails (expandable show-more; inline-button callback) — **PASSED
      2026-07-29** (expandable `||` blockquote collapsed + expanded; inline-button callback
      proven by the `ask` taps). Note: first attempt was a mis-authored test — a plain `>`
      quote renders full-length BY DESIGN; expandable must be requested with the `||`
      terminator (format.go) or comes automatic in voice readbacks.

Post-v0.1.0 publication — the stable release must exist before these can run:

- [x] Fresh-machine install validation against the published stable artifacts —
      **PASSED 2026-07-30**. All six published archives matched the published
      `SHA256SUMS`; the Linux archive contained all nine binaries and complete STT/Grok
      assets; an isolated stable broker boot installed and registered the embedded STT
      runtime successfully.
- [x] Auto-update live end-to-end verify: status-line notice → `/c3:update` →
      checksum-verified atomic swap, against a real published release — **PASSED
      2026-07-30**. An isolated published rc1 broker detected v0.1.0 and wrote the
      status-line update fields; the published rc1 updater downloaded v0.1.0, verified
      `SHA256SUMS`, atomically replaced the binary-only install, and the resulting stable
      broker booted with a repaired complete STT runtime. The installed stable binary now
      reports up to date.

## Pre-tag polish (non-blocking SEV3s from the cross-review rounds, 2026-07-29)

- [x] Mutation-coverage gap: `voiceFetchFailedOpening` is in the production opening table
      (`internal/broker/worker.go`) but the re-derived segment-replace unit table no longer
      exercises it — removing it would leave a `[voice download failed: …]` segment stale
      after a successful retranscribe with every test green. Add the one table row.
      (CODEX-REVIEW-5 finding 2; fixed in `cdf5704`.)
- [x] The advertised nonce-less STT degradation is unreachable on Go ≥1.25 (`crypto/rand.Read`
      never returns an error there — it aborts the process). Fail-closed either way; align the
      code/comment with reality or drop the branch. (CODEX-REVIEW-5 finding 1;
      fixed in `cdf5704`.)

## Packaging

- [x] GitHub-source marketplace edit — paired with the first published release.
      The released manifest uses repository `https://github.com/Andrometiq/c3` with
      source `./plugins/c3`, the correct GitHub marketplace-relative plugin path.

## Ship

- [x] Final PII audit immediately before the push (standing rule — re-run on the exact tree
      being pushed, not on an earlier one) — **PASSED 2026-07-30**: working tree and
      full-history gitleaks clean; wordlist findings reviewed as intentional public names
      or generic C3 paths; no absolute symlink leaks.
- [x] Ship WITH the documented `--dangerously-load-development-channels` flag —
      exercised on the released install during the final live permission-relay test.
- [x] Every release bumps `plugin.json` `version` — v0.1.0 ships plugin version `0.1.1`.
