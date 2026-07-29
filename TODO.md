# TODO — C3 v0.1 release

The v0.1 finish line — open release work across code, documentation, tests,
packaging, and human verification that blocks the tag or release safety. Shipped
history is in git; future and unbuilt work lives in [`ROADMAP.md`](ROADMAP.md).

## Release gates

- [ ] **Full release-readiness audit — AFTER the maintainer's final go-ahead, BEFORE the
      v0.1.0 tag.** The pre-rc1 audit no longer holds: the code has changed substantially
      since (loss-path fixes, identity fixes, degraded mode, port race, recovery ordering).
      Deliberately NOT run for release candidates — final tag only. Expect findings; fix
      only what is genuinely required, then push and tag. (Maintainer's instruction,
      2026-07-27.)

Need a live human tap — run these in a real Telegram session before the tag:

- [x] `ask` live-verify: button tap → choice returns to Claude — **PASSED 2026-07-29**
      (post-restart broker at 32f5c7d; tapped choice returned as the tool result)
- [ ] Permission-relay live-verify: a real Claude Code permission prompt → approve/deny over
      Telegram. **Cannot fire from an auto-approve session** (no prompt exists to relay — two
      forced probes auto-ran, broker log shows no registration). Run in the maintainer's final
      test session under normal permission mode: first non-allowlisted tool call relays; one
      Allow tap closes this. Mechanism last fully verified live 2026-07-12 on earlier code.
- [x] Smoke-test visual tails (expandable show-more; inline-button callback) — **PASSED
      2026-07-29** (expandable `||` blockquote collapsed + expanded; inline-button callback
      proven by the `ask` taps). Note: first attempt was a mis-authored test — a plain `>`
      quote renders full-length BY DESIGN; expandable must be requested with the `||`
      terminator (format.go) or comes automatic in voice readbacks.

Post-v0.1.0 publication — the stable release must exist before these can run:

- [ ] Fresh-machine install validation against the published stable artifacts
- [ ] Auto-update live end-to-end verify: status-line notice → `/c3:update` →
      checksum-verified atomic swap, against a real published release

## Pre-tag polish (non-blocking SEV3s from the cross-review rounds, 2026-07-29)

- [ ] Mutation-coverage gap: `voiceFetchFailedOpening` is in the production opening table
      (`internal/broker/worker.go`) but the re-derived segment-replace unit table no longer
      exercises it — removing it would leave a `[voice download failed: …]` segment stale
      after a successful retranscribe with every test green. Add the one table row.
      (CODEX-REVIEW-5 finding 2.)
- [ ] The advertised nonce-less STT degradation is unreachable on Go ≥1.25 (`crypto/rand.Read`
      never returns an error there — it aborts the process). Fail-closed either way; align the
      code/comment with reality or drop the branch. (CODEX-REVIEW-5 finding 1.)

## Packaging

- [ ] GitHub-source marketplace edit — paired with the first published release

## Ship

- [ ] Final PII audit immediately before the push (standing rule — re-run on the exact tree
      being pushed, not on an earlier one)
- [ ] Ship WITH the documented `--dangerously-load-development-channels` flag
- [ ] Every release bumps `plugin.json` `version` — a fixed version string pins the plugin, and
      Claude Code's auto-update won't ship it to existing users until it's bumped
