# TODO — C3 v0.1 release

The v0.1 finish line. The multiplexer, topic routing, durable queue, and
cross-CLI core already ship — this is what's left before the public push.
Future/unbuilt work lives in [`ROADMAP.md`](ROADMAP.md); shipped history is
in git.

## Docs restructure (this pass)

- [x] Add `junk/` to `.gitignore`; archive done-and-dusted docs (RESUME, build-log, GCP brief, plans/research/specs/superpowers)
- [x] Rewrite README front door: positioning, honest flag box, "how is this different from X?"
- [x] Dissolve RESUME.md; ROADMAP → v2-only; TODO → this checklist
- [x] ADAPTERS.md: `inbox` → `fetch_queue`; drop `architecture.md` + `internal/adapter/broker` refs; describe the real adapter reality
- [x] CHANNELS.md: real file tree + interface signatures; no `registry.go`
- [x] PLUGINS.md: real registrar (`builtinPlugins` in `cmd/c3-broker/main.go`); drop the dead `OnOutbound` hook; link the STT provider how-to
- [x] COMMANDS.md: complete the verb/tool tables; resolve the Codex rows
- [x] DEBUGGING.md: `c3-ping.md` → `ping.md`; scrub personal names
- [x] USAGE.md / INSTALL.md: genericize examples; describe the STT chain honestly
- [x] Bump `plugins/c3/.claude-plugin/plugin.json` to `0.1.0`

## Release gates

Done in the build (merged to master, green under `go test -race ./...`):

- [x] Forked-session queue-delivery blackhole fix — the adapter no longer acks-as-delivered what the host can't render; render-incapable inbound is held in the durable queue with a held-notice, claim preserved for outbound
- [x] Fixed the 2 flaky broker tests (fixture defect, not prod)
- [x] `install-claude-shim` existing-file/symlink clobber handling (idempotent; `--force` to overwrite a non-shim binary)
- [x] Noisy re-poll / dedup-skip: root-caused as a loss-free frontier redraw; backoff + logging fix landed (deferred wedge-trigger tracked in `docs/.loop/repoll-diagnosis`)
- [x] Codex policy 3-state error messaging: wired (codex adapter forwarder)

Open — need a live human tap (run these in a real Telegram session before the tag):

- [ ] `ask` live-verify: button tap → choice returns to Claude
- [ ] Permission-relay live-verify: a real Claude Code permission prompt → approve/deny over Telegram
- [ ] Smoke-test visual tails (expandable show-more; inline-button callback)

Open — post-first-tag (binaries only exist once the release workflow runs on a tag):

- [ ] Fresh-machine install validation (public-push blocker)

Open — minor / deferred:

- [ ] Live-verify auto-attach-on-resume end-to-end (shipped in master, gated by `auto_attach_on_resume` in mappings.json, default OFF), then consider flipping the default
- [ ] STT gemini provider: revive with a key, or redocument on the Sarvam default
- [x] `release <cwd>`: print the `/exit` workaround (the full IPC op is v2) — done: `runRelease` (cmd/c3-broker/status.go) prints the detach/steal guidance instead of failing opaquely

## Packaging

- [x] Prebuilt binaries — `make dist` / `.github/workflows/release.yml` cross-compile all
      six targets (linux, darwin, windows × amd64, arm64) into checksummed tarballs on a
      `v*` tag. Verified locally end-to-end: 6/6 tarballs + `SHA256SUMS`, checksums
      re-verified, and a tarball extracted and run outside the source tree reports its
      injected version. The assets themselves only exist once the tag is pushed.
- [ ] GitHub-source marketplace edit — paired with the first published release
- [x] Auto-update system: always-on status-line update notice + `/c3:update` / `c3-broker update` (checksum-verified atomic swap) + opt-in `auto_update` toggle (broker self-updates and restartlessly bounces). Binaries carry a build version via `-ldflags -X`. Needs one live end-to-end verify once the first GitHub release exists.

## Ship

- [ ] Final PII audit immediately before the push (standing rule — re-run on the exact tree
      being pushed, not on an earlier one)
- [ ] Ship WITH the documented `--dangerously-load-development-channels` flag
- [ ] Every release bumps `plugin.json` `version` — a fixed version string pins the plugin, and Claude Code's auto-update won't ship it to existing users until it's bumped
