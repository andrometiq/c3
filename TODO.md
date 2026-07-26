# TODO — C3 v0.1 release

The v0.1 finish line — only what is still open and still blocking the tag or the
public push. Shipped history is in git; future and unbuilt work lives in
[`ROADMAP.md`](ROADMAP.md).

## Release gates

Need a live human tap — run these in a real Telegram session before the tag:

- [ ] `ask` live-verify: button tap → choice returns to Claude
- [ ] Permission-relay live-verify: a real Claude Code permission prompt → approve/deny over Telegram
- [ ] Smoke-test visual tails (expandable show-more; inline-button callback)

Post-first-tag — the binaries only exist once the release workflow runs on a tag:

- [ ] Fresh-machine install validation (public-push blocker)
- [ ] Auto-update live end-to-end verify: status-line notice → `/c3:update` →
      checksum-verified atomic swap, against a real published release

## Packaging

- [ ] GitHub-source marketplace edit — paired with the first published release

## Ship

- [ ] Final PII audit immediately before the push (standing rule — re-run on the exact tree
      being pushed, not on an earlier one)
- [ ] Ship WITH the documented `--dangerously-load-development-channels` flag
- [ ] Every release bumps `plugin.json` `version` — a fixed version string pins the plugin, and
      Claude Code's auto-update won't ship it to existing users until it's bumped
