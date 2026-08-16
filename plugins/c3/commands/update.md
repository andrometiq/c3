---
description: Update C3 to the latest GitHub release — checksum-verified staged binary replacement.
---

!c3-broker update

Display the output verbatim. `c3-broker update` checks the latest GitHub release, downloads the tarball for this platform, verifies it against `SHA256SUMS`, validates all eleven shipped binaries, and replaces the nine core binaries in place. It refreshes `codex` only when the existing destination is positively identified as C3's already-installed launcher, so update cannot opt a user into Codex integration. It does **not** touch the running broker process — the replacement is on disk only. On Windows, installation is refused; fully quit C3 and re-extract the release tarball manually.

After a successful update the binaries are new but the **running broker is still the old version**. To load the new broker code, the safe path inside Claude Code is: quit Claude Code (Ctrl-D or `/exit`) and relaunch with `claude --dangerously-load-development-channels plugin:c3@c3` (append `--resume` to pick your session back up) — the next adapter spawn auto-spawns a fresh broker on the new binary. Do **not** `kill -TERM` the broker from inside Claude Code (killing it recycles this session's MCP adapter). From a **separate** terminal, `kill -TERM <pid>` (the command prints the pid) bounces it cleanly — adapters reconnect and re-spawn the new broker on their own.

Plugin files (these slash commands, hooks) update separately through Claude Code's marketplace, not this command — run `/plugin` and update the c3 marketplace if it offers a newer version.

If the output says C3 is already up to date, there's nothing to do. If it reports a dev build, this binary has no embedded release version (built from source, not a release) — nothing to update; install a prebuilt release to get auto-updates. If the check failed (network/API), it's non-fatal — try again later.
