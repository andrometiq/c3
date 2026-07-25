# Installing C3

Set-up steps for someone who has never run C3 before. Five minutes if everything goes well. If you've already used C3 and you're upgrading, skip to the bottom.

> This is the verbose human walkthrough. The agent-driven playbook (paste one
> `follow …` line and let Claude Code run it) lives in the top-level
> [`INSTALL.md`](../INSTALL.md), which also has the environment/platform routing
> table in its §0.

## Choose your environment and platform

C3 runs under three host environments. Pick yours — the binary install and
Telegram config are shared; only the host-integration wiring differs.

| Environment | What it is | Host integration | Inbound |
|---|---|---|---|
| **Claude Code (CLI)** | the `claude` terminal CLI (on Windows, the `claude.exe` bundled inside Claude Desktop) | plugin marketplace + dev-channels flag (Steps 1, 4, 4.5) | live push on Linux/macOS; **poll-only** on Windows (beta) |
| **Claude Desktop** | the desktop app's **Chat / Code** tabs | `c3-broker install-desktop` (see [`DESKTOP.md`](DESKTOP.md)) | **poll-only** — `fetch_queue` |
| **CoWork** | Claude Desktop's **Cowork** tab (± hourly poll task) | same `install-desktop` | **poll-only** + optional scheduled poll |

Codex is an optional add-on to any of these (Step 5).

**Platform support.** **Linux is primary and fully supported.** **macOS** is
supported (prebuilt binaries; `launchd` instead of systemd). **Windows (Claude
Desktop + the bundled Claude Code) is beta for v0.1.0** — it works and the known
Windows bugs are fixed, and the release does publish prebuilt Windows tarballs,
but inbound is poll-only (`/c3:fetch-queue`) and the Windows binaries are
cross-compiled without a clean-room CI pass. Windows-specific deltas are collected in the **Windows
(beta)** section below; skip the Linux-only steps (systemd, shell-rc `PATH`)
there.

The steps below are the **Claude Code (CLI)** path. For **Claude Desktop /
CoWork**, do the shared Prerequisites + Steps 2–3 (binaries + config), then
follow **[`DESKTOP.md`](DESKTOP.md)** for the `install-desktop` wiring instead
of Steps 1, 4, and 4.5.

## Prerequisites

You need:

- **Prebuilt binaries** (Linux + macOS, amd64/arm64) are attached to each release tag — the default path needs no toolchain. **Go ≥1.25** is only required if you build from source (contributors, or a platform without a prebuilt tarball): `go version` should print `go1.25` or newer. (The `go` directive in `go.mod` pins `1.25.0`; older toolchains will fail the build or silently auto-download 1.25.)
- **A Telegram bot + group, set up per the checklist below.** Five minutes if you've done it before, ten if not. (Why these steps: the bot needs to read every group message, send into topics, and create/rename/close topics — so it is promoted to admin with **Manage Topics**, and privacy mode is disabled in BotFather as belt-and-braces so it still reads messages even if admin promotion was fumbled.)

  Use **Telegram Desktop, iOS, Android, or macOS** for the group-side steps — *not Telegram Web*. Web's Topics-enable and admin-rights UIs are incomplete.

  1. **Create the bot.** Message `@BotFather` → `/newbot` → pick a display name and a username ending in `bot`. Copy the HTTP token (`1234567:abcdefg...`). Keep it private.
  2. **Disable privacy mode.** Same chat: `/setprivacy` → pick your bot → `Disable`. Without this, the bot only sees messages that mention or reply to it. (Not exposed via the Bot API — must be done in BotFather.)
  3. **Create a Telegram group** (a regular group is fine; it auto-promotes to a supergroup when you turn Topics on).
  4. **Add your bot** to the group.
  5. **Enable Topics** in group settings. Do this *before* the next step — "Allow create topics" only appears in the admin checklist once Topics are on.
  6. **Promote the bot to admin** with these rights checked: **Manage Topics**, **Send Messages**, **Delete Messages**, **Pin Messages**. Everything else off.
- **Your phone (or any Telegram client)** for pairing. Setup discovers your user id and the group's chat id automatically: you send a short code to the bot in DM, and another in the group. No id hunting.
- **For Codex integration:** Codex CLI installed (typically via `npm install -g @openai/codex` or similar). The C3 launcher will detect it. NVM users: take note — long-running shells hash `codex` to your NVM path, so the install step below symlinks both `~/.local/bin/codex` and the NVM bin path.
- **For voice transcription (STT plugin):** the shipped first-class STT plugin runs a chained pipeline. **Sarvam Saaras v3 is the working default** — set `SARVAM_API_KEY` and voice notes transcribe out of the box. **Gemini 3 Flash** (via OpenRouter) is an optional first-in-chain provider you can add with your own `OPENROUTER_API_KEY`; the chain tries it first and falls back to Sarvam. The plugin lives at `plugins/c3/stt/`; the broker subprocesses `python3` to run it. You need:
  - `python3` on PATH (3.11+ recommended).
  - API keys in `~/.claude/stt.env` — `SARVAM_API_KEY` for the Sarvam default, and optionally `OPENROUTER_API_KEY` for Gemini. Setting only one key works; the other provider is skipped.
  - No model downloads — both providers are remote APIs.

  If you don't need voice, set `mappings.json:plugins.stt.enabled=false` and skip the API keys. You can swap in a custom handler (whisper, local, anything that matches the argv contract) by setting `plugins.stt.handler_path` to your own script — see `docs/PLUGINS.md`.

## Step 1: Install the Claude Code plugin

Add C3's marketplace straight from GitHub and install the plugin:

```
/plugin marketplace add karthikeyan5/c3
/plugin install c3@c3
/reload-plugins
```

When `/plugin install` prompts for **user** vs **project** scope, choose
**user** — that makes C3 available in every Claude Code session, not just this
project.

> **Contributors / building from source:** if you want to build the binaries
> yourself or hack on C3, clone the repo into a **durable** location and add
> *that clone* as the marketplace instead (the plugin cache from a GitHub add
> doesn't carry the Go source `/c3:build` needs):
> ```bash
> git clone https://github.com/karthikeyan5/c3 ~/.local/share/c3
> ```
> then `/plugin marketplace add ~/.local/share/c3`. `/c3:build` compiles from
> this directory on every build, so pick a stable path (not `~/Downloads` or
> `/tmp`) and don't move or delete it.

## Step 2: Get the binaries onto your PATH

C3 ships nine Go binaries:

- `c3-broker` — the daemon
- `c3-claude-adapter` — Claude Code MCP server
- `c3-codex-adapter` — Codex MCP server
- `c3-grok-adapter` — Grok Build MCP server
- `c3-agy-adapter` — Agy MCP server
- `c3-desktop-adapter` — Claude Desktop MCP server (the `install-desktop` target)
- `claude-shim` — the `claude` wrapper that auto-injects the dev-channels flag (symlinked into PATH by `install-claude-shim`; see Step 4.5)
- `codex` — the C3 Codex **launcher**. Deliberately NOT installed by the step below: it is named `codex` so it can take the place of the real one, and this guide puts `~/.local/bin` first on `PATH`, so installing it would silently reroute every `codex` invocation on your machine through C3. Step 5 installs it, and only if you want Codex integration.
- `migrate-legacy` — one-shot migrator from a legacy Python-prototype config layout (only relevant if you have such a config)

**Prebuilt (recommended).** Download the release tarball for your platform,
verify it, and install the binaries into a directory on your `PATH`:

```bash
VERSION=v0.1.0
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m); [ "$ARCH" = x86_64 ] && ARCH=amd64; [ "$ARCH" = aarch64 ] && ARCH=arm64
pkg="c3_${VERSION}_${OS}_${ARCH}"
base="https://github.com/karthikeyan5/c3/releases/download/$VERSION"
curl -fsSL -O "$base/${pkg}.tar.gz"
curl -fsSL -O "$base/SHA256SUMS"

# Verify ONLY the tarball you downloaded. SHA256SUMS lists every platform, so
# checking the whole file would report the five you don't have as failures.
grep " ${pkg}.tar.gz$" SHA256SUMS > SHA256SUMS.this
sha256sum -c SHA256SUMS.this || shasum -a 256 -c SHA256SUMS.this

# The tarball unpacks into a ${pkg}/ directory — install the binaries out of it.
tar xzf "${pkg}.tar.gz"
mkdir -p ~/.local/bin
for b in c3-broker c3-claude-adapter c3-codex-adapter c3-grok-adapter \
         c3-agy-adapter c3-desktop-adapter claude-shim migrate-legacy; do
  install -m 0755 "${pkg}/${b}" ~/.local/bin/
done
```

Confirm the install dir (`~/.local/bin` here) is on your `PATH`. (**Windows:** a
prebuilt tarball *is* published — its binaries carry the `.exe` suffix — but
PATH is set with `setx`, not a shell rc. See the **Windows (beta)** section.)

**From source (contributors, or a platform without a prebuilt tarball).**
With the repo cloned and added as a local marketplace
(Step 1's contributor path), run `/c3:build` inside Claude Code — a slash
command that runs `go install ./cmd/...` and drops the nine binaries in `$GOBIN`
(default `~/go/bin`). Needs Go ≥1.25. Confirm `$GOBIN` (or `$(go env GOPATH)/bin`)
is on your `PATH`:

```bash
export PATH="$(go env GOPATH)/bin:$PATH"
```

## Step 3: Configure C3

Run setup **inside Claude Code** so the agent can guide you through the whole
flow — bot token, Telegram pairing, and STT keys:

```
/c3:setup
```

Prefer this over the bare terminal. If you're not in a Claude Code session,
the standalone equivalent is `c3-broker setup` — it collects the same
`mappings.json` fields, just without the agent walking you through it.

The guided flow walks through:

- Your bot token (the `1234567:abc...` string).
- **DM pairing** — you send a short code to the bot in DM; setup discovers and records your user id (`dm_chat_id`, `master_user_id`, and the operator allowlist entry) automatically.
- **Group pairing** — you send another code in the (Topics-enabled) group; setup discovers and records the group's chat id. You can add more groups later with `/c3:pair` or by editing `~/.config/c3/mappings.json`.
- (Optional) voice-transcription API keys.

Completed steps are skipped on re-runs.

**Token validation.** Before writing the file, `/c3:setup` calls Telegram's `getMe` with the token. If it 401s (bad token) or times out (no network), the command refuses to write `mappings.json` and prints the actual Telegram error. Re-run `/c3:setup` after fixing the token. This avoids the failure mode where a typo in the token gets silently saved and surfaces only on the next inbound poll.

It writes `~/.config/c3/mappings.json` at mode 600 with this skeleton:

```json
{
  "schema_version": 1,
  "channels": {
    "telegram": {
      "bot_token": "1234567:abc...",
      "default_group": "main",
      "groups": {"main": {"chat_id": -1001234567890, "title": "My C3 Group"}},
      "dm_chat_id": 12345678,
      "master_user_id": 12345678,
      "topics": [],
      "debounce_ms": 1500
    }
  },
  "mappings": {},
  "plugins": {"stt": {"enabled": true}}
}
```

## Step 4: Enable channel notifications

Claude Code requires explicit opt-in before it surfaces
`notifications/claude/channel` from any plugin. **Without this step the
broker delivers messages successfully but the CLI never sees them.**

Open `~/.claude/settings.json` and add (merge if other top-level keys
exist):

```json
"channelsEnabled": true,
"allowedChannelPlugins": [
  { "marketplace": "c3", "plugin": "c3" }
]
```

If `allowedChannelPlugins` already has other entries, keep them and
append `c3` alongside.

Then **restart Claude Code with the development-channels flag**:

```
claude --dangerously-load-development-channels plugin:c3@c3
```

The plain `claude` command keeps `allowedChannelPlugins` enabled but
also gates local-marketplace plugins behind the dev flag, so the c3
adapter's `notifications/claude/channel` frames don't render live in that
session. As of v0.1 they're not lost either — C3 detects a session that
can't render and holds inbound in the durable queue (a held-notice fires
in the topic; recover with `fetch_queue`); relaunch with the flag for live
rendering. The official-marketplace plugin distribution flow doesn't need
this flag; we do, until c3 is published through Anthropic's marketplace.

## Step 4.5: Install the Claude wrapper

`/c3:setup` runs this automatically when invoked from a Claude Code
session — it is COMPULSORY under HostClaude per the locked 2026-05-18
design. The standalone command is for **manual** install, re-install
(after a `claude` binary upgrade clobbered the symlink), or
`--force` scenarios:

```
c3-broker install-claude-shim
```

The shim is a tiny launcher symlinked at `~/.local/bin/claude` that
transparently adds `--dangerously-load-development-channels plugin:c3@c3`
to every `claude` invocation. Without it you type the long flag form by
hand on every session start — easy to forget, which is why v0.1 holds
flagless inbound in the durable queue instead of dropping it (the shim
still saves you the missed-live-rendering papercut).

The most common manual-install hiccup is an existing non-shim
`~/.local/bin/claude` (often from NVM, npm, or a hand-edited symlink to
the real claude binary). The installer refuses to overwrite it without
`--force`. Verify a successful one-time install **without** `--force`
first — that path persists the resolved real-claude target to
`~/.config/c3/claude-shim.json` so the shim's fallback lookup chain
still finds your binary. Use `--force` only if the standard install
already wrote that config or you know the real-claude target by hand.

## Step 5 (optional): Enable Codex integration

If you also use the Codex CLI, run:

```
# Step 4 deliberately did not install the launcher — this is the step that puts
# C3's `codex` in place of the real one. Prebuilt: take it from the tarball you
# extracted (re-extract if it's gone). From source, `go install ./cmd/...`
# already placed it.
install -m 0755 "${pkg}/codex" ~/.local/bin/

c3-broker install-codex-shim
```

This is a Go subcommand that idempotently:

1. Symlinks `~/.local/bin/codex` to the C3 `codex` launcher — **skipped on the prebuilt install**, where the launcher already *is* `~/.local/bin/codex` and there is nothing to shim.
2. Walks `~/.nvm/versions/node/*/bin/` and creates the same symlink in each version's bin dir. **This is required, not optional** — long-running shells hash `codex` to the NVM path; without these symlinks, your existing terminals bypass the C3 bridge entirely.
3. Verifies `~/.config/c3/mappings.json` exists (it does if you ran `/c3:setup`).
4. Verifies the broker is reachable.
5. Prints a one-line audit of every symlink it created or confirmed.

Open a fresh terminal (or `hash -r` your existing one) and run `which codex`. It should resolve to `~/.local/bin/codex`. From now on every `codex` invocation goes through the C3 launcher → app-server → adapter chain. Use Codex normally.

## Step 6: Verify

In a fresh Claude Code session (started with the dev-channels flag from
step 4) in some project directory:

```
attach
```

The agent should respond with a proposal: "I'd create a topic '<dirname>' in group 'main'. Confirm?"

Confirm. The topic appears in your Telegram supergroup. Send a message into it from your phone. It should appear in the CLI as a `<channel>` block. Reply via the agent's `reply` tool. The reply lands in the topic.

If voice messages are working: send a voice note from your phone. After a couple of seconds it should arrive in the CLI as `[Transcribed voice]: <text>`. STT can take 1-3 seconds depending on length.

## Claude Desktop / CoWork (instead of Steps 1, 4, 4.5)

If your environment is **Claude Desktop** (the app's Chat / Code tabs) or
**CoWork** (its Cowork tab) rather than the `claude` CLI, you still do the
shared **Prerequisites**, **Step 2** (binaries — including `c3-desktop-adapter`)
and **Step 3** (config). You **skip** the Claude-Code-only steps — the plugin
marketplace (Step 1), the `settings.json` channel opt-in (Step 4), and the
dev-channels flag / `claude-shim` (Step 4.5) — and instead register the adapter
with Claude Desktop:

```bash
c3-broker install-desktop
```

It merges an `mcpServers.c3` entry into Claude Desktop's per-OS config
(Windows `%APPDATA%\Claude\claude_desktop_config.json`, macOS
`~/Library/Application Support/Claude/...`, Linux `~/.config/Claude/...`),
preserving every other server. Then **fully quit and restart Claude Desktop**
(tray → Quit) so it reloads the config.

Claude Desktop is **poll-only** — inbound sits in the durable queue and you pull
it with `fetch_queue` (or the `/fetch-queue` slash command, or the "open the c3
inbox" live panel). There is no Telegram permission relay here. **CoWork** is
the same install; its only extra is an optional hourly **Cowork Scheduled Task**
that polls on a timer. Full walkthrough, the MSIX config trap, and caveats:
**[`DESKTOP.md`](DESKTOP.md)**.

## Windows (beta)

Windows is **beta for v0.1.0**. The known Windows bugs are fixed, but apply
these deltas instead of the Linux-only steps above:

- **Binaries — prebuilt tarball or source.** The release publishes
  `c3_<version>_windows_amd64.tar.gz` and `..._windows_arm64.tar.gz`, whose
  binaries carry the `.exe` suffix; they are cross-compiled and have not had a
  clean-room CI pass. To build instead, install **Go ≥1.25** (the portable zip
  needs no admin), clone the repo into a durable dir, and `go install ./cmd/...`.
- **PATH via `setx`, not a shell rc.** `setx PATH "%PATH%;%USERPROFILE%\.local\bin"`
  (or System Properties → Environment Variables), then **open a new terminal** —
  an already-open shell keeps the stale PATH.
- **`claude` isn't on PATH** — it's bundled inside the Claude Desktop app
  (`%APPDATA%\Claude\claude-code\<ver>\claude.exe`); run the Step 1 `/plugin`
  commands from that bundled Claude Code.
- **Config path** is `%USERPROFILE%\.config\c3\mappings.json`.
- **Inbound is poll-only** (both Claude Code and Desktop): the render-detector
  fails safe and holds inbound in the durable queue, so pull with
  `/c3:fetch-queue` (CLI) or `fetch_queue` / `/fetch-queue` (Desktop). Nothing
  is lost.
- **One bot token per machine** — a Windows box sharing a Linux box's token
  breaks both with `409 Conflict`; get a second bot from `@BotFather`.
- **STT needs a real Python + ffmpeg.** The Store `python3`/`python` alias is a
  no-op stub. `winget install Python.Python.3.12` + `winget install Gyan.FFmpeg`,
  then set `plugins.stt.handler_path` **and** `plugins.stt.python` (absolute
  `python.exe`) in `mappings.json` and restart the broker.
- **MSIX Desktop config trap** — Store-installed Claude Desktop loads its config
  from `...\Packages\Claude_*\LocalCache\Roaming\Claude\claude_desktop_config.json`;
  run `c3-broker install-desktop --config "<that path>"`.
- **Updates rebuild from source** — the auto-updater can't replace a running
  `.exe` on Windows. `git pull` → `go install ./cmd/...` → restart.
- **No systemd** — the default on-demand broker spawn is what you get
  (systemd supervision is Linux-only — see the top-level `INSTALL.md` §6).

See [`DESKTOP.md`](DESKTOP.md) for the remaining Windows caveats (broker
singleton, per-tab Desktop identity).

## Upgrading

Inside Claude Code, refresh the plugin files:

```
/plugin marketplace update
/plugin upgrade c3@c3
```

Then refresh the binaries: **prebuilt** — re-run Step 2's download with the new `VERSION`; **from source** — `/c3:build`. Marketplaces added from GitHub can also auto-update the plugin files at Claude Code startup; that's off by default for third-party marketplaces — toggle it in `/plugin → Marketplaces`. (Auto-update only refreshes plugin files; the binaries still come from the matching release tarball or a rebuild.)

State (`~/.config/c3/mappings.json`) is in XDG, not the plugin cache, so upgrades don't touch it.

For the Codex side, re-run `c3-broker install-codex-shim` after updating the binaries to refresh the symlinks against the new `codex` launcher.

## Uninstalling

```
/plugin uninstall c3@c3                          # removes the plugin from Claude Code
pkill c3-broker                                  # stop the daemon
rm ~/.local/bin/codex 2>/dev/null                # restore your real codex (if you'd installed the shim)
find ~/.nvm/versions/node -name codex -type l -delete 2>/dev/null   # remove NVM-side symlinks (CAUTION: only if they pointed at the C3 launcher)
rm -f "${XDG_RUNTIME_DIR:-/tmp}/c3.sock" /tmp/c3-$UID.sock "${XDG_RUNTIME_DIR:-/run/user/$UID}/c3-broker.pid" "${XDG_RUNTIME_DIR:-/run/user/$UID}/c3-broker.caps"   # broker scratch files
rm -f "/tmp/c3-codex-app-server-$UID.json"       # codex launcher scratch (per-uid path)
rm -rf ~/.config/c3                              # CONFIG; remove only if you don't want to keep mappings/topics
# binaries — remove them from wherever you installed them. The prebuilt path
# (Step 2) puts them in ~/.local/bin; `go install` puts them in $GOBIN, which is
# UNSET by default and then falls back to $(go env GOPATH)/bin.
bindir="$(go env GOBIN)"; [ -n "$bindir" ] || bindir="$(go env GOPATH)/bin"
for d in "$HOME/.local/bin" "$bindir"; do
  [ -d "$d" ] || continue
  rm -f "$d"/c3-broker "$d"/c3-claude-adapter "$d"/c3-codex-adapter \
        "$d"/c3-grok-adapter "$d"/c3-agy-adapter "$d"/c3-desktop-adapter \
        "$d"/claude-shim "$d"/migrate-legacy
  # "$d"/codex is C3's launcher shim shadowing your real codex — the line above
  # already restores it for ~/.local/bin; delete it here only if you installed
  # the shim into $bindir as well.
done
```

Optionally also remove the source clone (`rm -rf ~/.local/share/c3` or
wherever you cloned in step 1).

The `~/.config/c3/mappings.json` lives outside both plugins; uninstalling the plugins doesn't touch it. Decide separately whether to keep it (you might reinstall later).

## Troubleshooting first-install issues

- **Building from source fails with `command not found: go`** — install Go ≥1.25, or use the prebuilt tarball instead (Step 2).
- **Binaries installed but `c3-broker` not on PATH** — the install dir isn't on `PATH` (`~/.local/bin` for prebuilt, `$(go env GOPATH)/bin` for source). See Step 2.
- **`/c3:setup` says it can't reach Telegram** — check the bot token. Try `curl https://api.telegram.org/bot<TOKEN>/getMe`; should return your bot's info.
- **Topic creation fails** — the bot isn't an admin with `Manage Topics` in the supergroup. Group settings → Administrators → your bot → toggle "Manage Topics" on.
- **`which codex` still resolves to NVM** — re-run `c3-broker install-codex-shim`, then `hash -r` (bash/zsh) or open a new terminal. Verify with `readlink $(which codex)`; it should point at the C3 `codex` launcher.
- **Voice transcription doesn't fire** — the broker log surfaces a `[plugin] stt: ...` line per voice message; check `~/.local/state/c3/broker.log`. Common causes: `~/.claude/stt.env` is missing or has no `OPENROUTER_API_KEY` / `SARVAM_API_KEY` (look for `stt: msg=N error ... stderr-tail=...`); `python3` not on the broker's PATH; `mappings.json:plugins.stt.enabled` is `false`. Failing voice messages now surface as `[STT FAILED: <reason>]` in the CLI rather than silent `(voice message)`. If you don't need voice, set `enabled: false`.
