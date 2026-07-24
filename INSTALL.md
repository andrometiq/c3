# Install C3

> **For human users:** open a Claude Code session in any directory and paste:
>
>     follow https://github.com/karthikeyan5/c3/blob/master/INSTALL.md to install c3
>
> (or `follow /path/to/local/c3/INSTALL.md` if you've cloned locally)
>
> The agent does everything below. You'll only be asked for your Telegram
> bot token and two short pairing codes during step 4 — no id hunting.

> **For Claude Code (or any) agent reading this:** you are installing C3 for
> the user. Execute the numbered steps in order. Surface errors verbatim and
> stop on first failure. Don't paraphrase commands — run them exactly. Every
> step is idempotent (safe to re-run).

> **Platform support:** **Linux is the primary, fully-supported platform.**
> **Windows (Claude Desktop + Claude Code) is _beta_** — the install works and
> the known Windows-specific bugs are fixed, but inbound is **poll-only** there
> (pull messages with `/c3:fetch-queue`) and it hasn't yet had a full clean-room
> install pass. There's no prebuilt Windows release yet, so Windows installs
> build from source (needs Go).

---

## 1. Add the C3 marketplace and install the plugin

Tell the user to run these in this Claude Code session and confirm when done:

>     /plugin marketplace add karthikeyan5/c3
>     /plugin install c3@c3
>     /reload-plugins
>
> When `/plugin install` asks for **user** vs **project** scope, choose
> **user** — that makes C3 available in every Claude Code session, not
> just this project.

(Contributors building from source instead: have them
`git clone https://github.com/karthikeyan5/c3 ~/.local/share/c3` into a
durable directory and `/plugin marketplace add ~/.local/share/c3` — a
GitHub plugin cache doesn't carry the Go source `/c3:build` needs. Capture
the clone path; you'll need it for the from-source fallback in step 2.)

## 2. Install the binaries

C3 ships nine binaries: `c3-broker`, `c3-claude-adapter`, `c3-codex-adapter`,
`c3-grok-adapter`, `c3-agy-adapter`, `c3-desktop-adapter`, `claude-shim`, `codex`,
`migrate-legacy`. Prefer the prebuilt release
tarball; fall back to building from source only if there's no tarball for
the user's platform.

**Prebuilt (default).** Download, verify, and install into `~/.local/bin`:

```bash
VERSION=v0.1.0
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m); [ "$ARCH" = x86_64 ] && ARCH=amd64; [ "$ARCH" = aarch64 ] && ARCH=arm64
base="https://github.com/karthikeyan5/c3/releases/download/$VERSION"
curl -fsSL -O "$base/c3_${VERSION}_${OS}_${ARCH}.tar.gz"
curl -fsSL -O "$base/SHA256SUMS"
{ sha256sum --ignore-missing -c SHA256SUMS || shasum -a 256 -c SHA256SUMS; }
mkdir -p ~/.local/bin
tar xzf "c3_${VERSION}_${OS}_${ARCH}.tar.gz" -C ~/.local/bin \
  c3-broker c3-claude-adapter c3-codex-adapter c3-grok-adapter c3-agy-adapter c3-desktop-adapter claude-shim codex migrate-legacy
```

If the download 404s (no tarball for this platform), fall through to the
from-source path.

**From source (fallback).** Requires Go ≥1.25 — run `go version`; if it's
missing or older than 1.25, tell the user to install/upgrade from
https://go.dev/dl/ and stop. With the repo cloned + added as a local
marketplace (step 1's contributor path), locate the source and build:

```bash
PLUGIN_ROOT=$(ls -d ~/.claude/plugins/cache/*/c3 2>/dev/null | head -1)
SRC_ROOT=$(cd "$PLUGIN_ROOT/../.." 2>/dev/null && pwd)
if [ -z "$SRC_ROOT" ] || [ ! -f "$SRC_ROOT/go.mod" ]; then
  echo "ERROR: no go.mod found — the marketplace points at a GitHub source (plugin subtree only), not a local clone. Use the prebuilt tarball above, or clone the repo first and 'marketplace add' the clone path."
  exit 1
fi
echo "Building from $SRC_ROOT (1–3 minutes on first run)..."
cd "$SRC_ROOT" && go install ./cmd/...
```

Go installs to `$GOBIN` (default `$(go env GOPATH)/bin`).

## 3. Verify the binaries are installed

```bash
for bin in c3-broker c3-claude-adapter c3-codex-adapter c3-grok-adapter c3-agy-adapter c3-desktop-adapter claude-shim codex migrate-legacy; do
  command -v "$bin" >/dev/null && echo "  ✓ $bin" || echo "  ✗ $bin (missing)"
done
command -v c3-broker >/dev/null || echo "WARNING: the install dir is not on \$PATH"
```

The `codex` binary is the C3 launcher (only used if the user wants Codex
integration; a separate real Codex on PATH may shadow it — sorted out in the
optional codex-shim step below). `migrate-legacy` is a one-shot config
migrator most users never run.

If `c3-broker` isn't found, the install dir isn't on `PATH` (`~/.local/bin`
for prebuilt, `$(go env GOPATH)/bin` for source). Tell the user:

> "Append this to your shell rc (`~/.zshrc` or `~/.bashrc`):
>
>     export PATH=\"$HOME/.local/bin:$PATH\"
>
> Open a new terminal and re-run this install to verify."

…and stop.

## 4. Configure C3

If `~/.config/c3/mappings.json` already exists, validate it first:

```bash
c3-broker validate
```

If validation passes, tell the user:

> "Existing config at `~/.config/c3/mappings.json` — keeping it. Run
> `c3-broker setup` manually if you want to overwrite (it asks before
> overwriting)."

…and skip to step 5.

If validation FAILS (e.g. a half-corrupted carryover from a previous
install), surface the error verbatim and ask the user whether to
overwrite. On yes, back up the existing file (`cp
~/.config/c3/mappings.json ~/.config/c3/mappings.json.broken-$(date
+%s)`) and continue to interactive setup.

Otherwise, run the guided setup: follow the c3 plugin's `/c3:setup`
command flow (its driver is `plugins/c3/commands/setup.md` in the clone —
read it and drive the phased subcommands it describes). In short:

1. `printf %s 'THE_TOKEN' | c3-broker setup token` — validates the bot
   token via Telegram `getMe` BEFORE writing (on 401 or network failure
   it refuses to write and surfaces the actual error).
2. `c3-broker setup pair dm --code <4-digit code> --timeout-sec 240` —
   the user DMs the code to the bot; their user id is discovered and
   recorded automatically (no `@userinfobot` hunt).
3. `c3-broker setup pair group --code <fresh code> --name main --timeout-sec 240`
   — the user sends the code in the (Topics-enabled) group; the group
   chat id is discovered and recorded automatically (no `-100…` hunt).
4. `c3-broker setup stt` — optional voice-transcription keys.
5. `c3-broker setup finish` — host integration + broker restart + a
   stand-alone "what now" summary to relay to the user.

Completed steps are skipped automatically on re-runs. (Bare
`c3-broker setup` remains the interactive fallback for a plain terminal
without an agent — it walks the same token → pairing → STT flow on a TTY.)

### Speech-to-text (voice notes) — Python deps

Voice-note transcription runs a Python handler. **STT needs only system
`python3` + ffmpeg (`ffprobe`); no Python packages, no venv.** The provider
chain uses only the standard library (the Sarvam long-audio path is native
`urllib`), so a plain system `python3` works — even an externally-managed
(PEP 668) one. Override the interpreter via `mappings.json`
`plugins.stt.python` if you need a specific one.

Install **ffmpeg** (provides `ffprobe`, for audio-duration detection) via your
OS package manager. STT still works without it (REST-first), just less
precisely routed for long notes.

## 5. Verify

```bash
c3-broker validate
c3-broker status
```

`validate` exits 0 on a parseable + valid mappings.json. `status` reports
broker liveness, socket path, channels, plugin states. The broker won't
be running yet — that's fine; the next CLI session will spawn it.

## 5.5. Enable channel notifications (REQUIRED)

Claude Code requires explicit opt-in before it surfaces
`notifications/claude/channel` from any plugin. **Without this step the
broker delivers messages successfully but the CLI never sees them.**

Read `~/.claude/settings.json` and ensure these two top-level keys are
present:

```json
"channelsEnabled": true,
"allowedChannelPlugins": [
  { "marketplace": "c3", "plugin": "c3" }
]
```

If the user already has `allowedChannelPlugins` with other entries,
keep them and add `c3` alongside.

**Permission gotcha:** Claude Code's auto-permission classifier treats
`~/.claude/settings.json` edits as self-modification and almost always
denies the Write tool here. **When that denial fires, STOP. Do not retry.
Do not paraphrase. Print the literal block below to the user verbatim**
(both keys, including any other `allowedChannelPlugins` entries already
present), then ask "paste this into `~/.claude/settings.json` and tell
me when done":

```json
"channelsEnabled": true,
"allowedChannelPlugins": [
  { "marketplace": "c3", "plugin": "c3" }
]
```

Wait for the user's confirmation before proceeding.

## 6. (Optional) Enable Codex integration

Skip this step if the user only uses Claude Code. If they also use Codex,
run:

```bash
c3-broker install-codex-shim
```

This symlinks the C3 `codex` launcher into `~/.local/bin/codex` and into
every `~/.nvm/versions/node/*/bin/` so existing shells (which hash `codex`
to the NVM path) bypass NVM in favor of the launcher. It's idempotent;
re-running is safe. Tell the user to open a fresh terminal and verify with
`readlink $(which codex)` — it should point at `$GOBIN/codex`.

If they don't have Codex installed yet, skip — they can run this later
after `npm install -g @openai/codex` (or however they get Codex).

## 6.5. (Optional, Linux only) Supervise the broker with systemd

> **macOS:** there is no `systemd`/`systemctl` — skip this step. The default
> on-demand spawn model works fine; if you want always-on supervision, write a
> `launchd` LaunchAgent that runs `c3-broker` instead.

By default the broker is spawned on demand by the first adapter and stays up as
a singleton. If you want it auto-restarted even when **no CLI session is open**
(so a crash can't leave inbound silently dead until your next launch), enable the
opt-in `systemd --user` unit:

```bash
mkdir -p ~/.config/systemd/user
# from a clone:  cp docs/systemd/c3-broker.service ~/.config/systemd/user/
# prebuilt (no clone):
curl -fsSL -o ~/.config/systemd/user/c3-broker.service \
  https://raw.githubusercontent.com/karthikeyan5/c3/master/docs/systemd/c3-broker.service
# Edit ExecStart= to your c3-broker path (e.g. ~/.local/bin/c3-broker) before enabling.
systemctl --user daemon-reload
systemctl --user enable --now c3-broker.service
loginctl enable-linger "$USER"   # keep it running across logout
```

It coexists with adapter auto-spawn (the broker is a flock singleton). See
`docs/systemd/README.md` for details and uninstall.

**STT caveat:** a systemd-supervised broker has no `$CLAUDE_PLUGIN_ROOT`, so set
`plugins.stt.handler_path` in `~/.config/c3/mappings.json` to your cloned repo's
`plugins/c3/stt/stt-handler.py` or voice transcription silently turns off. (STT
needs only system `python3` + ffmpeg (`ffprobe`); no Python packages, no venv.)
Details in `docs/systemd/README.md`.

## 7. Tell the user the install is complete

> "Installation complete.
>
> **Restart this Claude Code session with the dev-channels flag**:
>
>     claude --dangerously-load-development-channels plugin:c3@c3
>
> A plain `claude` works for sending outbound, but **inbound won't render
> live** in that session (Claude Code only surfaces channel notifications
> from plugins opted-in via this flag or the production marketplace flow).
> It isn't lost, though — C3 detects a session that can't render and holds
> inbound in the durable queue; relaunch with the flag, or recover with
> `fetch_queue`.
>
> Then in any project directory, run `/c3:attach` and confirm the proposal —
> the broker will create a Telegram topic named after that directory.
>
> Useful slash commands going forward:
>   `/c3:attach`         — claim a Telegram topic for this session
>   `/c3:detach`         — release the current claim
>   `/c3:topics`         — list known topics + claim state
>   `/c3:status`         — broker health check
>   `/c3:setup`          — re-run guided setup (skips completed steps)
>   `/c3:build`          — rebuild binaries after `git pull` in the source dir
>   `/c3:reload-config`  — broker re-reads mappings.json (SIGHUP, no restart)
>
> Day-to-day guide: `docs/USAGE.md`."

End.

---

## Manual install (without an agent)

The same steps run by hand work fine — copy each shell block above into a
terminal. The only interactive step is `c3-broker setup` (a TTY flow that
walks token → pairing codes → STT). See [`docs/INSTALL.md`](docs/INSTALL.md)
for a more verbose human-targeted version.
