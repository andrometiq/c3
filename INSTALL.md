# Install C3

> **For human users:** open a Claude Code session in any directory and paste:
>
>     follow https://github.com/Andrometiq/c3/blob/master/INSTALL.md to install c3
>
> (or `follow /path/to/local/c3/INSTALL.md` if you've cloned locally)
>
> The agent does everything below. You'll only be asked for your Telegram
> bot token and two short pairing codes during setup — no id hunting.

> **For Claude Code (or any) agent reading this:** you are installing C3 for
> the user. **Start at §0 — ask which environment and platform they want,
> then follow only the sections that apply** (the routing table says which).
> Within a section, execute the steps in order. Surface errors verbatim and
> stop on first failure. Don't paraphrase commands — run them exactly. Every
> step is idempotent (safe to re-run).

---

## 0. Choose your environment and platform

C3 runs under three host environments. **Ask the user which one they want**
(they can add another later) — the binary install (§1) and Telegram config
(§2) are shared; only the host-integration step (§3) differs per environment.

| Environment | What it is | Host-integration (§3) | Inbound delivery |
|---|---|---|---|
| **Claude Code (CLI)** | the `claude` terminal CLI — on Windows, the `claude.exe` bundled inside Claude Desktop | §3A — plugin marketplace + dev-channels flag | live `<channel>` push on Linux/macOS; **poll-only** on Windows (beta) |
| **Claude Desktop** | the desktop app's **Chat / Code** tabs | §3B — `c3-broker install-desktop` | **poll-only** — pull with `fetch_queue` |
| **CoWork** | Claude Desktop's **Cowork** tab (optionally on a scheduled timer) | §3B — same `install-desktop` | **poll-only** + optional hourly poll task |

(Codex is an optional add-on to any of these — §5. **Grok Build**, the **Antigravity
CLI**, and **Cursor Agent CLI** are also supported; they are one-command add-ons layered on
the same binaries and config — §5B.)

### Platform support

> **Linux is the primary, fully-supported platform** — live inbound, prebuilt
> binaries, optional systemd supervision.
> **macOS** is supported — prebuilt binaries; use `launchd` where the Linux
> steps say `systemd`.
> **Windows (Claude Desktop + the bundled Claude Code) is _beta_ for v0.1.0.**
> The install works and the known Windows-specific bugs are fixed, and prebuilt
> Windows tarballs **are** published — but: inbound is **poll-only** (pull with
> `/c3:fetch-queue` / `fetch_queue`), the Windows binaries are cross-compiled
> without a clean-room CI pass, and **`c3-broker update` refuses to self-update
> on Windows** (a running `.exe` cannot be replaced in place — quit C3 and
> re-extract the tarball instead). All Windows specifics are collected in
> **§7 — apply those deltas instead of the Linux-only steps** (skip systemd and
> the shell-rc `PATH` edit; use `.exe` binaries and the User-scope `PATH` edit
> in §7).

### Routing

- **Claude Code CLI — Linux / macOS** → §1 → §2 → §3A → §4. Skip §7.
- **Claude Code CLI — Windows (beta)** → §1 → §2 → §3A → §4, applying the **§7** deltas at each step.
- **Claude Desktop or CoWork — any OS** → §1 → §2 → §3B → §4. On Windows also read **§7**.

---

## 1. Install the binaries (all environments)

C3 ships ten binaries: `c3-broker`, `c3-claude-adapter`, `c3-codex-adapter`,
`c3-grok-adapter`, `c3-agy-adapter`, `c3-cursor-adapter`, `c3-desktop-adapter`,
`claude-shim`, `codex`, `migrate-legacy`. Prefer the prebuilt release tarball; build from
source only if there's no tarball for the user's platform. (**Windows:** a
prebuilt tarball *is* published, but Windows is **beta** — see §7 for the
Windows-specific steps and caveats.)

**Prebuilt (default — Linux / macOS).** Download, verify, and install into
`~/.local/bin`. **No version is hardcoded** — `releases/latest/download/`
always resolves to the newest published release, so this block cannot go stale:

```bash
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m); [ "$ARCH" = x86_64 ] && ARCH=amd64; [ "$ARCH" = aarch64 ] && ARCH=arm64
base="https://github.com/Andrometiq/c3/releases/latest/download"

# SHA256SUMS has a fixed filename, so it downloads straight off `latest`. It
# also lists every tarball in that release — which is where the versioned
# tarball filename comes from. Nothing here needs editing at release time.
curl -fsSL -O "$base/SHA256SUMS"
pkg=$(grep -o "c3_[^ ]*_${OS}_${ARCH}\.tar\.gz" SHA256SUMS | head -1); pkg=${pkg%.tar.gz}
# If pkg is empty there is no tarball for this platform: STOP HERE, skip the rest
# of this block, and use the from-source path below instead.
[ -n "$pkg" ] || echo "STOP: no ${OS}/${ARCH} tarball in the latest release — use the from-source path below."
curl -fsSL -O "$base/${pkg}.tar.gz"

# Verify ONLY the tarball you downloaded. SHA256SUMS lists every platform, so
# checking the whole file would report the five you don't have as failures.
grep " ${pkg}.tar.gz$" SHA256SUMS > SHA256SUMS.this
sha256sum -c SHA256SUMS.this || shasum -a 256 -c SHA256SUMS.this

# The tarball unpacks into a ${pkg}/ directory — install the core binaries and
# their STT/Grok runtime assets. Stage the optional Codex launcher OFF PATH for
# §5.
# NOTE: `codex` is deliberately NOT installed here. It is C3's Codex *launcher*,
# and installing it onto PATH would SHADOW the user's real `codex` (this guide
# puts ~/.local/bin FIRST on PATH) for someone who never asked for Codex support.
# §5 installs it, and only if the user wants Codex integration.
tar xzf "${pkg}.tar.gz"
mkdir -p ~/.local/bin/plugins/c3/stt ~/.local/bin/plugins/c3-grok ~/.local/libexec/c3
for b in c3-broker c3-claude-adapter c3-codex-adapter c3-grok-adapter \
         c3-agy-adapter c3-cursor-adapter c3-desktop-adapter claude-shim migrate-legacy; do
  install -m 0755 "${pkg}/${b}" ~/.local/bin/
done
cp -R "${pkg}/plugins/c3/stt/." ~/.local/bin/plugins/c3/stt/
cp -R "${pkg}/plugins/c3-grok/." ~/.local/bin/plugins/c3-grok/
install -m 0755 "${pkg}/codex" ~/.local/libexec/c3/codex
```

The staged `~/.local/libexec/c3/codex` is not on `PATH`; it cannot shadow the
user's real Codex CLI. §5 materializes it next to `c3-broker` only after the
user opts in.

**If the first `curl` 404s, there is no published release to install** — either
none has been cut yet, or the newest one is a **pre-release** (`releases/latest`
deliberately skips pre-releases). That is not a broken install: use the
from-source path below, which works with no release at all. Check
<https://github.com/Andrometiq/c3/releases> to see what exists. A 404 on the
*second* `curl` (or an empty `pkg`) means the release simply has no tarball for
this OS/arch — same answer, build from source.

To install a *specific* older release instead, replace `latest/download` with
`download/<tag>` — for example `.../releases/download/v0.1.0` — and set `pkg`
to that tag's tarball name by hand. That form is an example only; the pinned
tag is not kept current in this guide, so prefer the `latest` block above.

**From source (fallback, and the path contributors use).** Requires Go ≥1.25
— run `go version`; if it's missing or older than 1.25, tell the user to
install/upgrade from https://go.dev/dl/ (on Windows the portable zip needs no
admin) and stop. Clone the repo into a **durable** directory (this same clone
is what Claude Code CLI users add as a marketplace in §3A, and what `/c3:build`
rebuilds from — pick a stable path, not `/tmp` or a Downloads folder) and
build:

```bash
# idempotent: pull if already cloned, else clone fresh
[ -d ~/.local/share/c3/.git ] && git -C ~/.local/share/c3 pull || git clone https://github.com/Andrometiq/c3 ~/.local/share/c3
cd ~/.local/share/c3 && go install \
  ./cmd/c3-broker ./cmd/c3-claude-adapter ./cmd/c3-codex-adapter \
  ./cmd/c3-grok-adapter ./cmd/c3-agy-adapter ./cmd/c3-cursor-adapter ./cmd/c3-desktop-adapter \
  ./cmd/claude-shim ./cmd/migrate-legacy
```

Go installs to `$GOBIN` (default `$(go env GOPATH)/bin`). (If the user already
added the c3 marketplace from a GitHub URL rather than a local clone, that
plugin cache carries only the plugin subtree, not the Go source — use the clone
above, or the prebuilt tarball.)

### Verify the binaries are installed

```bash
for bin in c3-broker c3-claude-adapter c3-codex-adapter c3-grok-adapter c3-agy-adapter c3-cursor-adapter c3-desktop-adapter claude-shim migrate-legacy; do
  command -v "$bin" >/dev/null && echo "  ✓ $bin" || echo "  ✗ $bin (missing)"
done
command -v c3-broker >/dev/null || echo "WARNING: the install dir is not on \$PATH"
```

`codex` is intentionally absent from that list: it is C3's Codex **launcher**,
not a C3 command, and it is named `codex` precisely so it can take the place of
the real one. Installing it for a user who only wants Claude Code would silently
reroute every `codex` invocation on their machine through C3. §5 installs it —
and only if they want Codex integration. `migrate-legacy` is a one-shot config
migrator most users never run.

If `c3-broker` isn't found, the install dir isn't on `PATH` (`~/.local/bin` for
prebuilt, `$(go env GOPATH)/bin` for source). Tell the user:

> "Append this to your shell rc (`~/.zshrc` or `~/.bashrc`):
>
>     export PATH=\"$HOME/.local/bin:$PATH\"
>
> Open a new terminal and re-run this install to verify."

…and stop. **(On Windows there is no shell rc — set the User `PATH` with the
Settings GUI or the PowerShell snippet in §7. Do not use `setx PATH` — see §7
for why.)**

## 2. Configure C3 (all environments)

If `~/.config/c3/mappings.json` already exists, validate it first
(**Windows:** config lives at `%USERPROFILE%\.config\c3\mappings.json` — §7):

```bash
c3-broker validate
```

If validation passes, tell the user:

> "Existing config at `~/.config/c3/mappings.json` — keeping it. Run
> `c3-broker setup` manually if you want to overwrite (it asks before
> overwriting)."

…and skip to step 3.

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
4. `c3-broker setup stt` — optional voice-transcription keys. **On Windows,
   STT needs extra setup (real Python, not the Store stub) — §7.**
5. `c3-broker setup finish` — installs the host launcher shim + restarts the
   broker + prints a stand-alone "what now" summary to relay to the user.
   (The shim it installs is the Claude Code `claude` wrapper — harmless if
   you only use Claude Desktop; the Desktop wiring is §3B.)

Completed steps are skipped automatically on re-runs. (Bare
`c3-broker setup` remains the interactive fallback for a plain terminal
without an agent — it walks the same token → pairing → STT flow on a TTY.)

> **One bot token per machine.** Telegram allows only **one** poller per bot
> token. If you'll run C3 on a second machine (e.g. a Windows box alongside a
> Linux one), give it its **own** bot token from `@BotFather` — two brokers on
> the same token both break with `409 Conflict`. See `docs/DESKTOP.md`.

### Speech-to-text (voice notes) — Python deps

Voice-note transcription runs a Python handler. **On Linux/macOS, STT needs
only system `python3` + ffmpeg (`ffprobe`); no Python packages, no venv.** The
provider chain uses only the standard library (the Sarvam long-audio path is
native `urllib`), so a plain system `python3` works — even an
externally-managed (PEP 668) one. Override the interpreter via `mappings.json`
`plugins.stt.python` if you need a specific one.

Install **ffmpeg** (provides `ffprobe`, for audio-duration detection) via your
OS package manager. STT still works without it (REST-first), just less
precisely routed for long notes. **Windows STT has its own gotchas (the Store
`python3` is a no-op stub; you must wire `handler_path` + `python`) — §7.**

## 3. Host integration — do the section for YOUR environment

Do **§3A** for Claude Code (CLI), or **§3B** for Claude Desktop / CoWork. Do
both only if the user wants both.

---

### 3A. Claude Code (CLI)

#### 3A.1 Add the C3 marketplace and install the plugin

Tell the user to run these in this Claude Code session and confirm when done:

>     /plugin marketplace add Andrometiq/c3
>     /plugin install c3@c3
>     /reload-plugins
>
> When `/plugin install` asks for **user** vs **project** scope, choose
> **user** — that makes C3 available in every Claude Code session, not
> just this project.

(If you built from source in §1, add *that clone* as the marketplace instead —
`/plugin marketplace add ~/.local/share/c3` — so `/c3:build` can find the Go
source. A GitHub-URL marketplace carries only the plugin subtree.)

**Windows:** there is no `claude` on `PATH` — run these `/plugin` commands
inside the Claude Code that's bundled in the Claude Desktop app (§7).

#### 3A.2 Enable channel notifications (REQUIRED for live inbound)

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

#### 3A.3 Verify config and broker

```bash
c3-broker validate
c3-broker status
```

`validate` exits 0 on a parseable + valid mappings.json. `status` reports
broker liveness, socket path, channels, plugin states. The broker won't
be running yet — that's fine; the next CLI session will spawn it.

#### 3A.4 Tell the user the install is complete

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
> `fetch_queue`. (The optional `claude-shim` installed in §2's `setup finish`
> injects this flag for you — see `docs/INSTALL.md`.)
>
> **On Windows, inbound is poll-only regardless of the flag** — pull it with
> `/c3:fetch-queue` (§7).
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
>   `/c3:fetch-queue`    — pull held inbound (the poll-only path on Windows)
>   `/c3:reload-config`  — broker re-reads mappings.json (SIGHUP, no restart)
>
> Day-to-day guide: `docs/USAGE.md`."

Continue to §4 to verify end-to-end.

---

### 3B. Claude Desktop / CoWork

Claude Desktop (and its Cowork tab) is a **pull bridge, not a push one**:
Claude Desktop can't surface a Telegram message on its own, so inbound waits
in the durable queue and you pull it with `fetch_queue`. There is **no**
dev-channels flag and **no** `settings.json` channel opt-in here (those are
Claude Code-only). Full detail and caveats: **[`docs/DESKTOP.md`](docs/DESKTOP.md).**

#### 3B.1 Register the adapter with Claude Desktop

```bash
c3-broker install-desktop
```

This **merges** an `mcpServers.c3` entry into Claude Desktop's config at the
per-OS default path (every other server and key is preserved):

| OS | Config path |
|----|-------------|
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Linux | `$XDG_CONFIG_HOME/Claude/claude_desktop_config.json` (default `~/.config/Claude/...`) |

It writes the **absolute** path to `c3-desktop-adapter` (Claude Desktop
requires an absolute command path). Pass `--config <path>` to target a
non-default file — needed for a **Microsoft Store (MSIX)** Claude Desktop on
Windows, whose real config lives under a `...\Packages\Claude_*\LocalCache\...`
path (§7).

#### 3B.2 Fully restart Claude Desktop

Tray icon → **Quit** (not just closing the window), then reopen. Claude Desktop
reads its config and spawns MCP servers only at startup.

#### 3B.3 First use

In a Desktop chat:

1. `attach name=<topic>` — claim (or create) your Telegram topic.
2. "check my messages" — Claude calls `fetch_queue` and reads back anything
   waiting in the queue. (Or use the `/fetch-queue` slash command; or "open the
   c3 inbox" for the live-view panel — see `docs/DESKTOP.md`.)
3. Ask it to `reply` / `react` to respond.

Tool calls surface as a local **Allow / Always allow** approval in the Desktop
GUI. There is **no** Telegram permission relay on Desktop (that's Claude
Code-only).

#### 3B.4 CoWork tab

CoWork is the same `c3-desktop-adapter` install — no extra step. The only
CoWork-specific extra is an **optional hourly Claude Cowork Scheduled Task**
that polls on a timer ("every hour, check my C3 messages and summarize") — the
closest Desktop gets to push (a cron, not an interrupt). Set it up from the
Cowork tab if you want timed pulls.

Continue to §4 to verify.

---

## 4. Verify end-to-end

- **Claude Code (CLI):** in a fresh session started with the dev-channels flag
  (§3A.4), `cd` into a project, run `/c3:attach`, confirm the topic, then send
  a message to that topic from your phone. On Linux/macOS it appears live as a
  `<channel>` block; on Windows pull it with `/c3:fetch-queue`. Reply via the
  agent's `reply` tool — it lands in the topic.
- **Claude Desktop / CoWork:** after the restart, confirm the `c3` tools appear
  in a chat, `attach name=<topic>`, send a message from your phone, then "check
  my messages" (`fetch_queue`) and confirm it reads back. Reply and confirm it
  lands.
- **Voice (optional):** send a voice note; it should transcribe to
  `[Transcribed voice]: …` (Linux/macOS out of the box; Windows needs the §7
  STT wiring).

## 5. (Optional) Enable Codex integration

Skip this step if the user doesn't use Codex. **This is the step that puts C3's
`codex` launcher on their PATH in place of the real Codex — don't run it for a
user who only wants Claude Code.**

Prebuilt installs already staged the launcher off `PATH` in §1. Source installs
must build that staged copy first. Then let the guarded installer materialize it
next to `c3-broker` and wire the PATH/NVM shims:

```bash
# Source only; skip when §1's prebuilt path already staged this file.
if [ ! -x "$HOME/.local/libexec/c3/codex" ]; then
  mkdir -p "$HOME/.local/libexec/c3"
  (cd "$HOME/.local/share/c3" &&
    go build -o "$HOME/.local/libexec/c3/codex" ./cmd/codex)
fi

c3-broker install-codex-shim
```

The installer verifies the staged executable is C3's launcher. If no live C3
launcher exists yet it copies the staged file next to `c3-broker`; an existing
live C3 launcher is authoritative, so a stale staged file can never downgrade
it. It then symlinks the live launcher into `~/.local/bin/codex` and every
`~/.nvm/versions/node/*/bin/` so existing shells (which hash `codex` to the NVM
path) bypass NVM in favor of the launcher. It refuses to replace an unrelated
regular file without `--force`. It's idempotent; re-running is safe. Tell the
user to open a fresh terminal and verify with `readlink $(which codex)` — on a
source install it should point at the C3 launcher next to `c3-broker`; on the
prebuilt `~/.local/bin` layout, that path is the launcher itself.

On the **prebuilt** install the launcher already lives at
`~/.local/bin/codex`, so that entry is skipped (there is nothing to shim) and
only the NVM directories get symlinks. If a target is a regular file rather
than a symlink — i.e. it may be a real `codex` binary — the command refuses
rather than delete it; pass `--force` to replace it deliberately.

If they don't have Codex installed yet, skip — they can run this later
after `npm install -g @openai/codex` (or however they get Codex).
(On Windows, Codex has no symlink/NVM equivalent yet and stays poll-only —
tracked as a beta follow-up.)

## 5B. (Optional) Grok Build / Antigravity / Cursor

All three are add-ons to the install above — same binaries (§1), same `mappings.json` (§2), one
extra command each. None replaces a §3 host; you can run them alongside one.

**Grok Build:**

```bash
c3-broker install-grok
```

Patches `~/.grok/config.toml` in place, touching only the keys C3 owns: it sets
`[cli] use_leader = true` (leader mode — required for live inbound) and points
`[mcp_servers.c3]` at `c3-grok-adapter`. If it can't make the change safely — for example a
commented-out `use_leader` it must not guess at — it refuses and prints the manual edit
instead of writing a half-configured file. Then follow the plugin-install and reload lines it
prints. Without leader mode the adapter still works, but inbound is pull-only
(`fetch_queue`). Detail: [`docs/GROK-INJECT.md`](docs/GROK-INJECT.md).

**Antigravity CLI:**

```bash
c3-broker install-agy
```

Writes a `c3` plugin into `~/.gemini/antigravity-cli/plugins/c3/` (`plugin.json`,
`mcp_config.json`, `hooks.json`) pointing at `c3-agy-adapter`, then prints the verification
steps. Antigravity has no async push, so inbound is **poll-only** — pull it with
`fetch_queue`. This is the newest adapter and the least travelled; expect rougher edges than
the Claude Code path.

**Cursor Agent CLI:**

```bash
c3-broker install-cursor
```

Merges `mcpServers.c3` → `c3-cursor-adapter` into `~/.cursor/mcp.json` (preserves other
servers) and installs `~/.cursor/commands/{fetch,c3-fetch}.md`. Cursor's stock interactive TUI
has no idle-wake / channel push, so inbound is **poll-only** — call `fetch_queue`, or use
`/fetch` / `/c3-fetch` (or the MCP prompt `fetch-queue`). Do not point Cursor at
`c3-claude-adapter` (black-hole risk). After install: `agent mcp enable c3` if prompted, then
`agent` and `attach`.

## 6. (Optional, Linux only) Supervise the broker with systemd

> **macOS / Windows:** there is no `systemd`/`systemctl` — skip this step. On
> macOS the default on-demand spawn works fine (write a `launchd` LaunchAgent
> for always-on supervision); on Windows see §7.

By default the broker is spawned on demand by the first adapter and stays up as
a singleton. If you want it auto-restarted even when **no CLI session is open**
(so a crash can't leave inbound silently dead until your next launch), enable the
opt-in `systemd --user` unit:

```bash
mkdir -p ~/.config/systemd/user
# from a clone:  cp docs/systemd/c3-broker.service ~/.config/systemd/user/
# prebuilt (no clone):
curl -fsSL -o ~/.config/systemd/user/c3-broker.service \
  https://raw.githubusercontent.com/Andrometiq/c3/master/docs/systemd/c3-broker.service
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

---

## 7. Windows (beta) — platform deltas

> **Windows is beta for v0.1.0.** Everything below is the standard
> "Unix-syscall-on-Windows" class of difference; the known bugs are fixed.
> Inbound is **poll-only** on Windows (for both Claude Code and Claude Desktop):
> the render-detector fails safe and **holds inbound in the durable queue**
> rather than risk losing it, so you pull messages with `/c3:fetch-queue`
> (Claude Code) or `fetch_queue` / `/fetch-queue` (Desktop). They are held, not
> dropped.

Apply these instead of the Linux-only steps referenced above:

- **§1 binaries — prebuilt tarball or source.** The release publishes
  `c3_<version>_windows_amd64.tar.gz` and `..._windows_arm64.tar.gz`; the
  binaries inside carry the `.exe` suffix. Extract it and put the eight core
  `.exe` files from §1 on your PATH; leave `codex.exe` out unless and until the
  Windows Codex integration graduates from its current beta limitation (the §1
  verify loop and every `c3-broker …` command work the same under Git Bash /
  PowerShell). These Windows binaries are
  cross-compiled and have **not** had a clean-room CI pass — that is what
  "beta" means here. To build them yourself instead, install **Go ≥1.25** (the
  portable zip needs no admin), then `git clone https://github.com/Andrometiq/c3`
  (or `git pull` if you already have it) into a durable dir and run §1's same
  eight-package `go install` command.
- **§1 PATH — edit the *User* PATH; there is no shell rc.** Two safe ways:

  **GUI (recommended).** Start → search "Edit environment variables for your
  account" → under **User variables**, select `Path` → **Edit** → **New** →
  add `%USERPROFILE%\.local\bin` → OK.

  **PowerShell (scriptable, idempotent).** Reads and writes the **User** scope
  only, and does nothing if the entry is already present:

  ```powershell
  $dir      = Join-Path $env:USERPROFILE '.local\bin'
  $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')   # User scope only
  $entries  = @($userPath -split ';' | Where-Object { $_ -ne '' })
  $already  = @($entries | ForEach-Object { $_.Trim().TrimEnd('\') }) -contains $dir.TrimEnd('\')

  if ($already) {
      "$dir is already on your User PATH - nothing changed."
  } else {
      [Environment]::SetEnvironmentVariable('Path', (($entries + $dir) -join ';'), 'User')
      $env:Path = "$env:Path;$dir"   # this session too, so you can carry on here
      "Added $dir to your User PATH. Other open terminals need a restart."
  }
  ```

  Then **open a NEW terminal** — an already-open shell keeps the stale PATH and
  reports `c3-broker: command not found`.

  > **Do not use `setx PATH "%PATH%;..."`.** It is a known PATH-destroyer, and
  > no amount of care makes it safe: `%PATH%` at that moment is the **merged
  > system+user** PATH, so it permanently copies the entire *system* PATH into
  > your *user* PATH; and `setx` truncates its value at 1024 characters without
  > warning. The two together silently shred a normal PATH. The methods above
  > touch only the User scope and have no length limit.
  >
  > One side effect of the PowerShell form: reading the User PATH through .NET
  > expands any `%VAR%` references, so entries written as `%USERPROFILE%\...`
  > get rewritten in expanded form. Harmless for most setups — use the GUI if
  > you deliberately rely on unexpanded entries.
- **`claude` is not on PATH.** On Windows, Claude Code is bundled inside the
  Claude Desktop app (`%APPDATA%\Claude\claude-code\<ver>\claude.exe`). Run the
  §3A `/plugin` commands from that bundled Claude Code.
- **§2 config path.** C3's config is at `%USERPROFILE%\.config\c3\mappings.json`.
- **§3B MSIX / Microsoft Store Desktop.** If Claude Desktop was installed from
  the Microsoft Store, edits to `%APPDATA%\Claude\` are ignored — the config
  that actually loads is under
  `...\Packages\Claude_*\LocalCache\Roaming\Claude\claude_desktop_config.json`.
  Run `c3-broker install-desktop --config "<that path>"`.
- **§2 STT setup.** The `python3`/`python` that Windows ships is the Microsoft
  Store **alias stub** — it exits 0 and transcribes nothing, masking the
  failure. Install a **real** interpreter and ffmpeg
  (`winget install Python.Python.3.12`, `winget install Gyan.FFmpeg`), then set
  both `plugins.stt.handler_path` (absolute path to the repo's
  `plugins/c3/stt/stt-handler.py`) **and** `plugins.stt.python` (absolute path
  to the real `python.exe`) in `mappings.json`, and restart the broker so it
  loads them.
- **Updates.** `c3-broker update --check` works, but installation is refused on
  Windows: replacing some live `.exe` files can leave a mixed-version install.
  Fully quit C3 / Claude Desktop / the coding CLI, then re-extract the newer
  release tarball over the installed binaries. A source install can instead use
  `git pull` → §1's eight-package `go install`, followed by a full restart.
- **Skip §6 (systemd).** There is no systemd on Windows; the default on-demand
  broker spawn is what you get.

Known beta rough edges (documented, not yet smoothed): the broker singleton
lock is weaker on Windows (a manual kill can briefly race a respawn — write
config to disk first, then kill once); Claude Desktop gives MCP servers no
per-tab id, so reopening a Desktop tab spawns a new adapter and you may need to
re-`attach` and confirm a one-tap "take over" to reclaim the topic. See
`docs/DESKTOP.md` for the full caveat list.

---

## Manual install (without an agent)

The same steps run by hand work fine — copy each shell block above into a
terminal. The only interactive step is `c3-broker setup` (a TTY flow that
walks token → pairing codes → STT). See [`docs/INSTALL.md`](docs/INSTALL.md)
for a more verbose human-targeted version.

End.
