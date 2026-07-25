# C3 on Claude Desktop (Windows)

The `c3-desktop-adapter` bridges C3 into **Claude Desktop** as an MCP server. Read the honest limitation first — it changes how you use it.

## What it is (and what it is not)

On Claude Desktop, C3 is a **pull bridge, not a push one.**

There is **no** "a Telegram message arrived → Claude speaks up." That live-render behaviour exists only in Claude Code (via its channel-notification support). Claude Desktop has no way for an MCP server to interrupt a chat, so:

- **Inbound is poll-only.** Telegram messages sent to your topic wait in C3's **durable on-disk queue**. Claude sees them only when *you ask it to check* — it calls `fetch_queue` and reads back whatever is waiting. Nothing is lost while you're away; it just doesn't surface on its own.
- **Outbound is on request.** Ask Claude to `reply` / `react` and it sends to Telegram.
- *(Optional)* an hourly **Claude Cowork Scheduled Task** can poll on a timer ("every hour, check my C3 messages and summarize"), which is the closest you get to push — a cron, not an interrupt.

If you want live "Claude speaks when Telegram pings," that's Claude Code with `--dangerously-load-development-channels`, not Desktop. See the top-level `README.md`.

## Prerequisites

Both the broker and the adapter run **locally on the Windows box** — a self-contained C3 node, no separate server:

- **`c3-broker`** built/installed and on `PATH`.
- **`c3-desktop-adapter.exe`** built/installed and on `PATH`.

Get the binaries either way:

- **Cross-compile** from a dev machine: `make build` builds every binary; `PLATFORMS` now includes `windows/amd64` and `windows/arm64`, so `make dist` produces a Windows release tarball containing `c3-desktop-adapter.exe`.
- **Build on the box:** with Go installed on Windows, `go install ./cmd/c3-broker ./cmd/c3-desktop-adapter`.

You also need a configured `~/.config/c3/mappings.json` (bot token, allowlist). Run `c3-broker setup` on the box, or copy over an existing config — but read the **Telegram single-consumer** caveat below before pointing it at a token another machine already polls.

## Install

### Option A — the installer (recommended)

```
c3-broker install-desktop
```

It writes/merges Claude Desktop's config at the per-OS default:

| OS | Config path |
|----|-------------|
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Linux | `$XDG_CONFIG_HOME/Claude/claude_desktop_config.json` (default `~/.config/Claude/...`) |

(The official Claude Desktop **Linux beta** landed 2026-06 — Debian/Ubuntu via Anthropic's apt repo, Arch via the `claude-desktop` / `claude-desktop-bin` AUR packages that repackage it. All read the XDG path above.) It **merges** — every other MCP server and every other key in the file is preserved; only the `mcpServers.c3` entry is added/updated. If the file is present but not valid JSON, it refuses to touch it and tells you to fix or remove it.

It resolves `c3-desktop-adapter.exe` on `PATH` and writes its **absolute** path (Claude Desktop requires an absolute command path). If the binary isn't on `PATH` yet, it writes the bare name and warns you to edit it once you've built it. Re-running the installer **replaces the `c3` entry** (any `args`/`env` you hand-added to it are overwritten); every other server is untouched.

`--config <path>` (or `--path <path>`) overrides the target file on any OS — useful to stage a config on Linux, or to target an MSIX install path (below).

### Option B — hand-edit

Open `%APPDATA%\Claude\claude_desktop_config.json` and add the `c3` server. Use an **absolute** path with escaped backslashes:

```json
{
  "mcpServers": {
    "c3": {
      "command": "C:\\Users\\you\\.local\\bin\\c3-desktop-adapter.exe"
    }
  }
}
```

If the file already has other servers, add `c3` alongside them — don't replace the object.

### Then, either way

**Fully quit and restart Claude Desktop** — tray icon → **Quit**, not just closing the window. It reads the config and spawns MCP servers only at startup.

## First use

In a Desktop chat:

1. `attach name=<topic>` — claim (or create) your Telegram topic.
2. "check my messages" — Claude calls `fetch_queue` and reads back anything waiting in the queue.
3. Ask it to `reply` / `react` to respond.

Tool calls surface as a local **Allow / Always allow** approval in the Desktop GUI (see caveats).

## The `/fetch-queue` slash command (low-ceremony pull)

C3 also exposes an MCP **prompt** named `fetch-queue`, which Claude Desktop surfaces as a **slash command**. Type `/fetch-queue` (it may appear namespaced under the `c3` server in the `/` menu) and the queue is pulled and dropped straight into the chat — no "please check my messages" sentence, and no tool-call reasoning turn to trigger the fetch. It's the one-keystroke version of the `fetch_queue` tool. (Kebab-case matches Claude's slash-command convention and keeps it distinct from the underscore `fetch_queue` *tool*.)

- **Default** drains the whole queue for the attached topic (consumes it, like `fetch_queue`).
- `limit=N` pulls the N oldest instead of everything.
- `ack=false` **peeks** without consuming — the messages stay queued.

It still needs an **attached** topic (run `attach` first); unattached, it reports "no route claimed". This is a *reduced-ceremony pull*, not live push — Desktop still can't surface a message on its own. (If a future test shows Desktop calling `prompts/get` more than once per invocation, the consuming default would need to become peek — verify on first use.)

## The C3 Inbox panel (live view)

Ask Claude to **"open the c3 inbox"** and it calls the `open_inbox` tool, which renders an MCP-App HTML panel inline in the chat. The panel splits **watching** a topic (read-only) from **owning** it (the exclusive claim), so it never fights a Claude Code session for a topic:

- **Watch** (default) — type a topic and click **Watch**. The panel calls the `observe` tool, a **read-only peek** that shows the durable queue every ~5s **without claiming the topic** and **without stealing it** from whoever owns it. A holder line names the current owner ("held by claude-code (pid N) — read-only" / "unclaimed" / "you own it"). This is the fix for the old tug-of-war: a Claude Code session in Telegram mode keeps its claim (and keeps replying to you) while the panel shows you the same inbox live. Merely opening the panel never steals anything.
- **Take over here** (explicit) — the *only* button that touches the exclusive claim. It appears when the watched topic is not owned by this Desktop session, and makes Desktop the owner so it can receive/consume/reply. The `attach` mode follows the observed state: held by another session → **steal** (evicts that holder — a deliberate one-tap decision, never automatic); unclaimed → plain attach; brand-new name → **create**.
- **Hand to Claude** / **Auto** — feed the waiting messages to Claude via `ui/message`. These **consume** the queue, so they are **owner-only**: they appear only once you've taken the topic over (a non-owner can't drain another agent's queue — that's the single-consumer no-loss invariant).

Two host behaviors are worth knowing (they are Claude Desktop limits, not C3 bugs — verified against the MCP-Apps `ext-apps` spec and the shipped app SDK):

- **`ui/message` DRAFTS into the composer on the Code tab — it does not auto-send.** Handed messages land in the message box; **press Enter** to send. There is no send/submit parameter and no gesture-free "start a turn" primitive, so this can't be forced. (The Cowork tab tends to send immediately.) **Auto** therefore means "feed the next message into the composer as you clear the last": if the composer already holds an unsent draft, the host rejects the next hand, so C3 leaves it queued and retries once you send — Auto stays armed (a long failure streak trips a circuit-breaker that disarms it with a reason).
- **The panel is inline and scrolls with the chat.** MCP Apps has no docked/side pane — only `inline`, `fullscreen`, `pip`. The panel offers a best-effort **Pop out** button (requests a floating picture-in-picture overlay so it stays visible); if the host doesn't support `pip` it says so. The reliable way to bring it back is to **re-summon it: "open the c3 inbox"** drops a fresh live panel at the bottom.

## Caveats

> **Telegram is single-consumer — use a separate bot token on Windows.**
> Telegram's `getUpdates` allows **one** poller per bot token. If the Windows broker polls the **same** token as a broker on another machine, Telegram returns **409 Conflict** and inbound breaks on both. Give the Windows box its **own bot token** (a second bot from `@BotFather`), or route it through the C3 Telegram proxy. Do **not** share one token across two live brokers.

- **Microsoft Store (MSIX) install trap.** If Claude Desktop was installed from the Microsoft Store, edits to `%APPDATA%\Claude\` are **ignored** — the config that actually loads lives under:
  ```
  ...\Packages\Claude_*\LocalCache\Roaming\Claude\claude_desktop_config.json
  ```
  Re-run `c3-broker install-desktop --config "<that path>"`, or hand-edit the file there.
- **protocolVersion `2025-11-25`.** The adapter negotiates this MCP protocol version. After restart, confirm the `c3` tools appear in the chat. If they're refused, it's almost always a version-negotiation mismatch — check the adapter and Claude Desktop versions.
- **Approval is a local GUI tap.** Approving a tool call is a click in the Desktop app; "Always allow" is per-chat. There is **no** Telegram permission relay here (that Allow/Deny-over-Telegram flow is Claude Code-only).
- **Auto-update is not wired for Windows.** `c3-broker update --check` works, but installation is refused because replacing some live `.exe` files can leave a mixed-version install. Fully quit C3 and Claude Desktop, then re-extract the newer release tarball over the installed binaries. A source install can instead use `git pull`, then `go install ./cmd/c3-broker ./cmd/c3-desktop-adapter`, followed by a full restart. (From-source builds report version `dev` and never auto-update, so the checker stays quiet.)

## Verify on the box tomorrow

Everything here is **compile-verified on Linux** but **runtime-unverified on Windows**. Confirm on the actual box:

- [ ] The native `.exe` loads over stdio directly — no `cmd /c` wrapper needed in `command`.
- [ ] Which config path actually loads — the plain installer path vs. the MSIX `...\Packages\Claude_*\...` path.
- [ ] `c3-desktop-adapter` shows its tools in a Desktop chat after a full quit + restart.
- [ ] `attach` → `fetch_queue` → `reply` round-trips against a real topic.
- [ ] The broker singleton + unix-socket equivalent work under `%LOCALAPPDATA%\c3` on Windows.
- [ ] STT (if you use voice notes) still transcribes on the Windows box.
