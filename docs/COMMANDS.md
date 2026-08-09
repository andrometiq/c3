# C3 commands — cross-CLI source of truth

The verb spec for C3's user-facing commands. Each verb is implemented
once in C3 (in the broker as a CLI subcommand, or in the adapter as an
MCP tool) and exposed by each CLI through a thin wrapper. **When a
verb's behavior changes, edit this file first**, then sync each CLI's
wrapper to match.

The principle: the actual logic lives in the
shared layer (broker / MCP); per-CLI surface area is intentionally
thin so the same set of verbs is trivial to add for every new CLI we
support.

## Verb table

| Verb            | Shared interface                  | Mode          | What it does                                                                                                          |
|-----------------|-----------------------------------|---------------|-----------------------------------------------------------------------------------------------------------------------|
| `status`        | `c3-broker status` (CLI)          | pure shell    | Daemon liveness, socket reachability, mappings.json validation, channel state, **live route claims** (via OpListClaims). |
| `topics`        | `c3-broker topics` (CLI)          | pure shell    | List every topic in mappings.json + which session (if any) currently claims it.                                       |
| `build`         | core `go install` package set (shell) | pure shell | Rebuild C3's eight core binaries; the PATH-shadowing Codex launcher remains opt-in. |
| `setup`         | `c3-broker setup …` (CLI)         | agent-guided / interactive | Configure C3. Primary path: the `/c3:setup` slash command drives the phased subcommands one step at a time — `setup token` (validate via getMe + record), `setup pair dm` / `setup pair group` (code-based id discovery: a 4-digit code sent in Telegram discovers the user id / group chat id — no id hunting), `setup stt`, `setup finish` (host integrations + broker restart). Bare `c3-broker setup` is the full interactive TTY flow (fallback for a plain terminal). Writes mappings.json (mode 0600). |
| `reload-config` | `pkill -HUP c3-broker`            | pure shell    | Signal the broker to re-read mappings.json. Non-disruptive — no process restart, in-memory pointer swap, live claims preserved. For binary updates, restart Claude Code instead. |
| `pair`          | `c3-broker pair …` (CLI)          | pure shell    | Arm a Telegram pairing window. A 4-digit code sent from Telegram allowlists the DM `user_id` (`pair dm`) or a group `chat_id` (`pair group <chat_id>`). The setup flow uses this under the hood for id-free discovery. |
| `ping`          | `c3-broker ping` (CLI)            | pure shell    | Send a one-shot "this is me" message to the attached topic, identifying which CLI session currently owns it. Run in each candidate tab to find the owner before force-stealing. |
| `sessions`      | `c3-broker sessions` (CLI)        | pure shell    | List every live Claude Code / Codex session the broker tracks — CWD, attached topic, and a "you are here" marker for the calling terminal. |
| `attach`        | `attach(expr=…)` (MCP tool)       | LLM dispatch  | Attach this session's adapter to a Telegram topic. Broker parses `expr` and either claims an explicit target, resumes the session's own topic, or proposes a picker/confirmation. |
| `detach`        | `detach()` (MCP tool)             | LLM dispatch  | Release the session's current claim (sends `OpRelease`). Claude + Codex + Grok. |
| `fetch-queue`   | `fetch_queue(limit=…)` (MCP tool) + `fetch-queue` (MCP prompt on Desktop/Cursor) | LLM dispatch / prompt inject | Drain held inbound for this session's topic. Bare = all; optional count fetches the oldest N. Claude: `/c3:fetch-queue` and short alias `/c3:fetch`. Desktop: `/fetch-queue` MCP prompt. Cursor: MCP prompt `fetch-queue` plus `~/.cursor/commands/{fetch,c3-fetch}.md` from `install-cursor`. |
| `release`       | `c3-broker release <cwd>` (CLI)   | pure shell    | **Stubbed in v1** — intended to drop a route claim by cwd without restarting the broker; returns 'not yet implemented' today; workaround is `/exit` the holding session. |

## MCP tools (agent-invoked)

Beyond `attach` / `detach` / `topics`, the adapters expose a set of message and interaction tools the agent calls directly (no slash wrapper). All are broker-dispatched; the `✓ / —` columns are per-CLI availability.

| Tool                 | Claude | Codex | Grok | What it does                                                                 |
|----------------------|:------:|:-----:|:----:|------------------------------------------------------------------------------|
| `reply`              |   ✓    |   ✓   |  ✓   | Send a markdown reply (text/media/quote-reply/buttons) into the attached topic. |
| `react`              |   ✓    |   ✓   |  ✓   | Set a single emoji reaction on a message (validated against Telegram's set).  |
| `edit_message`       |   ✓    |   ✓   |  ✓   | Edit a previously-sent message's text and/or inline keyboard.                 |
| `poll`               |   ✓    |   ✓   |  ✓   | Send a Telegram poll (regular or quiz; anonymous/multiple/timer options).     |
| `stop_poll`          |   ✓    |   ✓   |  ✓   | Force-close a bot-sent poll and return its final aggregate tally.             |
| `download_attachment`|   ✓    |   ✓   |  ✓   | Download an inbound attachment by `file_id` to the local cache.               |
| `fetch_queue`        |   ✓    |   ✓   |  ✓   | Drain held inbound from the durable queue (`limit` / `"all"`; `ack` peek vs consume). |
| `retranscribe`       |   ✓    |   ✓   |  ✓   | Re-run saved audio through the durable STT scheduler; resolve a pending row or append a transcript-update row. |
| `ask`                |   ✓    |   —   |  —   | Blocking human question (single/multi-select + Skip) via an inline keyboard.  |
| `codex_forward`      |   —    |   ✓   |  —   | Env-gated debug tool: forward a payload into the Codex app-server (diagnostics). |

Codex is at parity on the message/queue tools and lacks only `ask` (and `detach`, above). Grok matches that message/queue set **and** has `detach`; live inbound inject requires leader mode (`[cli] use_leader = true`) — see [`GROK-INJECT.md`](GROK-INJECT.md). The permission relay is Claude Code only.

## Telegram bot commands (human-typed, broker-owned)

Typed directly in Telegram, not agent tools. The channel intercepts them
**after** the allowlist gate (strangers get silence, never a reply) and the
broker answers directly — a bot command is never queued or routed to an
agent. Registered via `setMyCommands` so they autocomplete in Telegram's `/`
menu (menu hint only). Commands inside media captions are **not** intercepted
(the attachment would be swallowed); command-in-caption is unsupported in v0.1.

| Command | Cleared for | What it does |
|---|---|---|
| `/status` | any allowlisted sender | In a topic: that topic's queue depth + attach state. In DM/General: broker-wide summary. |
| `/queue` | any allowlisted sender | Index of non-empty pooled queues: `[serial] name · pending · oldest · newest`. **Metadata only — no message content, no kind counts.** In a group it lists only that group's queues; in the DM it lists everything. |
| `/queue <q> [start]` | **operators only** (DM-allowlisted user id); everyone else gets a silent drop | One queue's messages: oldest-first ordinals, kind icon + preview + sender + age, 25 per page (`/queue <q> 26` pages on). |
| `/drain <src> <sel> [to <t>]` | **operators only**; silent drop otherwise | Move the selected pending messages from `<src>` into `<t>` (default: the topic the command was typed in). Loss-free: fsync'd copy into the target **before** the atomic remove from the source; removed lines are also snapshotted to `.trash/` (14-day retention). The reply echoes the resolved names, the ordinal window, a first-message preview, and the target's new total. |

Grammar (shared by `<q>`, `<src>`, `<t>`):

- **Reference forms**: a topic **name** (case-insensitive; quote it if it has
  spaces: `"my project"`), a **serial** from the latest `/queue` (a bare
  integer is *always* a serial), `name:<n>` for a topic literally named with
  digits, or `dm` for the DM route (requires `dm_chat_id`; from a group, `dm`
  is target-only — its content is operator-private).
- **Selectors**: `all` · `first N` · `N` (= `first N`; clamps to what's
  pending) · `N-M` / `N..M` / `N to M` (inclusive ordinals, **1 = oldest**; an
  explicit range past the queue rejects with the actual count).
- **Target split**: the target is everything after the *last unquoted* ` to `.
  When the whole tail already reads as a complete selector (`/drain genie 6 to
  10`), it is the **range** and the target defaults — combine a range with a
  target as `6-10 to <t>`.
- **Scoping**: typed in a group, names and serials resolve only within that
  group ("in another group — run /queue there"); typed in the DM they resolve
  across everything. Ambiguous names (several groups share it) always reject.
- **Friction instead of a confirm card**: `all`-drains and cross-chat drains
  must address the source by NAME — a serial reference rejects with the
  resolved name to paste back.

All of this is a thin parser over `Broker.Drain` (`internal/broker/drain.go`)
— any future MCP verb or confirm card reuses the same core ("implemented
once").

## attach — the parser (lives in the broker)

`AttachReq.Expr` is a single user-supplied string. The broker's
`applyExprToAttachReq` (in `internal/broker/attach.go`) parses it. Rules:

| Input                              | Resolves to                                  |
|------------------------------------|----------------------------------------------|
| `""` (empty)                       | bare attach — never guesses (see below)      |
| `"dm"` / `"DM"` (case-insensitive) | `target = "dm"` (DM disambiguation may fire) |
| `"<int>"`                          | `topic_id = <int>`                           |
| `"create <name>"`                  | `name = <name>, create = true`               |
| `"-y <name>"` / `"yes <name>"`     | `name = <name>, create = true`               |
| `"<other string>"`                 | `name = <string>`                            |

Whitespace is trimmed. Unparsable input falls through to `name`.

### Bare `attach` (empty input) — never guesses

A bare `attach` (no `name`/`topic_id`/`target`/`create`) resolves in this order,
and **never silently binds a topic the session didn't choose**:

1. **Already attached** → idempotent no-op: the current claim is confirmed
   (OK), no re-claim, no re-notify.
2. **This session's own last topic is recoverable** → silent resume. The
   session's prior attachment is keyed on its stable session id (not on cwd),
   so this only ever re-claims the session's **own** route — never a
   neighbour's. This is the *only* silent bind in the system.
3. **Otherwise (first-time session, no own attachment)** → a friendly
   **`pick_topic` picker** (proposal flow below). The cwd only *seeds*
   suggestions (the current project's topic ranks first); it is never a claim.

Consequences worth stating:

- **cwd is a suggestion seed, not a claim.** A stale or wrong cwd→topic
  mapping can only *rank a suggestion*; it can never drain or bind a topic.
- **`create` needs an explicit name.** A bare `attach(create=true)` errors —
  there is no basename synthesis. Pass the name: `attach(name="<name>", create=true)`.
- **Codex resolves its stable thread id from the app-server and caches it for
  reconnect recovery.** Once that identity is settled and a prior attachment
  exists, step 2 can therefore silently reclaim Codex's own route just as it
  does for Claude. A first-time Codex thread still lands on the picker.

## attach — proposal flow

The broker may return `needs_confirmation: true` with a proposal action.
The slash command wrapper must handle each one — typically by asking the
user via the CLI's confirmation primitive (`AskUserQuestion` in Claude
Code, equivalent in Codex) and re-invoking attach with the confirmation
flag set.

| Proposal action            | What the wrapper should do                                                                                                       |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| `pick_topic`               | Bare attach with no own topic to resume. **Ask the user** which topic (via `AskUserQuestion`, or plain conversation on hosts without it) — presenting exactly the ranked suggestions, **never auto-pick**. Re-invoke with the exact command shown on the chosen line (`topic_id=<n>` for existing, `name=<n>, create=true` to create). "See the full list" → call `topics`, then attach by id. |
| `create`                   | Ask "create new topic <name> in group <group>?" → on yes, re-invoke with `name=<name>, create=true` (the name must be passed explicitly; a bare `create=true` errors).                                           |
| `use_existing_other_group` | Ask "claim existing <name> in group <other_group>, or create new in <default_group>?" → re-invoke per choice.                     |
| `disambiguate_dm`          | Ask "topic 'dm' exists; did you mean the topic or actual DM?" → topic: `topic_id=<from proposal>`. DM: `target="dm", steal=true`. |
| `force_steal`              | Ask "topic held by <holder cli pid cwd>; evict and take?" → on yes, re-invoke with `steal=true`. Only with explicit user OK.      |

## Per-CLI implementations

Claude Code exposes each verb as a plugin slash command under
`plugins/c3/commands/`. Codex has **no plugin slash-command layer**, so its C3
surface is the MCP tools (agent-invoked) plus the `c3-broker` subcommands run
from any shell — the shared broker logic is identical either way.

| Verb            | Claude Code                                     | Codex                                   |
|-----------------|-------------------------------------------------|-----------------------------------------|
| `status`        | `/c3:status` (`commands/status.md`)             | `c3-broker status` (shell)              |
| `topics`        | `/c3:topics` + `topics` MCP tool                | `topics` MCP tool · `c3-broker topics`  |
| `build`         | `/c3:build` (`commands/build.md`)               | core `go install` package set (shell)   |
| `setup`         | `/c3:setup` (`commands/setup.md`)               | `c3-broker setup` (TTY)                 |
| `reload-config` | `/c3:reload-config`                             | `pkill -HUP c3-broker`                  |
| `pair`          | `/c3:pair` (`commands/pair.md`)                 | `c3-broker pair …` (shell)              |
| `ping`          | `/c3:ping` (`commands/ping.md`)                 | `c3-broker ping` (shell)                |
| `sessions`      | `/c3:sessions` (`commands/sessions.md`)         | `c3-broker sessions` (shell)            |
| `attach`        | `/c3:attach` + `attach` MCP tool                | `attach` MCP tool                       |
| `detach`        | `/c3:detach` + `detach` MCP tool                | `detach` MCP tool                       |
| `update`        | `/c3:update` (`commands/update.md`)             | `c3-broker update [--check]` (shell)    |

When adding a new verb:
1. Implement the shared interface (broker subcommand or MCP tool).
2. Update the verb table above.
3. Add the per-CLI wrapper file(s) and update the per-CLI table.
4. Cover with a test in `cmd/c3-broker/...` (CLI subcommands) or
   `cmd/c3-claude-adapter/wire_test.go` (MCP tools) where practical.

## Change-management notes

- `attach.md` is the longest wrapper because the LLM has to handle four
  distinct proposal flows. Every other wrapper is one Bash line + a
  one-sentence display instruction. **Resist adding logic to the
  wrappers** — push it into the broker or MCP tool so all CLIs benefit.
- The `Expr` parser is the single chokepoint for `attach`'s "user typed
  a string" interface. New input forms should be added to
  `applyExprToAttachReq` and documented in the parser table above —
  not encoded into each CLI's wrapper.
- Force-steal flow exists because the broker now refuses to displace a
  live PID's claim by default (per the "broker is authority" principle).
  If you change that policy, update both the proposal-flow table here and
  the `tryClaim` doc-comment in `internal/broker/attach.go`.
