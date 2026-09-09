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
| `web link`      | `c3-broker web link` (CLI)         | pure shell    | Ask the running broker to mint a fresh web login link and deliver it through the configured Telegram operator DM. The token is not printed on the local IPC response. |
| `web ca`        | `c3-broker web ca` (CLI)           | pure shell    | Send the web channel's public private-CA certificate and SHA-256 fingerprint to the configured Telegram operator DM. No private key leaves the broker state directory. |
| `build`         | core `go install` package set (shell) | pure shell | Rebuild C3's ten core binaries; the PATH-shadowing Codex launcher remains opt-in. |
| `update`        | `c3-broker update [--check]` (CLI) | pure shell    | Checks the latest GitHub release and performs checksum-verified on-disk binary replacement; does not touch the running broker. |
| `setup`         | `c3-broker setup …` (CLI)         | agent-guided / interactive | Configure C3. Primary path: the `/c3:setup` slash command drives the phased subcommands one step at a time — `setup token` (validate via getMe + record), `setup pair dm` / `setup pair group` (code-based id discovery: a 4-digit code sent in Telegram discovers the user id / group chat id — no id hunting), `setup stt`, `setup finish` (host integrations + broker restart). Bare `c3-broker setup` is the full interactive TTY flow (fallback for a plain terminal). Writes mappings.json (mode 0600). |
| `reload-config` | `pkill -HUP c3-broker`            | pure shell    | Signal the broker to re-read mappings.json. Non-disruptive — no process restart, in-memory pointer swap, live claims preserved. For binary updates, restart Claude Code instead. |
| `pair`          | `c3-broker pair …` (CLI)          | pure shell    | Arm a Telegram pairing window. A 4-digit code sent from Telegram allowlists the DM `user_id` (`pair dm`) or a group `chat_id` (`pair group <chat_id>`). The setup flow uses this under the hood for id-free discovery. |
| `ping`          | `c3-broker ping` (CLI)            | pure shell    | Send a one-shot "this is me" message to the attached topic, identifying which CLI session currently owns it. Run in each candidate tab to find the owner before force-stealing. |
| `sessions`      | `c3-broker sessions` (CLI)        | pure shell    | List every live session the broker tracks — CWD, all held routes (output first), and a "you are here" marker for the calling terminal. |
| `attach`        | `attach(expr=…, add=…)` (MCP tool) | LLM dispatch | Attach this session to a Telegram DM/topic or web. A bare target switches; `+X` / `add=true` keeps prior routes and makes X output. |
| `on-the-go`     | `attach(expr="+web")` + output-mode protocol | LLM dispatch | Add web while keeping the Telegram topic held, make web output, and announce Drive mode plus the laptop permission/`ask` boundary. Claude command: `/c3:on-the-go`. |
| `off-the-go`    | `detach(target="web")` + output-mode protocol | LLM dispatch | Release only web; the still-held Telegram route becomes output, then announce Telegram mode. Claude command: `/c3:off-the-go`. |
| `output`        | `output(target=…)` (MCP tool)     | LLM dispatch  | Make one currently held route the output route without changing the held set. Claude command: `/c3:output`. |
| `detach`        | `detach(target=…)` (MCP tool)     | LLM dispatch  | Bare releases ALL held routes; optional `target` releases one held channel/topic route. |
| `fetch-queue`   | `fetch_queue(limit=…, channel=…)` (MCP tool) + `fetch-queue` (MCP prompt on Desktop/Cursor) | LLM dispatch / prompt inject | Drain held inbound across this session's routes (output first), or select one held route with `channel`; optional count fetches the oldest N. Claude: `/c3:fetch-queue` and short alias `/c3:fetch`. Desktop: `/fetch-queue` MCP prompt. Cursor: MCP prompt `fetch-queue` plus `~/.cursor/commands/{fetch,c3-fetch}.md` from `install-cursor`. dcode: `/skill:c3-fetch` user skill from `install-dcode`. |
| `release`       | `c3-broker release <cwd>` (CLI)   | pure shell    | **Stubbed in v1** — intended to drop a route claim by cwd without restarting the broker; returns 'not yet implemented' today; workaround is `/exit` the holding session. |

## MCP tools (agent-invoked)

Beyond `attach` / `detach` / `topics`, the adapters expose a set of message and interaction tools the agent calls directly (no slash wrapper). All are broker-dispatched; the `✓ / —` columns are per-CLI availability.

| Tool                 | Claude | Codex | Grok | dcode | What it does                                                                 |
|----------------------|:------:|:-----:|:----:|:-----:|------------------------------------------------------------------------------|
| `reply`              |   ✓    |   ✓   |  ✓   |   ✓   | Send to the output route, or one held route with `channel=<route>` (text/media/quote-reply/buttons). |
| `react`              |   ✓    |   ✓   |  ✓   |   ✓   | React on the output route or the held route named by `channel`; use the same route as the message. |
| `edit_message`       |   ✓    |   ✓   |  ✓   |   ✓   | Edit on the output route or the held route named by `channel`; message ids are per-route. |
| `output`             |   ✓    |   ✓   |  ✓   |   ✓   | Select the output route from the session's currently held set.                |
| `poll`               |   ✓    |   ✓   |  ✓   |   ✓   | Send a Telegram poll (regular or quiz; anonymous/multiple/timer options).     |
| `stop_poll`          |   ✓    |   ✓   |  ✓   |   ✓   | Force-close a bot-sent poll and return its final aggregate tally.             |
| `download_attachment`|   ✓    |   ✓   |  ✓   |   ✓   | Download an inbound attachment by `file_id` to the local cache.               |
| `fetch_queue`        |   ✓    |   ✓   |  ✓   |   ✓   | Drain all held routes output-first, or one held route via `channel` (`limit` / `"all"`; `ack` peek vs consume). |
| `retranscribe`       |   ✓    |   ✓   |  ✓   |   ✓   | Re-run saved audio through the durable STT scheduler; resolve a pending row or append a transcript-update row. |
| `ask`                |   ✓    |   —   |  —   |   —   | Blocking human question (single/multi-select + Skip) via an inline keyboard.  |
| `codex_forward`      |   —    |   ✓   |  —   |   —   | Env-gated debug tool: forward a payload into the Codex app-server (diagnostics). |

Codex is at parity on the message/queue and route-management tools and lacks only `ask`. Grok matches that set; live inbound inject requires leader mode (`[cli] use_leader = true`) — see [`GROK-INJECT.md`](GROK-INJECT.md). dcode matches that message/queue set and has the same route-management tools; live inbound needs `DEEPAGENTS_CODE_EXTERNAL_EVENT_SOCKET=1` at TUI launch (else pull-only). The permission relay is Claude Code only.

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
| `/drain <src> <sel> [to <t>]` | **operators only**; silent drop otherwise | Move the selected pending messages from `<src>` into `<t>` (default: the topic the command was typed in). Loss-free: fsync'd copy into the target **before** the atomic remove from the source; removed lines are also snapshotted to `.trash/` (90-day retention). The reply echoes the resolved names, the ordinal window, a first-message preview, and the target's new total. |

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
| `"web"` / `"telegram"` (case-insensitive) | channel selector |
| `"+<selector>"` (for example `+web`, `+c3`) | parse `<selector>` normally, set `add = true` |
| `"<int>"`                          | `topic_id = <int>`                           |
| `"create <name>"`                  | `name = <name>, create = true`               |
| `"-y <name>"` / `"yes <name>"`     | `name = <name>, create = true`               |
| `"<other string>"`                 | `name = <string>`                            |

Whitespace is trimmed. Unparsable input falls through to `name`.

`+web` / `+c3` add that route to the held set and make it output. The
structured equivalent is `add=true`. Without `+` or `add=true`, an explicit
target keeps the established SWITCH behavior: the new route is claimed and
the session's other routes are released.

Because the two exact tokens are selectors, a Telegram topic literally named
`web` or `telegram` must be selected with the structured `name="web"` /
`name="telegram"` form or by topic id.

Channel resolution is request-aware:

| Request | Channel result |
|---|---|
| explicit `channel` or selector token | that registered, enabled channel |
| `dm`, topic name, topic id, or create | the unique registered, enabled topic-capable channel (normally Telegram) |
| bare attach, already attached | the current route's channel |
| bare attach, recoverable session record | that session's recorded route channel |
| bare attach, no recorded route | the unique topic-capable channel and its picker |
| two topic-capable channels, no selector | refuse; never guess |

A successful `attach web` switches to the operator's web route and releases
the session's other routes. `attach +web` instead keeps those routes held and
makes web output. If the browser has
no live authenticated session, the broker sends a login link through the
Telegram DM and the attach response says so. The response also reminds the
agent to use reply-tool (“Telegram”) mode so `reply` lands on the claimed web
route, that **🔊 Voice** can read replies aloud, and that permission
prompts/`ask` still require the laptop. The attach tool result includes the
web manifest's capability guidance immediately, including the spoken-reply
rules.

## on-the-go / off-the-go — output-mode wrappers

`/c3:on-the-go` calls `attach(expr="+web")` once. Add-mode keeps the Telegram
topic held for input and makes web the output route, after which the wrapper
announces Drive mode plus the laptop-side permission boundary. The phrases
“start on-the-go mode” and “switch to the web chat” request the same sequence
explicitly; they are not inferred merely because a message came from a phone.

`/c3:off-the-go` calls `detach(target="web")` once. The Telegram topic was never
released, so it remains held and becomes output; nothing is re-attached or
guessed. The
phrases “end on-the-go mode,” “back to Telegram,” and “back to the topic” do the
same, followed by a one-line mode announcement. A web system notice says
“Spoken replies ON” or “Spoken replies OFF”; the agent uses speakable prose only
while ON. Permission prompts and `ask` are answered at the laptop in either
state.

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
from any shell — the shared broker logic is identical either way. dcode has no
plugin command component either, but its user skills ARE slash commands
(`/skill:<name>`), so `c3-broker install-dcode` installs the three most-used
verbs as `~/.deepagents/agent/skills/c3-{attach,fetch,topics}/SKILL.md`; the
rest of the surface is the MCP tools plus `c3-broker` subcommands, as on Codex.

| Verb            | Claude Code                                     | Codex                                   | dcode                                        |
|-----------------|-------------------------------------------------|-----------------------------------------|----------------------------------------------|
| `status`        | `/c3:status` (`commands/status.md`)             | `c3-broker status` (shell)              | `c3-broker status` (shell)                   |
| `topics`        | `/c3:topics` + `topics` MCP tool                | `topics` MCP tool · `c3-broker topics`  | `/skill:c3-topics` + `topics` MCP tool       |
| `web link`      | `c3-broker web link` (shell)                    | `c3-broker web link` (shell)            | `c3-broker web link` (shell)                 |
| `web ca`        | `c3-broker web ca` (shell)                      | `c3-broker web ca` (shell)              | `c3-broker web ca` (shell)                   |
| `build`         | `/c3:build` (`commands/build.md`)               | core `go install` package set (shell)   | core `go install` package set (shell)        |
| `setup`         | `/c3:setup` (`commands/setup.md`)               | `c3-broker setup` (TTY)                 | `c3-broker setup` (TTY)                      |
| `reload-config` | `/c3:reload-config`                             | `pkill -HUP c3-broker`                  | `pkill -HUP c3-broker`                       |
| `pair`          | `/c3:pair` (`commands/pair.md`)                 | `c3-broker pair …` (shell)              | `c3-broker pair …` (shell)                   |
| `ping`          | `/c3:ping` (`commands/ping.md`)                 | `c3-broker ping` (shell)                | `c3-broker ping` (shell)                     |
| `sessions`      | `/c3:sessions` (`commands/sessions.md`)         | `c3-broker sessions` (shell)            | `c3-broker sessions` (shell)                 |
| `attach`        | `/c3:attach` + `attach` MCP tool                | `attach` MCP tool                       | `/skill:c3-attach` + `attach` MCP tool       |
| `on-the-go`     | `/c3:on-the-go` + `attach` MCP tool             | trigger phrase + `attach` MCP tool      | —                                            |
| `off-the-go`    | `/c3:off-the-go` + `attach` MCP tool            | trigger phrase + `attach` MCP tool      | —                                            |
| `detach`        | `/c3:detach` + `detach` MCP tool                | `detach` MCP tool                       | `detach` MCP tool                            |
| `fetch-queue`   | `/c3:fetch-queue` / `/c3:fetch`                 | `fetch_queue` MCP tool                  | `/skill:c3-fetch` + `fetch_queue` MCP tool   |
| `update`        | `/c3:update` (`commands/update.md`)             | `c3-broker update [--check]` (shell)    | `c3-broker update [--check]` (shell)         |

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
