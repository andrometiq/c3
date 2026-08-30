---
description: Attach this session to a route. A bare target switches; +<target> adds it to the held set and makes it output.
argument-hint: "[empty | [+]dm | [+]web | [+]<topic-id> | [+]<name> | \"create <name>\" | \"-y <name>\"]"
allowed-tools: ["mcp__plugin_c3_c3__attach", "mcp__plugin_c3_c3__topics", "AskUserQuestion"]
---

User typed: $ARGUMENTS

Call `mcp__plugin_c3_c3__attach` with `expr` set to the user's argument string ("$ARGUMENTS" verbatim). The broker parses it (rules in `docs/COMMANDS.md`) and either:
- Silent-claims and returns `attached to "<name>"`
- Returns a proposal (`create` / `use_existing_other_group` / `disambiguate_dm` / `force_steal`) requiring confirmation

A bare target such as `web` or `c3` SWITCHES: it claims that route and releases the session's other routes. A `+` target such as `+web` or `+c3` ADDS the route, keeps the routes already held, and makes the added route the output. The structured equivalent is `add=true`. Adding a route this session already holds is refused. If the target is held by another session, use the ordinary `force_steal` proposal and explicit confirmation flow below; never steal automatically.

If the response is **needs_confirmation** with proposal action:

- **`pick_topic`**: this session isn't attached yet and the broker claimed NOTHING — it returned a ranked list of options. **You MUST call `AskUserQuestion`** presenting exactly the listed options — **never auto-pick, never assume, never attach silently.** On the user's choice, run the exact `attach(...)` command shown on that option's line (verbatim — it already carries the right `topic_id`/`group`/`target="dm"`/`create=true`). If an option is marked *held by* another session, picking it will prompt a `force_steal` confirmation — surface that. For **"See the full list"**, call the `topics` tool, show the topics, then attach by the chosen id (`attach(topic_id=<id>)`, adding `group="<g>"` if the topic is in a non-default group). For **"type your own"**, tell the user to run `/c3:attach <name>` (or you call `attach(name="<name>")`).

- **`create`**: Ask the user via `AskUserQuestion` whether to create the topic. On yes → re-invoke with the name explicitly: `attach(name="<the proposal's name>", create=true)` (or `attach(expr="create <name>")`, which also carries the name). A bare `attach(create=true)` no longer works — the name must be present. On no → tell them how to use an existing topic instead.

- **`use_existing_other_group`**: Ask the user via `AskUserQuestion` whether to claim the existing topic (in a different group) or create a new one in the default group. Re-invoke accordingly.

- **`disambiguate_dm`**: Ask the user via `AskUserQuestion` whether they meant the topic named "dm" or the actual Telegram DM. On topic → `attach(topic_id=<the topic_id from the proposal>)`. On DM → `attach(target="dm", steal=true)` (steal=true bypasses the disambiguation check).

- **`force_steal`**: Ask the user via `AskUserQuestion` — show holder cli/pid/cwd from the proposal — whether to evict and take the claim. On yes → re-invoke with `steal=true`. On no → leave it.

For successful attach: just display the broker's response. No extra commentary.

For errors that aren't proposals: display the error verbatim. The most common is "no telegram bot_token configured" which points at `/c3:setup`.
