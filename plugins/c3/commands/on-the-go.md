---
description: Switch this session to on-the-go mode in the web chat.
allowed-tools: ["mcp__plugin_c3_c3__attach", "AskUserQuestion"]
---

Before switching, remember the name of the Telegram topic this session currently holds so `/c3:off-the-go` can restore it.

The equivalent natural-language triggers are only the unambiguous imperatives "start on-the-go mode" and "switch to the web chat"; do not treat the bare phrase "on-the-go" as a mode switch.

Call `mcp__plugin_c3_c3__attach` with `expr` set to `web`. If it returns a confirmation proposal, follow the same explicit-confirmation rules as `/c3:attach`; never auto-steal another session's route.

After a successful attach, switch to reply-tool ("Telegram") mode so replies land on the web route. Announce in one line that on-the-go mode is active and permission prompts and `ask` still have to be answered at the laptop.

For errors, display the error verbatim and do not switch output mode.
