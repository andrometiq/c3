---
description: Switch this session to on-the-go mode in the web chat.
allowed-tools: ["mcp__plugin_c3_c3__attach", "AskUserQuestion"]
---

The equivalent natural-language triggers are only the unambiguous imperatives "start on-the-go mode" and "switch to the web chat"; do not treat the bare phrase "on-the-go" as a mode switch.

Call `mcp__plugin_c3_c3__attach` once with `expr` set to `+web`. This ADDS web, KEEPS the Telegram topic as a held input route, and makes web the output route. If it returns a confirmation proposal, follow the same explicit-confirmation rules as `/c3:attach`; never auto-steal another session's route.

After a successful attach, announce in one line that on-the-go Drive mode is active, replies go to web, the Telegram topic remains held for input, and permission prompts and `ask` still have to be answered at the laptop.

For errors, display the error verbatim and do not announce Drive mode.
