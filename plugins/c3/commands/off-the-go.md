---
description: End on-the-go mode and return this session to its previous Telegram topic.
allowed-tools: ["mcp__plugin_c3_c3__attach", "AskUserQuestion"]
---

Use the Telegram topic name this session held immediately before on-the-go mode. If it is unknown, ask the user which topic to restore — never guess.

Call `mcp__plugin_c3_c3__attach` with `name` set to that topic. If it returns a confirmation proposal, follow the same explicit-confirmation rules as `/c3:attach`; never auto-steal another session's route.

After a successful attach, announce in one line that on-the-go mode has ended, name the restored topic, and say that you are currently in Telegram mode.

For errors, display the error verbatim and do not announce that the mode ended.
