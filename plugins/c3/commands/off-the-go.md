---
description: End on-the-go mode, release web, and return output to the held Telegram route.
allowed-tools: ["mcp__plugin_c3_c3__detach"]
---

Call `mcp__plugin_c3_c3__detach` once with `target` set to `web`. The Telegram topic was never released, so the targeted detach keeps it held and makes it the output route; there is nothing to re-attach and nothing to guess.

After a successful detach, announce in one line that on-the-go mode has ended and the session is currently in Telegram mode.

For errors, display the error verbatim and do not announce that the mode ended.
