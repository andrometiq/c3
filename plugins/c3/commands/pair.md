---
description: Arm a Telegram pairing window. `dm` (default) or `group <chat_id>`. Broker prints a 4-digit code; type it from Telegram within 10 min to allowlist your DM user_id or group chat_id.
argument-hint: "[empty | dm | group <chat_id>]"
allowed-tools: Bash(c3-broker pair:*)
disable-model-invocation: true
---

Run `c3-broker pair $ARGUMENTS` with Bash, then display the broker's output verbatim. The broker has armed a 10-minute pairing window and printed a 4-digit code.

What the user does next:
- For `dm` (default): open Telegram, send exactly the 4-digit code (no whitespace, no other characters) to the bot. On match, the broker silently adds the user_id to `~/.config/c3/mappings.json:allowlist.users` and the bot becomes responsive to that user. Wrong codes are silently ignored — the bot still looks dead until the right code arrives or the window expires.
- For `group <chat_id>`: in that group, anyone sends the 4-digit code. On match, the broker allowlists the GROUP (chat_id), not the individual member — every member of a paired group can talk to the bot.

If the output says `is the broker running?`, the broker isn't up. Start a fresh Claude Code session or run `c3-broker` in a terminal; the next adapter spawn auto-spawns a fresh broker.

If the user has not paired DM yet, the broker auto-armed DM pairing on its own startup — check `~/.local/state/c3/broker.log` for `pairing: DM pairing AUTO-ARMED` to see the original code (no need to re-arm).
