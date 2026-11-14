---
description: Send a one-shot "this is me" message to Telegram identifying which CLI session currently owns the attached topic. Run in each candidate tab to find the owner before evicting.
allowed-tools: Bash(c3-broker ping)
---

!`c3-broker ping`

Display the broker's output verbatim. The broker has dispatched a short identification message to the Telegram route this session currently holds, naming the cwd / cli / PID / timestamp so the human reading Telegram can match the topic to the terminal that owns it.

Typical use: a `claude --resume` tab in another terminal still holds the claim and a new session can't attach without `force_steal`. Run `/c3:ping` in each candidate tab; the one whose identification message lands in Telegram is the live holder. See DEBUGGING.md → "Multi-session: alive-but-abandoned tabs" for the full workaround.
