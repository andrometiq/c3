---
description: List every live Claude Code / Codex session the broker is tracking, with CWD, attached topic, and a "you are here" marker for the calling terminal. Helps disambiguate which tab owns which topic.
allowed-tools: Bash(c3-broker sessions)
---

!`c3-broker sessions`

Display the broker's output verbatim. The broker has returned a snapshot of every connected adapter — CLI, PID, CWD, currently-attached Telegram destination, and a `This?` marker for the session that owns the terminal that fired the slash command (best-effort, may be blank on non-Linux or when the parent-PID walk fails).

Typical use: multiple `claude` / `codex` terminals are running, attaches got tangled, you want to know which tab owns the topic before evicting / re-attaching. Combine with `/c3:ping` to fingerprint a specific tab over Telegram.

If a session you expect to see is missing, its adapter may have already disconnected (CC quit, broker bounced before reattach). If the `This?` column is blank for every row, the parent-PID walk couldn't pin the calling terminal — the listing is still accurate, you just have to spot your own tab by CWD.
