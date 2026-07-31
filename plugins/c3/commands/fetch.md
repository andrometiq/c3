---
description: Drain this session's C3 Telegram queue — bare = all held messages; optional number fetches that many. Short alias for /c3:fetch-queue.
argument-hint: "[count]"
allowed-tools: ["mcp__plugin_c3_c3__fetch_queue"]
---

Pull the held inbound Telegram messages for this session's attached topic by calling `mcp__plugin_c3_c3__fetch_queue`, then render them for me.

- Argument (optional): `$ARGUMENTS`
- If no argument was given, call it with `limit: "all"` — drain the entire queue in one shot.
- If the argument is a positive integer N, call it with `limit: N` — fetch the oldest N.
- Always use `ack: true` (the default) so fetched messages are consumed from the queue.

Render each message: sender, reply-to (if any), the text/transcript, and note any attachment (with its `file_id`, and that `download_attachment` fetches the bytes). State how many remain in the queue. If the queue is empty, say so in one line. If the call errors (e.g. this session isn't attached to a topic), show the error verbatim and point at `/c3:attach`. No other commentary.
