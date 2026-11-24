---
description: Send the owner a Magic Link — a live link to a web page they change by pointing with the Magic Pencil and speaking or typing.
argument-hint: "<name> [path to an HTML file]"
allowed-tools: ["mcp__plugin_c3_c3__attach", "mcp__plugin_c3_c3__reply", "Read", "Write"]
---

User typed: $ARGUMENTS

A Magic Link is a live board used as a web page (follow the `c3:board` skill, section 6). Do this:

1. The first word is the name (`^[a-z0-9][a-z0-9-]{0,39}$`); if it breaks that rule, make a close valid one and say so. Call `mcp__plugin_c3_c3__attach` with `expr="+board:<name>"`.
2. The page: the HTML file given as the second argument, else the page the owner asked for in this conversation, else the starter page from the attach result. It must be one self-contained file (inline CSS, scripts and images; no outside URLs, storage or forms). Parts (`data-c3-part`) are optional. Keep it at a path you will edit again.
3. Publish it: `mcp__plugin_c3_c3__reply` with `channel="board:<name>"`, `text=""` and `media=[{kind: "file", path: "<absolute path>"}]`.
4. Give the owner the link from the attach result, where they talk to you (follow your output mode), with one line: "Tap the pencil on the left, draw around what to change, then type or speak." Add "long-press, Open in browser" when they use Telegram.

Then answer each pencil command as the skill says: change the area, resend the file with `reply_to`, and reply with one or two sentences of narration.
