---
description: Guided C3 setup — walks you through bot creation, code-based pairing (auto-discovers your user id and the group chat id), STT keys, and first attach.
---

You are driving C3's guided setup. Walk the user through it **one step at a time**, and **react to what is already configured — never re-show or re-ask a completed step**. Use the phased `c3-broker setup` subcommands below; do NOT run bare `c3-broker setup` from here (its interactive prompts need a real TTY — that flow is the fallback for users in a plain terminal).

**Step 0 — inspect current state.** Run `c3-broker status` and `ls ~/.claude/stt.env 2>/dev/null`. From the status output note: does mappings.json exist and validate, is `token=true` on the telegram channel, how many groups are configured. Tell the user in one short summary what is already done and what remains, then do only the missing steps. If everything is configured, jump to step 5.

**Step 1 — bot token.** If the user has no bot yet: in Telegram, message @BotFather → `/newbot` → pick a name and a username ending in `bot` → copy the HTTP token; then ALSO `/setprivacy` → pick the bot → **Disable** (required — otherwise the bot cannot see group messages). Ask the user to paste the token, then run:

```
printf %s 'THE_TOKEN' | c3-broker setup token
```

It validates the token via getMe and writes it to `~/.config/c3/mappings.json` (mode 0600). Tell the user the bot's @username it reports.

**Step 2 — pair the user's account (discovers their user id — no id hunting).** Generate a fresh random 4-digit code — run `shuf -i 1000-9999 -n 1` (or equivalent); never invent, reuse, or copy an example code — and tell the user: open a DM with @<bot username>, press START, and send exactly that code. Then run, passing the generated code via `--code` (it blocks until the code arrives — give the command a timeout at least as long as `--timeout-sec`):

```
c3-broker setup pair dm --code <GENERATED_CODE> --timeout-sec 240
```

On success it prints the captured user id and records `dm_chat_id`, `master_user_id`, and the allowlist entry. It pauses a running broker during the wait and restarts it after — that is expected. If the window expires: re-check the bot username and the exact code, then re-run with a fresh code. Last resort only (user already knows their numeric id): `c3-broker setup pair dm --id <user_id>`.

**Step 3 — group with Topics (discovers the group chat id — no id hunting).** Walk the user through, confirming each item:

1. Create a Telegram group (a regular group is fine; use a phone/desktop client, not Telegram Web).
2. Group settings → enable **Topics**.
3. Add the bot as a member.
4. Promote the bot to admin with exactly: Manage Topics, Send Messages, Delete Messages, Pin Messages.

Then generate another fresh random 4-digit code (`shuf -i 1000-9999 -n 1` again — never reuse the DM code or an example), ask the user to send it **in the group from their own just-paired account** (once a DM pairing exists, codes from other members are ignored), and run:

```
c3-broker setup pair group --code <GENERATED_CODE> --name main --timeout-sec 240
```

`--name` is the config name for the group (omit it to default to the group's Telegram title, else "main"). On success it prints the captured chat id and records the group, `default_group` (if unset), and the allowlist entry. If it expires, the usual causes are bot privacy mode still enabled (@BotFather → `/setprivacy` → Disable) or the bot not actually in the group. Last resort: `c3-broker setup pair group --id <chat_id> --name main`.

**Step 4 — voice transcription (optional, recommended).** Ask once. Voice notes are transcribed via a provider chain — default: Gemini 3 Flash (`google/gemini-3-flash-preview` via OpenRouter; handles multilingual audio and mid-sentence language switches well) → Sarvam Saaras v3 fallback. Keys: OpenRouter https://openrouter.ai/keys, Sarvam https://dashboard.sarvam.ai. If yes, offer two paths and let the user choose:

- Privacy-first: the user runs `c3-broker setup stt` in any terminal themselves (keys stay out of this chat).
- Quick: the user pastes the key(s) here and you run (empty string for a provider they skip):

```
printf 'y\n%s\n%s\n' 'OPENROUTER_KEY' 'SARVAM_KEY' | c3-broker setup stt
```

Mention in passing: users can add their OWN STT provider — a `transcribe()` module dropped at `plugins/c3/stt/stt-pkg/providers/<name>.py` joins the `--chain` (see `plugins/c3/stt/stt-pkg/README.md`).

**Step 5 — finish.** Run:

```
c3-broker setup finish
```

It installs the host launcher shim (Claude) or MCP registration (Codex), restarts the broker so the new config is live, and prints a stand-alone "Setup complete — what now" summary. Relay that summary to the user.

**Step 6 — first use (the 30-second tour).** Offer to attach this session right now: call the c3 `attach` tool (or point the user at `/c3:attach`) to bind this project to a Telegram topic. Then have them send a text or voice note from their phone to that topic and confirm it surfaces here as a `<channel>` block. Point out `/c3:status` (broker health), `/c3:topics` (topics + claims), and `/c3:pair` (allowlist another person or group later). If inbound doesn't surface, the session was likely launched without the dev-channels flag — have them relaunch with `claude --dangerously-load-development-channels=plugin:c3@c3` (they can append `--resume` themselves only if they want the previous session back).
