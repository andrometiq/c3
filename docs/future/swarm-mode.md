# C3 — Swarm mode (design capture)

**Status:** IMPLEMENTED (v1) — extra bots in `channels.telegram.bots`, mention+sticky gate, `/mute`, Telegram-mode default on swarm attach. Resume re-joins deaf (in-memory sticky). Exclusive claim is per bot, so two bots on one topic do not convert or collide.

## Why

Today C3 is **one session per topic**. Two CLIs (Claude and Codex, Grok and GLM, …) cannot share a Telegram topic: the second attach collides, and a second bot would either see every message or none. The operator wants several agents in **one** topic, talking to the human and to each other, without double-replies.

## Operator story

1. Attach two (or more) CLI sessions to the same topic in Swarm mode. They **join**; they do not take the exclusive claim.
2. A bot is **deaf** until it is addressed. Address it by tagging that agent's Telegram bot (`@glm_bot`, …).
3. After that tag, the bot is **armed**: later untagged messages in the topic go to that bot only.
4. `/mute` disarms listening. The session stays in the swarm; `detach` leaves it.
5. Agents talk to each other the same way: the planner tags the GLM bot with an instruction; GLM replies by tagging the planner's bot. The human sees the whole exchange in the topic.

## Why this cannot be "two exclusive claims"

`Routes` is `map[RouteKey]*Stub` — one living holder. The durable queue is count-off-HEAD, consumed by that holder. Swarm needs:

- a **member set** per route (several stubs), not a single holder
- **per-member inboxes** (copy or cursor), because one consume would steal a message from the other agents
- **addressing**, because otherwise every member would answer every line (the bug exclusive-claim exists to prevent)

Outbound `reply` is structurally bound to the claimed route (`rejectDestinationOverride`). Agent-to-agent in a shared topic is still that same route — fan-out is inbound routing, not a new destination. That is a different (and safer) shape than the roadmap "nudge another topic" feature.

## Addressing — one Telegram bot per agent (decided)

Each CLI session is bound to its **own BotFather bot** (`@claude_c3`, `@glm_bot`, …). The human tags that bot; Telegram mention is the address.

Implications:

- `mappings.json` grows from one `bot_token` to a set of named bots. The broker runs one poller per token.
- Route identity must include **which bot** (today `RouteKey` is `{telegram, chat, topic}` — two bots in the same topic would collide). Add a bot id, or treat each bot as its own channel name (`telegram:claude`).
- **Group Privacy must be off** (BotFather `/setprivacy` → Disable) on every swarm bot, or Telegram never delivers untagged follow-ups and sticky listening cannot work. Mentions still work with privacy on; sticky does not.
- Agent-to-agent: bot A posts `@glm_bot do X`. Bot B's poller sees it (privacy off, or because it was mentioned). Broker routes that inbound to the GLM session. No broker-internal fan-out required for the happy path; still filter so bot A does not treat its own outbound as inbound.

Callsign aliases (`@claude` in text without a real mention entity) can be a later convenience. v1 is real `@bot` mentions.

## Sticky listening

Independent per member, not a single talking-stick:

- Tag `@claude` → Claude armed
- Tag `@codex` → Codex armed too (parallel work)
- `/mute` with no mention disarms the last-addressed member; `/mute @that_bot` disarms one
- Untagged traffic goes to currently armed members only. If none are armed, nobody gets it (it stays in the topic for the human; it is not fanned out)

## Queue

Do not share the exclusive holder's count-off-HEAD consume. Each swarm member gets its own durable cursor or a copied per-member queue. A message addressed to Claude must not disappear from Codex's inbox, and an unaddressed line must not be consumed by a deaf member.

`observe` stays read-only peek. Swarm is live delivery + reply, so it is not observe.

## Protocol (agent instructions)

Add a Swarm block next to `ModeProtocol` in `internal/mode`: how to address other members by tagging their bot, that tagging arms them, that `/mute` stops listening, that you never answer untagged traffic meant for someone else.

## What Swarm is not

- Not CLI / Telegram / Drive **output** mode. Those stay per-session. Swarm is a **topic membership** mode.
- Not the roadmap nudge (short wake into another topic). Swarm is a shared conversation.
- Not text callsigns as the v1 address (real bot mentions). Callsigns can layer on later.

## Frozen decisions

- **Disarm verb:** `/mute`. Session stays attached and deaf until tagged again. `detach` leaves the swarm.
- **Addressing:** one Telegram bot per agent, not callsigns on the shared C3 bot.

## Frozen later

- **Convert vs refuse:** not needed. Each bot has its own RouteKey (`telegram:<bot>`), so two agents on one topic are two exclusive claims, not a member set.
- **Resume:** re-join deaf. Sticky arming is in-memory; tag the bot again after a broker restart.
