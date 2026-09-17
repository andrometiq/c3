# Grok live-inject path (feasibility, 2026-07-10)

Status: **proven in probe**; implemented in `cmd/c3-grok-adapter`.
Leader mode is **always required** for live inject (no non-leader fallback path).

This is the load-bearing piece for C3 × Grok: Telegram inbound must become a
real turn in the user's Grok session (not only sit in `fetch_queue`).

## Conclusion

**Live inject is possible.** It is not MCP channel notifications (Claude-style)
and not a Codex app-server WebSocket. It is:

> **Leader IPC client → wrap ACP `session/prompt` → shared agent session**

Codex analogy: Codex launcher + app-server + `turn/start` over WS.
Grok analogy: force **leader mode** + `c3-grok-adapter` (or a side forwarder)
speaks the leader socket and injects with `session/prompt`.

Without leader mode, a plain `grok` TUI embeds the agent in-process and exposes
**no external ACP endpoint**. An MCP child cannot start a turn from outside.

## Proven wire protocol

### Transport

- Unix socket: `$GROK_HOME/leader.sock` (default `~/.grok/leader.sock`)
- Framing: **4-byte big-endian length + UTF-8 JSON body** (not newline JSON,
  not LSP Content-Length)
- Leader exits when the last client disconnects (keep a conn up, or expect
  respawn)

### ClientMessage (internally tagged, `type` field)

Variants (lowercase): `register` | `acp` | `control` | `ping` | `disconnect`

**Register** (required first message):

```json
{
  "type": "register",
  "client_type": "stdio",
  "mode": "stdio",
  "capabilities": {}
}
```

Server replies:

```json
{"type":"registered","client_id":1,"ready":false,"leader_protocol_version":1,
 "leader_binary_version":"0.2.93","leader_capabilities":{...}}
{"type":"leader_ready"}
```

**ACP wrapper** — payload is a **JSON string**, not a nested object:

```json
{
  "type": "acp",
  "payload": "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",...}"
}
```

Server→client ACP frames use the same shape (`type: acp`, string `payload`).

### ACP methods used for inject

| Method | Role |
|--------|------|
| `initialize` / `notifications/initialized` | Handshake |
| `session/new` | Create session (probe) |
| `session/load` | Attach to existing TUI session by id (target for C3) |
| **`session/prompt`** | **Live inject a user turn** |

Probe evidence:

- After `session/prompt`, the agent emitted `session/update` with
  `sessionUpdate: user_message_chunk` containing our text.
- Queue notifications (`_x.ai/queue/changed`) fired — prompt entered the
  session run loop.
- Multi-client architecture is first-class (leader exists for sharing one agent
  across clients). Second client should `session/load` the same `sessionId`
  then `session/prompt`.

### Mid-turn inject

- `x.ai/queue/interject` as an ACP request returned **Method not found** on the
  agent (0.2.93). That name is used on the **pager** side for UI send-now; it is
  not the agent method we need.
- A second `session/prompt` while a turn is in flight is **not** treated as a
  Grok prompt-queue add. The adapter waits for the previous turn's ACP result
  (`drainPending`), then issues `session/prompt` so the Telegram follow-up
  becomes the **next** user message after the agent is free. A 120s landing
  timer must cover only that new prompt's echo — wrapping drain in it closed
  the leader conn mid-turn and left the follow-up as UNCERTAIN (never the next
  turn).
- Definite busy JSON-RPC errors (a TUI-originated turn the adapter did not
  start) retry until idle. Post-write silence stays UNCERTAIN and is never
  retried.
- True mid-turn *interject* (merge into the current turn without waiting) is
  still out of scope. C3 follow-ups wait for the turn boundary.

## What does *not* work (ruled out / weak)

| Approach | Verdict |
|----------|---------|
| Claude-style `notifications/claude/channel` from MCP | Host has no channel renderer; wrong dialect |
| Unsolicited MCP `notifications/message` as turn inject | No evidence it enters `notification_drain` / starts turns |
| Headless `grok -p` | Separate process; not the live TUI session |
| `grok agent stdio` alone | New agent, not the user's TUI session unless that stdio client is attached to the **same leader** and same `sessionId` |
| Default non-leader `grok` | No `leader.sock`; external inject impossible |

## C3 architecture for first cut

```
Telegram ──► c3-broker ──► c3-grok-adapter
                               │
                    ┌──────────┴──────────┐
                    │ MCP stdio (tools)   │  attach/reply/fetch_queue/…
                    │ Leader ACP client   │  session/prompt inject
                    └──────────┬──────────┘
                               │ $GROK_HOME/leader.sock
                    ┌──────────▼──────────┐
                    │  grok agent leader  │
                    │  (shared backend)   │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  grok TUI (--leader)│  user's visible session
                    └─────────────────────┘
```

### Requirements on the Grok session

1. **Leader mode on** for any session that should receive live Telegram inject:
   - `[cli] use_leader = true` in `~/.grok/config.toml`, or
   - launch with `--leader`, or
   - C3 ships a thin `grok` shim / setup step that enables it.
2. Adapter must know **stable session id** (`C3_GROK_SESSION_ID` primary,
   with `GROK_SESSION_ID` accepted as a fallback, via the SessionStart hook,
   same pattern as Claude's session-hook handoff) to `session/load` +
   claim C3 topic on resume.
3. MCP stdio still required for **outbound** tools (`reply`, `attach`, …) —
   leader path is **inbound inject only** (plus optional future permission
   surface).

### Adapter inbound algorithm (draft)

On broker `inbound` for the claimed route:

1. Format message as channel-ish text (sender, reply context, transcript, …).
2. Ensure leader conn (register + initialize once; reconnect forever).
3. `session/load` if not already bound to this session id.
4. `session/prompt` with the formatted text as a single text block.
5. Ack `inbound_delivered` only after the ACP request is accepted (and ideally
   after `user_message_chunk` / prompt result — exact ack bar TBD).
6. If leader unavailable or session not loadable → leave in durable queue +
   surface hold notice (same philosophy as Claude `CannotRenderChannels`).

### Packaging

- Binary: `cmd/c3-grok-adapter` (MCP + leader forwarder in one process, like
  Codex adapter + forwarder).
- Grok plugin: `.mcp.json` → adapter; SessionStart hook → `c3-broker session-hook`
  (or grok-specific writer using `C3_GROK_SESSION_ID`, with `GROK_SESSION_ID` accepted as a fallback).
- Setup: document / automate leader enable + plugin install.

## Probe notes (repro)

```bash
export GROK_HOME=/tmp/grok-c3-probe
cp ~/.grok/auth.json "$GROK_HOME/"
printf '[cli]\nuse_leader = true\n' > "$GROK_HOME/config.toml"
grok agent leader &
# then: length-prefixed register + acp session/new + session/prompt
```

Framing gotchas discovered the hard way:

- Content-Length framing → `Message too large` (first 4 bytes `"Cont"` read as BE u32).
- Raw ACP without `register` → `Expected Register message`.
- `type: "Register"` → must be lowercase `"register"`.
- Register requires `mode` (missing → `missing field mode`).
- ACP `payload` must be a **stringified** JSON object.

## Queue ack timing (bug class: re-held already-processed msgs)

**Bug (2026-07-10, msg 4224):** inject used `session/prompt` and only sent
`OpInboundDelivered` after the *agent turn finished*. Timeline:

1. Broker delivered 4224 → adapter started inject → text appeared in TUI.
2. Adapter process killed ~39s later while turn still running.
3. No ack → line stayed in durable queue.
4. Later attach reported 4224 as “held” even though the user already processed it.

**Fix:** ack as soon as ACP streams `user_message_chunk` (text landed). Drain
the remaining turn result before the next inject (serial), but do not hold the
queue line open for the whole model response.

## Lifecycle / multi-session (honest UX)

Grok’s leader model is **one shared agent process** + N TUI clients:

| User action | What actually happens |
|-------------|------------------------|
| `grok` with `use_leader=true` | Spawns/connects `leader.sock`; MCP adapters are children of **leader**, not the TUI. |
| Quit TUI only | Leader (+ MCP) often **stay up**; claim on broker **stays**; no re-attach notice. |
| Full stop | Kill leader (or reboot); MCP dies; broker releases claim when PID gone. |
| Rebuild adapter binary | **Does nothing** until leader respawns MCP (TUI restart alone is not enough). |
| Multiple Grok sessions | Share one leader; each needs its own session id for inject. Per-session MCP isolation is a Grok host property — verify with 2 concurrent sessions. |

**What “just works” should mean (target product):**

1. First launch in a project → attach once (or silent resume by stable session id).
2. Quit / resume same session → silent re-claim own topic (Claude already does this; Grok adapter needs SessionStart/session-id recover).
3. Binary updates → next MCP spawn picks them up (or document: `/mcps` reload / restart leader).
4. N sessions → N claims, inject only into the matching session.

## Smooth UX (implemented)

| Piece | Behavior |
|-------|----------|
| **Resume auto-attach** | After hello, adapter fires `OpRecoverSession` with Grok session UUID — strict, fail-closed resolution (`C3_GROK_SESSION_ID` pin > `GROK_SESSION_ID` fallback > single cwd match > single live match > PID ancestry; ambiguity refuses and the message stays queued). Silent re-claim + backlog notice. Re-fired on broker reconnect so a restarted broker re-learns the stable id. |
| **Stable id on attach** | Before attach, bind session id (cwd-matched when multiple sessions) so broker records session attachment for next resume. |
| **Clean leave** | Exits (including signals) keep the claim so the session can resume; the broker's conn-drop + PID-liveness reaping handles dead holders. Only the explicit `detach` tool sends `OpRelease`. (An earlier OpRelease-on-exit tombstoned the session attachment and broke resume — removed.) |
| **Host setup** | `c3-broker install-grok` → `use_leader=true` + pin `c3-grok-adapter` + print plugin steps. |
| **SessionStart hook** | Grok-aware `session-hook` (`C3_GROK_SESSION_ID` primary / `GROK_SESSION_ID` fallback / sessionId camelCase). |
| **Queue ack** | Ack only on a landing confirm bound to the prompted session whose `user_message_chunk` echoes a prefix of our injected text. Post-write silence/timeouts classify as UNCERTAIN: never acked, never blind-retried — the line stays in the durable queue (double-delivery over loss). A failed inject latches acks off until a full `fetch_queue` drain (`Remaining==0`) re-syncs the head. |
| **Mid-turn follow-up** | Drain the previous turn's ACP result, then `session/prompt` so a Telegram message sent while Grok is working becomes the next user turn. Do not wrap that drain in the 120s landing timer. Busy JSON-RPC errors retry until idle. |

## Still worth a soak test

- 2 concurrent Grok sessions, two topics, no cross-inject.
- Live: Telegram follow-up while a long tool-using turn is running — must appear as the next user message after the turn ends, not vanish as UNCERTAIN.
- Permission prompts: out of scope (Codex-tier).

## Decision for implementers

**Keep leader + `session/prompt`.** Lifecycle productized around Grok’s leader model.
