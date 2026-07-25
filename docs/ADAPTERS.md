# Writing C3 CLI Adapters

A C3 adapter is the bridge between the broker and a specific CLI's MCP-server expectations. The built-in adapters are `c3-claude-adapter` (Claude Code), `c3-codex-adapter` (Codex), `c3-grok-adapter` (Grok Build), and `c3-desktop-adapter` (Claude Desktop, poll-only — see [`DESKTOP.md`](DESKTOP.md)). If you want to integrate C3 with a CLI we don't yet support — Cursor, Aider, plain shell, your own thing — write an adapter.

Adapters are not channels. Channels move bytes between users and the broker over the network (Telegram, web, voice). Adapters move messages between the broker and a single CLI process over MCP stdio. The two never see each other directly — both talk to the broker.

## What an adapter does

Three jobs, every one of them small:

1. **Speak the host CLI's MCP protocol over stdio.** Claude Code and Codex both use a JSON-RPC 2.0 dialect very close to the MCP standard, with extensions for unsolicited notifications. A new CLI may differ in small ways; understand its dialect before starting.
2. **Maintain a connection to the broker over `$XDG_RUNTIME_DIR/c3.sock`** (falls back to `/tmp/c3-$UID/c3.sock` when `XDG_RUNTIME_DIR` is unset — see internal/broker/paths.go) and translate MCP tool calls into broker IPC ops.
3. **Translate inbound messages from the broker into whatever the host CLI can render.** Claude Code natively renders `notifications/claude/channel`. Codex doesn't render unsolicited notifications today, so the Codex adapter forwards them via WebSocket into the running app-server; anything held while a session is down is read back from the broker's durable queue with `fetch_queue`. A new CLI may need yet another delivery path.

## The broker IPC contract

Every adapter speaks the same broker IPC over `$XDG_RUNTIME_DIR/c3.sock`. Newline-delimited JSON, one message per line. The protocol is the same for all CLIs — the broker doesn't care what's on the other end.

| Direction | Op | Purpose |
|---|---|---|
| adapter → broker | `hello` | `{cli, pid, cwd}`. Broker replies with auto-attach state. |
| adapter → broker | `server_info` | Get serverInfo + capabilities + instructions for the host CLI's `initialize`. |
| adapter → broker | `tools_list` | Get the broker's tool list (channel-specific + plugin-registered + universal). |
| adapter → broker | `attach` | `{cwd?, name?, target?, topic_id?, group?, channel?, create?}`. See the attach parser + proposal flow in [`COMMANDS.md`](COMMANDS.md). |
| adapter → broker | `list_topics` | All known topics across channels + claim state. |
| adapter → broker | `tool_call` | Forward a tool invocation to the broker (which dispatches to the right channel/plugin). |
| broker → adapter | `hello_ack` | `{auto_attached, mapping?, claim_holder?, no_config?}`. |
| broker → adapter | `attached` | `{ok, channel, chat_id, topic_id, name, group?, needs_confirmation?, proposal?, err?}`. |
| broker → adapter | `tool_result` | Result of a forwarded tool call. |
| broker → adapter | `inbound` | Normalized inbound message routed to this adapter. |
| broker → adapter | `topics_list` | Listing response. |
| broker → adapter | `error` | Generic error. |

There is no shared adapter-client package to import. Each adapter hand-rolls its broker IPC on the low-level `internal/ipc` primitives (`ipc.NewConn`, `Conn.WriteJSON`, `Conn.ReadFrame`) dialed at `broker.SocketPath()`. Budget for real work: the three built-in adapters are ~2.3k LOC (Claude Code), ~1.5k LOC (Codex), and ~2.9k LOC (Grok Build), each reimplementing the handshake, attach, tool-forward, reconnect, and host-specific inbound translation described below. This is the hardest of C3's three extension seams.

## Adapter responsibilities, in order

On startup:

1. **Connect to the broker.** Open `$XDG_RUNTIME_DIR/c3.sock`. If the connect fails because the broker isn't running, spawn it (`exec.Command("c3-broker")` in a detached process group, then retry the connect with backoff for up to 10s). Singleton enforcement is on the broker side via flock; a race during spawn is safe.
2. **Send `hello`.** Include `cli` (your adapter's CLI name), `pid` (your adapter's pid; broker logs it for diagnostics), `cwd` (resolved-absolute path, used for cwd→mapping lookup).
3. **Wait for `hello_ack`.** Three cases:
   - `no_config: true` → broker doesn't have `~/.config/c3/mappings.json`. Build an `instructions` string telling the agent to run setup.
   - `auto_attached: true` → broker claimed a topic for your cwd. Build an `instructions` string saying so. Inbound notifications will start flowing.
   - `auto_attached: false, no_mapping: true` → no mapping; agent has to call `attach`. Build an `instructions` string explaining.
   - `auto_attached: false, claim_holder: {...}` → mapping exists but another session holds it. Tell the agent.
4. **Fetch `server_info` and `tools_list`** from the broker. Merge with any adapter-local tools.
5. **Run the MCP stdio loop.** Read JSON-RPC requests from stdin; respond to stdout.

While running:

- **`initialize` request from the CLI** → respond with the broker's `serverInfo`, `capabilities`, your assembled `instructions`, and the right `protocolVersion`.
- **`tools/list`** → return the merged tool list.
- **`tools/call`** → forward to broker via `tool_call` op (with a unique id), block on `tool_result`, return the result. Adapter-local tools (the ones you handle yourself, like `attach`) skip the forward and run inline.
- **`broker → inbound`** → translate to the host CLI's notification dialect and write to stdout.
- **`ping`** → respond `{}`.

On the broker dropping the connection:

- **Reconnect once.** Capture the connection generation before each write/read; if a write fails, attempt one reconnect, re-handshake (`server_info`), and re-claim the topic if you held one. Subsequent failures bubble up as errors to the CLI.
- All in-flight tool calls get woken up with a `broker reconnect` error so callers don't hang.

## Adapter-local tools

Most tools forward through to the broker. Two are conventionally adapter-local because they involve the adapter's own state:

- `attach` — wraps the broker's attach op. The adapter inspects the response and either returns a "claimed" success or surfaces the proposal to the agent. Adapter-local because the wording of the surfaced text differs by CLI (Claude Code natively renders `<channel>` blocks; Codex sees `notifications/message` log entries). Tool name is unprefixed across all adapters per spec §4.4.2 — the MCP server name (`c3` or `c3_codex` in `mcp_servers.<key>`) provides the namespace; per-tool prefixing is redundant.
- `topics` — wraps the broker's `list_topics` op. Adapter-local for the same reason — formatting.

Don't forward these through `tool_call`. Implement them in the adapter, hit the broker with the corresponding op directly.

## Translating inbound messages

The broker emits a normalized `Inbound{}` payload. Your adapter has to convert it to whatever the host CLI can ingest.

**Claude Code** uses `notifications/claude/channel` with rich `meta` attributes that render as `<channel source="..." chat_id="..." message_id="..." user="..." reply_to_message_id="..." reply_to_text="...">`. The adapter forwards the broker's payload nearly verbatim — broker shapes the meta to match.

**Codex** doesn't render unsolicited MCP notifications in the TUI today (open issues #18056, #17543, #15299). The Codex adapter therefore does three things in parallel:

1. Emit a `notifications/message` log notification (cheap; future-proofs for when Codex starts surfacing unsolicited notifications).
2. **If `C3_CODEX_REMOTE_BRIDGE=1`** is set in env (the C3 launcher sets it), forward the inbound as a real `turn/start` to the running Codex app-server via WebSocket. This is the path that makes a Telegram message appear as a normal turn in the user's TUI.

**Grok Build** has no channel-notification dialect. Live inject **requires leader mode** (`[cli] use_leader = true`). The Grok adapter registers as a client on `$GROK_HOME/leader.sock` and issues ACP `session/prompt` against the TUI session id (see [`GROK-INJECT.md`](GROK-INJECT.md)). Without a leader socket, inbound stays in the durable queue for `fetch_queue`.

**Claude Desktop** has no way for an MCP server to push into a chat at all, so `c3-desktop-adapter` is **pull-only**: inbound never surfaces on its own — it stays in the durable queue and the user drains it by asking Claude to call `fetch_queue` (an optional hourly Cowork Scheduled Task can poll on a timer). See [`DESKTOP.md`](DESKTOP.md).

Held messages — anything that arrived while no session was attached — are **not** buffered in the adapter. They live in the broker's durable per-route queue, and the agent drains them with the universal `fetch_queue` tool (all adapters expose it). An earlier Codex-only in-memory `inbox(limit, ack)` ring was retired in favor of that durable queue.

For a new CLI, look at what unsolicited-notification capability it has. If none, `fetch_queue` against the broker's durable queue is the safe baseline. If there's a push API (websocket, HTTP, IPC), use it; the adapter can have multiple delivery paths in parallel.

## Codex adapter specifics

The Codex adapter is unusual because Codex's `--remote` mode forces the bridge to span four processes:

```
codex (launcher binary)
  -> Codex app-server   (background process, holds MCP servers)
  -> c3-codex-adapter   (spawned by app-server as MCP stdio server)
  -> Codex TUI          (spawned by launcher with --remote)
```

The visible TUI talks to the app-server over WebSocket; the app-server runs MCP servers; one of those is the C3 adapter; the adapter talks to the broker. **The app-server, not the TUI, owns MCP server startup.** This is why the launcher injects MCP config args via `-c mcp_servers.c3_codex.*` flags into the **app-server's** invocation — the same flags get duplicated into the TUI invocation, but the app-server-side copy is the load-bearing one.

If you write a new CLI adapter, look at how that CLI handles MCP servers under any equivalent "remote/embedded" mode before assuming the TUI is where MCP lives. Get this wrong and you'll have an adapter that runs in the foreground but has none of the env vars it needs.

## Distribution

A CLI adapter binary is built from `cmd/<cli>-adapter/main.go` and installed
alongside the broker by `/c3:build`'s explicit core package set. The Claude Code
plugin's `.mcp.json` points at `c3-claude-adapter` by name; Codex's
`mcp_servers.c3_codex.command` points at `c3-codex-adapter`.

If your target CLI has a plugin marketplace, your adapter ships as a thin marketplace plugin manifest that references the binary. If it doesn't, document the manual MCP server registration steps in your adapter's `SETUP.md`.

## Adding a new adapter — checklist

- [ ] Cmd at `cmd/<cli>-adapter/main.go`
- [ ] Speaks broker IPC directly over `internal/ipc` (`ipc.NewConn` / `Conn.WriteJSON` / `Conn.ReadFrame`) at `broker.SocketPath()`
- [ ] MCP server speaks the host CLI's exact dialect (initialize fields, capabilities, notification methods)
- [ ] `attach` and `topics` implemented adapter-locally with the right user-facing wording
- [ ] Inbound translation matches what the host CLI renders (rich notification, buffer + poll, websocket push, or whichever combination)
- [ ] Reconnect-once-on-broker-drop with topic re-claim
- [ ] Marketplace manifest authored if the CLI has a marketplace
- [ ] `SETUP.md` for manual install steps if it doesn't
- [ ] Tests using the broker's mock IPC server

## Testing

The broker exposes a `broker.MockServer(t)` that runs an in-memory unix socket and responds with synthetic ops. Use it to drive your adapter through a full handshake → attach → tool call → inbound → reconnect cycle without spinning up a real broker.

For the host-CLI side, mock the stdin/stdout pair via two `os.Pipe()` halves and assert on the JSON-RPC traffic. The Claude and Codex adapters both have tests demonstrating the pattern.
