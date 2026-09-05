# Reliable Codex delivery

## Normal interactive use

Run `c3-broker install-codex-shim`, then start `c3-codex` (or the installed
`codex` shim). The explicit `c3-codex` entrypoint survives a Codex self-update
replacing the `codex` symlink. The launcher supports both standalone and NVM
Codex installations and loads the adapter from the same release directory.

The launcher supplies C3's MCP tools and a session-specific app-server endpoint.
Modern app-servers receive Telegram input through `thread/queue/add`, including
messages sent while the agent is working. Older servers that explicitly return
JSON-RPC method-not-found use the previous `thread/resume` + `turn/start` path.
A failed or uncertain queue response never triggers a second delivery path.

Recovery and delivery share the same conversation identity. Multiple loaded
threads without an explicit pin are refused; cwd is not a recipient selector.
Startup backlog generates an agent-visible recovery notice. A full acknowledged
`fetch_queue` drain restores live acknowledgements and invalidates stale pending
forwards, so one transport failure no longer requires restarting the adapter.
Button, reaction, poll-result, and system-event payloads survive delivery and
never consume ordinary queued messages.

`codex_forward` reports the actual transport, conversation, topic, and whether
queue recovery is required. It does not claim to register an endpoint without
changing anything, and it refuses attempts to redirect the current session.

## Existing local conversation

For a local Codex CLI that supports `codex queue --thread UUID --message TEXT`,
the Codex adapter can deliver to the existing TUI without a separate WebSocket
app-server. Set both variables on that session's adapter:

```text
C3_CODEX_QUEUE_BIN=/absolute/path/to/the/real/codex
C3_CODEX_THREAD_ID=<the exact current conversation UUID>
```

Use the real CLI executable, not an older C3 launcher that does not recognize the
`queue` subcommand. Do not put a single thread UUID in shared MCP configuration:
each adapter must be pinned to its own conversation. The adapter continues to
use the normal explicit topic attachment/session recovery protocol.

Native queue mode takes precedence over WebSocket mode when configured. It
passes message content as a literal process argument, checks the acknowledgement
names the pinned thread, and only then allows the existing serialized broker
acknowledgement path to consume the Telegram copy. Failed delivery leaves the
message in C3's durable queue. While Codex is busy, its queue processes messages
after the current turn; this transport does not interrupt an active turn.

Messages already held before attachment still require `fetch_queue` recovery.
Avoid consuming a queue simultaneously through manual fetch and live delivery.
The existing fail-closed forwarding latch preserves messages after a delivery
failure; fix the transport and completely drain the durable backlog to restore
normal acknowledgement behavior.

## Validation and limitations

Verify the native CLI supports `queue --help` before opting into native mode.
Codex MCP processes do not necessarily inherit `CODEX_THREAD_ID`; do not globally
register a native adapter with a guessed or shared conversation UUID. Use the
launcher for ordinary fresh/resumed sessions. A raw adapter without either
transport remains pull-only and explicitly reports that attachment does not
enable automatic delivery.

Codex uses its own approval system. C3's Claude-specific permission relay and
blocking `ask` integration are not supplied by this change. Voice messages arrive
as broker transcripts with the original attachment reference; other media retain
their attachment references for `download_attachment`.
