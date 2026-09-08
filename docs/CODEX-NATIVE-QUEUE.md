# Codex queued delivery

## Normal interactive use

Run `c3-broker install-codex-shim`, then start `c3-codex` or the installed
`codex` launcher. The explicit alias targets a separate C3-owned executable at
`~/.local/libexec/c3/codex-launcher`, so replacing `codex` does not replace it.
Re-run the shim installer after updating C3 to refresh that copy. Standalone and
NVM Codex installations are supported. Adapter discovery checks only siblings
of the resolved running launcher, then PATH; a bare launcher name never selects
an adapter from the project directory. `C3_CODEX_ADAPTER` is an explicit override.

The launcher supplies MCP tools and a session-specific app-server endpoint.
Inbound uses `thread/queue/add`, including while Codex is busy. C3 requests queued
input; it never automatically steers or interrupts an active turn. Codex controls
when queued input runs. This remains a bridge to the user's TUI, not a C3-owned
session runtime.

A definite unknown-method or unknown-variant rejection of `thread/queue/add`,
including an id-less JSON-RPC error, disables live submissions for that adapter
process and reports **pull-only: call `fetch_queue`**. There is no `turn/start`
compatibility path. Generic invalid requests, timeouts, connection loss, and
malformed acceptance never authorize a second submission or a broker ack.
Restart the adapter after upgrading an unsupported app-server.

## Identity, acknowledgement, and recovery

Startup recovery resolves and caches the conversation identity. Multiple loaded
threads are refused; cwd is not a recipient selector. An explicit
`C3_CODEX_THREAD_ID` supplies the pin. If startup resolution fails, explicit
attachment remains possible, but live delivery stays pull-only until identity
is pinned. Attach and failure notices explain this. Restart with an explicit pin,
or let a later broker reconnect retry startup resolution. Delivery itself never
rediscovers an unpinned conversation for each message.

WebSocket delivery requires a nonempty queued submission ID and a matching
returned client message identity. Retry identity depends on recipient
and message content, not mutable route labels. Native delivery requires successful
process exit and textual acknowledgement naming the pinned thread. Only then
does C3 acknowledge the exact broker delivery token. Events preserve their full
serialized payload, including system Source, Level, Title, and Message, and never
acknowledge durable message rows. Multi-route messages retain their origin tags.

These are **queue acceptance acknowledgements, not transcript receipts**. They do
not prove automatic idle wake, execution, crash durability, or exactly-once
processing. If Codex accepts input but its response is lost, the C3 copy remains
held; manually fetching it can duplicate input already in Codex. Native queue
invocation has no stable client message ID.

Destructive `fetch_queue` waits for in-flight delivery to finish. The broker
returns the durable identities of records actually consumed, under its existing
per-route confirmed-holder gate. Only matching pending submissions are excluded;
a fresh arrival before the response or an untouched sibling route stays eligible.
Debounce-held sources outside the fetch are not invalidated. If a partial fetch
intersects a processed merged batch, that batch is excluded and its surviving
sources remain held for a further pull, with a recovery notice. Obsolete success
and failure cannot acknowledge or re-block recovery.

An earlier failure does not suppress acknowledgements for later successful
messages: broker tokens consume exact records. Backlog attach notices do not use
up the one-shot transport-failure notice. Notices are best effort through MCP and,
when usable, the delivery transport. They cannot guarantee waking an unavailable
host. A canceled or timed-out destructive fetch has an unknown outcome, so live
delivery pauses until restart; further pulls remain available.

`codex_forward` reports transport, pin, topic, and whether a recovery notice was
sent, including
unsupported-queue and unresolved-identity pull-only states. It refuses rebinding.
Use a matching broker/adapter release for record-identity coordination.

## Opt-in existing local conversation

For a CLI supporting `codex queue --thread UUID --message TEXT`, configure only
that conversation's adapter:

```text
C3_CODEX_QUEUE_BIN=/absolute/path/to/the/real/codex
C3_CODEX_THREAD_ID=<the exact current conversation UUID>
```

Native mode is opt-in and takes precedence over WebSocket mode. It can reach an
existing local TUI without C3's separate app-server. The executable must be an
absolute path and the pin a UUID; message content is a literal process argument.
Do not put one conversation's UUID in shared MCP configuration. Without either
transport the adapter remains pull-only. Existing backlog still needs a pull.

C3's Claude permission relay and blocking `ask` integration are unavailable.
Voice arrives as broker transcripts; attachment references remain available for
`download_attachment`. This change does not establish receipt parity with Claude.
