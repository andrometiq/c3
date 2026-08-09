# Writing C3 Plugins

A C3 plugin extends the broker with capabilities that aren't tied to a specific channel or CLI. Speech-to-text is the one shipped example. This document is for plugin authors.

If you want to add a new transport (Slack, web chat, voice), you want a **channel** — see `CHANNELS.md`. If you want to bridge a new CLI to C3, you want an **adapter** — see `ADAPTERS.md`.

## Read this first: what is actually open in v0.1.0

There are two extension seams here, and they have very different costs.

| Seam | Posture in v0.1.0 | What it costs you |
|---|---|---|
| **Go plugin API** (`plugin.Host`, hooks, config, state) | **In-tree only — you fork the broker.** | Your plugin lives in *your* copy of this repo under `internal/plugin/builtins/<name>/`, you rebuild the broker binary, and you rebase onto every upstream release. You cannot ship your plugin as an independent artifact, and users install your broker rather than C3's. |
| **STT provider** (a Python file in `plugins/c3/stt/stt-pkg/providers/`) | **Open. Drop-in, no recompile.** | Nothing. It's a data-file change to the Python pipeline. [Contract below](#stt-provider-contract-frozen-for-v010). |

The reason for the first row is structural, not a policy choice: every Go package in this module lives under `internal/`, so Go's internal-import rule makes `github.com/Andrometiq/c3/internal/plugin` unimportable from any other module. The same is true of the channel and adapter seams. Promoting `plugin`, `c3types`, and `channel` out of `internal/` is on the roadmap; until then, "write a plugin" means "maintain a fork."

If what you want is a new transcription engine, you almost certainly want the STT provider seam and can skip the rest of this document.

## Hook points — what fires, and what doesn't

`plugin.Host` (`internal/plugin/host.go:18`) declares four callback subscription methods plus the `RegisterTools` registration seam. **Two callbacks are live. Two are declared but never invoked by the broker in v0.1.0.** They remain on the interface, so you will see them if you read `host.go`; this table is the authority on which ones actually run.

| Hook | Status in v0.1.0 | Semantics |
|---|---|---|
| `OnInbound` | **Live** | Called for debounce-merged non-voice inbounds and channel events before routing. Resolved voice rows use the durable voice-delivery path and do not re-enter this transform chain. Return a replacement `*Inbound` to mutate, or `drop=true` to short-circuit the chain and discard the message. |
| `OnVoiceReceived` | **Live** | Called by the bounded voice scheduler for automatic voice enrichment and manual `retranscribe`. First callback to return a **non-empty string with a nil error** wins; a callback that errors or returns `""` is skipped and the next one runs. |
| `RegisterTools` | **Registers, does not dispatch** | The registry accepts your tool and stores it. Nothing in the broker reads that map, and tool dispatch is a fixed switch. See [Tools](#tools-registered-but-not-dispatched-in-v010). |
| `OnOutbound` | **Declared, not yet invoked** | The signature and the chain runner (`FireOnOutbound`) exist and are correct, but no broker code path calls them. Outbound messages go straight from `dispatchReply` to the channel. Subscribing succeeds and the callback never runs. |
| `OnAttach` | **Declared, not yet invoked** | Same: `FireOnAttach` exists, the attach path does not call it. Subscribing succeeds and the callback never runs. |

**Subscribing to a hook that isn't invoked is silent.** `Register` returns `nil`, the broker logs the plugin as loaded, and the callback sits in the host forever. There is no startup validation and no warning — from inside your plugin, "no outbound has happened yet" and "this hook can never fire" look identical. If you subscribe to `OnOutbound` or `OnAttach` and see nothing, the hook is the reason; it is not your code.

**Ordering is registration order, not `priority`.** `FireOnInbound` and `FireOnVoiceReceived` iterate their callback slices in subscription order, which is the order of the `builtinPlugins` slice in `cmd/c3-broker/main.go`. The host never reads the `priority` config key. STT runs first because it is first in that slice.

**Each hook chain is synchronous and serial.** An `OnInbound` callback blocks its route worker until it returns. `OnVoiceReceived` runs off-worker, with at most two scheduler attempts executing concurrently across all routes. If you need to offload your own work, read the next section first — the panic rules change once you spawn a goroutine.

### Panic behaviour

There is no `recover` inside the plugin host; no hook is individually guarded. What catches a panic depends on which path fired it, and the blast radius is larger than your plugin in every case.

- **`OnInbound` panicking on the merged-message path** is caught by `flushInbounds`' guard. The broker and route worker survive. The affected non-voice rows are already durable but are not pushed; `fetch_queue` can still recover them.
- **`OnInbound` panicking on the event path** is caught by `flushEvent`'s guard. The event is dropped; broker and worker survive.
- **`OnVoiceReceived` panicking** is caught at the scheduler-runner boundary, so it does not close a `retranscribe` caller's IPC connection or terminate the broker. The pending queue row remains the source of truth and is recovered by the startup scan after a restart; a synchronous caller eventually reaches its own bounded timeout.
- **A goroutine you spawn yourself is outside every one of those guards.** An unrecovered panic in any goroutine terminates the whole broker process, and does so quietly — a goroutine panic never reaches the log path that broker failures normally use (`internal/broker/recover.go:8-16`). If you offload work, put `defer func() { if r := recover(); r != nil { host.Logf(...) } }()` at the top of your goroutine.

## Skeleton of an in-tree plugin

A plugin is a Go package whose root file exports `Register(host plugin.Host) error` — note `plugin.Host` is an **interface**, passed by value, not `*plugin.Host`.

```go
// internal/plugin/builtins/example/example.go
package example

import (
	"context"
	"fmt"
	"strings"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/plugin"
)

const Name = "example"

type config struct {
	Enabled bool   `json:"enabled"`
	Tag     string `json:"tag"`
}

func Register(host plugin.Host) error {
	cfg := config{Enabled: true, Tag: "[example]"}
	if err := host.Config(Name, &cfg); err != nil {
		return fmt.Errorf("%s: load config: %w", Name, err)
	}
	// The host does not enforce `enabled` — check it yourself, and return
	// before subscribing if it's false.
	if !cfg.Enabled {
		host.Logf("%s: disabled via mappings.json", Name)
		return nil
	}

	host.OnInbound(func(ctx context.Context, in *c3types.Inbound) (*c3types.Inbound, bool) {
		if in == nil {
			return nil, false
		}
		if strings.Contains(in.Text, "spam") {
			return nil, true // drop: short-circuits the rest of the chain
		}
		in.Text = cfg.Tag + " " + in.Text
		return in, false
	})

	host.Logf("%s: registered", Name)
	return nil
}
```

Register it in the broker's compiled-in plugin list — the `builtinPlugins` slice in `cmd/c3-broker/main.go` (there is no `internal/plugin/registry.go`):

```go
import "github.com/Andrometiq/c3/internal/plugin/builtins/example"

var builtinPlugins = []broker.BuiltinPlugin{
	{Name: stt.Name, Register: func(h plugin.Host) error { return stt.Register(h) }},
	{Name: example.Name, Register: func(h plugin.Host) error { return example.Register(h) }},
}
```

`RegisterBuiltinPlugins` calls each `Register` in slice order at boot, before channels start. Slice order *is* your hook order. Rebuild (`make build`) and the plugin loads on the next broker start.

## Configuration

Plugin config lives at `~/.config/c3/mappings.json` under `plugins.<name>`. `host.Config(name, &target)` JSON-round-trips that subtree into your struct; a missing subtree is not an error (you keep your defaults).

```json
{
  "plugins": {
    "example": {
      "enabled": true,
      "priority": 50,
      "tag": "[example]"
    }
  }
}
```

Two keys are conventional, and **neither is enforced by the host**:

- `enabled` — by convention a bool defaulting to `true`. `RegisterBuiltinPlugins` does not read it. Your `Register` must check it and return early, as the skeleton above does and as STT does (`stt.go:79-80`). A plugin that trusts the host here cannot be turned off.
- `priority` — accepted into config structs by convention, read by nothing. Chain order is registration order (see above). If you need to run before another plugin, move your entry earlier in `builtinPlugins`.

Anything else under `plugins.<name>` is yours.

## Persistent state

`host.State(name)` returns a JSON-backed `StateDir` (`Load`/`Save`) rooted at `$XDG_STATE_HOME/c3/state/<name>/`, falling back to `~/.local/state/c3/state/<name>/` when `XDG_STATE_HOME` is unset. Writes are atomic (temp file, fsync, rename, dir fsync). Keep entries small — under a megabyte, easily regenerable.

`host.CacheDir(name)` returns a path under `$XDG_CACHE_HOME/c3/<name>/` (fallback `~/.cache/c3/<name>/`) for anything large or disposable — model weights, indices. The directory is not created for you.

## Sending messages from a plugin

`host.Channel(name)` returns the registered `channel.Channel`, or an error if that channel isn't registered. Don't assume any particular channel exists.

```go
ch, err := host.Channel("telegram")
if err == nil {
	sentID, sendErr := ch.SendReply(c3types.ReplyArgs{ChatID: chatID, Text: "..."})
	_ = sentID
	if sendErr != nil {
		host.Logf("send failed: %v", sendErr)
	}
}
```

Arg and result types live in `internal/c3types`.

## Tools: registered but not dispatched in v0.1.0

`host.RegisterTools` hands your callback a registry with `Add`/`Remove`/`List`. `Add` is **first-writer-wins**: a held name is refused, the incumbent remains, and the only signal is a broker-log line. Prefix tool names with your plugin name (for example, `<plugin>_<verb>`) to avoid collisions. **Nothing else in the broker reads that map**, no adapter queries it, and there is no wire op to fetch plugin tools. Tool dispatch is a fixed switch over seven built-in names (`reply`, `react`, `edit_message`, `send_typing`, `poll`, `stop_poll`, `download_attachment`) with `unknown tool %q` as the default (`internal/broker/dispatch.go:19-38`).

So a plugin-registered tool is never listed to any CLI and is never routable. The shipped counter-example is instructive: STT's `retranscribe` is not a plugin tool at all — it is a dedicated IPC op (`ops.go:35`, `handler.go:157`), because that's what works today.

If you need a tool in v0.1.0, add an op to the IPC protocol and the dispatch switch in your fork. Wiring `RegisterTools` end-to-end is roadmap work.

One API wart to know if you use the registry anyway: `RegisterTools` takes `func(*plugin.ToolRegistry)` — a pointer to an interface. Method calls on a pointer-to-interface don't compile, so inside the callback you must dereference: `(*reg).Add(t)`.

## Logging

`host.Logf(format, args...)` writes to the broker's log with a `[plugin]` prefix. Never `fmt.Println` or write to `os.Stdout` — that stream is not yours.

There is no metrics API on `plugin.Host` in v0.1.0.

## Testing

There is no mock host shipped with the module. Write a small `fakeHost` implementing `plugin.Host` in your plugin's test package and capture the callbacks you care about — that's the pattern the STT tests use, and `internal/plugin/builtins/stt/stt_test.go:21` is a complete, working example to copy (it stubs all five subscription methods, routes `Config`/`ChannelConfig` from in-memory maps, and stashes the `OnVoiceReceived` callback so tests can invoke it directly).

## STT as a worked example

Two pieces ship together:

- **Go shim** at `internal/plugin/builtins/stt/` — compiled into the broker, subscribes to `OnVoiceReceived`, reads `plugins.stt.{enabled, handler_path, timeout_seconds, python, audio_retention}` from `mappings.json`, and subprocesses a Python handler per voice attempt. The broker-owned scheduler also reads `plugins.stt.voice_retry_expiry` (default `24h`; a Go duration string or number of seconds).
- **Python pipeline** at `plugins/c3/stt/` — `stt-handler.py` plus `stt-pkg/`, which holds the chained provider runner (`stt-pkg/stt.py`) and four bundled providers (`gemini-3-flash-openrouter`, `soniox-stt-async-v5`, `elevenlabs-scribe-v2`, `sarvam-saaras-v3`). The default chain tries those four in that order, skipping any provider whose key is unavailable; `C3_STT_CHAIN` overrides the order. `stt-pkg/vocabulary.txt` biases recognition toward domain terms. API keys are read from the provider modules' own env/file lookups.

### The handler contract

The shim invokes the handler as:

```
stdin (line 1):  <bot_token>\n
argv:            <python> <handler_path> <chat_id> <reply_msg_id> <file_id> <message_thread_id|"">
env:             C3_TELEGRAM_API_URL=<base url>   (only when a proxy base is configured)
                 STT_AUDIO_RETENTION=<n>
```

**The bot token is on stdin, not argv** — deliberately, so it never appears in `ps`, `/proc/<pid>/cmdline`, or audit logs. A handler that reads a token from `sys.argv` is both broken (every index is shifted by one) and a credential leak. Read line 1 of stdin.

Two more things a handler must tolerate: `C3_TELEGRAM_API_URL` should be honoured for `getFile` and the audio download, because direct `api.telegram.org` is blocked on some networks and ignoring it will simply time out; and on the deadline the shim SIGKILLs the handler's **entire process group**, so any grandchildren it spawned die with it.

The handler path resolves in exactly this order: `mappings.json:plugins.stt.handler_path` if set; `${CLAUDE_PLUGIN_ROOT}/stt/stt-handler.py`; `$C3_SRC_DIR/plugins/c3/stt/stt-handler.py`; `plugins/c3/stt/stt-handler.py` beside the resolved broker executable; the documented `~/.local/share/c3` checkout; then `~/src/c3`. Every automatic candidate must exist. `install-desktop` records the resolved path so Desktop and other non-plugin hosts remain independent of cwd; a missing bundle is an install error, not a silent `handler_missing`.

Self-update replaces the shipped handler/runner/provider set but preserves regular non-bundled `*.py` files in the installed provider directory. Those files are the documented drop-in extension seam, not disposable release debris.

### Failure surfacing

Failures are never silent. A transcription-stage failure returns, *as the
transcript*, `[STT FAILED: <reason> — see <broker log path>]`, with `<reason>`
one of `handler_missing`, `token_unavailable`, `timeout`, `killed`, `error`,
`empty`. If the handler cannot fetch the audio, it instead returns
`[STT FETCH FAILED: <server cause>]`. The scheduler parks fail-closed transient
network failures for automatic retry; permanent failures durably resolve to an
agent-facing recovery message.

The scheduler replaces an ordinary STT marker with an agent-facing recovery message
that names the `file_id`, `download_attachment`, `retranscribe`, and broker log.
It appends that message to any caption/rich text instead of clobbering the
sender's words. A preflight fetch refusal uses the server's cause; a permanent
handler fetch failure takes the ordinary recovery path. Two ordinary reason
values come from the scheduler rather than the shim: `no_transcript` when no
`OnVoiceReceived` subscriber returned anything, and `stt_failed` for an
unparseable failure return.

Voice intake is persist-first: the route worker stores a row carrying an honest
pending placeholder and advances the source offset before any network call. The
two-runner scheduler resolves that row after STT. Known-DOWN channel health gates
attempts without burning them; transient download failures retry with bounded
backoff, and a broker restart reconstructs pending work from the queue. Manual
`retranscribe` joins the same single-flight lease. If a pending row was consumed
while STT ran, the result is an additive transcript-update row rather than a
rewrite of consumed history.

Note the chain consequence: because the marker is a **non-empty string returned with a nil error**, it wins `FireOnVoiceReceived` and any `OnVoiceReceived` callback registered after STT will not run on that message. If you are writing a second voice plugin, register it *before* STT in `builtinPlugins`.

## STT provider contract (frozen for v0.1.0)

This is the one seam that needs no fork and no recompile. A provider is a single Python file at `plugins/c3/stt/stt-pkg/providers/<name>.py`, loaded by filename via `importlib` when `<name>` appears in the chain.

**Required — exactly one function:**

```python
transcribe(audio_path: str, audio_bytes: bytes) -> str | None
```

The loader rejects the module at load time with `provider <name> missing transcribe() function` if this symbol is absent — a loud, stderr-visible failure.

**Optional — two functions:**

```python
set_vocabulary(vocab: dict) -> None
```

Called immediately before **each** `transcribe` call when present. `vocab` is `{"terms": [{"preferred": str, "not": [str], "note": str}, ...], "context": str}`. Adapt it into whatever your API accepts — a system prompt, hotwords, a `prompt` parameter. Ignore it if your engine has no equivalent.

```python
available() -> str
```

Return `""` when the provider can run, or a short reason when credentials or
another prerequisite are missing. An unavailable provider is skipped without
burning retries. If the hook is absent the provider is attempted; if it raises,
the runner also treats it as available so a faulty probe cannot hide a working
provider.

**Return contract:**

| Your `transcribe` does | The runner does |
|---|---|
| returns a non-empty string | strips it, prints it to stdout, exits 0. Done — later providers don't run. |
| returns `None` or an empty/whitespace string | logs the attempt to stderr, retries per `--retries` (default 1 retry), then moves to the next provider in the chain. |
| raises | same as empty: logged with the exception type, retried, then falls through. |
| every provider exhausted | nothing on stdout, exit 1 — the Go shim surfaces `[STT FAILED: empty]`. |

Nothing you write may go to **stdout** except by returning it; stdout is the transcript channel. Use stderr for diagnostics.

### A complete minimal provider

```python
"""my-engine — example STT provider for C3."""
import os

_VOCAB = {"terms": [], "context": ""}


def set_vocabulary(vocab):
    """Optional. Called right before each transcribe()."""
    global _VOCAB
    _VOCAB = vocab or {"terms": [], "context": ""}


def transcribe(audio_path: str, audio_bytes: bytes) -> str | None:
    """Return the transcript, or None/'' to fall through to the next provider.

    audio_path:  absolute path to the audio file (for APIs that want a path)
    audio_bytes: the same file's bytes (for APIs that want an upload body)
    """
    key = os.environ.get("MY_ENGINE_API_KEY", "")
    if not key:
        raise RuntimeError("MY_ENGINE_API_KEY not set")  # logged, retried, falls through

    hints = ", ".join(t["preferred"] for t in _VOCAB.get("terms", []))
    text = call_my_engine(audio_bytes, api_key=key, hints=hints)  # your code

    return text.strip() or None
```

Drop that at `providers/my-engine.py` and test it standalone — no broker involved:

```bash
python3 plugins/c3/stt/stt-pkg/stt.py /path/to/audio.ogg --chain my-engine
```

### Getting your provider into the live chain

Dropping the file in is necessary but not sufficient. `stt.py` resolves the
chain from `--chain`, then `C3_STT_CHAIN`, then the shipped four-provider
default. The bundled handler does not pass `--chain`, so set `C3_STT_CHAIN` in
the broker environment (or in `~/.claude/stt.env`, which the handler loads) to
include your provider without editing shipped code:

```bash
C3_STT_CHAIN=my-engine,gemini-3-flash-openrouter,sarvam-saaras-v3
```

An explicit `--chain` still wins for standalone runs. Interactive setup may
rewrite `~/.claude/stt.env`; re-add a hand-written `C3_STT_CHAIN` line afterward
or provide it through the service/process environment.

Full provider how-to, including the bundled providers as reference adaptations: [`plugins/c3/stt/stt-pkg/README.md`](../plugins/c3/stt/stt-pkg/README.md).

## Checklist for an in-tree plugin

- [ ] You accept that this means maintaining a fork until the API leaves `internal/`
- [ ] Package under `internal/plugin/builtins/<name>/`
- [ ] `Register(host plugin.Host) error` exported — value, not pointer
- [ ] Subscribes only to `OnInbound` and/or `OnVoiceReceived` (the hooks that fire)
- [ ] `enabled` checked inside `Register` — the host will not do it for you
- [ ] Config read via `host.Config` with sane defaults for a missing subtree
- [ ] Hook subscriptions happen during `Register`, not from a goroutine that subscribes later
- [ ] Any goroutine you spawn has its own `recover` — an unrecovered one kills the broker
- [ ] Hot-path work under ~100 ms
- [ ] No `fmt.Println` / `os.Stdout.Write` — use `host.Logf`
- [ ] Tests with a local `fakeHost` (copy `stt_test.go`'s)
- [ ] Added to the `builtinPlugins` slice in `cmd/c3-broker/main.go`, in the position your chain order requires

## Not in v0.1.0

Documented so you can tell a gap from a bug:

- **External subprocess plugins.** A manifest under `~/.config/c3/plugins/<name>/` and a stdio JSON-RPC protocol are the intended shape. Nothing is implemented. Until then, in-tree/fork is the only Go plugin path.
- **`OnOutbound` and `OnAttach` wiring.** Declared on `plugin.Host`, chain runners written, no call sites.
- **Plugin tool listing and dispatch.** `RegisterTools` stores; nothing reads or routes.
- **Host-enforced `enabled` / `priority`.** Both are plugin-side conventions today.
- **A metrics API and a shipped mock host.** Neither exists.
