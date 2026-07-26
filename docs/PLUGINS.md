# Writing C3 Plugins

A C3 plugin extends the broker with capabilities that aren't tied to a specific channel or CLI. Speech-to-text is one. Photo OCR, custom translation, slash-command shortcuts, scheduled outbound messages — all natural plugin territory. This document is for plugin authors.

If you want to add a new transport (Slack, web chat, voice), you want a **channel**, not a plugin — see `CHANNELS.md`. If you want to bridge a new CLI to C3, you want an **adapter** — see `ADAPTERS.md`.

## What a plugin is

A plugin is a unit of code that subscribes to one or more **hooks** the broker exposes during normal operation. The plugin runs inside the broker process (compiled-in) or as a sibling subprocess (external) and gets called every time its hook fires. Plugins are stateless across calls unless they explicitly persist state.

There are two delivery models:

- **Compiled-in plugins (v1, today)** — Go packages under `internal/plugin/builtins/<name>/`, statically linked into the broker binary. Discovered at build time. The STT plugin is shipped this way.
- **External subprocess plugins (roadmap)** — separate executables registered via a manifest under `~/.config/c3/plugins/<name>/`. Spoken to over a stdio JSON-RPC protocol. Allows plugins in any language without recompiling the broker.

This guide covers compiled-in plugins. The subprocess-plugin protocol is documented at the end with a note that it's not implemented in v1.

## Hook points

The broker exposes four wired hooks. A plugin subscribes to whichever ones it cares about; absent subscriptions are no-ops. (Signatures are on `plugin.Host` in `internal/plugin/host.go`.)

| Hook | Signature | Semantics |
|---|---|---|
| `OnInbound` | `func(ctx, *Inbound) (*Inbound, drop bool)` | Called for every channel-emitted inbound message after debounce/dedup but before routing. Plugins can mutate (e.g. add a transcript), replace, or drop. Plugins are chained by `priority`; the first plugin to set `drop=true` short-circuits. |
| `OnVoiceReceived` | `func(ctx, VoicePayload) (string, error)` | Called only for channel events flagged as voice. First plugin to return a non-empty string wins; that string becomes the inbound's text. STT lives here. |
| `OnAttach` | `func(*Stub, *Mapping)` | Fired after a successful attach claim. Pure observer — return value is ignored. Use for logging, audit trails, custom welcomes. |
| `RegisterTools` | `func(*ToolRegistry)` | Called once at plugin load. Plugins can register MCP tools that adapters then expose to their CLIs. |

The broker calls hooks synchronously and serially within a single inbound flow — your plugin code blocks the message until it returns. Keep hot-path work fast (target sub-100ms); offload long work to a goroutine and return the message unchanged or with a placeholder marker the plugin will fill in later.

## Skeleton of a compiled-in plugin

A plugin is a Go package whose root file exports a `Register(host *plugin.Host) error` function:

```go
// internal/plugin/builtins/example/example.go
package example

import (
	"context"
	"fmt"

	"github.com/Andrometiq/c3/internal/plugin"
)

const Name = "example"

type config struct {
	Greeting string `json:"greeting"`
}

func Register(host plugin.Host) error {
	cfg, err := loadConfig(host)
	if err != nil {
		return fmt.Errorf("%s: load config: %w", Name, err)
	}
	host.OnAttach(func(sess *plugin.Stub, mapping *plugin.Mapping) {
		host.Logf("%s: %s attached to %s", Name, sess.CLI, mapping.Name)
	})
	host.RegisterTools(func(reg *plugin.ToolRegistry) {
		reg.Add(plugin.Tool{
			Name:        "example_greet",
			Description: "Send a greeting reply through the attached topic.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name": map[string]any{"type": "string"},
				},
			},
			Handler: func(ctx context.Context, args map[string]any) (any, error) {
				name, _ := args["name"].(string)
				return fmt.Sprintf("%s, %s!", cfg.Greeting, name), nil
			},
		})
	})
	return nil
}

func loadConfig(host plugin.Host) (*config, error) {
	cfg := &config{Greeting: "Hello"}
	if err := host.Config(Name, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
```

Then register it in the broker's compiled-in plugin list — the `builtinPlugins` slice in `cmd/c3-broker/main.go` (there is **no** `internal/plugin/registry.go`; that's the real registrar):

```go
import "github.com/Andrometiq/c3/internal/plugin/builtins/example"

var builtinPlugins = []broker.BuiltinPlugin{
	{Name: stt.Name, Register: func(h plugin.Host) error { return stt.Register(h) }},
	{Name: example.Name, Register: func(h plugin.Host) error { return example.Register(h) }},
}
```

The broker wires them in at boot via `br.RegisterBuiltinPlugins(builtinPlugins)`. Rebuild via `/c3:build` (or `make build`) and the new plugin loads on next broker start.

## Configuration

Plugin config lives at `~/.config/c3/mappings.json` under `plugins.<name>`. The host gives you a typed read via `host.Config(name, &target)`:

```json
{
  "plugins": {
    "example": {
      "enabled": true,
      "priority": 50,
      "greeting": "Hi"
    }
  }
}
```

Reserved fields:

- `enabled` (bool, default `true`) — when `false`, the host skips the plugin's hook subscriptions entirely. The plugin's `Register` is still called once at boot, so it should not assume `enabled=true` to do work outside of subscriptions.
- `priority` (int, default `100`) — chained-hook ordering for `OnInbound`. Lower runs first. STT defaults to `10` because transcription should land before any other plugin sees the inbound.

Anything else under `plugins.<name>` is yours.

## Persistent state

Beyond config, plugins occasionally need runtime state (caches, dedup tables, last-run timestamps). Use `host.State(name)` which returns a JSON-backed `*StateDir` rooted at `~/.config/c3/state/<plugin-name>/`. Treat this as small-data only — sub-megabyte, easily regenerable. For real datasets (model weights, indices), use `host.CacheDir(name)` rooted at `$XDG_CACHE_HOME/c3/<plugin-name>/`.

Two reasons for the split: state is sometimes worth backing up (XDG_CONFIG), cache never is (XDG_CACHE).

## Tools you can register

Tools registered via `RegisterTools` show up in **every adapter's** tool list automatically — Claude Code sees them, Codex sees them. The broker takes care of routing tool calls back to your plugin handler.

Tool name conventions: prefix with the plugin name (`example_greet`, `stt_retranscribe`) so users and the LLM can tell which plugin a tool came from.

The `Handler` function is called with the JSON arguments parsed against your schema. Return value is rendered back to the CLI in the standard MCP `content[].text` shape — strings work; structured returns are JSON-encoded.

For tools that need to send Telegram replies (or interact with another channel), get a handle via `host.Channel(name)`. Outbound arg/result types live in `internal/c3types`, and `SendReply` returns the sent message id:

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

The plugin should not assume any specific channel exists — degrade gracefully if it doesn't.

## Logging and observability

`host.Logf(format, args...)` writes to the broker's structured log. Don't `fmt.Println` — broker uses stdout for IPC.

Metrics: `host.Metric("plugin.example.invocations", 1, tags...)` if you need observability. Backend is no-op in v1; spec hook is in place for later.

## STT as a worked example

The STT plugin is the v1 shipped first-class example. Two pieces ship together:

- **Go shim** at `internal/plugin/builtins/stt/` — compiled into the broker, subscribes to `OnVoiceReceived`, reads `plugins.stt.{handler_path, timeout_seconds, enabled}` from `mappings.json`, and subprocesses a Python handler with `<bot_token> <chat_id> <reply_msg_id> <file_id> [<message_thread_id>]` on argv. The handler script is responsible for fetching the audio from Telegram (the bot token is passed in) and printing the transcript to stdout.
- **Python pipeline** at `plugins/c3/stt/` — `stt-handler.py` plus a `stt-pkg/` package with a chained provider runner (`stt-pkg/stt.py`) and three bundled providers (`gemini-3-flash-openrouter`, `sarvam-saaras-v3`, `elevenlabs-scribe-v2`). Default chain: Gemini first, Sarvam fallback; ElevenLabs is bundled and opt-in via `--chain`. Vocabulary file at `stt-pkg/vocabulary.txt` biases recognition toward domain-specific terms. API keys come from `~/.claude/stt.env`.

The handler path resolves in exactly this order: `mappings.json:plugins.stt.handler_path` if set, else `${CLAUDE_PLUGIN_ROOT}/stt/stt-handler.py` (the bundled pipeline, when the broker is launched by Claude Code), else empty — in which case every voice message surfaces as `[STT FAILED: handler_missing]` rather than silently dropping. Users who want a different STT engine (whisper, deepgram, a local model) override `plugins.stt.handler_path` to point at their own script — the argv contract is the only requirement.

Errors don't degrade silently. On any failure the shim returns `[STT FAILED: <reason>]` so the CLI sees the failure explicitly (`handler_missing`, `timeout`, `killed`, `error`, `empty`, `token_unavailable`). The worker also forces `[STT FAILED: no_transcript_plugin]` if no `OnVoiceReceived` plugin produced output at all (defense-in-depth).

The Python pipeline is a deliberate scope choice — STT is the only first-party plugin that uses a non-Go runtime, because the provider chain (and the room for adding new providers without recompiling) is more valuable than language uniformity. New plugins should default to pure Go.

Adding a new STT provider (a `transcribe(audio_path, audio_bytes)` Python file dropped into `providers/`, then named in `--chain`) is documented in [`plugins/c3/stt/stt-pkg/README.md`](../plugins/c3/stt/stt-pkg/README.md) — the provider how-to. Because that's a data-file change to the Python pipeline, it needs **no broker recompile**.

## External subprocess plugins (v1.x — not yet implemented)

Documented for forward-compatibility. A future external plugin lives at `~/.config/c3/plugins/<name>/manifest.json`:

```json
{
  "name": "ocr",
  "executable": "./ocr-plugin",
  "subscribes": ["OnInbound"],
  "config_schema": "./schema.json"
}
```

The broker spawns the executable on startup, sends newline-delimited JSON-RPC over stdio with one method per hook. Same hook signatures as the compiled-in interface, JSON-encoded.

This is not in v1. If you need an externally-distributed plugin before the protocol lands, fork the broker and add your plugin under `internal/plugin/builtins/`.

## Testing

A plugin should ship with Go tests under the same package. The host exposes a `plugin.MockHost()` you can pass to `Register` to drive hooks without spinning up the real broker:

```go
func TestExample_GreetTool(t *testing.T) {
	host := plugin.MockHost(t)
	if err := example.Register(host); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tool, ok := host.Tools()["example_greet"]
	if !ok {
		t.Fatal("example_greet not registered")
	}
	got, err := tool.Handler(t.Context(), map[string]any{"name": "World"})
	if err != nil {
		t.Fatal(err)
	}
	if got != "Hello, World!" {
		t.Errorf("got %q", got)
	}
}
```

The mock host gives you in-memory channels, a captured log, and helpers to fire hook events synthetically.

## Checklist for new plugins

- [ ] Package under `internal/plugin/builtins/<name>/`
- [ ] `Register(host plugin.Host) error` exported
- [ ] Hook subscriptions deterministic (no goroutines spawned in `Register` that subscribe later)
- [ ] Config types defined and read via `host.Config`
- [ ] `enabled`/`priority` honored
- [ ] Tools (if any) prefix-namespaced with the plugin name
- [ ] No `fmt.Println` / `os.Stdout.Write` — use `host.Logf`
- [ ] Tests using `plugin.MockHost`
- [ ] Added to the `builtinPlugins` slice in `cmd/c3-broker/main.go`
