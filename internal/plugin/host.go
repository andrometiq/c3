// Package plugin defines the broker-side plugin extension API.
// Spec §4.5.1.
package plugin

import (
	"context"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// Host is the broker-supplied interface that plugins receive at Register
// time. Plugins subscribe to hooks via the On* methods, register MCP
// tools via RegisterTools, read config from mappings.json via Config /
// ChannelConfig, and persist state via State / CacheDir. The Done
// channel closes when the broker is shutting down so plugin goroutines
// can exit cleanly.
type Host interface {
	OnInbound(fn func(ctx context.Context, msg *c3types.Inbound) (*c3types.Inbound, bool /*drop*/))
	OnVoiceReceived(fn func(ctx context.Context, payload c3types.VoicePayload) (string, error))
	OnOutbound(fn func(ctx context.Context, msg *c3types.Outbound) (*c3types.Outbound, bool /*drop*/))
	OnAttach(fn func(*Stub, *Mapping))

	RegisterTools(fn func(*ToolRegistry))

	Config(name string, target any) error        // mappings.json:plugins.<name>
	ChannelConfig(name string, target any) error // mappings.json:channels.<name>
	State(name string) StateDir
	CacheDir(name string) string

	Channel(name string) (channel.Channel, error)

	Logf(format string, args ...any)
	Done() <-chan struct{}
}

// Stub is the plugin-visible identity of one CLI adapter connection.
// Plugins receive these via OnAttach. CLI is the adapter family
// ("claude", "codex"); PID is the adapter process id; CWD is its
// working directory at hello time; ConnID is the broker's monotonic
// per-connection counter. The triple (CLI, PID, CWD) is the "logical
// session" used by the broker's claim-transfer logic.
type Stub struct {
	CLI    string
	PID    int
	CWD    string
	ConnID uint64
}

// Mapping describes the route a Stub has just attached to. Plugins
// receive this alongside the Stub in OnAttach. TopicID is nil for DM
// routes and non-nil for forum-topic routes (Telegram). Group is the
// channel-group identifier, empty when not applicable.
type Mapping struct {
	Channel string
	ChatID  int64
	TopicID *int64
	Name    string
	Group   string
}

// StateDir is a small JSON-backed key-value store under
// $XDG_STATE_HOME/c3/state/<plugin-name>/. Plugins use this for
// modest runtime state (cursors, rate-limit counters). Treat each
// entry as small (<1 MB); large data belongs under CacheDir.
type StateDir interface {
	Load(name string, target any) error
	Save(name string, target any) error
}

// ToolRegistry is the surface a plugin uses to expose MCP tools that
// every adapter (Claude, Codex, etc.) sees. Add registers a tool;
// Remove tears one down; List returns the current set. Tool names
// should be plugin-prefixed (e.g. `stt_retranscribe`) to avoid
// collisions across plugins.
type ToolRegistry interface {
	Add(t Tool)
	Remove(name string)
	List() []Tool
}

// Tool is one MCP tool a plugin exposes. Name is the tool identifier
// (recommend `<plugin>_<verb>` form); Description is what the agent
// reads to decide whether to call; InputSchema is JSON Schema for the
// arguments; Handler is invoked when an adapter forwards a tools/call
// for this tool. The return value is serialized into the MCP response;
// errors become MCP error frames.
type Tool struct {
	Name        string
	Description string
	InputSchema map[string]any
	Handler     func(ctx context.Context, args map[string]any) (any, error)
}
