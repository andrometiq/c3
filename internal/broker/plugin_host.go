package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/plugin"
)

// PluginHost is the broker's concrete plugin.Host implementation. It's the
// place where plugin subscriptions accumulate, hooks fire, and tools get
// registered into the broker's tool surface.
//
// One PluginHost lives on the Broker. Plugins call Host methods to register
// their interest; the broker calls fireOn* methods at the right points in
// the message pipeline.
type PluginHost struct {
	broker *Broker

	mu         sync.RWMutex
	onInbound  []func(context.Context, *c3types.Inbound) (*c3types.Inbound, bool)
	onVoice    []func(context.Context, c3types.VoicePayload) (string, error)
	onOutbound []func(context.Context, *c3types.Outbound) (*c3types.Outbound, bool)
	onAttach   []func(*plugin.Stub, *plugin.Mapping)
	tools      map[string]plugin.Tool
}

func newPluginHost(b *Broker) *PluginHost {
	return &PluginHost{
		broker: b,
		tools:  map[string]plugin.Tool{},
	}
}

// ─── plugin.Host implementation ──────────────────────────────────────────────

func (h *PluginHost) OnInbound(fn func(context.Context, *c3types.Inbound) (*c3types.Inbound, bool)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onInbound = append(h.onInbound, fn)
}

func (h *PluginHost) OnVoiceReceived(fn func(context.Context, c3types.VoicePayload) (string, error)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onVoice = append(h.onVoice, fn)
}

func (h *PluginHost) OnOutbound(fn func(context.Context, *c3types.Outbound) (*c3types.Outbound, bool)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onOutbound = append(h.onOutbound, fn)
}

func (h *PluginHost) OnAttach(fn func(*plugin.Stub, *plugin.Mapping)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onAttach = append(h.onAttach, fn)
}

func (h *PluginHost) RegisterTools(fn func(*plugin.ToolRegistry)) {
	reg := &toolRegistry{host: h}
	var i plugin.ToolRegistry = reg
	fn(&i)
}

func (h *PluginHost) Config(name string, target any) error {
	if h.broker == nil || h.broker.Mappings() == nil {
		return fmt.Errorf("plugin host: no mappings")
	}
	cfg, ok := h.broker.Mappings().Plugins[name]
	if !ok {
		// Empty config is valid — plugin uses its defaults.
		return nil
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("plugin host: marshal config: %w", err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("plugin host: unmarshal config into target: %w", err)
	}
	return nil
}

func (h *PluginHost) ChannelConfig(name string, target any) error {
	if h.broker == nil || h.broker.Mappings() == nil {
		return fmt.Errorf("plugin host: no mappings")
	}
	cc, ok := h.broker.Mappings().Channels[name]
	if !ok {
		return fmt.Errorf("plugin host: channel %q not in mappings", name)
	}
	data, err := json.Marshal(cc)
	if err != nil {
		return fmt.Errorf("plugin host: marshal channel config: %w", err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("plugin host: unmarshal channel config: %w", err)
	}
	return nil
}

func (h *PluginHost) State(name string) plugin.StateDir {
	return &xdgStateDir{base: filepath.Join(stateRoot(), name)}
}

func (h *PluginHost) CacheDir(name string) string {
	return filepath.Join(cacheRoot(), name)
}

func (h *PluginHost) Channel(name string) (channel.Channel, error) {
	return h.broker.Channel(name)
}

func (h *PluginHost) Logf(format string, args ...any) {
	log.Printf("[plugin] "+format, args...)
}

func (h *PluginHost) Done() <-chan struct{} {
	return h.broker.ctx.Done()
}

// ─── Hook firing (called by the broker / worker pipeline) ────────────────────

// FireOnVoiceReceived runs registered OnVoiceReceived callbacks in registration
// order. Returns the first non-empty transcript, or "" if no plugin had one.
// The broker passes voice payloads here BEFORE the OnInbound chain so plugin
// inbound transforms see the post-STT text per spec §4.5.1.
func (h *PluginHost) FireOnVoiceReceived(ctx context.Context, p c3types.VoicePayload) string {
	h.mu.RLock()
	cbs := append([]func(context.Context, c3types.VoicePayload) (string, error){}, h.onVoice...)
	h.mu.RUnlock()
	for _, fn := range cbs {
		if t, err := fn(ctx, p); err == nil && t != "" {
			return t
		}
	}
	return ""
}

// FireOnInbound runs registered OnInbound callbacks in registration order
// (chained). First plugin to set drop=true short-circuits with nil.
func (h *PluginHost) FireOnInbound(ctx context.Context, in *c3types.Inbound) *c3types.Inbound {
	h.mu.RLock()
	cbs := append([]func(context.Context, *c3types.Inbound) (*c3types.Inbound, bool){}, h.onInbound...)
	h.mu.RUnlock()
	for _, fn := range cbs {
		next, drop := fn(ctx, in)
		if drop {
			return nil
		}
		if next != nil {
			in = next
		}
	}
	return in
}

// FireOnOutbound is the symmetric outbound chain.
func (h *PluginHost) FireOnOutbound(ctx context.Context, out *c3types.Outbound) *c3types.Outbound {
	h.mu.RLock()
	cbs := append([]func(context.Context, *c3types.Outbound) (*c3types.Outbound, bool){}, h.onOutbound...)
	h.mu.RUnlock()
	for _, fn := range cbs {
		next, drop := fn(ctx, out)
		if drop {
			return nil
		}
		if next != nil {
			out = next
		}
	}
	return out
}

// FireOnAttach calls every registered OnAttach observer.
func (h *PluginHost) FireOnAttach(s *plugin.Stub, m *plugin.Mapping) {
	h.mu.RLock()
	cbs := append([]func(*plugin.Stub, *plugin.Mapping){}, h.onAttach...)
	h.mu.RUnlock()
	for _, fn := range cbs {
		fn(s, m)
	}
}

// ─── ToolRegistry ───────────────────────────────────────────────────────────

type toolRegistry struct{ host *PluginHost }

// Add registers a tool under its name. A name that is already registered is
// REFUSED and logged — the incumbent stays.
//
// It used to plain-assign, so a second plugin claiming a name silently replaced
// the first: the first plugin's tool vanished from List() with nothing said, and
// whichever plugin loaded last won by load order. plugin.ToolRegistry's own doc
// tells authors to prefix tool names "to avoid collisions across plugins" — that
// was advice the implementation did not enforce.
//
// First-writer-wins rather than last, on the same asymmetry the rest of this
// codebase uses: a refused registration is a log line the author reads and
// fixes, while a silent replacement is a tool that stops existing for reasons
// nobody can see. The registry has no consumers yet, which is exactly why this
// is cheap now and a behaviour change once a third party has written against it.
func (r *toolRegistry) Add(t plugin.Tool) {
	r.host.mu.Lock()
	defer r.host.mu.Unlock()
	if _, dup := r.host.tools[t.Name]; dup {
		log.Printf("plugin tool REFUSED name=%q: already registered by another plugin — the incumbent keeps the name; prefix tool names with your plugin name (e.g. \"<plugin>_<verb>\") to avoid this", t.Name)
		return
	}
	r.host.tools[t.Name] = t
}

func (r *toolRegistry) Remove(name string) {
	r.host.mu.Lock()
	delete(r.host.tools, name)
	r.host.mu.Unlock()
}

func (r *toolRegistry) List() []plugin.Tool {
	r.host.mu.RLock()
	defer r.host.mu.RUnlock()
	out := make([]plugin.Tool, 0, len(r.host.tools))
	for _, t := range r.host.tools {
		out = append(out, t)
	}
	return out
}

// ─── XDG state/cache dirs ───────────────────────────────────────────────────

type xdgStateDir struct{ base string }

func (s *xdgStateDir) Load(name string, target any) error {
	path := filepath.Join(s.base, name+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func (s *xdgStateDir) Save(name string, target any) error {
	if err := os.MkdirAll(s.base, 0700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(target, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.base, name+".json.tmp")
	final := filepath.Join(s.base, name+".json")
	if err := atomicWriteFile(tmp, final, data, 0600); err != nil {
		return err
	}
	return nil
}

// atomicWriteFile writes data to tmp, fsyncs it, renames to final, then
// fsyncs the parent directory. Without the two fsyncs, a crash between
// the rename and the kernel's journal flush can leave a zero-byte file
// at `final` (the directory entry is durable but the inode data isn't),
// which means losing plugin state. (daemon.md §5.1)
func atomicWriteFile(tmp, final string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	cleanup := func() { _ = os.Remove(tmp) }
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		cleanup()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		cleanup()
		return err
	}
	if err := f.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		cleanup()
		return err
	}
	// Windows disallows fsync on a directory handle (ERROR_ACCESS_DENIED); the
	// file bytes were already fsync'd before the rename, so skip the dir-fsync
	// there rather than fail the whole write. (Mirrors queue/store.go.)
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(filepath.Dir(final))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func stateRoot() string {
	if x := os.Getenv("XDG_STATE_HOME"); x != "" {
		return filepath.Join(x, "c3", "state")
	}
	home, _ := os.UserHomeDir()
	// The "state" element is NOT redundant: $XDG_STATE_HOME defaults to
	// $HOME/.local/state, so without it the two branches resolve the SAME
	// logical directory to two different paths and a plugin's state vanishes
	// when the broker is spawned with a different env (the 2026-05-09 env-fork
	// shape recorded in paths.go:10-22). The env branch matches the documented
	// contract — internal/plugin/host.go:62-64, "$XDG_STATE_HOME/c3/state/
	// <plugin-name>/" — so this branch has to agree with it.
	return filepath.Join(home, ".local", "state", "c3", "state")
}

func cacheRoot() string {
	if x := os.Getenv("XDG_CACHE_HOME"); x != "" {
		return filepath.Join(x, "c3")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".cache", "c3")
}
