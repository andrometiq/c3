package stt

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/plugin"
)

// fakeHost is a minimal plugin.Host capturing the OnVoiceReceived callback
// and routing Config / ChannelConfig from in-memory maps. Enough surface for
// the stt plugin's startup + per-call paths.
type fakeHost struct {
	cfg           Config
	channelCfg    map[string]any
	voiceCallback func(ctx context.Context, p c3types.VoicePayload) (string, error)
	logs          []string
	// channel, when set, is what Channel() hands back. nil ⇒ "not registered",
	// which is what a broker with no telegram channel really answers; returning
	// a nil channel with a nil error would let a caller's type assertion decide
	// the fallback by accident instead of by rule.
	channel channel.Channel
}

func (h *fakeHost) OnInbound(fn func(context.Context, *c3types.Inbound) (*c3types.Inbound, bool)) {
}
func (h *fakeHost) OnVoiceReceived(fn func(context.Context, c3types.VoicePayload) (string, error)) {
	h.voiceCallback = fn
}
func (h *fakeHost) OnOutbound(fn func(context.Context, *c3types.Outbound) (*c3types.Outbound, bool)) {
}
func (h *fakeHost) OnAttach(fn func(*plugin.Stub, *plugin.Mapping)) {}
func (h *fakeHost) RegisterTools(fn func(*plugin.ToolRegistry))     {}

func (h *fakeHost) Config(name string, target any) error {
	if name != Name {
		return nil
	}
	t, ok := target.(*Config)
	if !ok {
		return nil
	}
	*t = h.cfg
	return nil
}

func (h *fakeHost) ChannelConfig(name string, target any) error {
	cc, ok := h.channelCfg[name]
	if !ok {
		return nil
	}
	// Mirror the real host: JSON round-trip into whatever struct the caller
	// passes, so adding fields to the read struct (e.g. api_base_url) doesn't
	// break this mock the way an exact type-assertion did.
	b, err := json.Marshal(cc)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, target)
}

func (h *fakeHost) State(name string) plugin.StateDir { return nil }
func (h *fakeHost) CacheDir(name string) string       { return "" }
func (h *fakeHost) Channel(name string) (channel.Channel, error) {
	if h.channel == nil {
		return nil, errors.New("plugin host: channel not registered")
	}
	return h.channel, nil
}
func (h *fakeHost) Logf(format string, args ...any) {
	h.logs = append(h.logs, format)
}
func (h *fakeHost) Done() <-chan struct{} { return make(chan struct{}) }

func TestRegister_HandlerMissingAtStartup_StillRegistersCallback(t *testing.T) {
	h := &fakeHost{
		cfg: Config{
			Enabled:     true,
			HandlerPath: "/nonexistent/path/stt-handler.py",
			Timeout:     30,
		},
		channelCfg: map[string]any{
			"telegram": map[string]string{"bot_token": "tok"},
		},
	}
	if err := Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if h.voiceCallback == nil {
		t.Fatal("expected OnVoiceReceived to be registered even when handler is missing — bug repro: 2026-05-14, broken symlink silently disabled STT")
	}

	transcript, err := h.voiceCallback(context.Background(), c3types.VoicePayload{MessageID: 1})
	if err != nil {
		t.Fatalf("callback error: %v", err)
	}
	if !strings.Contains(transcript, "[STT FAILED:") || !strings.Contains(transcript, "handler_missing") {
		t.Errorf("missing-handler transcript = %q, want marker containing handler_missing", transcript)
	}
}

func TestRegister_HandlerAppearsAfterStartup_NextCallTranscribes(t *testing.T) {
	tmp := t.TempDir()
	handler := filepath.Join(tmp, "stt-handler.py")

	h := &fakeHost{
		cfg: Config{
			Enabled:     true,
			HandlerPath: handler,
			Timeout:     5,
		},
		channelCfg: map[string]any{
			"telegram": map[string]string{"bot_token": "tok"},
		},
	}
	if err := Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// First call: handler missing → marker.
	t1, _ := h.voiceCallback(context.Background(), c3types.VoicePayload{MessageID: 1})
	if !strings.Contains(t1, "handler_missing") {
		t.Errorf("first call before handler appears: %q, want handler_missing marker", t1)
	}

	// Restore the handler — a script that just prints a fixed transcript.
	const script = "#!/usr/bin/env python3\nprint('recovered transcript')\n"
	if err := os.WriteFile(handler, []byte(script), 0o755); err != nil {
		t.Fatalf("write handler: %v", err)
	}

	// Second call should now run the handler — no broker restart needed.
	t2, _ := h.voiceCallback(context.Background(), c3types.VoicePayload{MessageID: 2})
	if !strings.Contains(t2, "recovered transcript") {
		t.Errorf("after handler restored: %q, want 'recovered transcript' — graceful recovery without restart", t2)
	}
}

// TestRunHandler_DeadlineKillsGrandchild guards the I-7 fix: on the ctx
// deadline the WHOLE process group must die, not just the direct child. The
// handler spawns a grandchild that would write a sentinel AFTER the deadline;
// with the old default cancel (Process.Kill on the direct PID only) the
// grandchild reparents to init and writes the sentinel, leaking work + paid API
// spend. With Setpgid + the group-kill Cancel, the grandchild dies with the
// handler and the sentinel never appears.
//
// Red without the fix (sentinel written → fail), green with it.
func TestRunHandler_DeadlineKillsGrandchild(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available; process-group kill test needs a real interpreter")
	}

	tmp := t.TempDir()
	handler := filepath.Join(tmp, "stt-handler.py")
	started := filepath.Join(tmp, "started")
	sentinel := filepath.Join(tmp, "sentinel")

	// The handler: mark that it ran, spawn a grandchild that touches the
	// sentinel after 3s (well past the 1s deadline below), then block so the
	// deadline kills this process. If the group kill works, the grandchild is
	// SIGKILL'd before it can touch the sentinel.
	script := "import os, subprocess, time\n" +
		"open(os.environ['C3_TEST_STARTED'], 'w').close()\n" +
		"subprocess.Popen(['sh', '-c', 'sleep 3; touch \"$C3_TEST_SENTINEL\"'])\n" +
		"time.sleep(30)\n"
	if err := os.WriteFile(handler, []byte(script), 0o755); err != nil {
		t.Fatalf("write handler: %v", err)
	}
	t.Setenv("C3_TEST_STARTED", started)
	t.Setenv("C3_TEST_SENTINEL", sentinel)

	h := &fakeHost{
		cfg: Config{
			Enabled:     true,
			HandlerPath: handler,
			Timeout:     1, // ctx deadline at ~1s → fires the group-kill Cancel
		},
		channelCfg: map[string]any{
			"telegram": map[string]string{"bot_token": "tok"},
		},
	}
	if err := Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}

	start := time.Now()
	// Returns once the deadline kills the handler (group-killed; WaitDelay
	// backstops any inherited-pipe hang). We only care about the side effect.
	_, _ = h.voiceCallback(context.Background(), c3types.VoicePayload{MessageID: 1})

	// Sanity: the handler actually ran (else the test would pass vacuously).
	if _, err := os.Stat(started); err != nil {
		t.Skipf("handler did not run (interpreter/env issue: %v); cannot assert group kill", err)
	}

	// Wait past the grandchild's 3s delay, then assert it was killed before it
	// could touch the sentinel.
	if d := time.Until(start.Add(4 * time.Second)); d > 0 {
		time.Sleep(d)
	}
	if _, err := os.Stat(sentinel); err == nil {
		t.Fatalf("grandchild survived the deadline and wrote %s — process group was not killed (I-7 regression)", sentinel)
	}
}

func TestRegister_DisabledInConfig_DoesNotRegister(t *testing.T) {
	h := &fakeHost{
		cfg: Config{Enabled: false},
	}
	if err := Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if h.voiceCallback != nil {
		t.Error("disabled plugin should not register OnVoiceReceived")
	}
}
