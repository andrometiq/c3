package tts

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/plugin"
)

type fakeHost struct {
	cfg         *Config
	synthesizer func(context.Context, c3types.SpeechRequest) (c3types.SpeechResult, error)
	registered  int
	logs        []string
}

func (h *fakeHost) OnInbound(func(context.Context, *c3types.Inbound) (*c3types.Inbound, bool)) {
}
func (h *fakeHost) OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error)) {
}
func (h *fakeHost) OnOutbound(func(context.Context, *c3types.Outbound) (*c3types.Outbound, bool)) {
}
func (h *fakeHost) OnAttach(func(*plugin.Stub, *plugin.Mapping)) {}
func (h *fakeHost) RegisterSynthesizer(fn func(context.Context, c3types.SpeechRequest) (c3types.SpeechResult, error)) {
	h.registered++
	h.synthesizer = fn
}
func (h *fakeHost) RegisterTools(func(*plugin.ToolRegistry)) {}
func (h *fakeHost) Config(name string, target any) error {
	if name == Name && h.cfg != nil {
		*(target.(*Config)) = *h.cfg
	}
	return nil
}
func (h *fakeHost) ChannelConfig(string, any) error         { return nil }
func (h *fakeHost) State(string) plugin.StateDir            { return nil }
func (h *fakeHost) CacheDir(string) string                  { return "" }
func (h *fakeHost) Channel(string) (channel.Channel, error) { return nil, errors.New("none") }
func (h *fakeHost) Done() <-chan struct{}                   { return make(chan struct{}) }
func (h *fakeHost) Logf(format string, args ...any) {
	h.logs = append(h.logs, fmt.Sprintf(format, args...))
}

func writeHandler(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tts-handler.py")
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSynthesizePassesTextArgsAndEnvironmentAndReturnsMP3(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
	handler := writeHandler(t, `
import os, sys
assert sys.argv[1:] == ['--language', 'ta'], sys.argv
assert os.environ['C3_TTS_CHAIN'] == 'fake,backup'
assert os.environ['C3_TTS_DEADLINE_SECONDS'] == '5'
assert sys.stdin.read() == 'hello from stdin'
sys.stderr.write('provider=fake chunks=1 bytes=7 ms=1\n')
sys.stdout.buffer.write(b'ID3fake')
`)
	result, err := Synthesize(context.Background(), Config{
		Enabled: true, HandlerPath: handler, Timeout: 5, Chain: "fake,backup",
	}, c3types.SpeechRequest{Text: "hello from stdin", Language: "ta"})
	if err != nil {
		t.Fatalf("Synthesize: %v", err)
	}
	if string(result.Audio) != "ID3fake" || result.MIME != "audio/mpeg" || result.Provider != "fake" {
		t.Fatalf("result = %#v", result)
	}
}

func TestSynthesizeExitThreeIsNothingToSay(t *testing.T) {
	handler := writeHandler(t, "import sys\nsys.exit(3)\n")
	_, err := Synthesize(context.Background(), Config{Enabled: true, HandlerPath: handler, Timeout: 5}, c3types.SpeechRequest{Text: "---"})
	if !errors.Is(err, c3types.ErrNothingToSay) {
		t.Fatalf("error = %v, want ErrNothingToSay", err)
	}
}

func TestSynthesizeEmptyStdoutIncludesLastStderrLine(t *testing.T) {
	handler := writeHandler(t, "import sys\nprint('first', file=sys.stderr)\nprint('last reason', file=sys.stderr)\n")
	_, err := Synthesize(context.Background(), Config{Enabled: true, HandlerPath: handler, Timeout: 5}, c3types.SpeechRequest{Text: "hello"})
	if err == nil || !strings.Contains(err.Error(), "empty stdout") || !strings.Contains(err.Error(), "last reason") {
		t.Fatalf("error = %v", err)
	}
}

func TestSynthesizeRejectsStdoutOverAudioLimit(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
	handler := writeHandler(t, `
import sys
sys.stdout.buffer.write(b'x' * ((25 << 20) + 1))
`)
	result, err := Synthesize(
		context.Background(),
		Config{Enabled: true, HandlerPath: handler, Timeout: 5},
		c3types.SpeechRequest{Text: "hello"},
	)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error = %v, want exceeds error", err)
	}
	if strings.Contains(err.Error(), "timed out") {
		t.Fatalf("error = %v, must not be a timeout", err)
	}
	if len(result.Audio) != 0 || result.MIME != "" || result.Provider != "" {
		t.Fatalf("result = %#v, want no SpeechResult", result)
	}
}

func TestSynthesizeDeadlineKillsProcessTree(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows tree kill is best-effort direct-child kill")
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
	tmp := t.TempDir()
	started := filepath.Join(tmp, "started")
	sentinel := filepath.Join(tmp, "sentinel")
	handler := writeHandler(t, `
import os, subprocess, time
open(os.environ['C3_TEST_STARTED'], 'w').close()
subprocess.Popen(['sh', '-c', 'sleep 2; touch "$C3_TEST_SENTINEL"'])
time.sleep(30)
`)
	t.Setenv("C3_TEST_STARTED", started)
	t.Setenv("C3_TEST_SENTINEL", sentinel)
	begin := time.Now()
	_, err := Synthesize(context.Background(), Config{Enabled: true, HandlerPath: handler, Timeout: 1}, c3types.SpeechRequest{Text: "x"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}
	if _, err := os.Stat(started); err != nil {
		t.Fatalf("handler did not start: %v", err)
	}
	if delay := time.Until(begin.Add(3 * time.Second)); delay > 0 {
		time.Sleep(delay)
	}
	if _, err := os.Stat(sentinel); err == nil {
		t.Fatal("handler grandchild survived the deadline")
	}
}

func TestConfigDefaultsAndCaps(t *testing.T) {
	defaults, err := LoadConfig(&fakeHost{})
	if err != nil {
		t.Fatal(err)
	}
	if !defaults.Enabled || defaults.Timeout != defaultTimeoutSeconds || pythonExe(defaults) != "python3" {
		t.Fatalf("defaults = %#v", defaults)
	}
	capped, err := LoadConfig(&fakeHost{cfg: &Config{Enabled: true, Timeout: 999}})
	if err != nil {
		t.Fatal(err)
	}
	if capped.Timeout != maxTimeoutSeconds {
		t.Fatalf("timeout = %d, want %d", capped.Timeout, maxTimeoutSeconds)
	}
	if got := timeoutForText(120, strings.Repeat("x", 250)); got != 122*time.Second {
		t.Fatalf("scaled timeout = %v", got)
	}
	if got := timeoutForText(599, strings.Repeat("x", 1000)); got != 600*time.Second {
		t.Fatalf("capped scaled timeout = %v", got)
	}
}

func TestRegisterRegistersExactlyOneSynthesizer(t *testing.T) {
	handler := writeHandler(t, "import sys\nsys.stdout.buffer.write(b'ID3ok')\n")
	host := &fakeHost{cfg: &Config{Enabled: true, HandlerPath: handler, Timeout: 5}}
	if err := Register(host); err != nil {
		t.Fatal(err)
	}
	if host.registered != 1 || host.synthesizer == nil {
		t.Fatalf("registered = %d, callback nil=%v", host.registered, host.synthesizer == nil)
	}
}

func TestRegisterDisabledDoesNotRegister(t *testing.T) {
	host := &fakeHost{cfg: &Config{Enabled: false}}
	if err := Register(host); err != nil {
		t.Fatal(err)
	}
	if host.registered != 0 {
		t.Fatalf("registered = %d", host.registered)
	}
}
