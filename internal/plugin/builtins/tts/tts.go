// Package tts is C3's text-to-speech plugin. It subprocesses the bundled
// Python provider chain and exposes it through plugin.Host's synthesizer hook.
package tts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/plugin"
	"github.com/Andrometiq/c3/internal/updater"
)

// Name is the plugin identifier and mappings.json:plugins key.
const Name = "tts"

const (
	defaultTimeoutSeconds = 120
	maxTimeoutSeconds     = 600
	maxAudioBytes         = 25 << 20
	deadlineEnvVar        = "C3_TTS_DEADLINE_SECONDS"
	chainEnvVar           = "C3_TTS_CHAIN"
	ttsHandlerRelative    = "plugins/c3/tts/tts-handler.py"
)

// Config is mappings.json:plugins.tts.
type Config struct {
	Enabled     bool   `json:"enabled"`
	HandlerPath string `json:"handler_path"`
	Timeout     int    `json:"timeout_seconds"`
	Python      string `json:"python"`
	Chain       string `json:"chain"`
	Language    string `json:"language"`
}

var ttsExecutablePath = os.Executable

// LoadConfig reads the plugin stanza and applies all runtime defaults and caps.
func LoadConfig(host plugin.Host) (Config, error) {
	cfg := Config{Enabled: true, Timeout: defaultTimeoutSeconds}
	if err := host.Config(Name, &cfg); err != nil {
		return Config{}, fmt.Errorf("tts: read config: %w", err)
	}
	return normalizeConfig(cfg), nil
}

func normalizeConfig(cfg Config) Config {
	if cfg.HandlerPath == "" {
		cfg.HandlerPath = defaultHandlerPath()
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeoutSeconds
	}
	if cfg.Timeout > maxTimeoutSeconds {
		cfg.Timeout = maxTimeoutSeconds
	}
	return cfg
}

// Register installs exactly one synthesizer unless the plugin is disabled.
func Register(host plugin.Host) error {
	cfg, err := LoadConfig(host)
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		host.Logf("tts: plugin disabled via mappings.json:plugins.tts.enabled=false")
		return nil
	}
	if _, err := os.Stat(cfg.HandlerPath); err != nil {
		host.Logf("tts: handler %s missing at startup (%v); synthesis will fail until it is restored", cfg.HandlerPath, err)
	} else {
		host.Logf("tts: registered with handler=%s timeout=%ds", cfg.HandlerPath, cfg.Timeout)
	}
	host.Logf("tts: python=%s", pythonExe(cfg))
	host.RegisterSynthesizer(func(ctx context.Context, req c3types.SpeechRequest) (c3types.SpeechResult, error) {
		return synthesize(ctx, cfg, req, host.Logf)
	})
	return nil
}

// Synthesize runs the configured handler. Stdout is capped at 25 MiB and is
// reserved for raw MP3 bytes; handler stderr is emitted as one broker-log line
// per record.
func Synthesize(ctx context.Context, cfg Config, req c3types.SpeechRequest) (c3types.SpeechResult, error) {
	return synthesize(ctx, normalizeConfig(cfg), req, func(format string, args ...any) {
		log.Printf("[plugin] "+format, args...)
	})
}

func synthesize(ctx context.Context, cfg Config, req c3types.SpeechRequest, logf func(string, ...any)) (c3types.SpeechResult, error) {
	if !cfg.Enabled {
		return c3types.SpeechResult{}, c3types.ErrNoSynthesizer
	}
	if cfg.HandlerPath == "" {
		return c3types.SpeechResult{}, fmt.Errorf("tts: no handler found")
	}
	if info, err := os.Stat(cfg.HandlerPath); err != nil || info.IsDir() {
		if err == nil {
			err = fmt.Errorf("is a directory")
		}
		return c3types.SpeechResult{}, fmt.Errorf("tts: handler %s: %w", cfg.HandlerPath, err)
	}

	timeout := timeoutForText(cfg.Timeout, req.Text)
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	args := []string{cfg.HandlerPath}
	if language := strings.TrimSpace(req.Language); language != "" {
		args = append(args, "--language", language)
	} else if language := strings.TrimSpace(cfg.Language); language != "" {
		args = append(args, "--language", language)
	}
	cmd := exec.CommandContext(tctx, pythonExe(cfg), args...)
	cmd.Stdin = strings.NewReader(req.Text)
	cmd.Env = handlerEnv(cfg, int(timeout.Seconds()))
	cmd.SysProcAttr = ttsSysProcAttr()
	cmd.Cancel = func() error { return ttsKillTree(cmd) }
	cmd.WaitDelay = 5 * time.Second

	stdout := &limitedBuffer{limit: maxAudioBytes}
	stderr := &lineRecorder{logf: func(line string) { logf("tts: %s", line) }}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	started := time.Now()
	err := cmd.Run()
	stderr.flush()
	elapsed := time.Since(started).Round(time.Millisecond)

	if errors.Is(err, errAudioLimit) || stdout.exceeded || stdout.Len() >= maxAudioBytes {
		return c3types.SpeechResult{}, fmt.Errorf("tts: handler stdout exceeds %d bytes", maxAudioBytes)
	}
	if tctx.Err() != nil {
		return c3types.SpeechResult{}, fmt.Errorf("tts: handler timed out after %s: %w", elapsed, tctx.Err())
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 3 {
			return c3types.SpeechResult{}, fmt.Errorf("tts: %w", c3types.ErrNothingToSay)
		}
		return c3types.SpeechResult{}, handlerError(err, stderr.lastLine())
	}
	if stdout.Len() == 0 {
		return c3types.SpeechResult{}, handlerError(errors.New("empty stdout"), stderr.lastLine())
	}
	return c3types.SpeechResult{
		Audio:    append([]byte(nil), stdout.Bytes()...),
		MIME:     "audio/mpeg",
		Provider: providerFromLine(stderr.lastLine()),
	}, nil
}

// Check runs the handler's non-synthesizing diagnostics mode.
func Check(ctx context.Context, cfg Config) ([]byte, error) {
	cfg = normalizeConfig(cfg)
	if cfg.HandlerPath == "" {
		return nil, fmt.Errorf("tts: no handler found")
	}
	timeout := time.Duration(cfg.Timeout) * time.Second
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(tctx, pythonExe(cfg), cfg.HandlerPath, "--check")
	cmd.Env = handlerEnv(cfg, cfg.Timeout)
	cmd.SysProcAttr = ttsSysProcAttr()
	cmd.Cancel = func() error { return ttsKillTree(cmd) }
	cmd.WaitDelay = 5 * time.Second
	stdout := &limitedBuffer{limit: 1 << 20}
	var stderr bytes.Buffer
	cmd.Stdout = stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if tctx.Err() != nil {
			return nil, fmt.Errorf("tts check: %w", tctx.Err())
		}
		return nil, handlerError(err, lastLine(stderr.String()))
	}
	return append([]byte(nil), stdout.Bytes()...), nil
}

func timeoutForText(base int, text string) time.Duration {
	if base <= 0 {
		base = defaultTimeoutSeconds
	}
	seconds := base + utf8.RuneCountInString(text)/100
	if seconds > maxTimeoutSeconds {
		seconds = maxTimeoutSeconds
	}
	return time.Duration(seconds) * time.Second
}

func pythonExe(cfg Config) string {
	if value := strings.TrimSpace(cfg.Python); value != "" {
		return value
	}
	return "python3"
}

func handlerEnv(cfg Config, deadlineSeconds int) []string {
	env := withoutEnv(os.Environ(), deadlineEnvVar)
	env = append(env, deadlineEnvVar+"="+strconv.Itoa(deadlineSeconds))
	if chain := strings.TrimSpace(cfg.Chain); chain != "" {
		env = withoutEnv(env, chainEnvVar)
		env = append(env, chainEnvVar+"="+chain)
	}
	return env
}

func withoutEnv(env []string, name string) []string {
	prefix := name + "="
	kept := env[:0]
	for _, value := range env {
		if !strings.HasPrefix(value, prefix) {
			kept = append(kept, value)
		}
	}
	return kept
}

func handlerError(err error, stderr string) error {
	if stderr != "" {
		return fmt.Errorf("tts: handler: %w: %s", err, stderr)
	}
	return fmt.Errorf("tts: handler: %w", err)
}

func providerFromLine(line string) string {
	for _, field := range strings.Fields(line) {
		if value, ok := strings.CutPrefix(field, "provider="); ok {
			return value
		}
	}
	return ""
}

func lastLine(value string) string {
	lines := strings.Split(strings.TrimSpace(value), "\n")
	if len(lines) == 0 {
		return ""
	}
	return strings.TrimSuffix(lines[len(lines)-1], "\r")
}

var errAudioLimit = errors.New("TTS audio limit exceeded")

type limitedBuffer struct {
	bytes.Buffer
	limit    int
	exceeded bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	remaining := b.limit - b.Len()
	if remaining <= 0 {
		b.exceeded = true
		return 0, errAudioLimit
	}
	if len(p) > remaining {
		b.exceeded = true
		n, _ := b.Buffer.Write(p[:remaining])
		return n, errAudioLimit
	}
	return b.Buffer.Write(p)
}

type lineRecorder struct {
	logf      func(string)
	partial   []byte
	last      string
	truncated bool
}

const maxStderrLineBytes = 64 << 10

func (w *lineRecorder) Write(p []byte) (int, error) {
	written := len(p)
	for len(p) > 0 {
		newline := bytes.IndexByte(p, '\n')
		if newline < 0 {
			w.append(p)
			break
		}
		w.append(p[:newline])
		w.emit()
		p = p[newline+1:]
	}
	return written, nil
}

func (w *lineRecorder) append(p []byte) {
	remaining := maxStderrLineBytes - len(w.partial)
	if remaining > 0 {
		if len(p) < remaining {
			remaining = len(p)
		}
		w.partial = append(w.partial, p[:remaining]...)
	}
	if remaining < len(p) {
		w.truncated = true
	}
}

func (w *lineRecorder) emit() {
	line := strings.TrimSuffix(string(w.partial), "\r")
	if w.truncated {
		line += "…[truncated]"
	}
	w.partial = w.partial[:0]
	w.truncated = false
	if line == "" {
		return
	}
	w.last = line
	w.logf(line)
}

func (w *lineRecorder) flush() {
	if len(w.partial) > 0 || w.truncated {
		w.emit()
	}
}

func (w *lineRecorder) lastLine() string { return w.last }

func defaultHandlerPath() string {
	if root := strings.TrimSpace(os.Getenv("CLAUDE_PLUGIN_ROOT")); root != "" {
		if path := existingHandlerPath(filepath.Join(root, "tts", "tts-handler.py")); path != "" {
			return path
		}
	}
	if root := strings.TrimSpace(os.Getenv("C3_SRC_DIR")); root != "" {
		if path := existingHandlerPath(filepath.Join(root, ttsHandlerRelative)); path != "" {
			return path
		}
	}
	if executable, err := ttsExecutablePath(); err == nil {
		if resolved, resolveErr := filepath.EvalSymlinks(executable); resolveErr == nil {
			executable = resolved
		}
		if path := existingReleaseHandlerPath(filepath.Join(filepath.Dir(executable), ttsHandlerRelative)); path != "" {
			return path
		}
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		if path := existingReleaseHandlerPath(filepath.Join(home, ".local", "share", "c3", ttsHandlerRelative)); path != "" {
			return path
		}
		return existingHandlerPath(filepath.Join(home, "src", "c3", ttsHandlerRelative))
	}
	return ""
}

func existingReleaseHandlerPath(path string) string {
	if err := updater.ValidateTTSBundle(filepath.Dir(path)); err != nil {
		return ""
	}
	return existingHandlerPath(path)
}

func existingHandlerPath(path string) string {
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		return path
	}
	return ""
}
