package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/mappings"
)

// sandboxSetupEnv isolates a setup-flow test from the real machine: fresh
// HOME + XDG_CONFIG_HOME (mappings.json, stt.env) and a fresh
// XDG_RUNTIME_DIR (broker socket, pid file, flock) — the same isolation
// discipline the other setup tests use — plus fake broker-lifecycle seams so
// no real broker is ever stopped or spawned. Returns a counter of
// ensureBrokerUpFn calls.
func sandboxSetupEnv(t *testing.T) *int {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", home+"/.config")
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("C3_HOST_CLI", "claude")

	prevStop, prevEnsure := stopBrokerFn, ensureBrokerUpFn
	ensureCalls := 0
	stopBrokerFn = func() (bool, string) { return false, "" }
	ensureBrokerUpFn = func() { ensureCalls++ }
	t.Cleanup(func() { stopBrokerFn, ensureBrokerUpFn = prevStop, prevEnsure })

	prevShim := installClaudeShimFn
	installClaudeShimFn = func(args []string) error { return nil }
	t.Cleanup(func() { installClaudeShimFn = prevShim })

	prevInstall := installRunFn
	installRunFn = func(ctx context.Context) installResult { return installResult{skipped: true} }
	t.Cleanup(func() { installRunFn = prevInstall })

	return &ensureCalls
}

// writeTestMappings writes mf at the sandboxed default path and returns it.
func writeTestMappings(t *testing.T, mf *mappings.MappingsFile) string {
	t.Helper()
	mfPath, err := mappings.DefaultPath()
	if err != nil {
		t.Fatalf("mappings.DefaultPath: %v", err)
	}
	if err := writeMappingsFile(mfPath, mf); err != nil {
		t.Fatalf("writeMappingsFile: %v", err)
	}
	return mfPath
}

// TestRunSetupPair_HoldsSingletonLockDuringWindow — the F1 regression: the
// pairing "pause" only holds if setup owns the broker singleton flock for
// the whole window; otherwise an adapter-respawned broker re-reads the token
// and steals getUpdates (gate-dropping the code). While the poller is
// waiting, a simulated broker start (AcquireSingleton, the exact call
// runDaemon makes) must FAIL; after setup returns it must succeed again.
func TestRunSetupPair_HoldsSingletonLockDuringWindow(t *testing.T) {
	sandboxSetupEnv(t)

	// Config with a token so `setup pair` proceeds to the window.
	mf := skeletonMappings()
	applyBotToken(mf, "123:TESTTOKEN")
	writeTestMappings(t, mf)

	// Fake Bot API: first getUpdates returns nothing and signals the test;
	// later calls block until the test has asserted on the lock.
	firstPoll := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			close(firstPoll)
			fmt.Fprint(w, `{"ok":true,"result":[]}`)
			return
		}
		<-release
		fmt.Fprint(w, `{"ok":true,"result":[{"update_id":1,"message":{"text":"4242","from":{"id":777},"chat":{"id":777}}}]}`)
	}))
	defer srv.Close()
	t.Setenv("C3_TELEGRAM_API_URL", srv.URL)

	errCh := make(chan error, 1)
	go func() {
		errCh <- runSetupPair([]string{"dm", "--code", "4242", "--timeout-sec", "60"})
	}()

	select {
	case <-firstPoll:
	case <-time.After(10 * time.Second):
		t.Fatal("pairing window never polled the fake Bot API")
	}

	// The window is open — a broker must NOT be able to take the singleton
	// lock now (flock is per open-file-description, so this in-process probe
	// conflicts with setup's held lock exactly like a spawned broker would).
	if lock, err := broker.AcquireSingleton(broker.PidFilePath()); err == nil {
		lock.Release()
		t.Error("broker singleton lock was acquirable DURING the pairing window — a respawned broker would steal getUpdates")
	}

	close(release)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runSetupPair: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runSetupPair did not finish after the code arrived")
	}

	// After setup returns, the lock must be free for the restarted broker.
	lock, err := broker.AcquireSingleton(broker.PidFilePath())
	if err != nil {
		t.Fatalf("singleton lock still held after setup returned: %v", err)
	}
	lock.Release()

	// And the pairing must have landed on disk.
	mfPath, _ := mappings.DefaultPath()
	got, err := mappings.Read(mfPath)
	if err != nil {
		t.Fatalf("re-read mappings: %v", err)
	}
	if !got.IsUserAllowed(777) {
		t.Error("paired user 777 not allowlisted after setup pair dm")
	}
}

// TestRunSetupInteractive_ErrorPathRestoresBroker — the F3 regression: an
// error return between the pre-pairing broker stop and the final restart
// (here: DM pairing failing all three attempts on a fatal token error) must
// still restore the broker via the deferred ensureBrokerUp, and must release
// the singleton flock.
func TestRunSetupInteractive_ErrorPathRestoresBroker(t *testing.T) {
	ensureCalls := sandboxSetupEnv(t)
	t.Setenv("C3_NO_PROMPT", "1")

	// Report a running broker so the flow takes the stop path.
	stopBrokerFn = func() (bool, string) { return true, "(fake broker stopped)" }

	// The interactive error path returns WITHOUT joining the background
	// install goroutine; signal from inside the fake so the test can join it
	// before t.Cleanup restores the installRunFn seam (else: data race).
	installed := make(chan struct{})
	installRunFn = func(ctx context.Context) installResult {
		close(installed)
		return installResult{skipped: true}
	}

	// Fake Bot API: getMe succeeds (the existing token validates), but
	// getUpdates rejects the token — a FATAL pairing error, so each of the
	// three DM attempts fails immediately and the flow errors out mid-way.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/getMe") {
			fmt.Fprint(w, `{"ok":true,"result":{"username":"my_test_bot"}}`)
			return
		}
		fmt.Fprint(w, `{"ok":false,"error_code":401,"description":"Unauthorized"}`)
	}))
	defer srv.Close()
	t.Setenv("C3_TELEGRAM_API_URL", srv.URL)

	mf := skeletonMappings()
	applyBotToken(mf, "123:TESTTOKEN")
	writeTestMappings(t, mf)

	// All interactive reads hit EOF (empty line → defaults): Continue? →
	// yes, keep existing token → yes, pairing arm prompt → start waiting.
	devnull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("open %s: %v", os.DevNull, err)
	}
	prevStdin := os.Stdin
	os.Stdin = devnull
	t.Cleanup(func() { os.Stdin = prevStdin; devnull.Close() })

	if err := runSetupInteractive(); err == nil {
		t.Fatal("runSetupInteractive = nil, want the DM-pairing failure error")
	}
	select {
	case <-installed:
	case <-time.After(5 * time.Second):
		t.Fatal("background install goroutine never ran")
	}

	if *ensureCalls != 1 {
		t.Errorf("ensureBrokerUp called %d times after the error return, want 1 (broker must be restored)", *ensureCalls)
	}
	lock, lockErr := broker.AcquireSingleton(broker.PidFilePath())
	if lockErr != nil {
		t.Fatalf("singleton lock still held after the error return: %v", lockErr)
	}
	lock.Release()
}

// legacyNoAllowlistMappings builds the F4 fixture: a pre-allowlist-schema
// config — dm_chat_id + a group recorded, NO allowlist key at all.
func legacyNoAllowlistMappings() *mappings.MappingsFile {
	mf := skeletonMappings()
	mf.Channels[telegramChannelName] = mappings.ChannelConfig{
		BotToken:     "123:TESTTOKEN",
		DMChatID:     777,
		DefaultGroup: "main",
		Groups:       map[string]mappings.GroupConfig{"main": {ChatID: -100200, Title: "Ops"}},
	}
	return mf
}

// TestRepairAllowlist_LegacyConfig — the F4 unit: a legacy config without
// allowlist entries gets them re-applied idempotently; an intact config is
// left alone.
func TestRepairAllowlist_LegacyConfig(t *testing.T) {
	mf := legacyNoAllowlistMappings()
	if !repairAllowlist(mf) {
		t.Fatal("repairAllowlist(legacy) = false, want true (entries were missing)")
	}
	if !mf.IsUserAllowed(777) {
		t.Error("dm user 777 not allowlisted after repair")
	}
	if !mf.IsGroupAllowed(-100200) {
		t.Error("group -100200 not allowlisted after repair")
	}
	if mf.Channels[telegramChannelName].MasterUserID != 777 {
		t.Error("master_user_id not backfilled by the DM repair")
	}
	if repairAllowlist(mf) {
		t.Error("second repairAllowlist = true, want false (idempotent)")
	}
	if got := len(mf.AllowlistOrEmpty().Users); got != 1 {
		t.Errorf("allowlist.users has %d entries after double repair, want 1", got)
	}
}

// TestRunSetupFinish_RepairsLegacyAllowlist — the F4 flow: `setup finish`
// (which BOTH setup paths run) must persist the repaired allowlist for a
// legacy config, so a config previously reported "already paired" stops
// being default-deny dead.
func TestRunSetupFinish_RepairsLegacyAllowlist(t *testing.T) {
	sandboxSetupEnv(t)
	mfPath := writeTestMappings(t, legacyNoAllowlistMappings())

	if err := runSetupFinish(); err != nil {
		t.Fatalf("runSetupFinish: %v", err)
	}

	got, err := mappings.Read(mfPath)
	if err != nil {
		t.Fatalf("re-read mappings: %v", err)
	}
	if !got.IsUserAllowed(777) {
		t.Error("legacy config: dm user 777 still not allowlisted after setup finish")
	}
	if !got.IsGroupAllowed(-100200) {
		t.Error("legacy config: group -100200 still not allowlisted after setup finish")
	}
}

// TestSetupHTTPErrors_RedactToken — the F6 regression: a transport-level
// error string embeds the full token-bearing request URL (*url.Error), and
// setup prints these errors. Every setup HTTP path must redact the token.
func TestSetupHTTPErrors_RedactToken(t *testing.T) {
	// Grab a URL, then close the server so every request fails at the
	// transport level with the URL embedded in the error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	base := srv.URL
	srv.Close()

	const token = "123456:SECRETTOKENVALUE"

	if _, err := validateBotTokenAt(base, token); err == nil {
		t.Fatal("validateBotTokenAt(closed server) = nil, want transport error")
	} else {
		if strings.Contains(err.Error(), token) {
			t.Errorf("validateBotTokenAt error leaks the bot token: %v", err)
		}
		if !strings.Contains(err.Error(), "<redacted>") {
			t.Errorf("validateBotTokenAt error does not show the redaction marker (did the URL go missing entirely?): %v", err)
		}
	}

	p := newTestPoller(base, "1234", pairTargetDM)
	p.token = token
	if _, fatal, err := p.fetchPairUpdates(0); err == nil {
		t.Fatal("fetchPairUpdates(closed server) = nil, want transport error")
	} else {
		if fatal {
			t.Errorf("transport error reported as fatal; want transient")
		}
		if strings.Contains(err.Error(), token) {
			t.Errorf("fetchPairUpdates error leaks the bot token: %v", err)
		}
		if !strings.Contains(err.Error(), "<redacted>") {
			t.Errorf("fetchPairUpdates error does not show the redaction marker: %v", err)
		}
	}
}

// TestRedactToken — unit coverage for the helper itself.
func TestRedactToken(t *testing.T) {
	if got := redactToken(`Get "https://api.example.org/bot12:SECRET/getMe": dial refused`, "12:SECRET"); strings.Contains(got, "SECRET") {
		t.Errorf("redactToken left the token in place: %q", got)
	} else if !strings.Contains(got, "/bot<redacted>/") {
		t.Errorf("redactToken did not substitute the marker in the path segment: %q", got)
	}
	if got := redactToken("plain message", ""); got != "plain message" {
		t.Errorf("redactToken with empty token mutated the message: %q", got)
	}
}
