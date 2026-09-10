package main

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/buildidentity"
	"github.com/Andrometiq/c3/internal/ipc"
)

func TestRunStatusNegotiatedInboxAndHistory(t *testing.T) {
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "C3_") || strings.HasPrefix(key, "CLAUDE_CODE_") {
			t.Setenv(key, "")
		}
	}
	// P8: "live: inbox, confirmed <age>" across broker status and history.
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	oldHealth, oldClaims, oldSessions := statusFetchHealth, statusFetchClaims, statusFetchSessions
	t.Cleanup(func() { statusFetchHealth, statusFetchClaims, statusFetchSessions = oldHealth, oldClaims, oldSessions })
	statusFetchSessions = func() ([]ipc.SessionEntry, error) { return nil, nil }
	statusFetchHealth = func() (*ipc.HealthListMsg, error) { return &ipc.HealthListMsg{}, nil }
	// Exercise the ldflags-backed build identity independently of release version.
	oldBuild := buildidentity.ID
	buildidentity.ID = "status-stamped-build"
	t.Cleanup(func() { buildidentity.ID = oldBuild })
	for _, state := range []string{"waiting", "live_inbox", "pull_only"} {
		statusFetchClaims = func() (*ipc.ClaimsListMsg, error) {
			entry := ipc.ClaimEntry{Channel: "telegram", HolderCLI: "claude", HolderBuild: "test-build", Connected: true, RenderState: state}
			if state != "waiting" {
				entry.ConfirmedAt = time.Now()
				entry.ConfirmedTransport = "inbox"
			}
			return &ipc.ClaimsListMsg{Claims: []ipc.ClaimEntry{entry}}, nil
		}
		out := captureStdout(t, func() {
			if err := runStatus(); err != nil {
				t.Fatal(err)
			}
		})
		if !strings.Contains(out, "  version:   status-stamped-build\n") {
			t.Fatalf("status omitted the stamped CLI build identity:\n%s", out)
		}
		want := map[string]string{"waiting": "Live route: waiting.", "live_inbox": "live: inbox, confirmed", "pull_only": "was inbox, confirmed"}[state]
		if !strings.Contains(out, want) || !strings.Contains(out, "    build: test-build\n") {
			t.Fatal(out)
		}
	}
}

func TestRunStatusReceiptShapeDrift(t *testing.T) {
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "C3_") || strings.HasPrefix(key, "CLAUDE_") {
			t.Setenv(key, "")
		}
	}
	for _, name := range []string{"XDG_RUNTIME_DIR", "XDG_STATE_HOME", "XDG_CONFIG_HOME"} {
		t.Setenv(name, t.TempDir())
	}
	oldHealth, oldClaims, oldSessions := statusFetchHealth, statusFetchClaims, statusFetchSessions
	t.Cleanup(func() { statusFetchHealth, statusFetchClaims, statusFetchSessions = oldHealth, oldClaims, oldSessions })
	statusFetchHealth = func() (*ipc.HealthListMsg, error) { return &ipc.HealthListMsg{}, nil }
	statusFetchClaims = func() (*ipc.ClaimsListMsg, error) { return &ipc.ClaimsListMsg{}, nil }
	statusFetchSessions = func() ([]ipc.SessionEntry, error) {
		return []ipc.SessionEntry{{CLI: "claude", PID: 123, ReceiptShapeDrift: "2.1.266"}}, nil
	}
	out := captureStdout(t, func() {
		if err := runStatus(); err != nil {
			t.Fatal(err)
		}
	})
	want := "receipt shapes may have changed (host 2.1.266): run scripts/live-matrix/run.sh --collect-only --fixtures"
	if strings.Count(out, want) != 1 {
		t.Fatal(out)
	}
	statusFetchSessions = func() ([]ipc.SessionEntry, error) { return []ipc.SessionEntry{{CLI: "claude", PID: 123}}, nil }
	out = captureStdout(t, func() {
		if err := runStatus(); err != nil {
			t.Fatal(err)
		}
	})
	if strings.Contains(out, "receipt shapes") {
		t.Fatal("healthy session alarmed")
	}
}
