package main

import (
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

func TestRunStatusNegotiatedInboxAndHistory(t *testing.T) {
	// P8: "live: inbox, confirmed <age>" across broker status and history.
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	oldHealth, oldClaims := statusFetchHealth, statusFetchClaims
	t.Cleanup(func() { statusFetchHealth, statusFetchClaims = oldHealth, oldClaims })
	statusFetchHealth = func() (*ipc.HealthListMsg, error) { return &ipc.HealthListMsg{}, nil }
	for _, state := range []string{"waiting", "live_inbox", "pull_only"} {
		statusFetchClaims = func() (*ipc.ClaimsListMsg, error) {
			entry := ipc.ClaimEntry{Channel: "telegram", HolderCLI: "claude", Connected: true, RenderState: state}
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
		want := map[string]string{"waiting": "Live route: waiting.", "live_inbox": "live: inbox, confirmed", "pull_only": "was inbox, confirmed"}[state]
		if !strings.Contains(out, want) {
			t.Fatal(out)
		}
	}
}
