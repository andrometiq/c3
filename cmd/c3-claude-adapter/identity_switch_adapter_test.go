package main

import (
	"fmt"
	"testing"

	"github.com/Andrometiq/c3/internal/sessionhandoff"
)

func writeIdentityHandoff(t *testing.T, key, stableID string, unixNano int64) {
	t.Helper()
	if err := sessionhandoff.Write(key, sessionhandoff.Entry{
		StableSessionID: stableID,
		CWD:             "/projects/" + stableID,
		UnixNano:        unixNano,
	}); err != nil {
		t.Fatalf("write handoff %q -> %q: %v", key, stableID, err)
	}
}

// TestResolveTerminalHandoff is T8's resolver coverage. It pins the chain walk,
// strict monotonic guard, cycle stop, depth cap, and unchanged single-hop case.
func TestResolveTerminalHandoff(t *testing.T) {
	t.Run("walks to terminal entry", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		writeIdentityHandoff(t, "spawn", "session-a", 10)
		writeIdentityHandoff(t, "session-a", "session-b", 20)
		writeIdentityHandoff(t, "session-b", "session-c", 30)

		got, ok := resolveTerminalHandoff("spawn")
		if !ok || got.StableSessionID != "session-c" || got.UnixNano != 30 {
			t.Fatalf("resolver did not walk the complete handoff chain: ok=%v entry=%+v", ok, got)
		}
	})

	t.Run("rejects stale link", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		writeIdentityHandoff(t, "spawn", "session-current", 20)
		writeIdentityHandoff(t, "session-current", "session-stale", 20)
		writeIdentityHandoff(t, "session-stale", "session-wrong", 30)

		got, ok := resolveTerminalHandoff("spawn")
		if !ok || got.StableSessionID != "session-current" || got.UnixNano != 20 {
			t.Fatalf("resolver followed a non-newer stale handoff link: ok=%v entry=%+v", ok, got)
		}
	})

	t.Run("stops on cycle", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		writeIdentityHandoff(t, "spawn", "session-a", 10)
		writeIdentityHandoff(t, "session-a", "session-b", 20)
		writeIdentityHandoff(t, "session-b", "session-a", 30)

		got, ok := resolveTerminalHandoff("spawn")
		if !ok || got.StableSessionID != "session-a" || got.UnixNano != 30 {
			t.Fatalf("resolver did not stop at the newest accepted entry before a cycle: ok=%v entry=%+v", ok, got)
		}
	})

	t.Run("stops at depth cap", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		writeIdentityHandoff(t, "spawn", "session-00", 1)
		for i := 0; i <= terminalHandoffDepthCap; i++ {
			writeIdentityHandoff(t,
				fmt.Sprintf("session-%02d", i),
				fmt.Sprintf("session-%02d", i+1),
				int64(i+2),
			)
		}

		got, ok := resolveTerminalHandoff("spawn")
		want := fmt.Sprintf("session-%02d", terminalHandoffDepthCap)
		if !ok || got.StableSessionID != want {
			t.Fatalf("resolver exceeded depth cap %d: ok=%v stable=%q want=%q", terminalHandoffDepthCap, ok, got.StableSessionID, want)
		}
	})

	t.Run("single hop unchanged", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		writeIdentityHandoff(t, "spawn", "only-session", 10)

		got, ok := resolveTerminalHandoff("spawn")
		if !ok || got.StableSessionID != "only-session" || got.CWD != "/projects/only-session" {
			t.Fatalf("single-hop handoff behavior changed: ok=%v entry=%+v", ok, got)
		}
	})
}
