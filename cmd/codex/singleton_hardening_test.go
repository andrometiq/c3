//go:build !windows

package main

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestAppServerLog_IsPerPortAndPerUID mirrors
// TestAppServerMeta_IsPerPortAndNamesItsOwnHolder for the LOG beside the meta
// record. The meta path was moved to one-per-(uid,port); the log was left as a
// single hardcoded /tmp/c3-codex-app-server.log.
//
// The defect it guards: N app-servers are the NORMAL case (chooseAppServerURL
// walks to the next free port and ensureAppServer refuses to share a busy one),
// so one shared log interleaves every app-server's stdout+stderr with no port
// or pid on any line — it cannot answer "which app-server did what", which is
// the only question a cross-talk incident asks it. And without the uid
// component, user A's 0600 file makes user B's open fail, dropping B's
// app-server output into B's terminal, which the Codex TUI is about to take
// over.
func TestAppServerLog_IsPerPortAndPerUID(t *testing.T) {
	a, b := appServerLogPath(1455), appServerLogPath(1456)

	if a == b {
		t.Errorf("two app-servers on different ports share ONE log path (%s): their stdout+stderr interleave with no port marker, so the log cannot say which app-server wrote a line", a)
	}
	uid := strconv.Itoa(os.Getuid())
	for _, p := range []string{a, b} {
		if !strings.Contains(p, uid) {
			t.Errorf("app-server log path %q is not uid-namespaced (uid=%s): another user's 0600 file makes this open fail, and the app-server's output falls back into the user's terminal", p, uid)
		}
	}
	if a == appServerMetaPath(1455) {
		t.Errorf("app-server log and meta record resolve to the same path (%s)", a)
	}
}

// TestEnsureAppServer_OpensThePerPortLog pins the CALL SITE, not just the helper.
//
// Why this test exists: a mutation pass proved the test above is not enough. It
// exercises appServerLogPath in isolation, so reverting only the call site —
// leaving the helper byte-identical and simply opening the old hardcoded
// /tmp/c3-codex-app-server.log again — restored the entire defect with the suite
// green. That is the extracted-helper trap: the helper is tested, the behaviour
// is not, and the helper survives as dead code kept alive by its own test.
//
// ensureAppServer opens the log BEFORE exec.Start, so pointing it at a binary
// that cannot exist exercises the real open and then fails harmlessly — no
// app-server is ever spawned.
func TestEnsureAppServer_OpensThePerPortLog(t *testing.T) {
	port := freePort(t)
	path := appServerLogPath(port)
	_ = os.Remove(path)
	t.Cleanup(func() { _ = os.Remove(path) })

	err := ensureAppServer("/nonexistent/codex-binary", "/usr/local/bin/c3-codex-adapter",
		"ws://127.0.0.1:"+strconv.Itoa(port), "/tmp/x", "x")
	if err == nil {
		t.Fatal("spawning a nonexistent binary should have failed; this test relies on that")
	}

	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("ensureAppServer did not open the per-port log %s (%v) — it is opening some other path, "+
			"so every app-server on this machine still shares one log and appServerLogPath is dead code", path, statErr)
	}
}
