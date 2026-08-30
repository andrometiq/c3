package broker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// FIX 2 (2026-06-03): the on-attach welcome must render the RESOLVED
// project dir, not the parent launch dir. persistMapping already refines
// launchCWD → launchCWD/<topicName> via resolveAttachCWD when that subdir
// exists, but sendWelcome rendered the raw stub.CWD — so the saved mapping
// and the displayed welcome disagreed. sendWelcome now resolves the same
// way and threads the result into welcomeText as resolvedCWD.

// TestWelcomeText_RendersResolvedCWD is the core FIX 2 test. A session
// launched in the parent "/home/u/projects" attaches to topic
// "widget" whose project lives at "/home/u/projects/widget". The
// welcome must show the resolved sub-path, not the bare parent.
func TestWelcomeText_RendersResolvedCWD(t *testing.T) {
	stub := &Stub{CLI: "claude", CWD: "/home/u/projects"}
	got := welcomeText(stub, "widget", "/home/u/projects/widget")
	if !strings.Contains(got, "projects/widget") {
		t.Errorf("welcome should render resolved dir, got %q", got)
	}
}

// TestWelcomeText_EmptyResolvedFallsBackToStubCWD guards the DM / no-refine
// case: when resolvedCWD is "" the welcome must fall back to stub.CWD
// (home-shortened) exactly as before — no regression.
func TestWelcomeText_EmptyResolvedFallsBackToStubCWD(t *testing.T) {
	stub := &Stub{CLI: "claude", CWD: "/tmp/proj"}
	got := welcomeText(stub, "label", "")
	if !strings.Contains(got, "/tmp/proj") {
		t.Errorf("empty resolvedCWD should fall back to stub.CWD, got %q", got)
	}
}

// TestWelcomeText_NoCWD_NoResolved_StillFriendly guards the genuine DM
// case (stub.CWD=="" AND resolvedCWD==""): the cwd-less friendly branch
// must still fire (no directory line, but cli + label present).
func TestWelcomeText_NoCWD_NoResolved_StillFriendly(t *testing.T) {
	stub := &Stub{CLI: "claude"}
	got := welcomeText(stub, "dm", "")
	if got == "" {
		t.Fatal("cwd-less welcome should still return something")
	}
	if !strings.Contains(got, "claude") || !strings.Contains(got, "dm") {
		t.Errorf("cwd-less welcome: %q", got)
	}
}

// TestSendWelcome_RendersResolvedSubdir is the integration-level guard:
// a stub launched in a parent dir attaching to a topic named after an
// existing subdir must get a welcome whose directory line shows the
// resolved subdir. Uses a real on-disk temp tree so resolveAttachCWD
// actually refines downward.
func TestSendWelcome_RendersResolvedSubdir(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	// Real parent dir with a "c3" subdir so resolveAttachCWD refines to it.
	parent := t.TempDir()
	sub := filepath.Join(parent, "c3")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	stub := &Stub{CLI: "claude", PID: 7, CWD: parent}
	tid := int64(281)
	key := MakeRouteKey("telegram", -100, &tid)
	if !b.tryClaim(nil, stub, key, "c3", false, false, false) {
		t.Fatal("claim failed")
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(fc.sendRepliesSnapshot()) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	calls := fc.sendRepliesSnapshot()
	if len(calls) != 1 {
		t.Fatalf("want 1 welcome SendReply, got %d", len(calls))
	}
	// The resolved subdir ends in "/c3"; the raw parent does not.
	if !strings.HasSuffix(strings.TrimRight(extractDirLine(calls[0].Text), "`"), "/c3") {
		t.Errorf("welcome should render resolved subdir ending in /c3, got %q", calls[0].Text)
	}
}

// extractDirLine pulls the backtick-wrapped directory token out of the
// welcome text so the assertion targets the rendered dir specifically.
func extractDirLine(text string) string {
	const marker = "📁 `"
	i := strings.Index(text, marker)
	if i < 0 {
		return text
	}
	rest := text[i+len(marker):]
	if j := strings.Index(rest, "`"); j >= 0 {
		return rest[:j]
	}
	return rest
}
