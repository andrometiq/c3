package broker

import (
	"strings"
	"testing"
)

// FIX 2 (2026-06-04): /c3:ping (and /c3:sessions IsThisSession) must match
// the calling session when req.PID is the CLI pid but the stub registered
// under its ADAPTER pid. The Claude adapter registers stub.PID = its own pid
// (e.g. 9823, comm "c3-claude-adapt"); the caller's bestEffortCallerPID walk
// resolves the CLAUDE pid (e.g. 9801, the adapter's parent). req.PID(9801) !=
// stub.PID(9823) so the old direct-PID match silently failed → "not attached".
//
// The fix: the broker treats a stub as matching req.PID when
//   req.PID == stub.PID  OR  req.PID == CLISessionPID(stub.PID)
// where CLISessionPID walks up from the adapter pid (strict predicate) to the
// real claude ancestor. These tests inject the resolver so no real /proc is
// needed.

// TestPing_MatchesByCLIAncestorOfStubPID is the core regression. Stub
// registered under adapter pid 9823; the injected resolver maps 9823→9801
// (claude). Ping carries req.PID=9801 → must target the stub's route.
//
// RED on today's code: 9801 != 9823 and there is no CLI-ancestor rule.
func TestPing_MatchesByCLIAncestorOfStubPID(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	// Inject a synthetic resolver: the adapter pid 9823 resolves to claude 9801.
	b.sessionPIDResolver = func(pid int) int {
		if pid == 9823 {
			return 9801
		}
		return 0
	}

	// Stub registered under the ADAPTER pid (9823), claims feature-x.
	adapter := b.Stubs.Register("claude", 9823, "/p", nil)
	tid := int64(412)
	r := MakeRouteKey("telegram", -200, &tid)
	if !b.tryClaim(nil, adapter, r, "feature-x", false, false) {
		t.Fatal("adapter stub: claim failed")
	}
	waitForReplies(fc, 1)
	beforePing := len(fc.sendRepliesSnapshot())

	// Ping carries the CLAUDE pid (9801) — the adapter's parent.
	resp := pingOverIPC(t, b, 9801, "/elsewhere")
	if !resp.OK {
		t.Fatalf("ping reply not OK: %q (CLI-ancestor match must bridge claude→adapter)", resp.Err)
	}
	if resp.Topic != "feature-x" {
		t.Errorf("CLI-ancestor match targeted wrong stub: topic=%q, want %q", resp.Topic, "feature-x")
	}
	calls := fc.sendRepliesSnapshot()
	if len(calls) != beforePing+1 {
		t.Fatalf("expected exactly one ping SendReply (was %d, now %d)", beforePing, len(calls))
	}
	last := calls[len(calls)-1]
	if last.ChatID != -200 || last.TopicID == nil || *last.TopicID != 412 {
		t.Errorf("ping went to wrong destination: chat=%d topic=%v; want chat=-200 topic=412", last.ChatID, last.TopicID)
	}
}

// TestPing_DirectPIDMatchStillWorks: back-compat. A stub registered directly
// under the CLI pid (9801) must still match req.PID=9801 even though the
// resolver would map 9801→0 (a real claude pid has no claude ANCESTOR in this
// synthetic tree). The direct-equality arm must hold.
func TestPing_DirectPIDMatchStillWorks(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	// Resolver maps the CLI pid to 0 (no further CLI ancestor) — so the match
	// must come from the direct-equality arm, not the ancestor arm.
	b.sessionPIDResolver = func(pid int) int { return 0 }

	stub := b.Stubs.Register("claude", 9801, "/p", nil)
	tid := int64(281)
	r := MakeRouteKey("telegram", -100, &tid)
	if !b.tryClaim(nil, stub, r, "c3", false, false) {
		t.Fatal("stub: claim failed")
	}
	waitForReplies(fc, 1)
	beforePing := len(fc.sendRepliesSnapshot())

	resp := pingOverIPC(t, b, 9801, "/elsewhere")
	if !resp.OK {
		t.Fatalf("direct PID match must still work: %q", resp.Err)
	}
	if resp.Topic != "c3" {
		t.Errorf("direct PID match targeted wrong stub: topic=%q, want %q", resp.Topic, "c3")
	}
	if got := len(fc.sendRepliesSnapshot()); got != beforePing+1 {
		t.Fatalf("expected exactly one ping SendReply, got %d extra", got-beforePing)
	}
}

// TestPing_NoPIDMatch_FallsBackToCWD: req.PID is set but matches NO stub by
// pid OR CLI-ancestor; a stub at the caller's CWD must still be targeted via
// the tertiary CWD fallback (robustness add — a failed walk still has a
// chance).
func TestPing_NoPIDMatch_FallsBackToCWD(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	// Resolver never bridges anything.
	b.sessionPIDResolver = func(pid int) int { return 0 }

	// Stub at CWD "/p" registered under a pid that the ping won't carry.
	stub := b.Stubs.Register("claude", 100, "/p", nil)
	tid := int64(281)
	r := MakeRouteKey("telegram", -100, &tid)
	if !b.tryClaim(nil, stub, r, "c3", false, false) {
		t.Fatal("stub: claim failed")
	}
	waitForReplies(fc, 1)
	beforePing := len(fc.sendRepliesSnapshot())

	// req.PID=7777 matches nothing by pid/ancestor; CWD "/p" matches the stub.
	resp := pingOverIPC(t, b, 7777, "/p")
	if !resp.OK {
		t.Fatalf("CWD tertiary fallback must rescue the ping: %q", resp.Err)
	}
	if resp.Topic != "c3" {
		t.Errorf("CWD fallback targeted wrong stub: topic=%q, want %q", resp.Topic, "c3")
	}
	if got := len(fc.sendRepliesSnapshot()); got != beforePing+1 {
		t.Fatalf("expected exactly one ping SendReply, got %d extra", got-beforePing)
	}
}

// TestPing_NoPIDMatch_NoCWDMatch_NotAttached: req.PID set, nothing matches by
// pid/ancestor, AND no stub shares the caller's CWD → "not attached".
func TestPing_NoPIDMatch_NoCWDMatch_NotAttached(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	b.sessionPIDResolver = func(pid int) int { return 0 }

	stub := b.Stubs.Register("claude", 100, "/somewhere-else", nil)
	tid := int64(281)
	r := MakeRouteKey("telegram", -100, &tid)
	if !b.tryClaim(nil, stub, r, "c3", false, false) {
		t.Fatal("stub: claim failed")
	}
	waitForReplies(fc, 1)
	beforePing := len(fc.sendRepliesSnapshot())

	resp := pingOverIPC(t, b, 7777, "/p")
	if resp.OK {
		t.Fatalf("ping should be 'not attached' when nothing matches, got OK=true: %+v", resp)
	}
	if !strings.Contains(strings.ToLower(resp.Err), "not attached") {
		t.Errorf("Err should mention 'not attached', got %q", resp.Err)
	}
	if got := len(fc.sendRepliesSnapshot()); got != beforePing {
		t.Errorf("ping must not SendReply on the unattached path, got %d extra", got-beforePing)
	}
}

// TestPing_CWDFallback_RefusesTwoEmptyCWDs pins the identity guard on the
// tertiary match. An empty path is not evidence that a transient caller and an
// attached session are the same session; os.Getwd can fail independently in
// both processes.
func TestPing_CWDFallback_RefusesTwoEmptyCWDs(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	stub := b.Stubs.Register("claude", 100, "", nil)
	tid := int64(281)
	r := MakeRouteKey("telegram", -100, &tid)
	if !b.tryClaim(nil, stub, r, "c3", false, false) {
		t.Fatal("stub: claim failed")
	}
	waitForReplies(fc, 1)
	beforePing := len(fc.sendRepliesSnapshot())

	resp := pingOverIPC(t, b, 0, "")
	if resp.OK {
		t.Fatalf("two empty CWDs compared equal in the ping fallback, targeting an unrelated session: %+v", resp)
	}
	if got := len(fc.sendRepliesSnapshot()); got != beforePing {
		t.Fatalf("the empty-CWD fallback sent to an unrelated session: replies before=%d after=%d", beforePing, got)
	}
}

// TestPing_CWDFallback_RefusesIncompleteStubIdentity ensures that a path alone
// cannot make an anonymous adapter the user's session. A fallback target still
// has to have the CLI+PID identity required everywhere else claims persist.
func TestPing_CWDFallback_RefusesIncompleteStubIdentity(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	stub := b.Stubs.Register("", 0, "/shared", nil)
	tid := int64(281)
	r := MakeRouteKey("telegram", -100, &tid)
	if !b.tryClaim(nil, stub, r, "c3", false, false) {
		t.Fatal("stub: claim failed")
	}
	waitForReplies(fc, 1)
	beforePing := len(fc.sendRepliesSnapshot())

	resp := pingOverIPC(t, b, 0, "/shared")
	if resp.OK {
		t.Fatalf("a matching CWD promoted an incomplete stub identity into the caller's session: %+v", resp)
	}
	if got := len(fc.sendRepliesSnapshot()); got != beforePing {
		t.Fatalf("the CWD fallback sent to an anonymous adapter: replies before=%d after=%d", beforePing, got)
	}
}

// TestListSessions_MarksThisSession_ByCLIAncestor: the same pid-split breaks
// /c3:sessions IsThisSession. A stub under adapter pid 9823 must be marked
// IsThisSession when the caller's req.PID is the resolved claude pid 9801.
func TestListSessions_MarksThisSession_ByCLIAncestor(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	b.sessionPIDResolver = func(pid int) int {
		if pid == 9823 {
			return 9801
		}
		return 0
	}

	b.Stubs.Register("claude", 9823, "/p/one", nil) // adapter pid
	b.Stubs.Register("claude", 5151, "/p/two", nil) // unrelated

	pinger, done := peerPair(t, b)
	defer done()
	pingerHelloAck(t, pinger, "c3-broker-cli", 9999, "/wherever")

	resp := askSessions(t, pinger, 9801, "/wherever") // caller resolved claude pid
	var thisCount int
	for _, e := range resp.Sessions {
		if e.IsThisSession {
			thisCount++
			if e.PID != 9823 {
				t.Errorf("IsThisSession marked wrong stub: PID=%d, want 9823 (adapter via CLI-ancestor)", e.PID)
			}
		}
	}
	if thisCount != 1 {
		t.Errorf("expected exactly one IsThisSession=true, got %d", thisCount)
	}
}
