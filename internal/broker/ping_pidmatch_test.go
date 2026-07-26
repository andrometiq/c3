package broker

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

// FIX 1 (2026-06-03): /c3:ping must match the calling session by PID
// (primary), falling back to CWD only when the PID hint is absent. The
// slash command runs from a project subdir while the adapter stub stored
// the parent launch dir, so exact-CWD matching can never bridge that gap
// (see internal/broker/handler.go handlePingThisSession). PID is the
// stable identity that survives the CWD collapse. Mirrors the already
// shipped /c3:sessions PID-tagging convention.

// pingOverIPC fires a PingThisSessionReq with the given PID + CWD through
// a transient peer and returns the broker's reply. Mirrors the IPC dance
// in the existing ping tests.
func pingOverIPC(t *testing.T, b *Broker, pid int, cwd string) ipc.PingThisSessionReplyMsg {
	t.Helper()
	pinger, done := peerPair(t, b)
	t.Cleanup(done)
	if err := pinger.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "c3-broker-cli", PID: 9999, CWD: cwd}); err != nil {
		t.Fatal(err)
	}
	if _, err := pinger.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if err := pinger.WriteJSON(ipc.PingThisSessionReq{Op: ipc.OpPingThisSession, PID: pid, CWD: cwd}); err != nil {
		t.Fatal(err)
	}
	raw, err := pinger.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.PingThisSessionReplyMsg
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("parse ping reply: %v", err)
	}
	return resp
}

// waitForReplies blocks (briefly) until the fake channel has at least n
// SendReply calls, so async welcomes don't race the ping assertion.
func waitForReplies(fc *fakeChannel, n int) {
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(fc.sendRepliesSnapshot()) >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestPing_PIDMatch_PrefersChildStubOverParentCWD is the core FIX 1 test.
// Two stubs: a parent at CWD "/p" (PID 100, route R1=c3) and a child at
// CWD "/p/sub" (PID 200, route R2=feature-x). The ping carries PID=200
// (the child) but CWD="/p" (the parent — exactly the launch-dir collapse).
// PID-primary matching must target R2 (the child's claim), not R1.
//
// Today: CWD-only matching picks the "/p" stub (R1) → this is RED.
func TestPing_PIDMatch_PrefersChildStubOverParentCWD(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	// Parent stub at "/p", PID 100 → claims R1 (topic 281 "c3", chat -100).
	parent := b.Stubs.Register("claude", 100, "/p", nil)
	tid1 := int64(281)
	r1 := MakeRouteKey("telegram", -100, &tid1)
	if !b.tryClaim(nil, parent, r1, "c3", false, false) {
		t.Fatal("parent stub: claim failed")
	}

	// Child stub at "/p/sub", PID 200 → claims R2 (topic 412 "feature-x", chat -200).
	child := b.Stubs.Register("claude", 200, "/p/sub", nil)
	tid2 := int64(412)
	r2 := MakeRouteKey("telegram", -200, &tid2)
	if !b.tryClaim(nil, child, r2, "feature-x", false, false) {
		t.Fatal("child stub: claim failed")
	}

	waitForReplies(fc, 2) // let both welcomes land
	beforePing := len(fc.sendRepliesSnapshot())

	// Ping: PID is the CHILD (200), CWD is the PARENT ("/p").
	resp := pingOverIPC(t, b, 200, "/p")
	if !resp.OK {
		t.Fatalf("ping reply not OK: %q", resp.Err)
	}
	if resp.Topic != "feature-x" {
		t.Errorf("PID-match targeted wrong stub: topic=%q, want %q (child via PID)", resp.Topic, "feature-x")
	}

	calls := fc.sendRepliesSnapshot()
	if len(calls) != beforePing+1 {
		t.Fatalf("expected exactly one ping SendReply (was %d, now %d)", beforePing, len(calls))
	}
	r := calls[len(calls)-1]
	if r.ChatID != -200 || r.TopicID == nil || *r.TopicID != 412 {
		t.Errorf("ping went to parent's destination: chat=%d topic=%v; want chat=-200 topic=412", r.ChatID, r.TopicID)
	}
}

// TestPing_PIDZero_FallsBackToCWDMatch guards the fallback: when the PID
// hint is absent (PPID walk failed → PID==0), the broker must fall back to
// CWD matching and target the parent stub at "/p" (R1=c3).
func TestPing_PIDZero_FallsBackToCWDMatch(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	parent := b.Stubs.Register("claude", 100, "/p", nil)
	tid1 := int64(281)
	r1 := MakeRouteKey("telegram", -100, &tid1)
	if !b.tryClaim(nil, parent, r1, "c3", false, false) {
		t.Fatal("parent stub: claim failed")
	}

	child := b.Stubs.Register("claude", 200, "/p/sub", nil)
	tid2 := int64(412)
	r2 := MakeRouteKey("telegram", -200, &tid2)
	if !b.tryClaim(nil, child, r2, "feature-x", false, false) {
		t.Fatal("child stub: claim failed")
	}

	waitForReplies(fc, 2)
	beforePing := len(fc.sendRepliesSnapshot())

	// PID=0 (walk failed) + CWD="/p" → CWD fallback targets the parent (R1).
	resp := pingOverIPC(t, b, 0, "/p")
	if !resp.OK {
		t.Fatalf("ping reply not OK: %q", resp.Err)
	}
	if resp.Topic != "c3" {
		t.Errorf("CWD fallback targeted wrong stub: topic=%q, want %q (parent via CWD)", resp.Topic, "c3")
	}

	calls := fc.sendRepliesSnapshot()
	if len(calls) != beforePing+1 {
		t.Fatalf("expected exactly one ping SendReply (was %d, now %d)", beforePing, len(calls))
	}
	r := calls[len(calls)-1]
	if r.ChatID != -100 || r.TopicID == nil || *r.TopicID != 281 {
		t.Errorf("CWD fallback went to wrong destination: chat=%d topic=%v; want chat=-100 topic=281", r.ChatID, r.TopicID)
	}
}

// TestPing_PIDMatch_NoLiveStubReturnsNotAttached covers a PID that matches
// no live attached stub: the broker must reply OK=false / "not attached"
// and NOT SendReply. A non-matching PID must not silently fall through to a
// CWD match against an unrelated stub.
func TestPing_PIDMatch_NoLiveStubReturnsNotAttached(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	// One attached stub at a DIFFERENT cwd so a stray CWD fallback can't
	// rescue the ping; PID 999 matches nothing.
	other := b.Stubs.Register("claude", 100, "/elsewhere", nil)
	tid := int64(281)
	key := MakeRouteKey("telegram", -100, &tid)
	if !b.tryClaim(nil, other, key, "c3", false, false) {
		t.Fatal("stub: claim failed")
	}
	waitForReplies(fc, 1)
	beforePing := len(fc.sendRepliesSnapshot())

	resp := pingOverIPC(t, b, 999, "/p")
	if resp.OK {
		t.Fatalf("ping should fail when PID matches no live stub, got OK=true: %+v", resp)
	}
	if !strings.Contains(strings.ToLower(resp.Err), "not attached") {
		t.Errorf("ping Err should mention 'not attached', got %q", resp.Err)
	}
	if got := len(fc.sendRepliesSnapshot()); got != beforePing {
		t.Errorf("ping should not SendReply on the unattached path, got %d extra calls", got-beforePing)
	}
}

// TestPing_PIDMatch_TieHighestConnIDWins preserves the determinism
// guarantee in the PID-match phase: if two live stubs share a PID (e.g. a
// reconnect re-registered the same logical session under a new ConnID
// before the old stub was reaped), the highest-ConnID stub wins — the same
// tiebreak /c3:sessions and the 2026-05-19 m1 fix established for the
// CWD phase.
func TestPing_PIDMatch_TieHighestConnIDWins(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	const pid = 500

	// Older stub (same PID) → claims topic 281 ("c3").
	older := b.Stubs.Register("claude", pid, "/p", nil)
	tid1 := int64(281)
	r1 := MakeRouteKey("telegram", -100, &tid1)
	if !b.tryClaim(nil, older, r1, "c3", false, false) {
		t.Fatal("older stub: claim failed")
	}

	// Newer stub, SAME PID, higher ConnID → claims topic 412 ("feature-x").
	newer := b.Stubs.Register("claude", pid, "/p", nil)
	tid2 := int64(412)
	r2 := MakeRouteKey("telegram", -200, &tid2)
	if !b.tryClaim(nil, newer, r2, "feature-x", false, false) {
		t.Fatal("newer stub: claim failed")
	}
	if newer.ConnID <= older.ConnID {
		t.Fatalf("test setup wrong: newer.ConnID=%d not > older.ConnID=%d", newer.ConnID, older.ConnID)
	}

	waitForReplies(fc, 2)
	beforePing := len(fc.sendRepliesSnapshot())

	resp := pingOverIPC(t, b, pid, "/p")
	if !resp.OK {
		t.Fatalf("ping reply not OK: %q", resp.Err)
	}
	if resp.Topic != "feature-x" {
		t.Errorf("PID tie: targeted wrong stub: topic=%q, want %q (highest ConnID)", resp.Topic, "feature-x")
	}

	calls := fc.sendRepliesSnapshot()
	if len(calls) != beforePing+1 {
		t.Fatalf("expected exactly one ping SendReply (was %d, now %d)", beforePing, len(calls))
	}
	r := calls[len(calls)-1]
	if r.ChatID != -200 || r.TopicID == nil || *r.TopicID != 412 {
		t.Errorf("PID tie went to older stub's destination: chat=%d topic=%v; want chat=-200 topic=412", r.ChatID, r.TopicID)
	}
}
