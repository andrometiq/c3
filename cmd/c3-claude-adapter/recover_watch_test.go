package main

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/sessionhandoff"
)

// TestWatchForHandoff_FiresOnLateArrival covers BUG #1: the SessionStart hook
// writes the handoff with UNBOUNDED latency on a busy machine (measured +11s and
// +91s on real resumes), past the old fixed ~10s blocking window that returned
// silently and lost the recovery. The background watch must keep looking and
// surface a handoff that lands AFTER the watcher started polling.
func TestWatchForHandoff_FiresOnLateArrival(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	const inst = "inst-late"

	a := newAdapter()
	type result struct {
		entry sessionhandoff.Entry
		ok    bool
	}
	resCh := make(chan result, 1)
	go func() {
		e, ok := a.watchForHandoff(context.Background(), inst, 5*time.Millisecond, 2*time.Second)
		resCh <- result{e, ok}
	}()

	// Land the handoff only AFTER the watcher has already been polling for a while
	// — the old bounded poll would have given up. (Production budget is 5 min; the
	// short test budget keeps this fast.)
	time.Sleep(150 * time.Millisecond)
	if err := sessionhandoff.Write(inst, sessionhandoff.Entry{
		StableSessionID: "stable-xyz", CWD: "/projects/c3", UnixNano: time.Now().UnixNano(),
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case r := <-resCh:
		if !r.ok || r.entry.StableSessionID != "stable-xyz" {
			t.Fatalf("late handoff not surfaced: ok=%v entry=%+v", r.ok, r.entry)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watchForHandoff did not surface the late handoff")
	}
}

// TestWatchForHandoff_ExpiresCleanlyWhenNoHandoff covers the non-resume case: a
// session that never gets a handoff (the hook never ran / not a resume) must let
// the watch expire cleanly within its budget — no fire, no hang, no harm.
func TestWatchForHandoff_ExpiresCleanlyWhenNoHandoff(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	a := newAdapter()

	start := time.Now()
	_, ok := a.watchForHandoff(context.Background(), "never", 5*time.Millisecond, 60*time.Millisecond)
	if ok {
		t.Fatal("watchForHandoff must not fire when no handoff ever appears")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("watch should expire promptly within its budget; took %s", elapsed)
	}
}

// TestRecoverWatchBudget_OutlastsOldWindow documents the BUG #1 intent: the watch
// must comfortably outlast the old ~10s fixed window that silently missed the
// late hook writes.
func TestRecoverWatchBudget_OutlastsOldWindow(t *testing.T) {
	if recoverWatchBudget <= 10*time.Second {
		t.Fatalf("recoverWatchBudget=%s must outlast the old ~10s fixed window", recoverWatchBudget)
	}
}

// waitLastAttach polls a.lastAttach until fireRecover (which runs in a goroutine)
// has recorded the remembered request, or fails after a short budget.
func waitLastAttach(t *testing.T, a *adapter) ipc.AttachReq {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		a.amu.Lock()
		la := a.lastAttach
		a.amu.Unlock()
		if la != nil {
			return *la
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("fireRecover did not remember an attach within 2s")
	return ipc.AttachReq{}
}

// TestFireRecover_RemembersDMByTargetNotName (item B): a DM auto-resume must be
// remembered as {Target:"dm"}, NOT {Name:"dm"} — a replayed attach(name="dm")
// onto a fresh broker can silently bind a TOPIC literally named "dm" (which is why
// the disambiguate_dm flow exists). resp.TopicID==nil is the DM signal.
func TestFireRecover_RemembersDMByTargetNotName(t *testing.T) {
	a := newAdapter()
	pipeA, pipeB := net.Pipe()
	defer pipeA.Close()
	defer pipeB.Close()
	a.bmu.Lock()
	a.conn = ipc.NewConn(pipeA)
	a.bmu.Unlock()
	peer := ipc.NewConn(pipeB)

	entry := sessionhandoff.Entry{StableSessionID: "stable-dm", CWD: "/projects/c3"}
	go a.fireRecover(context.Background(), entry)

	if _, err := peer.ReadFrame(); err != nil { // drain the RecoverSessionReq
		t.Fatalf("read recover req: %v", err)
	}
	respRaw, _ := json.Marshal(ipc.RecoverSessionResp{
		Op: ipc.OpRecoverSessionResult, Recovered: true,
		Channel: "telegram", ChatID: 42, Name: "dm", TopicID: nil,
	})
	a.dispatchRecoverSessionResult(respRaw)

	req := waitLastAttach(t, a)
	if req.Target != "dm" || req.Name != "" || req.TopicID != nil {
		t.Fatalf("DM recover must be remembered as {Target:dm}, got %+v (a name=\"dm\" replay could bind a topic named dm)", req)
	}
	if req.CWD != "/projects/c3" {
		t.Errorf("remembered CWD = %q, want /projects/c3", req.CWD)
	}
}

// TestFireRecover_RemembersTopicById (item B/C): a topic auto-resume is remembered
// id-addressed ({TopicID, Group}), not by {Name, Group}, so a fresh-broker replay
// re-claims it via attachByTopicID even across groups.
func TestFireRecover_RemembersTopicById(t *testing.T) {
	a := newAdapter()
	pipeA, pipeB := net.Pipe()
	defer pipeA.Close()
	defer pipeB.Close()
	a.bmu.Lock()
	a.conn = ipc.NewConn(pipeA)
	a.bmu.Unlock()
	peer := ipc.NewConn(pipeB)

	entry := sessionhandoff.Entry{StableSessionID: "stable-topic", CWD: "/projects/c3"}
	go a.fireRecover(context.Background(), entry)

	if _, err := peer.ReadFrame(); err != nil {
		t.Fatalf("read recover req: %v", err)
	}
	tid := int64(412)
	respRaw, _ := json.Marshal(ipc.RecoverSessionResp{
		Op: ipc.OpRecoverSessionResult, Recovered: true,
		Channel: "telegram", ChatID: -200, TopicID: &tid, Name: "feature-x", Group: "work",
	})
	a.dispatchRecoverSessionResult(respRaw)

	req := waitLastAttach(t, a)
	if req.TopicID == nil || *req.TopicID != 412 || req.Group != "work" || req.ChatID != -200 || req.Name != "" || req.Target != "" {
		t.Fatalf("topic recover must be remembered id-addressed {TopicID:412 Group:work ChatID:-200}, got %+v", req)
	}
}

// TestFireRecover_SendsOnceIdempotent covers the safety requirement: the
// background watch and the first-tools/call belt-and-suspenders recheck both call
// fireRecover, but the broker must never see a duplicate RecoverSessionReq for
// the same session.
func TestFireRecover_SendsOnceIdempotent(t *testing.T) {
	a := newAdapter()
	pipeA, pipeB := net.Pipe()
	defer pipeA.Close()
	defer pipeB.Close()
	a.bmu.Lock()
	a.conn = ipc.NewConn(pipeA)
	a.bmu.Unlock()
	peer := ipc.NewConn(pipeB)

	entry := sessionhandoff.Entry{StableSessionID: "stable-1", CWD: "/projects/c3"}

	// First fire: a RecoverSessionReq must be written. Run in a goroutine because
	// fireRecover blocks on the broker response.
	go a.fireRecover(context.Background(), entry)

	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("read first recover frame: %v", err)
	}
	op, err := ipc.PeekOp(raw)
	if err != nil || op != ipc.OpRecoverSession {
		t.Fatalf("first frame op=%q err=%v, want %q", op, err, ipc.OpRecoverSession)
	}
	var req ipc.RecoverSessionReq
	if err := json.Unmarshal(raw, &req); err != nil || req.StableSessionID != "stable-1" {
		t.Fatalf("recover req malformed: %+v err=%v", req, err)
	}
	// Unblock the first fire with a (non-recovered) response so it returns.
	respRaw, _ := json.Marshal(ipc.RecoverSessionResp{Op: ipc.OpRecoverSessionResult, Recovered: false})
	a.dispatchRecoverSessionResult(respRaw)

	// Second fire (e.g. the belt-and-suspenders recheck): must be a no-op — no
	// second frame on the wire.
	a.fireRecover(context.Background(), entry)
	_ = pipeB.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if _, err := peer.ReadFrame(); err == nil {
		t.Fatal("fireRecover must not send a second RecoverSessionReq (idempotent)")
	}
}
