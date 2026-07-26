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

// These tests pin ONE rule, the one the recover path kept getting wrong:
// identity must be SETTLED before anything that depends on it is answered. An
// attach is answered FROM this session's identity, so it must not be answered
// while the recovery that establishes that identity is still in flight — and
// "in flight" is not what `recoverFired` records. recoverFired records ENTRY
// into fireRecover; reading entry as completion released identity-dependent
// calls early, and a fireRecover that entered and then returned without sending
// anything left it set forever, so every later attach was answered with no
// identity at all.

// recoveryBroker is the broker's side of the adapter's IPC pipe. A goroutine
// drains every frame the adapter writes into `frames`, so a test can assert both
// that a frame ARRIVES and — the half that matters here — that one does NOT.
type recoveryBroker struct {
	frames chan []byte
}

// newRecoveryBroker wires a to a fresh net.Pipe and starts reading its frames.
func newRecoveryBroker(t *testing.T, a *adapter) *recoveryBroker {
	t.Helper()
	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() { _ = adapterSide.Close(); _ = brokerSide.Close() })
	a.bmu.Lock()
	a.conn = ipc.NewConn(adapterSide)
	a.bmu.Unlock()

	b := &recoveryBroker{frames: make(chan []byte, 8)}
	peer := ipc.NewConn(brokerSide)
	go func() {
		for {
			raw, err := peer.ReadFrame()
			if err != nil {
				return
			}
			b.frames <- raw
		}
	}()
	return b
}

// nextOp returns the op of the next frame, or ("", false) if none arrives in d.
func (b *recoveryBroker) nextOp(t *testing.T, d time.Duration) (ipc.Op, bool) {
	t.Helper()
	select {
	case raw := <-b.frames:
		op, err := ipc.PeekOp(raw)
		if err != nil {
			t.Fatalf("unparseable frame %s: %v", raw, err)
		}
		return op, true
	case <-time.After(d):
		return "", false
	}
}

// answerRecover feeds the broker's RecoverSessionResp back the way brokerReader
// would, unblocking the in-flight fireRecover.
func answerRecover(t *testing.T, a *adapter) {
	t.Helper()
	raw, err := json.Marshal(ipc.RecoverSessionResp{Op: ipc.OpRecoverSessionResult, Recovered: false})
	if err != nil {
		t.Fatalf("marshal recover resp: %v", err)
	}
	a.dispatchRecoverSessionResult(raw)
}

func recoveryTestEntry() sessionhandoff.Entry {
	return sessionhandoff.Entry{StableSessionID: "stable-recovery", CWD: "/projects/c3"}
}

// writeRecoveryHandoff plants the SessionStart-hook handoff this adapter keys
// its recovery on, so a test can drive the REAL startup path rather than
// hand-setting the flags that path is supposed to set.
func writeRecoveryHandoff(t *testing.T) {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "inst-recovery")
	e := recoveryTestEntry()
	e.UnixNano = time.Now().UnixNano()
	if err := sessionhandoff.Write("inst-recovery", e); err != nil {
		t.Fatalf("write handoff: %v", err)
	}
}

// An attach must not be answered while this session is still finding out who it
// is. The recovery may be about to silently re-claim this session's OWN topic;
// an attach that overtakes it is answered by a broker that has not yet learned
// the stable session id, which is how the same instant produced two opposite
// answers — the channel said "attached", the CLI showed a topic picker.
func TestToolAttach_IsNotAnsweredBeforeThisSessionsIdentityIsSettled(t *testing.T) {
	writeRecoveryHandoff(t)
	a := newAdapter()
	broker := newRecoveryBroker(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Drive the REAL resume path: the background watch finds the handoff and
	// fires the recovery. It reaches the broker but is NOT answered, so this
	// session still does not know who it is.
	go a.recoverSessionOnResume(ctx)
	if op, ok := broker.nextOp(t, 5*time.Second); !ok || op != ipc.OpRecoverSession {
		t.Fatalf("recovery never reached the broker (op=%q ok=%v), so this test cannot prove anything", op, ok)
	}

	// Build the request on the test goroutine — newAttachReq can call t.Fatalf.
	attachReq := newAttachReq(t, map[string]any{"name": "some-topic"})
	attachDone := make(chan struct{})
	go func() {
		defer close(attachDone)
		_, _ = a.toolAttach(ctx, attachReq)
	}()

	if op, ok := broker.nextOp(t, 500*time.Millisecond); ok {
		t.Fatalf("a %q frame reached the broker while the session recovery was still in flight: the attach was "+
			"answered before this session knew its identity, so the broker answers it from no stable session id "+
			"(the picker) while the recovery re-claims this session's own topic behind it", op)
	}

	answerRecover(t, a) // identity settled — the attach may go out now
	op, ok := broker.nextOp(t, 5*time.Second)
	if !ok {
		t.Fatal("the attach never went out after identity settled — the identity gate is a deadlock, not an ordering")
	}
	if op != ipc.OpAttach {
		t.Fatalf("frame op = %q, want %q", op, ipc.OpAttach)
	}
	drivePendingAttached(t, a, ipc.AttachedMsg{Op: ipc.OpAttached, OK: false, Err: "test"})
	select {
	case <-attachDone:
	case <-time.After(5 * time.Second):
		t.Fatal("toolAttach never returned after its response arrived")
	}
}

// The first tools/call is held so the recover lands FIRST. It must be held until
// the recovery FINISHES, not until it STARTS: `recoverFired` is set on entry to
// fireRecover, so treating it as "already handled" releases the call while the
// identity question is still open.
func TestFirstToolsCall_IsHeldUntilRecoveryFinishesNotUntilItStarts(t *testing.T) {
	a := newAdapter()
	broker := newRecoveryBroker(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a.recoverStarted.Store(true)
	go a.fireRecover(ctx, recoveryTestEntry())
	if op, ok := broker.nextOp(t, 5*time.Second); !ok || op != ipc.OpRecoverSession {
		t.Fatalf("recovery never reached the broker (op=%q ok=%v), so this test cannot prove anything", op, ok)
	}

	released := make(chan struct{})
	go func() {
		defer close(released)
		a.recheckRecoverOnFirstActivity(ctx)
	}()
	select {
	case <-released:
		t.Fatal("the first tools/call was released while the recovery it depends on was still in flight: " +
			"`recoverFired` records ENTRY into the recovery, not its completion, so reading it as 'already " +
			"handled' answers the call before this session knows its identity")
	case <-time.After(500 * time.Millisecond):
	}

	answerRecover(t, a)
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("the first tools/call was never released after identity settled — the gate is a deadlock")
	}
}

// Same rule when the first tools/call is the one that STARTS the recovery (the
// handoff landed between watch polls). It fires the recover so the broker learns
// who this session is before the call it is holding reaches it — which is only
// true if it then waits for that recover to be answered.
func TestFirstToolsCall_HoldsTheCallForTheRecoveryItStartsItself(t *testing.T) {
	writeRecoveryHandoff(t)
	a := newAdapter()
	broker := newRecoveryBroker(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	released := make(chan struct{})
	go func() {
		defer close(released)
		a.recheckRecoverOnFirstActivity(ctx)
	}()
	if op, ok := broker.nextOp(t, 5*time.Second); !ok || op != ipc.OpRecoverSession {
		t.Fatalf("the first tools/call did not fire the recovery from the handoff (op=%q ok=%v)", op, ok)
	}
	select {
	case <-released:
		t.Fatal("the first tools/call fired the recovery and then released itself without waiting for the " +
			"answer: the call it was holding can still overtake the recover, and an `attach` answered there is " +
			"answered before this session knows its identity")
	case <-time.After(500 * time.Millisecond):
	}

	answerRecover(t, a)
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("the first tools/call was never released after identity settled — the gate is a deadlock")
	}
}

// THE STUCK STATE. fireRecover can take the once-per-connection guard and then
// send nothing at all (no broker connection, or a failed write). If the guard
// stays set, the broker never learns this session's stable id and no later
// attempt can ever tell it — every attach from that point on is recorded against
// nothing and answered with no identity. That is a permanently wedged session,
// not a race.
func TestFireRecover_ASendThatNeverHappenedMustNotWedgeTheSession(t *testing.T) {
	a := newAdapter() // deliberately NO broker connection
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a.recoverStarted.Store(true)
	a.fireRecover(ctx, recoveryTestEntry()) // returns at once: currentConn() == nil

	if a.recoverFired.Load() {
		t.Fatal("the recovery sent NOTHING (no broker connection) yet left the once-per-connection flag set: " +
			"every later attempt short-circuits on a recovery that never happened, the broker never learns this " +
			"session's stable id, and every subsequent attach is answered with no identity")
	}

	// Prove the consequence, not just the flag: a later attempt must actually
	// reach the broker once a connection exists.
	broker := newRecoveryBroker(t, a)
	go a.fireRecover(ctx, recoveryTestEntry())
	if op, ok := broker.nextOp(t, 5*time.Second); !ok || op != ipc.OpRecoverSession {
		t.Fatalf("the retry never reached the broker (op=%q ok=%v): this session can never be registered, so "+
			"every attach from here on is answered with no identity", op, ok)
	}
	answerRecover(t, a)
}

// A recovery that fails must still ANSWER the identity question. "This session
// could not be identified" is a settled answer; leaving the question open makes
// every identity-dependent call wait out the whole settle budget, and a hung
// attach is a worse failure than an unidentified one.
func TestFireRecover_SettlesIdentityEvenWhenItCannotSend(t *testing.T) {
	a := newAdapter() // no broker connection: fireRecover fails without sending
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a.recoverStarted.Store(true)
	a.fireRecover(ctx, recoveryTestEntry())

	settled := make(chan struct{})
	go func() {
		defer close(settled)
		a.awaitIdentitySettled(ctx)
	}()
	select {
	case <-settled:
	case <-time.After(2 * time.Second):
		t.Fatalf("a recovery that failed without sending never settled the identity question: every attach now "+
			"blocks for the full %v settle budget before proceeding anyway", recoverSettleBudget)
	}
}

// A session that never started a recovery has nothing to wait for. Waiting
// anyway would stall the attach of every fresh (non-resumed) session for the
// full settle budget on a question nobody is going to answer.
func TestAwaitIdentitySettled_DoesNotWaitWhenNoRecoveryWasEverStarted(t *testing.T) {
	a := newAdapter()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		a.awaitIdentitySettled(ctx)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("a session that never started a recovery still waited on the identity gate: every attach in a "+
			"fresh session would stall for up to %v on an answer that is never coming", recoverSettleBudget)
	}
}

// Giving up on a wedged recovery must ANSWER the identity question, not just
// release one caller. Otherwise the budget is paid again by every later
// identity-dependent call — the first tools/call waits it out, then the `attach`
// inside that same call waits it out a second time.
func TestAwaitIdentitySettled_GivingUpIsAnAnswerAndIsPaidOnlyOnce(t *testing.T) {
	prev := recoverSettleBudget
	recoverSettleBudget = 50 * time.Millisecond
	t.Cleanup(func() { recoverSettleBudget = prev })

	a := newAdapter()
	broker := newRecoveryBroker(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// A recovery that is never answered: the gate can only close by giving up.
	a.recoverStarted.Store(true)
	go a.fireRecover(ctx, recoveryTestEntry())
	if op, ok := broker.nextOp(t, 5*time.Second); !ok || op != ipc.OpRecoverSession {
		t.Fatalf("recovery never reached the broker (op=%q ok=%v)", op, ok)
	}

	a.awaitIdentitySettled(ctx) // first caller pays the budget

	start := time.Now()
	a.awaitIdentitySettled(ctx) // second caller must not pay it again
	if waited := time.Since(start); waited > 20*time.Millisecond {
		t.Fatalf("the second identity-dependent call waited %v after the first had already given up: one wedged "+
			"recovery costs one budget per CALLER instead of one per session, so a first tools/call that is "+
			"itself an attach stalls for two full budgets", waited)
	}
}
