package main

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/ipc"
)

// These tests pin ONE rule, the one the recover path kept getting wrong:
// identity must be SETTLED before anything that depends on it is answered. An
// attach is answered FROM this session's identity, so it must not be answered
// while the recovery that establishes that identity is still in flight — and
// "in flight" is not what `recoverFired` records. recoverFired records ENTRY
// into fireRecover; toolAttach called fireRecover and went straight on to write
// the attach, and a fireRecover that entered and then returned without sending
// anything left the flag set forever, so every later attach was answered with no
// identity at all.

const recoveryTestSID = "conv-recovery"

// recoveryBroker drains every frame the adapter writes, so a test can assert
// both that a frame ARRIVES and — the half that matters here — that one does NOT.
type recoveryBroker struct {
	frames chan []byte
}

// newRecoveryAdapter builds an adapter wired to a broker pipe whose frames the
// returned recoveryBroker reads, with a deterministic conversation id.
func newRecoveryAdapter(t *testing.T) (*adapter, *recoveryBroker) {
	t.Helper()
	t.Setenv("ANTIGRAVITY_CONVERSATION_ID", recoveryTestSID)
	t.Setenv("C3_AGY_CWD", t.TempDir())
	a := newAdapter()

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
	return a, b
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

// newRecoveryAttachReq builds the attach tool call.
func newRecoveryAttachReq(t *testing.T, args map[string]any) *mcp.CallToolRequest {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatalf("marshal args: %v", err)
	}
	return &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "attach", Arguments: raw}}
}

// driveAttached answers the adapter's pending attach so toolAttach unwinds.
func driveAttached(t *testing.T, a *adapter) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		a.pmu.Lock()
		ch, ok := a.pending["attached"]
		if ok {
			delete(a.pending, "attached")
			a.pmu.Unlock()
			ch <- ipc.ToolResultMsg{Result: map[string]any{
				"_attached": ipc.AttachedMsg{Op: ipc.OpAttached, OK: false, Err: "test"},
			}}
			return
		}
		a.pmu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("pending['attached'] never registered")
}

// An attach must not be answered while this session is still finding out who it
// is. The recovery may be about to silently re-claim this session's OWN topic;
// an attach that overtakes it is answered by a broker that has not yet learned
// the stable session id.
func TestToolAttach_IsNotAnsweredBeforeThisSessionsIdentityIsSettled(t *testing.T) {
	a, broker := newRecoveryAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Drive the REAL startup path: trySessionRecover reads the conversation id
	// and fires. The recovery reaches the broker but is NOT answered, so this
	// session still does not know who it is.
	go a.trySessionRecover(ctx)
	if op, ok := broker.nextOp(t, 5*time.Second); !ok || op != ipc.OpRecoverSession {
		t.Fatalf("recovery never reached the broker (op=%q ok=%v), so this test cannot prove anything", op, ok)
	}

	// Build the request on the test goroutine — the helper can call t.Fatalf.
	attachReq := newRecoveryAttachReq(t, map[string]any{"name": "some-topic"})
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
	driveAttached(t, a)
	select {
	case <-attachDone:
	case <-time.After(5 * time.Second):
		t.Fatal("toolAttach never returned after its response arrived")
	}
}

// THE STUCK STATE. fireRecover can take the once-per-connection guard and then
// send nothing at all (no broker connection, or a failed write). If the guard
// stays set, the broker never learns this session's stable id and no later
// attempt can ever tell it — every attach from that point on is recorded against
// nothing and answered with no identity. That is a permanently wedged session,
// not a race.
func TestFireRecover_ASendThatNeverHappenedMustNotWedgeTheSession(t *testing.T) {
	a, broker := newRecoveryAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a.bmu.Lock()
	live := a.conn
	a.conn = nil // the broker connection is down at the moment of the attempt
	a.bmu.Unlock()

	a.recoverStarted.Store(true)
	a.fireRecover(ctx, recoveryTestSID, t.TempDir()) // returns at once: nothing sent

	if a.recoverFired.Load() {
		t.Fatal("the recovery sent NOTHING (no broker connection) yet left the once-per-connection flag set: " +
			"every later attempt short-circuits on a recovery that never happened, the broker never learns this " +
			"session's stable id, and every subsequent attach is answered with no identity")
	}

	// Prove the consequence, not just the flag: a later attempt must actually
	// reach the broker once the connection is back.
	a.bmu.Lock()
	a.conn = live
	a.bmu.Unlock()
	go a.fireRecover(ctx, recoveryTestSID, t.TempDir())
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
	a, _ := newRecoveryAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a.bmu.Lock()
	a.conn = nil
	a.bmu.Unlock()

	a.recoverStarted.Store(true)
	a.fireRecover(ctx, recoveryTestSID, t.TempDir())

	settled := make(chan struct{})
	go func() {
		defer close(settled)
		_ = a.awaitIdentitySettled(ctx, false)
	}()
	select {
	case <-settled:
	case <-time.After(2 * time.Second):
		t.Fatalf("a recovery that failed without sending never settled the identity question: every attach now "+
			"blocks for the full %v settle budget before proceeding anyway", recoverSettleBudget)
	}
}

// A session that never started a recovery has nothing to wait for. Waiting
// anyway would stall the attach of every session with no Antigravity
// conversation id for the full settle budget on an answer nobody will give.
func TestAwaitIdentitySettled_DoesNotWaitWhenNoRecoveryWasEverStarted(t *testing.T) {
	a, _ := newRecoveryAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = a.awaitIdentitySettled(ctx, false)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("a session that never started a recovery still waited on the identity gate: every attach would "+
			"stall for up to %v on an answer that is never coming", recoverSettleBudget)
	}
}

func unresolvedIdentityAdapter(t *testing.T) (*adapter, *recoveryBroker) {
	t.Helper()
	a, broker := newRecoveryAdapter(t)
	a.recoverStarted.Store(true)
	a.recoverFired.Store(true)
	return a, broker
}

func TestToolAttach_CanceledIdentityWaitWritesNoAttach(t *testing.T) {
	a, broker := unresolvedIdentityAdapter(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := a.toolAttach(ctx, newRecoveryAttachReq(t, map[string]any{"name": "other-topic"}))
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError {
		t.Fatalf("canceled identity wait returned success: %+v", result)
	}
	if op, ok := broker.nextOp(t, 100*time.Millisecond); ok {
		t.Fatalf("a canceled identity wait still wrote a %q broker frame", op)
	}
}

func TestToolAttach_BareIdentityTimeoutRefusesWithoutBrokerWrite(t *testing.T) {
	prev := recoverSettleBudget
	recoverSettleBudget = 20 * time.Millisecond
	t.Cleanup(func() { recoverSettleBudget = prev })

	a, broker := unresolvedIdentityAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	result, err := a.toolAttach(ctx, newRecoveryAttachReq(t, map[string]any{}))
	if err != nil {
		t.Fatal(err)
	}
	if op, ok := broker.nextOp(t, 100*time.Millisecond); ok {
		t.Fatalf("a bare unsettled attach wrote a %q broker frame", op)
	}
	if !result.IsError || len(result.Content) == 0 {
		t.Fatalf("bare unsettled attach did not return a retryable error: %+v", result)
	}
	text, ok := result.Content[0].(*mcp.TextContent)
	if !ok || !strings.Contains(text.Text, "identity still resolving; retry attach") {
		t.Fatalf("bare unsettled attach error did not name the remedy: %+v", result.Content)
	}
}

func TestToolAttach_ExplicitTargetWaitsPastBareBudgetUntilIdentitySettles(t *testing.T) {
	prev := recoverSettleBudget
	recoverSettleBudget = 20 * time.Millisecond
	t.Cleanup(func() { recoverSettleBudget = prev })

	a, broker := unresolvedIdentityAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	attachReq := newRecoveryAttachReq(t, map[string]any{"name": "other-topic"})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = a.toolAttach(ctx, attachReq)
	}()

	if op, ok := broker.nextOp(t, 100*time.Millisecond); ok {
		t.Fatalf("an explicit attach overtook unsettled identity after the bare budget with a %q frame", op)
	}
	a.markIdentitySettled()
	if op, ok := broker.nextOp(t, time.Second); !ok || op != ipc.OpAttach {
		t.Fatalf("explicit attach was not released after identity settled (op=%q ok=%v)", op, ok)
	}
	driveAttached(t, a)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("explicit attach did not return after the broker response")
	}
}
