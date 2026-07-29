package main

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
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
// into fireRecover; ensureStableSessionRegistered read that entry as "already
// registered" and returned straight into the attach write, and a fireRecover
// that entered and then returned without sending anything left it set forever,
// so every later attach was answered with no identity at all.

// recoveryBroker drains every frame the adapter writes, so a test can assert
// both that a frame ARRIVES and — the half that matters here — that one does NOT.
type recoveryBroker struct {
	frames chan []byte
}

// observedDoneContext exposes each waitIdentityGate select without relying on
// scheduler timing. Done is called as the select arms, so tests can place a
// reconnect exactly while a waiter is blocked on a particular identity epoch.
type observedDoneContext struct {
	context.Context
	doneCalls chan struct{}
}

func (c *observedDoneContext) Done() <-chan struct{} {
	select {
	case c.doneCalls <- struct{}{}:
	default:
	}
	return c.Context.Done()
}

// newRecoveryBroker starts reading the broker side of an adapter's IPC pipe.
func newRecoveryBroker(peer *ipc.Conn) *recoveryBroker {
	b := &recoveryBroker{frames: make(chan []byte, 8)}
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

// newRecoveryAdapter builds an adapter with a broker pipe whose frames the
// returned recoveryBroker reads, and a deterministic session identity (env-pinned
// so the test never depends on this machine's active_sessions.json).
func newRecoveryAdapter(t *testing.T) (*adapter, *recoveryBroker) {
	t.Helper()
	t.Setenv("GROK_HOME", t.TempDir()) // no active_sessions.json to stumble over
	t.Setenv("C3_GROK_SESSION_ID", "sess-recovery")
	a, peer := newForwardTestAdapter(t, filepath.Join(t.TempDir(), "missing.sock"))
	return a, newRecoveryBroker(peer)
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

	// Drive the REAL startup path: trySessionRecover resolves this session's id
	// and fires. The recovery reaches the broker but is NOT answered, so this
	// session still does not know who it is.
	a.trySessionRecover(ctx)
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

// Same rule on the branch where there is no session id left to register: an
// unresolvable id is not a licence to overtake a recovery that is already in
// flight with the id it DID resolve.
func TestEnsureStableSessionRegistered_WaitsEvenWhenItHasNoIdToRegister(t *testing.T) {
	a, broker := newRecoveryAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Drive the REAL startup path: trySessionRecover resolves the id and fires.
	a.trySessionRecover(ctx)
	if op, ok := broker.nextOp(t, 5*time.Second); !ok || op != ipc.OpRecoverSession {
		t.Fatalf("recovery never reached the broker (op=%q ok=%v), so this test cannot prove anything", op, ok)
	}

	// Now make the id unresolvable: no env pin, no active_sessions.json, no
	// leader-bound id.
	t.Setenv("C3_GROK_SESSION_ID", "")
	t.Setenv("GROK_SESSION_ID", "")
	a.leader.mu.Lock()
	a.leader.sessionID = ""
	a.leader.mu.Unlock()
	if sid := a.stableSessionID(); sid != "" {
		t.Fatalf("stableSessionID() = %q, want empty — this test needs the no-id branch", sid)
	}

	released := make(chan struct{})
	go func() {
		defer close(released)
		_, _ = a.ensureStableSessionRegistered(ctx, false)
	}()
	select {
	case <-released:
		t.Fatal("the no-id branch returned into the attach write while a recovery was still in flight: the " +
			"attach is answered before this session knows its identity")
	case <-time.After(500 * time.Millisecond):
	}

	answerRecover(t, a)
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("the no-id branch never returned after identity settled — the gate is a deadlock")
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
	a.fireRecover(ctx, "sess-recovery", t.TempDir()) // returns at once: nothing sent

	if a.recoverFired.Load() {
		t.Fatal("the recovery sent NOTHING (no broker connection) yet left the once-per-connection flag set: " +
			"every later ensureStableSessionRegistered short-circuits on a recovery that never happened, the " +
			"broker never learns this session's stable id, and every subsequent attach is answered with no identity")
	}

	// Prove the consequence, not just the flag: a later attempt must actually
	// reach the broker once the connection is back.
	a.bmu.Lock()
	a.conn = live
	a.bmu.Unlock()
	go a.fireRecover(ctx, "sess-recovery", t.TempDir())
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
	a.fireRecover(ctx, "sess-recovery", t.TempDir())

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
// anyway would stall the attach of every session with no resolvable Grok
// session id for the full settle budget on an answer nobody is going to give.
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

func TestReconnectRecoveryRearmsIdentitySettlement(t *testing.T) {
	t.Setenv("C3_GROK_SESSION_ID", "session-reconnect")
	a := newAdapter()
	old := a.identityGate()
	a.markIdentitySettled()
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	a.bmu.Lock()
	a.conn = ipc.NewConn(c1)
	a.bmu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requestSeen := make(chan struct{})
	go func() {
		_, _ = ipc.NewConn(c2).ReadFrame()
		close(requestSeen)
	}()
	a.refireRecoverOnReconnect(ctx)
	current := a.identityGate()
	if old == current {
		t.Fatal("reconnect recovery reused the permanently closed identity gate")
	}
	select {
	case <-current:
		t.Fatal("reconnect identity epoch was already settled before re-registration")
	default:
	}
	select {
	case <-requestSeen:
	case <-time.After(time.Second):
		t.Fatal("reconnect recovery did not send registration")
	}
	cancel()
	select {
	case <-current:
	case <-time.After(time.Second):
		t.Fatal("current reconnect identity epoch did not settle")
	}
}

func TestIdentityEpoch_OldWaiterChasesReconnectAndReturnsNewConn(t *testing.T) {
	a, _ := newRecoveryAdapter(t)
	a.recoverStarted.Store(true)
	oldEpoch := a.currentIdentityEpoch()

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &observedDoneContext{
		Context:   baseCtx,
		doneCalls: make(chan struct{}, 4),
	}
	type waitResult struct {
		conn *ipc.Conn
		err  error
	}
	result := make(chan waitResult, 1)
	go func() {
		conn, err := a.connAfterIdentityEpoch(ctx, false, oldEpoch)
		result <- waitResult{conn: conn, err: err}
	}()

	select {
	case <-ctx.doneCalls:
	case <-time.After(time.Second):
		t.Fatal("old identity waiter never armed")
	}

	a.beginReconnectIdentityEpoch()
	newGate := a.identityGate()
	if newGate == oldEpoch.gate {
		t.Fatal("reconnect did not open a new identity epoch")
	}

	adapterSide, brokerSide := net.Pipe()
	defer adapterSide.Close()
	defer brokerSide.Close()
	newConn := ipc.NewConn(adapterSide)
	a.bmu.Lock()
	a.conn = newConn
	a.bmu.Unlock()

	select {
	case <-ctx.doneCalls:
		// The old gate woke the waiter, which revalidated the epoch and armed a
		// second wait on the new gate.
	case got := <-result:
		t.Fatalf("old waiter returned before the new identity epoch settled: conn=%p err=%v", got.conn, got.err)
	case <-time.After(time.Second):
		t.Fatal("old waiter did not chase the new identity epoch")
	}

	select {
	case <-newGate:
		t.Fatal("new identity gate was settled before the new recovery answered")
	default:
	}
	a.settleIdentity(newGate)

	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("identity waiter returned error: %v", got.err)
		}
		if got.conn != newConn {
			t.Fatalf("identity waiter returned conn %p, want current epoch conn %p", got.conn, newConn)
		}
	case <-time.After(time.Second):
		t.Fatal("identity waiter did not return after the new epoch settled")
	}
}

func TestIdentityEpoch_OldRecoveryCannotSettleNewGate(t *testing.T) {
	a, broker := newRecoveryAdapter(t)
	a.recoverStarted.Store(true)
	oldEpoch := a.currentIdentityEpoch()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cwd := t.TempDir()
	recoveryDone := make(chan struct{})
	go func() {
		defer close(recoveryDone)
		a.fireRecoverEpoch(ctx, "sess-recovery", cwd, oldEpoch)
	}()
	if op, ok := broker.nextOp(t, time.Second); !ok || op != ipc.OpRecoverSession {
		t.Fatalf("old recovery did not enter its response wait (op=%q ok=%v)", op, ok)
	}

	a.beginReconnectIdentityEpoch()
	newGate := a.identityGate()
	if newGate == oldEpoch.gate {
		t.Fatal("reconnect did not open a new identity epoch")
	}

	cancel()
	select {
	case <-recoveryDone:
	case <-time.After(time.Second):
		t.Fatal("old recovery did not stop after cancellation")
	}
	select {
	case <-newGate:
		t.Fatal("old recovery completion settled the new identity epoch")
	default:
	}
	a.settleIdentity(newGate)
}

func TestReconnectRefire_ReusesClaimedEpochOnSameConnection(t *testing.T) {
	a, broker := newRecoveryAdapter(t)
	a.recoverStarted.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	epoch := a.currentIdentityEpoch()
	go a.fireRecoverEpoch(ctx, "sess-recovery", t.TempDir(), epoch)
	if op, ok := broker.nextOp(t, time.Second); !ok || op != ipc.OpRecoverSession {
		t.Fatalf("first recovery did not reach the broker (op=%q ok=%v)", op, ok)
	}

	a.refireRecoverOnReconnect(ctx)
	if got := a.identityGate(); got != epoch.gate {
		t.Fatal("refire replaced an open, claimed epoch and enabled a second recovery on the same connection")
	}
	if op, ok := broker.nextOp(t, 100*time.Millisecond); ok {
		t.Fatalf("refire sent a second %q recovery on the same connection; its uncorrelated response can be delivered to the wrong waiter", op)
	}

	answerRecover(t, a)
	select {
	case <-epoch.gate:
	case <-time.After(time.Second):
		t.Fatal("the connection's one recovery did not settle its reused epoch")
	}
}

func TestReconnectEpoch_CannotRegisterBeforeHelloCompletes(t *testing.T) {
	a, _ := newRecoveryAdapter(t)
	a.beginReconnectIdentityEpoch()

	adapterSide, brokerSide := net.Pipe()
	defer adapterSide.Close()
	defer brokerSide.Close()
	conn := ipc.NewConn(adapterSide)
	broker := newRecoveryBroker(ipc.NewConn(brokerSide))
	a.bmu.Lock()
	a.conn = conn // connectBroker has published it; hello is still in flight
	a.bmu.Unlock()
	epoch := a.currentIdentityEpoch()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.fireRecoverEpoch(ctx, "sess-recovery", t.TempDir(), epoch)
	if op, ok := broker.nextOp(t, 100*time.Millisecond); ok {
		t.Fatalf("recovery wrote %q before hello completed; hello must be the replacement connection's first frame", op)
	}
	select {
	case <-epoch.gate:
		t.Fatal("pre-hello recovery attempt settled the replacement gate")
	default:
	}

	epoch = a.prepareReconnectRecoveryEpoch() // models successful hello
	go a.fireRecoverEpoch(ctx, "sess-recovery", t.TempDir(), epoch)
	if op, ok := broker.nextOp(t, time.Second); !ok || op != ipc.OpRecoverSession {
		t.Fatalf("post-hello recovery did not register (op=%q ok=%v)", op, ok)
	}
	answerRecover(t, a)
	select {
	case <-epoch.gate:
	case <-time.After(time.Second):
		t.Fatal("post-hello recovery did not settle the replacement gate")
	}
}

func TestReconnectHello_GatesGenericToolUntilValidatedAck(t *testing.T) {
	a := newAdapter()
	a.beginReconnectIdentityEpoch()

	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() {
		_ = adapterSide.Close()
		_ = brokerSide.Close()
	})
	a.publishBrokerConnection(ipc.NewConn(adapterSide))
	broker := newRecoveryBroker(ipc.NewConn(brokerSide))
	forward := a.toolForward("reply")
	req := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{
		Name: "reply", Arguments: json.RawMessage(`{"text":"hello"}`),
	}}

	// The freshly dialed connection is published only for hello. An ordinary
	// tool must receive the normal reconnect retry, never become C1's first
	// frame.
	preHelloDone := make(chan error, 1)
	go func() {
		_, err := forward(context.Background(), req)
		preHelloDone <- err
	}()
	select {
	case raw := <-broker.frames:
		op, err := ipc.PeekOp(raw)
		if err != nil {
			t.Fatalf("pre-hello frame is malformed: %v", err)
		}
		t.Fatalf("generic tool wrote %q before hello started", op)
	case err := <-preHelloDone:
		if err != nil {
			t.Fatalf("pre-hello tool call returned transport error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pre-hello tool did not return with a reconnect retry")
	}

	helloDone := make(chan error, 1)
	go func() { helloDone <- a.hello() }()
	if op, ok := broker.nextOp(t, time.Second); !ok || op != ipc.OpHello {
		t.Fatalf("first replacement frame = %q, want hello", op)
	}
	if _, err := forward(context.Background(), req); err != nil {
		t.Fatalf("during-hello tool call returned transport error: %v", err)
	}
	if op, ok := broker.nextOp(t, 100*time.Millisecond); ok {
		t.Fatalf("generic tool wrote %q while hello awaited its ack", op)
	}
	if err := ipc.NewConn(brokerSide).WriteJSON(ipc.HelloAckMsg{Op: ipc.OpHelloAck}); err != nil {
		t.Fatalf("write hello ack: %v", err)
	}
	if err := <-helloDone; err != nil {
		t.Fatalf("hello failed: %v", err)
	}

	toolDone := make(chan struct{})
	go func() {
		_, _ = forward(context.Background(), req)
		close(toolDone)
	}()
	var raw []byte
	select {
	case raw = <-broker.frames:
	case <-time.After(time.Second):
		t.Fatal("generic tool remained blocked after validated hello ack")
	}
	var call ipc.ToolCallReq
	if err := json.Unmarshal(raw, &call); err != nil || call.Op != ipc.OpToolCall {
		t.Fatalf("post-hello frame = %s err=%v, want tool_call", raw, err)
	}
	result, err := json.Marshal(ipc.ToolResultMsg{Op: ipc.OpToolResult, ID: call.ID, Result: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	a.dispatchToolResult(result)
	select {
	case <-toolDone:
	case <-time.After(time.Second):
		t.Fatal("post-hello generic tool did not return after its broker result")
	}
}

func TestHello_NonAckKeepsProductionConnectionUnavailable(t *testing.T) {
	a := newAdapter()
	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() {
		_ = adapterSide.Close()
		_ = brokerSide.Close()
	})
	a.publishBrokerConnection(ipc.NewConn(adapterSide))
	broker := newRecoveryBroker(ipc.NewConn(brokerSide))

	done := make(chan error, 1)
	go func() { done <- a.hello() }()
	if op, ok := broker.nextOp(t, time.Second); !ok || op != ipc.OpHello {
		t.Fatalf("first frame = %q, want hello", op)
	}
	if err := ipc.NewConn(brokerSide).WriteJSON(ipc.ErrorMsg{Op: ipc.OpError, Err: "expected hello first"}); err != nil {
		t.Fatalf("write non-ack response: %v", err)
	}
	if err := <-done; err == nil {
		t.Fatal("hello accepted a non-hello_ack response")
	}
	if got := a.currentConn(); got != nil {
		t.Fatalf("non-hello_ack made production conn %p available", got)
	}
}

func TestHello_OldAckCannotPublishReplacementConnection(t *testing.T) {
	a := newAdapter()
	oldAdapter, oldBroker := net.Pipe()
	newAdapter, newBroker := net.Pipe()
	t.Cleanup(func() {
		_ = oldAdapter.Close()
		_ = oldBroker.Close()
		_ = newAdapter.Close()
		_ = newBroker.Close()
	})
	oldConn := ipc.NewConn(oldAdapter)
	newConn := ipc.NewConn(newAdapter)
	peer := ipc.NewConn(oldBroker)
	a.helloAck = ipc.HelloAckMsg{ConnID: 99, ProtocolVersion: ipc.ProtocolVersion}
	a.brokerVersion.Store(int64(ipc.ProtocolVersion))
	a.publishBrokerConnection(oldConn)

	helloSeen := make(chan struct{})
	allowAck := make(chan struct{})
	go func() {
		_, _ = peer.ReadFrame()
		close(helloSeen)
		<-allowAck
		_ = peer.WriteJSON(ipc.HelloAckMsg{
			Op: ipc.OpHelloAck, ConnID: 1, ProtocolVersion: ipc.ProtocolVersion + 1,
		})
	}()
	helloDone := make(chan error, 1)
	go func() { helloDone <- a.hello() }()
	select {
	case <-helloSeen:
	case <-time.After(time.Second):
		t.Fatal("hello frame was not sent")
	}

	a.publishBrokerConnection(newConn)
	close(allowAck)
	if err := <-helloDone; err == nil {
		t.Fatal("old connection's HelloAck published its replacement as usable")
	}
	if got := a.currentConn(); got != nil {
		t.Fatalf("replacement connection %p became usable from old connection's HelloAck", got)
	}
	if a.helloAck.ConnID != 99 || a.brokerVersion.Load() != int64(ipc.ProtocolVersion) {
		t.Fatalf("old connection's HelloAck replaced current metadata: ack=%+v version=%d", a.helloAck, a.brokerVersion.Load())
	}
}

func TestReconnectRecovery_SerializesUncorrelatedResponseSlot(t *testing.T) {
	a, oldBroker := newRecoveryAdapter(t)
	oldCtx, cancelOld := context.WithCancel(context.Background())
	defer cancelOld()
	oldEpoch := a.currentIdentityEpoch()
	oldDone := make(chan struct{})
	go func() {
		defer close(oldDone)
		a.fireRecoverEpoch(oldCtx, "sess-recovery", t.TempDir(), oldEpoch)
	}()
	if op, ok := oldBroker.nextOp(t, time.Second); !ok || op != ipc.OpRecoverSession {
		t.Fatalf("old recovery did not install its response waiter (op=%q ok=%v)", op, ok)
	}

	a.beginReconnectIdentityEpoch()
	adapterSide, brokerSide := net.Pipe()
	defer adapterSide.Close()
	defer brokerSide.Close()
	a.bmu.Lock()
	a.conn = ipc.NewConn(adapterSide)
	a.bmu.Unlock()
	newBroker := newRecoveryBroker(ipc.NewConn(brokerSide))
	newCtx, cancelNew := context.WithCancel(context.Background())
	defer cancelNew()
	a.refireRecoverOnReconnect(newCtx)

	if op, ok := newBroker.nextOp(t, 100*time.Millisecond); ok {
		t.Fatalf("new %q recovery overlapped the old singleton response waiter", op)
	}
	cancelOld()
	select {
	case <-oldDone:
	case <-time.After(time.Second):
		t.Fatal("old recovery did not release the response slot after cancellation")
	}
	if op, ok := newBroker.nextOp(t, time.Second); !ok || op != ipc.OpRecoverSession {
		t.Fatalf("new recovery did not start after the old waiter left (op=%q ok=%v)", op, ok)
	}
	answerRecover(t, a)
	newGate := a.identityGate()
	select {
	case <-newGate:
	case <-time.After(time.Second):
		t.Fatal("new recovery response did not settle the replacement epoch")
	}
}
