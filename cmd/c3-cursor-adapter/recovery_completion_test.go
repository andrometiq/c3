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
	t.Setenv("CURSOR_CONVERSATION_ID", recoveryTestSID)
	t.Setenv("C3_CURSOR_CWD", t.TempDir())
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

	if a.currentIdentityEpoch().recoverFired.Load() {
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
// anyway would stall the attach of every session with no Cursor
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
	a.currentIdentityEpoch().recoverFired.Store(true)
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
	t.Setenv("CURSOR_CONVERSATION_ID", "session-reconnect")
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

func TestAwaitIdentityEpoch_ChasesReconnectInsteadOfReturningOldConnection(t *testing.T) {
	a := newAdapter()
	a.recoverStarted.Store(true)

	oldClient, oldPeer := net.Pipe()
	defer oldClient.Close()
	defer oldPeer.Close()
	a.publishBrokerConnection(ipc.NewConn(oldClient))
	a.markCurrentIdentityEpochReady()
	old := a.currentIdentityEpoch()

	newClient, newPeer := net.Pipe()
	defer newClient.Close()
	defer newPeer.Close()
	a.publishBrokerConnection(ipc.NewConn(newClient))
	current := a.currentIdentityEpoch()

	// A waiter already holding old sees its answer after the reconnect. It must
	// chase current rather than return old and let toolAttach adopt newClient.
	a.settleIdentity(old)
	a.settleIdentity(current)
	got, err := a.awaitIdentityEpoch(context.Background(), false, old)
	if err != nil {
		t.Fatal(err)
	}
	if got != current || got.conn != current.conn {
		t.Fatalf("old waiter returned epoch %+v / conn %p; want current epoch %+v / conn %p", got, got.conn, current, current.conn)
	}
}

func TestOldRecoverySettlementCannotCloseReconnectGate(t *testing.T) {
	a := newAdapter()

	oldClient, oldPeer := net.Pipe()
	defer oldClient.Close()
	defer oldPeer.Close()
	a.publishBrokerConnection(ipc.NewConn(oldClient))
	a.markCurrentIdentityEpochReady()
	old := a.currentIdentityEpoch()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requestSeen := make(chan error, 1)
	go func() {
		_, err := ipc.NewConn(oldPeer).ReadFrame()
		requestSeen <- err
	}()
	go a.fireRecover(ctx, recoveryTestSID, t.TempDir())
	select {
	case err := <-requestSeen:
		if err != nil {
			t.Fatalf("old recovery did not reach its broker connection: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("old recovery never reached its broker connection")
	}

	newClient, newPeer := net.Pipe()
	defer newClient.Close()
	defer newPeer.Close()
	a.publishBrokerConnection(ipc.NewConn(newClient))
	current := a.currentIdentityEpoch()

	// Model the old RecoverSession goroutine returning after the reconnect. Its
	// deferred settlement must answer only old, leaving current unresolved.
	cancel()
	select {
	case <-old.gate:
	case <-time.After(time.Second):
		t.Fatal("old recovery did not settle its own identity gate after cancellation")
	}
	select {
	case <-current.gate:
		t.Fatal("old recovery settled the new connection's identity gate")
	default:
	}
}

func TestReconnectRecoveryCannotClaimBeforeHelloAck(t *testing.T) {
	t.Setenv("CURSOR_CONVERSATION_ID", recoveryTestSID)
	a := newAdapter()

	adapterSide, brokerSide := net.Pipe()
	defer adapterSide.Close()
	defer brokerSide.Close()
	a.publishBrokerConnection(ipc.NewConn(adapterSide))
	epoch := a.currentIdentityEpoch()

	broker := &recoveryBroker{frames: make(chan []byte, 1)}
	peer := ipc.NewConn(brokerSide)
	go func() {
		raw, err := peer.ReadFrame()
		if err == nil {
			broker.frames <- raw
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a.recoverStarted.Store(true)
	a.fireRecover(ctx, recoveryTestSID, t.TempDir())

	if op, ok := broker.nextOp(t, 100*time.Millisecond); ok {
		t.Fatalf("reconnect recovery wrote %q before HelloAck", op)
	}
	if epoch.recoverFired.Load() {
		t.Fatal("pre-Hello recovery claimed the reconnect epoch")
	}
	select {
	case <-epoch.gate:
		t.Fatal("pre-Hello recovery settled the reconnect identity gate")
	default:
	}

	a.markCurrentIdentityEpochReady() // models a validated HelloAck
	go a.fireRecover(ctx, recoveryTestSID, t.TempDir())
	if op, ok := broker.nextOp(t, time.Second); !ok || op != ipc.OpRecoverSession {
		t.Fatalf("post-Hello recovery did not register (op=%q ok=%v)", op, ok)
	}
	answerRecover(t, a)
	select {
	case <-epoch.gate:
	case <-time.After(time.Second):
		t.Fatal("post-Hello recovery response did not settle the reconnect identity gate")
	}
}

func TestToolForwardCannotWriteBeforeHelloAck(t *testing.T) {
	a := newAdapter()
	adapterSide, brokerSide := net.Pipe()
	defer adapterSide.Close()
	defer brokerSide.Close()
	conn := ipc.NewConn(adapterSide)
	peer := ipc.NewConn(brokerSide)
	a.publishBrokerConnection(conn)

	helloDone := make(chan error, 1)
	go func() { helloDone <- a.hello() }()
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("read hello: %v", err)
	}
	if op, err := ipc.PeekOp(raw); err != nil || op != ipc.OpHello {
		t.Fatalf("first frame = %q (err=%v), want Hello", op, err)
	}

	broker := &recoveryBroker{frames: make(chan []byte, 2)}
	go func() {
		for {
			raw, err := peer.ReadFrame()
			if err != nil {
				return
			}
			broker.frames <- raw
		}
	}()
	req := newRecoveryAttachReq(t, map[string]any{"text": "hello"})
	preCtx, preCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer preCancel()
	result, err := a.toolForward("reply")(preCtx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError {
		t.Fatalf("pre-Hello tool call returned success: %+v", result)
	}
	if op, ok := broker.nextOp(t, 100*time.Millisecond); ok {
		t.Fatalf("ordinary tool wrote %q before HelloAck", op)
	}

	if err := peer.WriteJSON(ipc.HelloAckMsg{
		Op: ipc.OpHelloAck, ProtocolVersion: ipc.ProtocolVersion,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-helloDone:
		if err != nil {
			t.Fatalf("hello failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("hello did not return after HelloAck")
	}

	ctx, cancel := context.WithCancel(context.Background())
	callDone := make(chan struct{})
	go func() {
		defer close(callDone)
		_, _ = a.toolForward("reply")(ctx, req)
	}()
	if op, ok := broker.nextOp(t, time.Second); !ok || op != ipc.OpToolCall {
		t.Fatalf("ordinary tool did not write after HelloAck (op=%q ok=%v)", op, ok)
	}
	cancel()
	select {
	case <-callDone:
	case <-time.After(time.Second):
		t.Fatal("ordinary tool did not return after cancellation")
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

func TestToolAttachWithoutSessionIDCannotWriteBeforeHelloAck(t *testing.T) {
	t.Setenv("CURSOR_CONVERSATION_ID", "")
	a := newAdapter()
	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() {
		_ = adapterSide.Close()
		_ = brokerSide.Close()
	})
	a.publishBrokerConnection(ipc.NewConn(adapterSide))
	broker := &recoveryBroker{frames: make(chan []byte, 1)}
	peer := ipc.NewConn(brokerSide)
	go func() {
		if raw, err := peer.ReadFrame(); err == nil {
			broker.frames <- raw
		}
	}()

	type attachResult struct {
		result *mcp.CallToolResult
		err    error
	}
	req := newRecoveryAttachReq(t, map[string]any{"name": "topic"})
	done := make(chan attachResult, 1)
	go func() {
		result, err := a.toolAttach(context.Background(), req)
		done <- attachResult{result: result, err: err}
	}()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if !got.result.IsError {
			t.Fatalf("pre-Hello attach returned success: %+v", got.result)
		}
	case raw := <-broker.frames:
		op, _ := ipc.PeekOp(raw)
		driveAttached(t, a)
		t.Fatalf("attach without a session id wrote %q before HelloAck", op)
	case <-time.After(time.Second):
		t.Fatal("pre-Hello attach did not refuse promptly")
	}
}
