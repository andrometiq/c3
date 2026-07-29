package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/sessionhandoff"
)

func TestIdentityWait_ReconnectChasesCurrentEpoch(t *testing.T) {
	a := newAdapter()
	a.recoverStarted.Store(true)

	oldAdapter, oldBroker := net.Pipe()
	newAdapterConn, newBroker := net.Pipe()
	t.Cleanup(func() {
		_ = oldAdapter.Close()
		_ = oldBroker.Close()
		_ = newAdapterConn.Close()
		_ = newBroker.Close()
	})
	oldConn := ipc.NewConn(oldAdapter)
	newConn := ipc.NewConn(newAdapterConn)

	a.bmu.Lock()
	a.conn = oldConn
	a.bmu.Unlock()
	oldGate, _, _ := a.identityRecoverySnapshot()

	type result struct {
		conn *ipc.Conn
		err  error
	}
	done := make(chan result, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		conn, err := a.connAfterIdentityGate(ctx, oldGate)
		done <- result{conn: conn, err: err}
	}()

	a.reopenIdentity() // opens the new gate before the new conn is published
	a.bmu.Lock()
	a.conn = newConn
	a.bmu.Unlock()
	newGate, _, _ := a.identityRecoverySnapshot()

	select {
	case got := <-done:
		t.Fatalf("old identity answer escaped onto a replacement connection before its recovery settled: conn=%p want_wait_for=%p err=%v", got.conn, newConn, got.err)
	case <-time.After(50 * time.Millisecond):
	}

	a.settleIdentity(newGate)
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("wait for replacement identity failed: %v", got.err)
		}
		if got.conn != newConn {
			t.Fatalf("identity wait returned the wrong connection: got=%p want=%p", got.conn, newConn)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("identity wait did not follow the replacement epoch after the old gate closed")
	}
}

func TestIdentityRecovery_OldCompletionCannotSettleNewGate(t *testing.T) {
	a := newAdapter()
	oldGate := a.identityGate()
	a.reopenIdentity()
	newGate := a.identityGate()

	a.settleIdentity(oldGate)
	select {
	case <-newGate:
		t.Fatal("an old recovery completion settled the replacement identity gate")
	default:
	}

	a.settleIdentity(newGate)
	select {
	case <-newGate:
	case <-time.After(time.Second):
		t.Fatal("the recovery that owned the replacement gate did not settle it")
	}
}

func TestReconnectIdentity_UnknownSessionSettlesAgainstFreshConn(t *testing.T) {
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	a := newAdapter()
	a.recoverStarted.Store(true) // an earlier attempt answered "unidentified"

	oldAdapterSide, oldBrokerSide := net.Pipe()
	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() {
		_ = oldAdapterSide.Close()
		_ = oldBrokerSide.Close()
		_ = adapterSide.Close()
		_ = brokerSide.Close()
	})
	oldConn := ipc.NewConn(oldAdapterSide)
	conn := ipc.NewConn(adapterSide)
	a.bmu.Lock()
	a.conn = oldConn
	a.bmu.Unlock()
	if retired := a.prepareIdentityReconnect(); retired != oldConn {
		t.Fatalf("reconnect retired conn %p, want old conn %p", retired, oldConn)
	}
	if got := a.currentConn(); got != nil {
		t.Fatalf("new identity gate was published while old conn %p was still visible", got)
	}
	gate := a.identityGate()

	// A handoff recovery appearing after preparation but before the replacement
	// conn is published cannot claim or settle the prepared gate against nil/old
	// conn. The successful reconnect restore below owns that answer.
	a.fireRecover(context.Background(), sessionhandoff.Entry{
		StableSessionID: "late-session", CWD: "/work",
	})
	select {
	case <-gate:
		t.Fatal("pre-publish recovery settled the replacement identity gate")
	default:
	}

	a.bmu.Lock()
	a.conn = conn
	a.bmu.Unlock()

	a.activateIdentityReconnect()
	a.restoreSessionAfterReconnect(context.Background())
	select {
	case <-gate:
	case <-time.After(time.Second):
		t.Fatal("fresh broker epoch stayed open even though no recoverable identity exists")
	}

	got, err := a.connAfterIdentitySettled(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != conn {
		t.Fatalf("unidentified reconnect settled against conn %p, want fresh conn %p", got, conn)
	}
}

func TestReconnectIdentity_TimeoutCannotSettlePreparedGate(t *testing.T) {
	prev := recoverSettleBudget
	recoverSettleBudget = 20 * time.Millisecond
	t.Cleanup(func() { recoverSettleBudget = prev })

	a := newAdapter()
	a.prepareIdentityReconnect()
	gate := a.identityGate()
	if _, err := a.connAfterIdentitySettled(context.Background()); !errors.Is(err, errIdentityStillResolving) {
		t.Fatalf("prepared reconnect timeout error = %v, want retryable identity error", err)
	}
	select {
	case <-gate:
		t.Fatal("a caller timeout settled the reconnect gate before a replacement identity recovery ran")
	default:
	}
}

func TestReconnectIdentity_PreHelloConnectionCannotBeClaimed(t *testing.T) {
	a := newAdapter()
	a.prepareIdentityReconnect()

	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() {
		_ = adapterSide.Close()
		_ = brokerSide.Close()
	})
	conn := ipc.NewConn(adapterSide)
	peer := ipc.NewConn(brokerSide)
	a.bmu.Lock()
	a.conn = conn // dial succeeded; replacement hello has not completed
	a.bmu.Unlock()
	gate := a.identityGate()

	frames := make(chan []byte, 1)
	go func() {
		raw, _ := peer.ReadFrame()
		frames <- raw
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entry := sessionhandoff.Entry{StableSessionID: "late-session", CWD: "/work"}
	preHelloDone := make(chan struct{})
	go func() {
		a.fireRecover(ctx, entry)
		close(preHelloDone)
	}()
	select {
	case raw := <-frames:
		t.Fatalf("recovery wrote before hello completed: %s", raw)
	case <-preHelloDone:
	case <-time.After(time.Second):
		t.Fatal("pre-hello recovery did not refuse the prepared epoch promptly")
	}
	select {
	case <-gate:
		t.Fatal("pre-hello recovery settled the prepared reconnect gate")
	default:
	}

	a.activateIdentityReconnect() // models a validated HelloAck
	go a.fireRecover(ctx, entry)
	select {
	case raw := <-frames:
		if op, err := ipc.PeekOp(raw); err != nil || op != ipc.OpRecoverSession {
			t.Fatalf("post-hello frame = %q err=%v, want recover_session", op, err)
		}
	case <-time.After(time.Second):
		t.Fatal("recovery did not become claimable after hello completed")
	}
	resp, err := json.Marshal(ipc.RecoverSessionResp{Op: ipc.OpRecoverSessionResult})
	if err != nil {
		t.Fatal(err)
	}
	a.dispatchRecoverSessionResult(resp)
	select {
	case <-gate:
	case <-time.After(time.Second):
		t.Fatal("post-hello recovery response did not settle the reconnect gate")
	}
}

func TestIdentitySwitch_CannotClearReconnectPreparedWhileWaitingForRecoveryLock(t *testing.T) {
	a := newAdapter()
	a.runCtx = context.Background()
	establishSettledIdentity(a, sessionhandoff.Entry{
		StableSessionID: "conversation-a", CWD: "/work/a",
	})

	// Queue the switch behind the recovery lock. The reconnect then wins before
	// the switch goroutine reaches its identity-epoch transition.
	a.recoverMu.Lock()
	switched := make(chan bool, 1)
	go func() {
		switched <- a.beginIdentitySwitchMode(context.Background(), "conversation-a", sessionhandoff.Entry{
			StableSessionID: "conversation-b", CWD: "/work/b",
		}, false)
	}()
	a.prepareIdentityReconnect()
	preparedGate := a.identityGate()
	a.recoverMu.Unlock()

	select {
	case got := <-switched:
		if got {
			t.Fatal("identity switch replaced a reconnect-prepared epoch while waiting for the recovery lock")
		}
	case <-time.After(time.Second):
		t.Fatal("identity switch did not return after the recovery lock was released")
	}
	if !a.identityReconnectPending() {
		t.Fatal("identity switch cleared the reconnect pre-Hello guard")
	}
	if a.identityGate() != preparedGate {
		t.Fatal("identity switch replaced the reconnect-prepared identity gate")
	}
	select {
	case <-preparedGate:
		t.Fatal("identity switch settled the reconnect-prepared identity gate")
	default:
	}
}

func TestBrokerHelloGate_GenericToolCannotWriteBeforeAck(t *testing.T) {
	a := newAdapter()
	a.prepareIdentityReconnect()

	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() {
		_ = adapterSide.Close()
		_ = brokerSide.Close()
	})
	conn := ipc.NewConn(adapterSide)
	peer := ipc.NewConn(brokerSide)
	a.bmu.Lock()
	a.conn = conn // dial published; HelloAck has not arrived
	a.bmu.Unlock()

	frames := make(chan []byte, 1)
	go func() {
		raw, _ := peer.ReadFrame()
		frames <- raw
	}()
	args, err := json.Marshal(map[string]any{"text": "test"})
	if err != nil {
		t.Fatal(err)
	}
	req := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Arguments: args}}
	type callResult struct {
		result *mcp.CallToolResult
		err    error
	}
	preHelloDone := make(chan callResult, 1)
	go func() {
		result, err := a.toolForward("reply")(context.Background(), req)
		preHelloDone <- callResult{result: result, err: err}
	}()
	select {
	case raw := <-frames:
		var tc ipc.ToolCallReq
		if err := json.Unmarshal(raw, &tc); err == nil {
			resp, _ := json.Marshal(ipc.ToolResultMsg{Op: ipc.OpToolResult, ID: tc.ID})
			a.dispatchToolResult(resp)
		}
		t.Fatalf("generic tool wrote before HelloAck: %s", raw)
	case got := <-preHelloDone:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if !got.result.IsError {
			t.Fatalf("pre-Hello tool call returned success: %+v", got.result)
		}
	case <-time.After(time.Second):
		t.Fatal("pre-Hello generic tool call did not refuse promptly")
	}

	// Model the successful end of hello and prove the same generic path becomes
	// usable afterward.
	a.bmu.Lock()
	a.helloPending = false
	a.bmu.Unlock()
	done := make(chan struct{})
	go func() {
		_, _ = a.toolForward("reply")(context.Background(), req)
		close(done)
	}()
	var tc ipc.ToolCallReq
	select {
	case raw := <-frames:
		if err := json.Unmarshal(raw, &tc); err != nil {
			t.Fatal(err)
		}
		if tc.Op != ipc.OpToolCall {
			t.Fatalf("post-Hello frame op = %q, want %q", tc.Op, ipc.OpToolCall)
		}
	case <-time.After(time.Second):
		t.Fatal("generic tool did not write after HelloAck")
	}
	resp, err := json.Marshal(ipc.ToolResultMsg{Op: ipc.OpToolResult, ID: tc.ID})
	if err != nil {
		t.Fatal(err)
	}
	a.dispatchToolResult(resp)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("post-Hello generic tool did not return")
	}
}

func TestBrokerHelloGate_RejectsNonAckWithoutPublishingConnection(t *testing.T) {
	a := newAdapter()
	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() {
		_ = adapterSide.Close()
		_ = brokerSide.Close()
	})
	conn := ipc.NewConn(adapterSide)
	peer := ipc.NewConn(brokerSide)
	a.bmu.Lock()
	a.conn = conn
	a.helloPending = true
	a.bmu.Unlock()

	peerDone := make(chan error, 1)
	go func() {
		if _, err := peer.ReadFrame(); err != nil {
			peerDone <- err
			return
		}
		peerDone <- peer.WriteJSON(ipc.ErrorMsg{Op: ipc.OpError, Err: "expected hello"})
	}()
	if err := a.hello(); err == nil {
		t.Fatal("non-HelloAck response published the broker connection as usable")
	}
	if err := <-peerDone; err != nil {
		t.Fatal(err)
	}
	if got := a.currentConn(); got != nil {
		t.Fatalf("connection %p became usable without HelloAck", got)
	}
}

func TestBrokerHelloGate_OldAckCannotPublishReplacementConnection(t *testing.T) {
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
	a.bmu.Lock()
	a.conn = oldConn
	a.helloPending = true
	a.bmu.Unlock()

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

	a.bmu.Lock()
	a.conn = newConn
	a.helloPending = true
	a.bmu.Unlock()
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
