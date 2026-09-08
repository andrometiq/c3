package main

import (
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// A full local buffer leaves the message held and emits a recovery notice.
func TestHandleInbound_Codex_ForwardBufferFullDrop_LatchesBlocked(t *testing.T) {
	t.Setenv("C3_CODEX_ALLOW_MANUAL_FORWARD", "1")
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "")
	t.Setenv("C3_CODEX_APP_SERVER_WS", "ws://127.0.0.1:1")
	t.Setenv("C3_CODEX_THREAD_ID", "")

	// Build an adapter WITHOUT starting codexForwardLoop, and pre-fill forwardCh to
	// capacity, so the next enqueue deterministically hits the buffer-full `default:`
	// drop branch (no drain race). transport is nil → latchForwardBlocked latches but
	// skips the best-effort recovery nudge (covered separately below).
	a := &adapter{
		pending:   map[string]chan ipc.ToolResultMsg{},
		fqPending: map[string]chan ipc.FetchQueueResp{},
		rtPending: map[string]chan ipc.RetranscribeResp{},
		forwardCh: make(chan codexForwardReq, 1),
	}
	a.forwardCh <- codexForwardReq{} // 1/1 → buffer full

	peerSide, brokerSide := net.Pipe()
	t.Cleanup(func() { peerSide.Close(); brokerSide.Close() })
	a.bmu.Lock()
	a.conn = ipc.NewConn(brokerSide)
	a.bmu.Unlock()
	peer := ipc.NewConn(peerSide)

	if a.recoveryNoticeSent.Load() {
		t.Fatal("precondition: recoveryNoticeSent must start false")
	}

	msg := ipc.InboundMsg{Op: ipc.OpInbound, Covered: 1, Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 42, Text: "dropped"}}
	raw, _ := json.Marshal(msg)
	a.handleInbound(raw) // forwardCh full → default: drop → latchForwardBlocked

	if !a.recoveryNoticeSent.Load() {
		t.Fatal("a buffer-full forward drop must surface held-message recovery")
	}
	if _, got := readDeliveredAck(t, peer, 300*time.Millisecond); got {
		t.Fatal("the drop path itself must not send an ack")
	}
}

// Recovery regression (review finding 2): latching recoveryNoticeSent must fire EXACTLY ONE
// fetch_queue recovery nudge on the false→true transition (so the stuck head message is
// drainable even in bridge mode, where the steady-state nudge is suppressed), and be an
// idempotent no-op thereafter — no per-inbound nudge spam.
func TestLatchForwardBlocked_FiresExactlyOneRecoveryNudge(t *testing.T) {
	a, buf := adapterWithCaptureTransport(t)

	if a.recoveryNoticeSent.Load() {
		t.Fatal("precondition: recoveryNoticeSent must start false")
	}
	a.latchForwardBlocked("WS forward failed")
	a.latchForwardBlocked("forward queue full") // already latched → must be a no-op

	if !a.recoveryNoticeSent.Load() {
		t.Fatal("recoveryNoticeSent must remain latched after the first transition")
	}
	if n := strings.Count(buf.String(), "notifications/message"); n != 1 {
		t.Fatalf("latch must fire EXACTLY ONE recovery nudge (got %d) — it is one-shot per session", n)
	}
	if data := notifyData(t, buf); !strings.Contains(data, "fetch_queue") {
		t.Fatalf("recovery nudge = %q, want it to prompt the agent to call fetch_queue", data)
	}
}
