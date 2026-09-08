package broker

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

func TestHelloRenderStateCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		cannot      bool
		want        string
	}{
		{"legacy capable", "", false, ipc.RenderCapable},
		{"legacy queue", "", true, ipc.RenderQueueOnly},
		{"new probing", ipc.RenderProbing, true, ipc.RenderProbing},
		{"new capable", ipc.RenderCapable, false, ipc.RenderCapable},
		{"new queue", ipc.RenderQueueOnly, true, ipc.RenderQueueOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("C3_QUEUE_DIR", t.TempDir())
			b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
			defer b.Shutdown()
			client, server := net.Pipe()
			defer client.Close()
			done := make(chan struct{})
			go func() { b.HandleConn(server); close(done) }()
			peer := ipc.NewConn(client)
			if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", CannotRenderChannels: tc.cannot, RenderState: tc.state, RenderReason: "test reason"}); err != nil {
				t.Fatal(err)
			}
			raw, err := peer.ReadFrame()
			if err != nil {
				t.Fatal(err)
			}
			var ack ipc.HelloAckMsg
			if json.Unmarshal(raw, &ack) != nil || ack.Op != ipc.OpHelloAck {
				t.Fatalf("hello rejected: %s", raw)
			}
			stubs := b.Stubs.Snapshot()
			if len(stubs) != 1 || stubs[0].RenderRoute().State != tc.want {
				t.Fatalf("hello state: %+v", stubs)
			}
			if err := peer.WriteJSON(ipc.ListSessionsReq{Op: ipc.OpListSessions}); err != nil {
				t.Fatal(err)
			}
			raw, err = peer.ReadFrame()
			if err != nil {
				t.Fatal(err)
			}
			var sessions ipc.ListSessionsReplyMsg
			if json.Unmarshal(raw, &sessions) != nil || len(sessions.Sessions) != 1 || sessions.Sessions[0].RenderState != tc.want {
				t.Fatalf("session status: %s", raw)
			}
			client.Close()
			<-done
		})
	}
}

func TestProbeHoldsLaterInboundAndTimeoutNotifiesOnce(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()
	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	stub, pushed := liveHolder(t, b, key)
	stub.AddRoute(key)
	stub.SetRenderRoute(ipc.RenderProbing, "channels flag present, awaiting confirmation", true)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()
	w.forwardOrFallback(context.Background(), inbound(tid, 1, "first"), 1)
	select {
	case <-pushed:
	case <-time.After(time.Second):
		t.Fatal("probe not pushed")
	}
	w.forwardOrFallback(context.Background(), inbound(tid, 2, "second"), 1)
	select {
	case <-pushed:
		t.Fatal("second probing inbound pushed")
	default:
	}
	if n, _ := b.Queue.Pending(queueRouteKey(key)); n != 1 {
		t.Fatalf("second inbound not held: %d", n)
	}
	// The production cooldown must delay, then deliver, the latest route state.
	if b.HeldNotices.cooldown != defaultHeldNoticeCooldown {
		t.Fatal("non-production cooldown")
	}
	b.HeldNotices.ShouldSend(key) // occupy the real ten-second Held window
	before := len(fc.sendRepliesSnapshot())
	raw, _ := json.Marshal(ipc.RenderStateMsg{Op: ipc.OpRenderState, RenderRoute: ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "live push not confirmed"}})
	intermediate, _ := json.Marshal(ipc.RenderStateMsg{Op: ipc.OpRenderState, RenderRoute: ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "session transcript unavailable"}})
	b.handleRenderState(stub, intermediate)
	b.handleRenderState(stub, raw)
	b.handleRenderState(stub, raw)
	if got := len(fc.sendRepliesSnapshot()); got != before {
		t.Fatal("route notice bypassed production cooldown")
	}
	deadline := time.Now().Add(defaultHeldNoticeCooldown + 2*time.Second)
	for len(fc.sendRepliesSnapshot()) == before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	replies := fc.sendRepliesSnapshot()
	if len(replies) != before+1 || (!strings.Contains(replies[len(replies)-1].Text, "Held — nothing lost") || !strings.Contains(replies[len(replies)-1].Text, "live push not confirmed")) {
		t.Fatalf("timeout notices: %+v", replies)
	}
	if stub.CanRenderPush() {
		t.Fatal("timeout still accepts pushes")
	}
	if text := b.statusForTopic(key.Channel, key.ChatID, &tid); !strings.Contains(text, "Live route: queue-only (live push not confirmed)") {
		t.Fatalf("status: %s", text)
	}
}

func TestPendingAckSurvivesOutboundAndClearsOnlyConfirmedToken(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()
	one, two := inbound(tid, 1, "one"), inbound(tid, 2, "two")
	if err := b.Queue.Append(queueRouteKey(key), one); err != nil {
		t.Fatal(err)
	}
	if err := b.Queue.Append(queueRouteKey(key), two); err != nil {
		t.Fatal(err)
	}
	rows, err := b.Queue.PeekTracked(queueRouteKey(key), 10)
	if err != nil {
		t.Fatal(err)
	}
	w.trackPendingAck([]*c3types.Inbound{one}, rows[0].RecordID)
	w.trackPendingAck([]*c3types.Inbound{two}, rows[1].RecordID)
	w.recordCoveredByPush(1, "token-one", []string{rows[0].RecordID})
	w.recordCoveredByPush(2, "token-two", []string{rows[1].RecordID})
	result := make(chan OutboundResult, 1)
	w.dispatchOutbound(context.Background(), &OutboundJob{Tool: "reply", Args: map[string]any{"text": "unrelated"}, ResultCh: result})
	if got := <-result; got.Err != nil {
		t.Fatal(got.Err)
	}
	if len(w.pendingAck) != 2 {
		t.Fatal("unrelated outbound cleared unconfirmed messages")
	}
	owner := &Stub{ReceiptConfirming: true, ConnID: 100}
	b.Routes.Claim(key, owner)
	owner.AddRoute(key)
	owner.MarkRouteConfirmed(key)
	w.handleConsume(context.Background(), &ConsumeJob{MessageID: 2, Token: "token-two", Count: 1, Owner: owner})
	if len(w.pendingAck) != 1 || w.pendingAck[0].ids[0] != rows[0].RecordID {
		t.Fatalf("wrong pending delivery removed: %+v", w.pendingAck)
	}
	left, err := b.Queue.Peek(queueRouteKey(key), 10)
	if err != nil || len(left) != 1 || left[0].MessageID != 1 {
		t.Fatalf("wrong durable record consumed: %+v %v", left, err)
	}
}

func TestAttachRouteNoticeCoalesces(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()
	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	stub, _ := liveHolder(t, b, key)
	stub.AddRoute(key)
	stub.SetRenderRoute(ipc.RenderQueueOnly, "no dev-channels flag on host", true)
	for i := 0; i < 3; i++ {
		b.withRouteSet(stub, ipc.AttachedMsg{OK: true})
	}
	deadline := time.Now().Add(time.Second)
	for len(fc.sendRepliesSnapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	replies := fc.sendRepliesSnapshot()
	if len(replies) != 1 || !strings.Contains(replies[0].Text, "Live route: queue-only (no dev-channels flag on host)") {
		t.Fatalf("attach notices: %+v", replies)
	}
}

func TestRenderPromotionSchedulesLatestNotice(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()
	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	stub, _ := liveHolder(t, b, key)
	stub.AddRoute(key)
	stub.SetRenderRoute(ipc.RenderProbing, "awaiting confirmation", true)
	raw, _ := json.Marshal(ipc.RenderStateMsg{Op: ipc.OpRenderState, RenderRoute: ipc.RenderRoute{State: ipc.RenderCapable}})
	b.handleRenderState(stub, raw)
	deadline := time.Now().Add(time.Second)
	for len(fc.sendRepliesSnapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	replies := fc.sendRepliesSnapshot()
	if len(replies) != 1 || !strings.Contains(replies[0].Text, "Live route: channel.") {
		t.Fatalf("promotion notice: %+v", replies)
	}
}
