package broker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

func TestCrossSessionRouteNoticeStatusAndReceipt(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()
	b.notices.window = 20 * time.Millisecond
	key := drainSrc()
	stub, _ := liveHolder(t, b, key)
	stub.AddRoute(key)
	stub.MarkRouteConfirmed(key)
	stub.ReceiptConfirming = true // hello field presence is pinned separately
	stub.SetRenderRoute(ipc.RenderProbing, "cross-session awaiting confirmation", true)
	if !stub.tryRenderPush() || stub.tryRenderPush() {
		t.Fatal("probe reservation broken")
	}
	reason := "channel not registered; permission relay unavailable"
	update := func(state, reason string) {
		raw, _ := json.Marshal(ipc.RenderStateMsg{Op: ipc.OpRenderState, RenderRoute: ipc.RenderRoute{State: state, Reason: reason}})
		b.handleRenderState(stub, raw)
	}
	update(ipc.RenderCrossSession, reason)
	if !stub.tryRenderPush() || !stub.tryRenderPush() {
		t.Fatal("confirmed cross-session cannot deliver")
	}
	want := "Live route: cross-session (" + reason + ")"
	deadline := time.Now().Add(time.Second)
	for len(fc.sendRepliesSnapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if replies := fc.sendRepliesSnapshot(); len(replies) != 1 || !strings.Contains(replies[0].Text, want) {
		t.Fatalf("route notice: %+v", replies)
	}
	if text := b.statusForTopic(key.Channel, key.ChatID, &key.TopicID); !strings.Contains(text, want) {
		t.Fatalf("status: %s", text)
	}

	w := &RouteWorker{key: key, broker: b}
	one, two := inbound(key.TopicID, 1, "held"), inbound(key.TopicID, 2, "received")
	first, err := b.Queue.AppendTracked(queueRouteKey(key), one)
	if err != nil {
		t.Fatal(err)
	}
	second, err := b.Queue.AppendTracked(queueRouteKey(key), two)
	if err != nil {
		t.Fatal(err)
	}
	w.trackPendingAck([]*c3types.Inbound{one}, first)
	w.trackPendingAck([]*c3types.Inbound{two}, second)
	w.recordCoveredByPush(2, "peer-token", []string{second})
	w.handleConsume(context.Background(), &ConsumeJob{MessageID: 2, Token: "peer-token", Count: 1, Owner: stub})
	w.handleConsume(context.Background(), &ConsumeJob{MessageID: 2, Token: "peer-token", Count: 1, Owner: stub})
	rows, err := b.Queue.PeekTracked(queueRouteKey(key), -1)
	if err != nil || len(rows) != 1 || rows[0].RecordID != first || len(w.pendingAck) != 1 || w.pendingAck[0].ids[0] != first {
		t.Fatal("receipt consumed wrong row or failed to retire recovery")
	}

	// A failed cross-session route retains the remaining row and emits exactly
	// one coalesced held notice, even when the same update is published twice.
	b.HeldNotices.mu.Lock()
	b.HeldNotices.lastByKey[key] = time.Now().Add(-time.Hour)
	b.HeldNotices.mu.Unlock()
	update(ipc.RenderQueueOnly, "cross-session push not confirmed")
	update(ipc.RenderQueueOnly, "cross-session push not confirmed")
	deadline = time.Now().Add(time.Second)
	for len(fc.sendRepliesSnapshot()) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	replies := fc.sendRepliesSnapshot()
	if len(replies) != 2 || !strings.Contains(replies[1].Text, "Held — nothing lost") || !strings.Contains(replies[1].Text, "cross-session push not confirmed") {
		t.Fatalf("held notice: %+v", replies)
	}
	if stub.tryRenderPush() || stub.CanRenderPush() {
		t.Fatal("failure still permits push")
	}
	if n, _ := b.Queue.Pending(queueRouteKey(key)); n != 1 {
		t.Fatal("failure lost durable row")
	}
}
