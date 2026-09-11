package broker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

func TestCodexRenderUpdateHoldsNextInboundAndRecoveryPushes(t *testing.T) {
	for _, reason := range []string{"queue method unsupported", "fetch outcome unknown; restart required", "conversation identity unresolved"} {
		t.Run(reason, func(t *testing.T) {
			t.Setenv("C3_QUEUE_DIR", t.TempDir())
			fc := &fakeChannel{editMessages: true}
			b := brokerWithChannel(t, mfWithTelegram(), fc)
			defer b.Shutdown()
			b.HeldNotices.cooldown = 0 // distinct arrivals, without wall-clock waits
			tid := int64(914)
			key := MakeRouteKey("telegram", -1001234567890, &tid)
			stub, pushed := liveHolder(t, b, key)
			stub.CLI = "codex"
			// Feed the same additive wire frame Codex publishes. Leave the stub's
			// notice-route list empty so only per-arrival Held replies are counted.
			raw, _ := json.Marshal(ipc.RenderStateMsg{Op: ipc.OpRenderState, RenderRoute: ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: reason}})
			b.handleRenderState(stub, raw)
			if stub.CanRenderPush() || stub.RenderRoute().Reason != reason {
				t.Fatal("broker ignored pull-only update")
			}
			w := newRouteWorker(context.Background(), key, time.Hour, b)
			defer w.Stop()
			for i := 1; i <= 2; i++ {
				w.forwardOrFallback(context.Background(), inbound(tid, i, "held"), 1)
				waitNoticeReplies(t, fc, 1)
			}
			if n, _ := b.Queue.Pending(queueRouteKey(key)); n != 2 {
				t.Fatalf("held count %d", n)
			}
			replies := fc.sendRepliesSnapshot()
			if len(replies) != 1 {
				t.Fatalf("per-message Telegram notices: %+v", replies)
			}
			for _, reply := range replies {
				if !strings.Contains(reply.Text, "Held — nothing lost") {
					t.Fatalf("notice: %s", reply.Text)
				}
			}
			select {
			case <-pushed:
				t.Fatal("pull-only inbound pushed")
			case <-time.After(50 * time.Millisecond):
			}
			if got := b.statusForTopic(key.Channel, key.ChatID, &tid); !strings.Contains(got, "Live delivery unavailable; messages are held and recoverable with fetch_queue") {
				t.Fatalf("status: %s", got)
			}
			raw, _ = json.Marshal(ipc.RenderStateMsg{Op: ipc.OpRenderState, RenderRoute: ipc.RenderRoute{State: ipc.RenderCapable}})
			b.handleRenderState(stub, raw)
			w.forwardOrFallback(context.Background(), inbound(tid, 3, "live"), 1)
			select {
			case <-pushed:
			case <-time.After(time.Second):
				t.Fatal("recovery did not restore push")
			}
			if len(fc.sendRepliesSnapshot()) != 1 {
				t.Fatal("capable arrival was held")
			}
		})
	}
}

func TestRetireConsumedRecordsHandlesShortConsume(t *testing.T) {
	for _, count := range []int{0, 1, 2} {
		w := &RouteWorker{}
		one, two := &c3types.Inbound{Text: "one"}, &c3types.Inbound{Text: "two"}
		w.trackPendingAck([]*c3types.Inbound{one}, "one")
		w.trackPendingAck([]*c3types.Inbound{two}, "two")
		msgs := make([]c3types.Inbound, count)
		w.retireConsumedRecords(msgs, []queue.TrackedInbound{{RecordID: "one"}, {RecordID: "two"}})
		if len(w.pendingAck) != 2-count {
			t.Fatalf("consume %d retired unreturned records: %+v", count, w.pendingAck)
		}
		for i, id := range []string{"one", "two"} {
			if i < count && msgs[i].ConsumedRecordID != id {
				t.Fatalf("consumed record %d: %+v", i, msgs[i])
			}
		}
	}
}
