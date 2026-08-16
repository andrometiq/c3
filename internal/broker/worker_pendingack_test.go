package broker

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

// The silent-loss safety net (2026-07-12 dentist incident): a message delivered
// to a holder that then dies WITHOUT handling it was consumed from the durable
// queue on the adapter's blind delivered-ack, so it vanished with no warning.
// The fix tracks such deliveries (pendingAck) and, on confirmed holder death,
// re-queues them + notifies the topic.

func inbound(tid int64, id int, text string) *c3types.Inbound {
	return &c3types.Inbound{
		Channel: "telegram", ChatID: -1001234567890, TopicID: &tid,
		MessageID: int64(id), Text: text, Timestamp: time.Now(),
	}
}

// deadPID returns a PID that is guaranteed not to be running: a short-lived
// child that has already exited AND been reaped (exec.Run waited on it), so
// kill -0 on it returns ESRCH.
func deadPID(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("true") // no shell: a bare binary that exits immediately
	if err := cmd.Run(); err != nil {
		t.Fatalf("spawn short-lived proc: %v", err)
	}
	return cmd.Process.Pid
}

// TestFlushPendingAck_RequeuesAndNotifies: the core recovery — tracked deliveries
// are re-queued (recoverable via fetch_queue) and one notice is posted so the
// operator is told. The tracker is cleared afterward.
func TestFlushPendingAck_RequeuesAndNotifies(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	w.pendingAck = [][]*c3types.Inbound{{inbound(tid, 1, "first")}, {inbound(tid, 2, "second")}}
	w.flushPendingAck("The session exited")

	if n, _ := b.Queue.Pending(queueRouteKey(key)); n != 2 {
		t.Errorf("both unprocessed deliveries must be re-queued; got Pending=%d", n)
	}
	replies := fc.sendRepliesSnapshot()
	if len(replies) != 1 {
		t.Fatalf("flush must post exactly one notice; got %d", len(replies))
	}
	if txt := replies[0].Text; !strings.Contains(txt, "queue") || !strings.Contains(txt, "exited") {
		t.Errorf("notice must tell the operator the session exited and messages are re-queued; got %q", txt)
	}
	if len(w.pendingAck) != 0 {
		t.Errorf("pendingAck must be cleared after a flush; got %d", len(w.pendingAck))
	}
}

// TestFlushPendingAck_EmptyNoop: nothing tracked → no re-queue, no notice.
func TestFlushPendingAck_EmptyNoop(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	w.flushPendingAck("The session exited")

	if n, _ := b.Queue.Pending(queueRouteKey(key)); n != 0 {
		t.Errorf("empty flush must not queue anything; got Pending=%d", n)
	}
	if got := len(fc.sendRepliesSnapshot()); got != 0 {
		t.Errorf("empty flush must not notify; got %d", got)
	}
}

// TestDelivered_TracksPendingAck: a human message delivered to a live holder is
// tracked (so it can be recovered if the holder later dies), but a synthesized
// EVENT is not (events are never queued and carry no lost content).
func TestDelivered_TracksPendingAck(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	_, pushed := liveHolder(t, b, key) // render-capable, alive

	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	w.forwardOrFallback(context.Background(), inbound(tid, 1, "hi"), 1)
	select {
	case <-pushed:
	case <-time.After(2 * time.Second):
		t.Fatal("capable holder must be delivered the message")
	}
	if len(w.pendingAck) != 1 || len(w.pendingAck[0]) != 1 || w.pendingAck[0][0].MessageID != 1 {
		t.Fatalf("a delivered human message must be tracked; got %+v", w.pendingAck)
	}

	event := &c3types.Inbound{
		Channel: "telegram", ChatID: -1001234567890, TopicID: &tid,
		MessageID: 2, Kind: c3types.InboundPollResult,
		Event: &c3types.InboundEvent{PollResult: &c3types.PollResult{PollID: "p", IsClosed: true}},
	}
	w.forwardOrFallback(context.Background(), event, 0)
	<-pushed
	if len(w.pendingAck) != 1 {
		t.Errorf("an event must NOT be tracked; pendingAck=%d", len(w.pendingAck))
	}
}

// TestTrackPendingAck_CapDropsOldest: the tracker is bounded — past maxPendingAck
// the oldest are dropped so a badly-behind route can't grow it without bound.
func TestTrackPendingAck_CapDropsOldest(t *testing.T) {
	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	w := newRouteWorker(context.Background(), key, time.Hour, nil)
	defer w.Stop()

	for i := 1; i <= maxPendingAck+3; i++ {
		w.trackPendingAck([]*c3types.Inbound{inbound(tid, i, "m")})
	}
	if len(w.pendingAck) != maxPendingAck {
		t.Fatalf("tracker must cap at %d; got %d", maxPendingAck, len(w.pendingAck))
	}
	if w.pendingAck[0][0].MessageID != 4 {
		t.Errorf("the 3 oldest must be dropped (newest kept); oldest kept id=%d, want 4", w.pendingAck[0][0].MessageID)
	}
}

// TestStalePath_FlushesPendingAck: when a new inbound finds the holder DEAD, the
// STALE branch re-queues the earlier unprocessed deliveries and notifies — the
// end-to-end wiring of the dentist fix.
func TestStalePath_FlushesPendingAck(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)

	dead := &Stub{CLI: "claude", PID: deadPID(t), CWD: "/x", ConnID: 9}
	b.Routes.Claim(key, dead)
	if dead.IsAlive() {
		t.Skip("dead PID was unexpectedly alive (reuse) — skipping")
	}

	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()
	w.pendingAck = [][]*c3types.Inbound{{inbound(tid, 1, "earlier")}} // an earlier delivery

	w.forwardOrFallback(context.Background(), inbound(tid, 2, "new"), 1)

	// msg 1 re-queued (flush) + msg 2 held (fallback) = 2 durably queued.
	if n, _ := b.Queue.Pending(queueRouteKey(key)); n != 2 {
		t.Errorf("STALE must re-queue the earlier delivery + hold the new one; got Pending=%d", n)
	}
	var sawFlushNotice bool
	for _, r := range fc.sendRepliesSnapshot() {
		if strings.Contains(r.Text, "exited") && strings.Contains(r.Text, "queue") {
			sawFlushNotice = true
		}
	}
	if !sawFlushNotice {
		t.Errorf("STALE must fire the silent-loss notice; replies=%+v", fc.sendRepliesSnapshot())
	}
	if len(w.pendingAck) != 0 {
		t.Errorf("pendingAck must be cleared after the STALE flush; got %d", len(w.pendingAck))
	}
}
