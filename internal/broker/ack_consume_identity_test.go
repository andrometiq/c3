package broker

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// capturingHolder claims the route with a live stub and returns a channel of the
// decoded OpInbound pushes the broker writes to it, so a test can assert the
// values that actually go over the wire.
func capturingHolder(t *testing.T, b *Broker, key RouteKey) <-chan ipc.InboundMsg {
	t.Helper()
	adapterEnd, holderEnd := net.Pipe()
	t.Cleanup(func() { adapterEnd.Close(); holderEnd.Close() })
	adapterConn := ipc.NewConn(adapterEnd)
	pushes := make(chan ipc.InboundMsg, 8)
	go func() {
		for {
			raw, err := adapterConn.ReadFrame()
			if err != nil {
				return
			}
			if op, _ := ipc.PeekOp(raw); op != ipc.OpInbound {
				continue
			}
			var m ipc.InboundMsg
			if json.Unmarshal(raw, &m) == nil {
				select {
				case pushes <- m:
				default:
				}
			}
		}
	}()
	stub := &Stub{CLI: "claude", PID: os.Getpid(), CWD: "/work", ConnID: 1}
	stub.Reattach(ipc.NewConn(holderEnd), 1)
	b.Routes.Claim(key, stub)
	return pushes
}

// Release audit 2026-07-25 — data loss on the live-push delivered-ack.
//
// The adapter acks a live push with the Covered count it was given, and the
// broker used to drop that many lines off the queue HEAD. That is only the same
// set of lines when the queue held nothing but this push's own lines. It often
// does not: forwardOrFallback computes `pending = n - covered` specifically to
// tell the agent how much OLDER backlog is still queued, so pushing while older
// held lines sit ahead is an expected state (a session that reattaches, or whose
// adapter reconnects after a BOUNCE, with held lines pending).
//
// In that state the head lines were the OLD undelivered backlog, so the ack
// destroyed a message nobody had ever seen and left the message that WAS
// delivered sitting in the queue to be handed out again by fetch_queue.
//
// The fix records which lines each push covered (keyed by the pushed MessageID,
// which every adapter echoes back as InboundDeliveredMsg.UpdateID) and removes
// exactly those by identity.

// These tests reuse liveHolder from worker_render_hold_test.go, which registers a
// live, connected stub holding the route and drains its adapter end so the
// broker's blocking push completes.

// The regression: an ack must never consume a line the push did not deliver.
func TestLiveAck_ConsumesCoveredLines_NotQueueHead(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	qrk := queueRouteKey(key)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()
	ctx := context.Background()

	// Two messages arrive with no session attached -> held in the durable queue.
	w.forwardOrFallback(ctx, inbound(tid, 1, "held-one"), 1)
	w.forwardOrFallback(ctx, inbound(tid, 2, "held-two"), 1)
	if n, _ := b.Queue.Pending(qrk); n != 2 {
		t.Fatalf("setup: want 2 held, got %d", n)
	}

	// A session attaches and is fully live.
	_, _ = liveHolder(t, b, key)

	// A third message arrives. flushInbounds persists it, then pushes with
	// Covered=1 and the id of the line it just appended.
	third := inbound(tid, 3, "live-three")
	if err := b.Queue.Append(qrk, third); err != nil {
		t.Fatalf("append third: %v", err)
	}
	w.forwardOrFallbackCovering(ctx, third, 1, []int64{third.MessageID}, true)

	if n, _ := b.Queue.Pending(qrk); n != 3 {
		t.Fatalf("all three should still be queued until the ack; got %d", n)
	}

	// The adapter acks that push.
	w.handleConsume(ctx, &ConsumeJob{MessageID: third.MessageID, Count: 1})

	left, err := b.Queue.Peek(qrk, 10)
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	survived := map[int64]bool{}
	for _, m := range left {
		survived[m.MessageID] = true
	}

	if !survived[1] || !survived[2] {
		t.Errorf("DATA LOSS: messages 1 and 2 were never delivered to any session and must stay queued; remaining=%v", survived)
	}
	if survived[3] {
		t.Errorf("DUPLICATE: message 3 was delivered live and acked, so it must be consumed; it is still queued and fetch_queue would hand it out again")
	}
	if n, _ := b.Queue.Pending(qrk); n != 2 {
		t.Errorf("exactly the one delivered line should have been consumed; pending=%d, want 2", n)
	}
}

// The common case must be unchanged: with no older backlog, the push's own line
// is the head, and acking consumes it.
func TestLiveAck_NoBacklog_ConsumesThePushedLine(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	qrk := queueRouteKey(key)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()
	ctx := context.Background()

	_, _ = liveHolder(t, b, key)

	only := inbound(tid, 7, "only")
	if err := b.Queue.Append(qrk, only); err != nil {
		t.Fatalf("append: %v", err)
	}
	w.forwardOrFallbackCovering(ctx, only, 1, []int64{only.MessageID}, true)
	w.handleConsume(ctx, &ConsumeJob{MessageID: only.MessageID, Count: 1})

	if n, _ := b.Queue.Pending(qrk); n != 0 {
		t.Errorf("the delivered line must be consumed; pending=%d, want 0", n)
	}
}

// A merged push covers several stored lines and is acked once — all of them must
// go, and only them.
func TestLiveAck_MergedPush_ConsumesEveryCoveredLine(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	qrk := queueRouteKey(key)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()
	ctx := context.Background()

	// One older held line, then a live session, then a merged batch of three.
	w.forwardOrFallback(ctx, inbound(tid, 1, "held-one"), 1)
	_, _ = liveHolder(t, b, key)

	var ids []int64
	for _, id := range []int{10, 11, 12} {
		in := inbound(tid, id, "batch")
		if err := b.Queue.Append(qrk, in); err != nil {
			t.Fatalf("append %d: %v", id, err)
		}
		ids = append(ids, in.MessageID)
	}
	// The merged push presents as the last message of the batch.
	merged := inbound(tid, 12, "batch-merged")
	w.forwardOrFallbackCovering(ctx, merged, len(ids), ids, true)
	w.handleConsume(ctx, &ConsumeJob{MessageID: merged.MessageID, Count: len(ids)})

	left, err := b.Queue.Peek(qrk, 10)
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if len(left) != 1 || left[0].MessageID != 1 {
		t.Errorf("only the never-delivered held line should remain; got %+v", left)
	}
}

// A missing identity record must fail toward keeping the queue. Falling back to
// head-consume is the original data-loss bug under a different trigger: the
// missing record may have hit the tracking cap while older, undelivered backlog
// still sits at the head. A duplicate delivery is recoverable; destroying an
// unrelated message is not.
func TestLiveAck_NoCoveredRecord_KeepsQueue(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	qrk := queueRouteKey(key)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()
	ctx := context.Background()

	for _, in := range []*c3types.Inbound{
		inbound(tid, 4, "older-undelivered"),
		inbound(tid, 5, "delivered-but-untracked"),
	} {
		if err := b.Queue.Append(qrk, in); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	// No recordCoveredByPush for this push id.
	w.handleConsume(ctx, &ConsumeJob{MessageID: 5, Count: 1})

	left, err := b.Queue.Peek(qrk, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 2 || left[0].MessageID != 4 || left[1].MessageID != 5 {
		t.Errorf("missing identity must consume nothing; got %+v", left)
	}
}

// The record map is bounded. Once full it retains the oldest outstanding
// records and declines to track newer pushes; their acks then fail toward a
// recoverable duplicate rather than a destructive head-consume.
func TestCoveredByPush_BoundedFailTowardKeeping(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	for i := 1; i <= maxCoveredByPush+10; i++ {
		w.recordCoveredByPush(int64(i), "", []int64{int64(i)})
	}
	if got := len(w.coveredByPush); got != maxCoveredByPush {
		t.Errorf("map must stay bounded at %d; got %d", maxCoveredByPush, got)
	}
	if got := len(w.coveredOrder); got != maxCoveredByPush {
		t.Errorf("order list must stay bounded at %d; got %d", maxCoveredByPush, got)
	}
	if ids := w.takeCoveredByPush(1, ""); len(ids) != 1 || ids[0] != 1 {
		t.Errorf("the oldest outstanding record must survive the cap; got %v", ids)
	}
	if ids := w.takeCoveredByPush(int64(maxCoveredByPush+10), ""); ids != nil {
		t.Errorf("a push beyond the cap must be left untracked; got %v", ids)
	}
}

// Two live pushes can legitimately share a MessageID (Telegram edited_message).
// Their identity records must queue FIFO under that shared ack key; overwriting
// the first record makes the first ack consume the second push's lines.
func TestCoveredByPush_DuplicatePushIDQueuesFIFO(t *testing.T) {
	w := &RouteWorker{}
	w.recordCoveredByPush(7, "first", []int64{1, 7})
	w.recordCoveredByPush(7, "second", []int64{2, 7})

	first := w.takeCoveredByPush(7, "first")
	second := w.takeCoveredByPush(7, "second")
	if len(first) != 2 || first[0] != 1 || first[1] != 7 {
		t.Fatalf("first duplicate-key record overwritten: got %v", first)
	}
	if len(second) != 2 || second[0] != 2 || second[1] != 7 {
		t.Fatalf("second duplicate-key record missing/out of order: got %v", second)
	}
	if w.takeCoveredByPush(7, "first") != nil {
		t.Error("both queued records should be cleared after two takes")
	}
}

func TestFlushPendingAck_ClearsCoveredRecordsWithoutPendingTurn(t *testing.T) {
	w := &RouteWorker{}
	w.recordCoveredByPush(7, "", []int64{7})

	// An outbound can clear pendingAck before the holder dies, while its
	// delivered-ack identity record is still outstanding. Death must still clear
	// holder-local identity state so it cannot bleed into a replacement holder.
	w.flushPendingAck("holder exited")

	if len(w.coveredByPush) != 0 || len(w.coveredOrder) != 0 {
		t.Fatalf("dead holder's covered records survived: map=%v order=%v", w.coveredByPush, w.coveredOrder)
	}
}

// A batch that persisted NOTHING — every message dedup-suppressed, or every
// Append failed — covers zero stored lines, so its push must advertise
// Covered=0 and its ack must consume nothing.
//
// covEffective normalises a covered count of 0 up to 1 for callers that have no
// queue wired (unit tests). Applying that to a real batch made a push which
// stored nothing still claim one line, and the ack then ate an unrelated queued
// message. This is the second trigger of the same data-loss class as
// TestLiveAck_ConsumesCoveredLines_NotQueueHead.
func TestLivePush_ZeroAppended_AdvertisesZeroCovered(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	qrk := queueRouteKey(key)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()
	ctx := context.Background()

	// One real held line the push must not touch.
	if err := b.Queue.Append(qrk, inbound(tid, 1, "innocent-bystander")); err != nil {
		t.Fatalf("append: %v", err)
	}

	pushes := capturingHolder(t, b, key)

	// A push whose caller knows it appended nothing.
	w.forwardOrFallbackCovering(ctx, inbound(tid, 2, "stored-nothing"), 0, nil, true)

	select {
	case m := <-pushes:
		if m.Covered != 0 {
			t.Errorf("a push that stored no lines must advertise Covered=0; got %d — its ack would consume a queued message it never delivered", m.Covered)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no push observed")
	}

	// And the bystander is still there.
	if n, _ := b.Queue.Pending(qrk); n != 1 {
		t.Errorf("the unrelated held line must survive; pending=%d, want 1", n)
	}
}
