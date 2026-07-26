package broker

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

// C1 (broker side): a delivered EVENT push must NOT consume any queued backlog.
// handleConsume with Count<=0 (the event-ack shape) must skip the consume — a
// 0→1 bump here would drop a real queued message the event never delivered.
func TestHandleConsume_ZeroCountDoesNotConsumeBacklog(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	const n = 3
	for i := int64(1); i <= n; i++ {
		_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: i, Text: "m", Timestamp: time.Now()})
	}

	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	// A zero-covered ack (the event shape) must consume nothing.
	w.handleConsume(context.Background(), &ConsumeJob{MessageID: 99, Count: 0})

	if got, _ := b.Queue.Pending(qrk); got != n {
		t.Fatalf("event/zero-covered ack consumed backlog; pending=%d, want %d (all still queued)", got, n)
	}
}

// C1 (broker side, end-to-end through the ack handler): a live EVENT delivered
// while N backlog messages are queued must leave all N still queued. The adapter
// guard means an event push is never acked; even if an older adapter did ack with
// Count=0 (the event's Covered), handleInboundDelivered + handleConsume drop it.
func TestHandleInboundDelivered_EventAckLeavesBacklogIntact(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	const n = 4
	for i := int64(1); i <= n; i++ {
		_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: i, Text: "m", Timestamp: time.Now()})
	}
	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)
	stub.MarkRouteConfirmed() // confirmed claim, so the Count=0 no-op is proven by the Count<1 branch, not the §5 tripwire

	// An event push acked with Count=0 (its Covered) — must be a no-op consume.
	raw, _ := json.Marshal(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, UpdateID: 50, OK: true, Count: 0})
	b.handleInboundDelivered(stub, raw)

	// Give any (incorrectly) submitted JobConsume a chance to run, then assert
	// nothing was consumed.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got, _ := b.Queue.Pending(qrk); got != n {
			t.Fatalf("event ack consumed backlog; pending dropped to %d, want %d", got, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got, _ := b.Queue.Pending(qrk); got != n {
		t.Fatalf("after event ack, pending=%d, want %d (no backlog message lost)", got, n)
	}
}

// C1 (forwardOrFallback): an EVENT pushed to a live claimed holder must be
// stamped Covered=0 on the wire (an event is never queued, so it covers zero
// stored lines). A merged text push covers its appended lines; an event covers 0.
func TestForwardOrFallback_EventStampsCoveredZero(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)

	// A live holder with a real conn so forwardOrFallback takes the claimed branch
	// and writes the InboundMsg we can read off the wire.
	agent, brokerSide := newConnPair(t)
	stub := &Stub{CLI: "claude", PID: 1, ConnID: 7, Conn: brokerSide}
	b.Routes.Claim(key, stub)

	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	event := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 2200,
		Kind:  c3types.InboundPollResult,
		Event: &c3types.InboundEvent{PollResult: &c3types.PollResult{PollID: "p", IsClosed: true}},
	}
	go w.forwardOrFallback(context.Background(), event, 0)

	raw, err := agent.ReadFrame()
	if err != nil {
		t.Fatalf("read pushed event frame: %v", err)
	}
	var msg ipc.InboundMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatal(err)
	}
	if msg.Covered != 0 {
		t.Fatalf("event push Covered = %d, want 0 (events cover zero stored lines)", msg.Covered)
	}
}

// C2: a disk-full Append for message M must NOT poison the dedup set. The first
// (failing) Append must leave M unrecorded so a LATER successful redelivery of M
// IS persisted (not dedup-suppressed and lost), and the offset then advances.
func TestFlushInbounds_AppendFailDoesNotPoisonDedup(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}

	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	// Track which update_ids got marked persisted (the offset-eligibility signal).
	var persisted []int64
	b.SetPersistedCallback(func(in *c3types.Inbound) { persisted = append(persisted, in.MessageID) })

	// Force the FIRST Append to fail deterministically: build a store, then remove
	// its directory out from under it so Append's OpenFile(O_CREATE) fails. Swap
	// back to the working store for the redelivery.
	good := b.Queue
	brokenDir := t.TempDir()
	brokenStore, err := queue.NewStore(brokenDir)
	if err != nil {
		t.Fatal(err)
	}
	if rmErr := os.RemoveAll(brokenDir); rmErr != nil {
		t.Fatalf("setup: remove broken dir: %v", rmErr)
	}
	b.Queue = brokenStore

	in := &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 77, Text: "hi", Timestamp: time.Now()}
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})

	if len(persisted) != 0 {
		t.Fatalf("failed Append must NOT markPersisted; persisted=%v, want none", persisted)
	}

	// Restore a working store and redeliver the SAME message_id. It must NOT be
	// dedup-suppressed (the failed Append never recorded it) — it must persist now.
	b.Queue = good
	in2 := &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 77, Text: "hi", Timestamp: time.Now()}
	w.flushInbounds(context.Background(), []*c3types.Inbound{in2})

	if got, _ := b.Queue.Pending(qrk); got != 1 {
		t.Fatalf("redelivery after a failed Append must persist; pending=%d, want 1 (message not lost)", got)
	}
	if len(persisted) != 1 || persisted[0] != 77 {
		t.Fatalf("redelivery must markPersisted update 77; persisted=%v", persisted)
	}
}

// C3: a dedup-SKIP must still markPersisted so the redelivery's update_id is
// MarkDone'd and the contiguous-prefix offset advances over BOTH the original and
// the redelivery — never wedging. Feed the same message_id twice; the second is a
// dedup-skip; assert the message is stored ONCE and BOTH update_ids were marked.
func TestFlushInbounds_DedupSkipMarksPersisted(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	var marked int
	b.SetPersistedCallback(func(in *c3types.Inbound) { marked++ })

	first := &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 8, Text: "hi", Timestamp: time.Now()}
	w.flushInbounds(context.Background(), []*c3types.Inbound{first})
	// Redelivery (same message_id) — a dedup-skip.
	redeliver := &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 8, Text: "hi", Timestamp: time.Now()}
	w.flushInbounds(context.Background(), []*c3types.Inbound{redeliver})

	if got, _ := b.Queue.Pending(qrk); got != 1 {
		t.Fatalf("dedup-skip must store the message ONCE; pending=%d, want 1", got)
	}
	// markPersisted must fire for BOTH deliveries: the first (real Append) AND the
	// dedup-skip (so its update_id is MarkDone'd and the offset doesn't stall).
	if marked != 2 {
		t.Fatalf("markPersisted fired %d times; want 2 (real Append + dedup-skip), else the redelivery's update_id strands the offset", marked)
	}
}

// Item 1 (worker side): on a durable Append FAILURE, flushInbounds must fire the
// persist-FAILED callback (so the telegram channel evicts the poll-side dedup and
// the held offset's redelivery genuinely retries) and must NOT fire the persist
// (success) callback (the offset must HOLD — loss-free).
func TestFlushInbounds_AppendFailNotifiesPersistFailed(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	var persisted, failed []int64
	b.SetPersistedCallback(func(in *c3types.Inbound) { persisted = append(persisted, in.MessageID) })
	b.SetPersistFailedCallback(func(in *c3types.Inbound) { failed = append(failed, in.MessageID) })

	// Force the Append to fail: a store whose backing dir was removed.
	brokenDir := t.TempDir()
	brokenStore, err := queue.NewStore(brokenDir)
	if err != nil {
		t.Fatal(err)
	}
	if rmErr := os.RemoveAll(brokenDir); rmErr != nil {
		t.Fatalf("setup: remove broken dir: %v", rmErr)
	}
	b.Queue = brokenStore

	in := &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 77, Text: "hi", Timestamp: time.Now()}
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})

	if len(persisted) != 0 {
		t.Fatalf("a failed Append must NOT fire the persist(success) callback; persisted=%v", persisted)
	}
	if len(failed) != 1 || failed[0] != 77 {
		t.Fatalf("a failed Append must fire the persist-FAILED callback for msg 77; failed=%v", failed)
	}
}

// Item 3: with the durable queue DISABLED (b.Queue == nil — init failed), a
// ROUTED inbound must STILL fire the persist callback so its source update_id is
// marked done and the offset advances. Otherwise every inbound wedges the
// contiguous-prefix offset forever (no routed message ever marks done). The
// in-memory-only degrade is accepted; the offset must not stall.
func TestFlushInbounds_NilQueueStillMarksPersisted(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	// Disable the durable queue (simulate the init-failed degrade).
	b.Queue = nil

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	var marked []int64
	b.SetPersistedCallback(func(in *c3types.Inbound) { marked = append(marked, in.MessageID) })

	batch := []*c3types.Inbound{
		{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 10, Text: "a", Timestamp: time.Now()},
		{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 11, Text: "b", Timestamp: time.Now()},
	}
	w.flushInbounds(context.Background(), batch)

	if len(marked) != 2 || marked[0] != 10 || marked[1] != 11 {
		t.Fatalf("nil-Queue routed inbounds must STILL markPersisted (offset advances); marked=%v, want [10 11]", marked)
	}
}

// I5: a batch mixing one NEW + one ALREADY-SEEN message must yield Covered=1
// (only the new line was appended), not 2. The merged push's Covered is the count
// of ACTUAL successful Appends — so the Claude ack consumes exactly 1.
func TestFlushInbounds_MixedBatchCoveredCountsActualAppends(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}

	// A live holder with a real conn so forwardOrFallback writes the merged push.
	agent, brokerSide := newConnPair(t)
	stub := &Stub{CLI: "claude", PID: 1, ConnID: 7, Conn: brokerSide}
	b.Routes.Claim(key, stub)

	// Install the worker in the pool so the later handler-submitted consume
	// reaches the SAME worker that recorded this push's covered identities.
	b.Workers.mu.Lock()
	w := b.Workers.spawnLocked(key)
	b.Workers.mu.Unlock()

	// Pre-seed msg 1 as already-seen by appending+recording it (simulate a prior
	// delivery). Feed a batch [1 (seen), 2 (new)]: only 2 is appended → Covered=1.
	w.dedup.record(1)
	_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 1, Text: "old", Timestamp: time.Now()})

	for _, in := range []*c3types.Inbound{
		{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 1, Text: "dup", Timestamp: time.Now()},
		{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 2, Text: "new", Timestamp: time.Now()},
	} {
		if !w.Submit(Job{Kind: JobInbound, Inbound: in}) {
			t.Fatal("submit inbound batch")
		}
	}

	raw, err := agent.ReadFrame()
	if err != nil {
		t.Fatalf("read merged push: %v", err)
	}
	var msg ipc.InboundMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatal(err)
	}
	if msg.Covered != 1 {
		t.Fatalf("mixed batch (1 new + 1 already-seen) Covered = %d, want 1 (only the new line appended)", msg.Covered)
	}

	// And the ack of Count=1 must consume exactly one (the new line + the pre-seed
	// = 2 queued; consuming 1 leaves 1).
	stub.SetRoute(&key)
	stub.MarkRouteConfirmed() // live-push ack consume requires a confirmed claim (§5 tripwire)
	ackRaw, _ := json.Marshal(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, UpdateID: 2, OK: true, Count: msg.Covered})
	b.handleInboundDelivered(stub, ackRaw)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if n, _ := b.Queue.Pending(qrk); n == 1 {
			break
		}
		if time.Now().After(deadline) {
			n, _ := b.Queue.Pending(qrk)
			t.Fatalf("ack(Count=1) should consume exactly 1; pending=%d, want 1", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// I7: per-topic /status reads the in-memory index (StatusFor), not the queue
// files. Appending bumps the index; statusForTopic reflects it without any
// off-goroutine file read.
func TestStatusForTopic_UsesIndexNotFiles(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 1, Text: "x", Timestamp: time.Now().Add(-2 * time.Hour)})

	out := b.statusForTopic("telegram", -100, &tid)
	if want := "1 queued"; !strings.Contains(out, want) {
		t.Fatalf("statusForTopic = %q, want it to contain %q (from the index)", out, want)
	}

	// StatusFor (the index read) must agree with the appended count.
	if st := b.Queue.StatusFor(qrk); st.Pending != 1 {
		t.Fatalf("StatusFor.Pending = %d, want 1 (index-backed)", st.Pending)
	}
}

// I7: the attach backlog summary comes from ONE worker job (JobBacklog) that
// returns the total + preview atomically — exercised via backlogSummary.
func TestBacklogSummary_AtomicViaWorkerJob(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	for i := int64(1); i <= 5; i++ {
		_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: i, Text: "m", Timestamp: time.Now()})
	}

	count, items := b.backlogSummary(key)
	if count != 5 {
		t.Fatalf("backlog total = %d, want 5", count)
	}
	if len(items) != backlogSummaryMax {
		t.Fatalf("backlog preview has %d items, want %d (capped)", len(items), backlogSummaryMax)
	}
	if items[0].MessageID != 1 {
		t.Fatalf("backlog preview must be oldest-first; first id = %d, want 1", items[0].MessageID)
	}

	// The backlog read is a PEEK — it must not have consumed anything.
	if n, _ := b.Queue.Pending(qrk); n != 5 {
		t.Fatalf("backlog summary must not consume; pending=%d, want 5", n)
	}
}

// The "ALSO" item: handleInboundDelivered surfaces a dropped consume. We can't
// easily force a full worker queue here, but we CAN assert the happy path still
// dispatches the consume (the !ok branch only logs). This guards that the Submit
// return value is consulted (it compiles + runs through the ok branch).
func TestHandleInboundDelivered_DispatchesConsume(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 1, Text: "m", Timestamp: time.Now()})
	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)
	stub.MarkRouteConfirmed() // live-push ack consume requires a confirmed claim (§5 tripwire)
	b.Workers.mu.Lock()
	w := b.Workers.spawnLocked(key)
	b.Workers.mu.Unlock()
	w.recordCoveredByPush(1, []int64{1})
	// The other half of the synthetic push: the ack is routed by the route the
	// push went out on, recorded on the stub at push time (see Stub.pushRoutes).
	// Without it this fabricated ack matches no push and consumes nothing.
	stub.RecordPushRoute(1, key)

	raw, _ := json.Marshal(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, UpdateID: 1, OK: true, Count: 1})
	b.handleInboundDelivered(stub, raw)

	deadline := time.Now().Add(2 * time.Second)
	for {
		if n, _ := b.Queue.Pending(qrk); n == 0 {
			break
		}
		if time.Now().After(deadline) {
			n, _ := b.Queue.Pending(qrk)
			t.Fatalf("ack(Count=1) should consume the single queued line; pending=%d, want 0", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
