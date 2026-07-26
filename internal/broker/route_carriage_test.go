package broker

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// Release audit 2026-07-26 — authoritative state was RE-INFERRED at use time
// instead of being CARRIED with the thing that depends on it.
//
// Two instances, both in the delivered-ack path:
//
//  1. ipc.InboundDeliveredMsg carries no route, so the DESTRUCTIVE consume was
//     dispatched to stub.CurrentRoute() *at ack time* rather than to the route
//     the push actually went out on. A topic switch between push and ack — the
//     grok adapter acks only after injectWithRetry wins, up to ~2 min of
//     mid-turn backoff — lands the consume on a worker that never made the push.
//  2. A reconnect transferred the claim in Routes but never re-bound the new
//     stub, so Routes and the stub disagreed about who holds the topic.

// fastDebounceTelegram is mfWithTelegram with the inbound debounce cut to a few
// milliseconds, so a test that drives TWO real pushes does not spend 3s waiting
// out the 1500ms default window.
func fastDebounceTelegram() *mappings.MappingsFile {
	mf := mfWithTelegram()
	cc := mf.Channels["telegram"]
	cc.DebounceMS = 20
	mf.Channels["telegram"] = cc
	return mf
}

func inboundOn(chatID int64, topicID *int64, msgID int64, text string) *c3types.Inbound {
	return &c3types.Inbound{
		Channel: "telegram", ChatID: chatID, TopicID: topicID,
		MessageID: msgID, Text: text, Timestamp: time.Now(),
	}
}

func waitPush(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("no live push observed")
	}
}

// pendingWithin polls a route's pending count until it reaches want or the
// deadline expires, then returns whatever it last read. The consume is dispatched
// asynchronously onto the route's worker, so a bare read races it.
func pendingWithin(t *testing.T, b *Broker, key RouteKey, want int) int {
	t.Helper()
	qrk := queueRouteKey(key)
	deadline := time.Now().Add(2 * time.Second)
	for {
		n, _ := b.Queue.Pending(qrk)
		if n == want || time.Now().After(deadline) {
			return n
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The destructive case, end to end: one session, two topics in DIFFERENT chats.
// Telegram message ids are unique per CHAT, not globally, so the same id can be
// outstanding on both — which is exactly what lets a mis-routed ack destroy a
// real line rather than merely miss.
//
// This drives REAL pushes through the worker pool (not a hand-planted
// coveredByPush entry), so it exercises the production record site as well as the
// routing decision.
func TestInboundDelivered_LateAckAfterCrossChatSwitch_ConsumesPushedRoute(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, fastDebounceTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tidA, tidB := int64(281), int64(412)
	keyA := MakeRouteKey("telegram", -100, &tidA) // group 1
	keyB := MakeRouteKey("telegram", -200, &tidB) // group 2 — ids collide across chats

	stub, pushed := liveHolder(t, b, keyA)
	stub.SetRoute(&keyA)
	stub.MarkRouteConfirmed()

	// Message 42 arrives on A and is pushed live. The adapter is mid-turn and has
	// NOT acked yet (grok backs off up to ~2min inside injectWithRetry).
	if !b.Workers.Submit(keyA, Job{Kind: JobInbound, Inbound: inboundOn(-100, &tidA, 42, "the-A-message")}) {
		t.Fatal("submit inbound on route A")
	}
	waitPush(t, pushed)

	// Mid-turn, the agent attaches to a topic in ANOTHER group.
	b.Routes.Claim(keyB, stub)
	b.Routes.Release(keyA, stub.ConnID)
	stub.SetRoute(&keyB)
	stub.MarkRouteConfirmed()

	// A message with the SAME id arrives there and is pushed live too.
	if !b.Workers.Submit(keyB, Job{Kind: JobInbound, Inbound: inboundOn(-200, &tidB, 42, "the-B-message")}) {
		t.Fatal("submit inbound on route B")
	}
	waitPush(t, pushed)

	if n := pendingWithin(t, b, keyA, 1); n != 1 {
		t.Fatalf("setup: route A pending=%d, want 1", n)
	}
	if n := pendingWithin(t, b, keyB, 1); n != 1 {
		t.Fatalf("setup: route B pending=%d, want 1", n)
	}

	// The late ack for A's push finally lands, while the stub holds B.
	raw, _ := json.Marshal(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, UpdateID: 42, OK: true, Count: 1})
	b.handleInboundDelivered(stub, raw)

	if n := pendingWithin(t, b, keyA, 0); n != 0 {
		t.Errorf("the ack must consume the line on the route the push went out on; route A pending=%d, want 0 — the delivered line stays queued and fetch_queue hands it out again as a duplicate, with a permanently inflated held count", n)
	}
	if n := pendingWithin(t, b, keyB, 1); n != 1 {
		t.Errorf("DATA LOSS: the ack for route A's push consumed route B's line; route B pending=%d, want 1 — the delivered-ack was dispatched to the stub's CURRENT route instead of the route the push actually went out on", n)
	}
}

// The amplifier: handleConsume's by-identity branch ignores job.Count entirely —
// it removes every id in the record it finds. So a stray ack claiming ONE line
// removes however many the wrong worker's record holds; if that was a merged
// batch, the whole batch goes. Routing the ack by the pushed route is what keeps
// "an ack can never delete more than it claims" true.
func TestInboundDelivered_StrayAckNeverDeletesTheWrongRoutesMergedBatch(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, fastDebounceTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tidA, tidB := int64(281), int64(412)
	keyA := MakeRouteKey("telegram", -100, &tidA)
	keyB := MakeRouteKey("telegram", -200, &tidB)

	stub, pushed := liveHolder(t, b, keyA)
	stub.SetRoute(&keyA)
	stub.MarkRouteConfirmed()

	// A real push of one line on A, un-acked.
	if !b.Workers.Submit(keyA, Job{Kind: JobInbound, Inbound: inboundOn(-100, &tidA, 42, "the-A-message")}) {
		t.Fatal("submit inbound on route A")
	}
	waitPush(t, pushed)

	// B holds an outstanding MERGED push of three lines under the same id.
	qrkB := queueRouteKey(keyB)
	for _, id := range []int64{40, 41, 42} {
		if err := b.Queue.Append(qrkB, inboundOn(-200, &tidB, id, "b")); err != nil {
			t.Fatal(err)
		}
	}
	b.Workers.mu.Lock()
	wB := b.Workers.spawnLocked(keyB)
	b.Workers.mu.Unlock()
	wB.recordCoveredByPush(42, []int64{40, 41, 42})

	// The agent switches to B mid-turn; A's ack lands afterwards claiming ONE line.
	b.Routes.Claim(keyB, stub)
	b.Routes.Release(keyA, stub.ConnID)
	stub.SetRoute(&keyB)
	stub.MarkRouteConfirmed()

	raw, _ := json.Marshal(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, UpdateID: 42, OK: true, Count: 1})
	b.handleInboundDelivered(stub, raw)

	if n := pendingWithin(t, b, keyA, 0); n != 0 {
		t.Errorf("the ack must consume the line on the route the push went out on; route A pending=%d, want 0", n)
	}
	if n := pendingWithin(t, b, keyB, 3); n != 3 {
		t.Errorf("DATA LOSS, amplified: an ack claiming Count=1 removed %d of route B's 3 lines (pending=%d, want 3) — handleConsume's by-id branch removes the WHOLE record it finds, so a mis-routed ack destroys a merged batch, not one line", 3-n, n)
	}
}

// The by-identity guard in handleConsume is the LAST line of defence, and after
// the routing fix the older test that named it (route_confirmed_test.go's
// TestHandleInboundDelivered_RequiresConfirmedRouteAndDeliveryIdentity) no longer
// reaches it: its fabricated ack matches no recorded push, so the consume is now
// dropped in handleInboundDelivered before the worker is ever consulted. This
// pins the guard on the path that DOES reach it — a genuine push record, no
// covered-line identities — so a re-introduced "just drop Count lines off the
// head" fallback fails here.
func TestConsume_PushRouteKnownButNoCoveredIdentity_LeavesQueueIntact(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(281)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queueRouteKey(key)
	for i := int64(1); i <= 3; i++ {
		if err := b.Queue.Append(qrk, inboundOn(-100, &tid, i, "m")); err != nil {
			t.Fatal(err)
		}
	}
	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)
	stub.MarkRouteConfirmed()
	// The push route IS known — only the covered-line identities are missing (the
	// push covered lines whose ids the caller could not enumerate, or the worker
	// was reaped between push and ack).
	stub.RecordPushRoute(7, key)

	raw, _ := json.Marshal(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, UpdateID: 7, OK: true, Count: 2})
	b.handleInboundDelivered(stub, raw)

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n, _ := b.Queue.Pending(qrk); n != 3 {
			t.Fatalf("an ack whose covered-line identities are unknown consumed the queue HEAD; pending dropped to %d, want 3 — head-dropping destroys older backlog nobody has seen and leaves the delivered line queued", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// reconnectHello drives a hello for the given logical session on a fresh conn and
// waits for the ack, which the broker writes only AFTER the reconnect branch has
// run. Returns the peer so the caller can keep the conn open.
func reconnectHello(t *testing.T, b *Broker, pid int, cwd string) *ipc.Conn {
	t.Helper()
	peer, done := peerPair(t, b)
	t.Cleanup(done)
	if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: pid, CWD: cwd}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	return peer
}

// A reconnect must carry the CLAIM onto the new stub, not just the routing-table
// entry. Routes saying "this stub holds the topic" while the stub says it holds
// nothing is two views of one fact disagreeing — and the adapter's attach replay
// is not a fix, because it can fail (write error, ValidateTopic error, nothing to
// replay), in which case the split is permanent.
func TestHelloReconnect_RebindsClaimOntoNewStub(t *testing.T) {
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(281)
	key := MakeRouteKey("telegram", -100, &tid)
	pid, cwd := os.Getpid(), "/proj"

	reconnectHello(t, b, pid, cwd)
	stubs := b.Stubs.Snapshot()
	if len(stubs) != 1 {
		t.Fatalf("setup: %d stubs registered, want 1", len(stubs))
	}
	old := stubs[0]
	// Claim exactly as tryClaim does.
	b.Routes.Claim(key, old)
	old.SetRoute(&key)
	old.MarkRouteConfirmed()

	// Same logical session (CLI, PID, CWD) reconnecting on a fresh conn.
	reconnectHello(t, b, pid, cwd)

	holder, claimed := b.Routes.Holder(key)
	if !claimed {
		t.Fatal("reconnect dropped the claim entirely")
	}
	if holder == old {
		t.Fatal("setup: the claim was not transferred to the new stub")
	}
	if got := holder.CurrentRoute(); got == nil || *got != key {
		t.Errorf("reconnect transferred the claim in Routes but left the new stub UNBOUND (CurrentRoute=%v, want %v) — every stub-derived path reads stub.CurrentRoute(), so reply/react/poll answer \"no route claimed\" and a bare attach falls through to the picker while inbound keeps arriving on this same stub", got, key)
	}
	if !holder.RouteConfirmed() {
		t.Errorf("reconnect left routeConfirmed FALSE on the new stub — the §5 tripwire then drops every delivered-ack consume, so delivered lines stay queued and fetch_queue hands them out again with an inflated held count")
	}
}

// The delivered-ack records are per-CONNECTION, so a reconnect must carry them
// too: an ack that arrives after the adapter came back still has to resolve to
// the route its push went out on.
func TestHelloReconnect_AckForPrereconnectPushStillConsumes(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(281)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queueRouteKey(key)
	if err := b.Queue.Append(qrk, inboundOn(-100, &tid, 1, "m")); err != nil {
		t.Fatal(err)
	}
	pid, cwd := os.Getpid(), "/proj"

	reconnectHello(t, b, pid, cwd)
	stubs := b.Stubs.Snapshot()
	if len(stubs) != 1 {
		t.Fatalf("setup: %d stubs registered, want 1", len(stubs))
	}
	old := stubs[0]
	b.Routes.Claim(key, old)
	old.SetRoute(&key)
	old.MarkRouteConfirmed()

	// A live push made on the OLD connection, not yet acked.
	b.Workers.mu.Lock()
	w := b.Workers.spawnLocked(key)
	b.Workers.mu.Unlock()
	w.recordCoveredByPush(1, []int64{1})
	old.RecordPushRoute(1, key)

	// The adapter reconnects, then its ack for that push finally lands.
	reconnectHello(t, b, pid, cwd)
	holder, claimed := b.Routes.Holder(key)
	if !claimed || holder == old {
		t.Fatal("setup: the claim was not transferred to a new stub")
	}
	raw, _ := json.Marshal(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, UpdateID: 1, OK: true, Count: 1})
	b.handleInboundDelivered(holder, raw)

	if n := pendingWithin(t, b, key, 0); n != 0 {
		t.Errorf("the ack for a push made BEFORE the reconnect consumed nothing (pending=%d, want 0) — the reconnect did not carry the push→route records onto the new stub, so the delivered line stays queued and is handed out again by fetch_queue", n)
	}
}

// A stub holds at most one claim (tryClaim releases the old route before claiming
// the new one). TransferAllByConnID ranges a map, so if that invariant is ever
// broken, "bind the first transferred key" would bind a NONDETERMINISTIC one of
// N — silently, and differently on each run. Report it and leave the route
// unbound instead.
func TestHelloReconnect_MultipleClaims_LeavesRouteUnbound(t *testing.T) {
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tidA, tidB := int64(281), int64(412)
	keyA := MakeRouteKey("telegram", -100, &tidA)
	keyB := MakeRouteKey("telegram", -200, &tidB)
	pid, cwd := os.Getpid(), "/proj"

	reconnectHello(t, b, pid, cwd)
	stubs := b.Stubs.Snapshot()
	if len(stubs) != 1 {
		t.Fatalf("setup: %d stubs registered, want 1", len(stubs))
	}
	old := stubs[0]
	// Deliberately violate single-claim-per-stub.
	b.Routes.Claim(keyA, old)
	b.Routes.Claim(keyB, old)
	old.SetRoute(&keyA)
	old.MarkRouteConfirmed()

	reconnectHello(t, b, pid, cwd)

	holder, claimed := b.Routes.Holder(keyA)
	if !claimed || holder == old {
		t.Fatal("setup: the claims were not transferred to a new stub")
	}
	if got := holder.CurrentRoute(); got != nil {
		t.Errorf("reconnect bound one of 2 transferred claims (CurrentRoute=%v) — the choice comes from a map range and is nondeterministic, so a broken single-claim invariant must be reported and left unbound, never guessed at", *got)
	}
	if holder.RouteConfirmed() {
		t.Error("reconnect confirmed a route it never bound")
	}
}
