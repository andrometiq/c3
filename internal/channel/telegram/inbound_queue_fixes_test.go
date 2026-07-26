package telegram

import (
	"testing"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// A saturated route worker must HOLD the Telegram offset, not advance past the
// message.
//
// This inverts the original I4 behaviour. I4 marked the update done on a
// worker-queue-full drop so a >64 burst could not wedge the contiguous-prefix
// offset for every route — but the cost was silent, unrecoverable loss of a
// message the user sent, with no notice and no .trash copy. The maintainer's call
// (v0.1.0 release audit) is that redelivery churn is preferable to deliberate
// loss: Emit now absorbs a momentary burst (submitGraceWindow) and, if the route
// is still saturated, the update stays in-flight so Telegram redelivers it.
func TestDispatchMessage_EmitSaturated_HoldsOffsetForRedelivery(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow, emitDrops: true}
	c := makeChannel(h)
	c.offTrk = newOffsetTracker(100)
	c.msgToUpdate = map[int64][]int64{}
	// makeChannel leaves c.dedup nil. The poll loop always has one, and the
	// forget-on-saturation branch this test exists to cover is guarded by
	// `c.dedup != nil` — so without this the branch is never reached and the
	// dedup assertion below silently tests nothing.
	c.dedup = newUpdateDedup(2000, 5*time.Minute)

	// Record update 101 exactly as the poll loop would before dispatching it,
	// so there is a real entry for the saturated path to forget. dedupKey needs
	// the Message coordinates, not just the update id.
	seed := &gotgbot.Update{UpdateId: 101, Message: textMsg("hi", 42)}
	if c.dedup.SeenOrAdd(seed) {
		t.Fatal("precondition: update 101 must not already be recorded")
	}

	// Update 101 is the next contiguous id; its Emit will be DROPPED.
	c.offTrk.Register(101)
	c.dispatchMessage(101, textMsg("hi", 42), false, nil)

	// Emit was attempted (and dropped).
	if got := h.emitCount(); got != 1 {
		t.Fatalf("Emit should be attempted once; got %d", got)
	}
	// The committed offset MUST stay at 100: advancing past an unpersisted message
	// destroys it, because Telegram never redelivers an acknowledged update.
	if got := c.offTrk.Committed(); got != 100 {
		t.Fatalf("MESSAGE LOSS: offset advanced past a message that was never persisted; committed=%d, want 100 (held for redelivery)", got)
	}
	// The orphaned msgToUpdate seam must be cleared (no leak) — the redelivery
	// stages a fresh entry.
	c.mu.Lock()
	_, leaked := c.msgToUpdate[textMsg("hi", 42).MessageId]
	c.mu.Unlock()
	if leaked {
		t.Fatal("msgToUpdate entry for a saturated inbound must be cleared (leak)")
	}
	// And the poll-side dedup record must be forgotten, or the redelivery is
	// dedup-skipped and the "loss-free retry" never actually retries. SeenOrAdd
	// returning false means no entry survives — i.e. forget() ran.
	if c.dedup.SeenOrAdd(&gotgbot.Update{UpdateId: 101, Message: textMsg("hi", 42)}) {
		t.Fatal("dedup entry must be forgotten so the Telegram redelivery re-dispatches")
	}
}

// I4 companion: a SUCCESSFUL Emit must NOT mark the update done itself — that is
// the persist callback's job (offset advances only after Append+fsync). The seam
// stays staged and committed holds until the persist callback fires.
func TestDispatchMessage_EmitOK_LeavesUpdateInFlight(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow, emitDrops: false}
	c := makeChannel(h)
	c.offTrk = newOffsetTracker(100)
	c.msgToUpdate = map[int64][]int64{}

	c.offTrk.Register(101)
	c.dispatchMessage(101, textMsg("hi", 42), false, nil)

	// Emit accepted ⇒ the update is still in-flight (NOT done) until the broker's
	// persist callback MarkDone-s it. The offset must hold at 100.
	if got := c.offTrk.Committed(); got != 100 {
		t.Fatalf("a successfully-emitted (not-yet-persisted) update must keep the offset at 100; got %d", got)
	}
	// The seam must be staged (FIFO slice of one) for the persist callback to resolve.
	c.mu.Lock()
	ids, staged := c.msgToUpdate[textMsg("hi", 42).MessageId]
	c.mu.Unlock()
	if !staged || len(ids) != 1 || ids[0] != 101 {
		t.Fatalf("msgToUpdate must stage msg→[update] (got staged=%v ids=%v) so the persist callback can MarkDone it", staged, ids)
	}
}

// Item 2: two in-flight updates sharing a message_id (an original + an
// edited_message arriving during the original's persist window) must BOTH mark
// done, in FIFO stage order. The old scalar msgToUpdate overwrote the first —
// wedging it (never marked done) and marking the possibly-unpersisted second.
// The FIFO seam resolves the first-staged update on the first persist callback,
// the second on the second.
func TestSeam_TwoUpdatesSameMessageID_FIFOBothMarkDone(t *testing.T) {
	c := &Channel{}
	c.offTrk = newOffsetTracker(500)
	c.msgToUpdate = map[int64][]int64{}

	const msgID = 42
	// Stage update 501, then edited_message 502 for the SAME message_id.
	for _, uid := range []int64{501, 502} {
		c.offTrk.Register(uid)
		c.mu.Lock()
		c.seamStageLocked(msgID, uid)
		c.mu.Unlock()
	}

	// First persist callback resolves the FIRST-staged update (501).
	c.onPersisted(&c3types.Inbound{MessageID: msgID})
	if got := c.offTrk.Committed(); got != 501 {
		t.Fatalf("first persist must MarkDone the first-staged update; committed=%d, want 501", got)
	}
	// Second persist callback resolves the second (502).
	c.onPersisted(&c3types.Inbound{MessageID: msgID})
	if got := c.offTrk.Committed(); got != 502 {
		t.Fatalf("second persist must MarkDone the second-staged update; committed=%d, want 502", got)
	}
	// Seam fully drained (key removed) — no leak.
	c.mu.Lock()
	_, leaked := c.msgToUpdate[msgID]
	c.mu.Unlock()
	if leaked {
		t.Fatal("seam must be empty after both updates resolved (no leak)")
	}
}

// Item 2 (cleanup path): when a SECOND update sharing a message_id has its Emit
// DROPPED, the cleanup must remove EXACTLY the entry it just staged — not the
// front, which belongs to an earlier still-in-flight update on the same
// message_id. It marks the dropped update done and leaves the earlier one staged.
func TestSeam_EmitDropRemovesOnlyTheDroppedEntry(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow, emitDrops: true}
	c := makeChannel(h)
	c.offTrk = newOffsetTracker(500)
	c.msgToUpdate = map[int64][]int64{}

	const msgID = 100 // textMsg uses MessageId=100
	// An earlier update 501 for this message_id is already staged + in-flight
	// (Emitted OK previously, not yet persisted).
	c.offTrk.Register(501)
	c.mu.Lock()
	c.seamStageLocked(msgID, 501)
	c.mu.Unlock()

	// A second update 502 (an edit) for the SAME message_id arrives; its route is
	// saturated. The cleanup must remove EXACTLY 502 (just staged), never the
	// front 501, which belongs to an earlier still-in-flight update.
	c.offTrk.Register(502)
	c.dispatchMessage(502, textMsg("edit", 42), true, nil)

	c.mu.Lock()
	ids := c.msgToUpdate[msgID]
	c.mu.Unlock()
	if len(ids) != 1 || ids[0] != 501 {
		t.Fatalf("saturation cleanup must leave the earlier in-flight 501 staged; seam=%v, want [501]", ids)
	}
	// BOTH stay in-flight now: 501 awaiting its persist, 502 held for redelivery.
	if got := c.offTrk.Committed(); got != 500 {
		t.Fatalf("501 still in-flight must hold committed at 500; got %d", got)
	}
	// Even after 501 persists, the prefix must NOT jump past 502 — 502 was never
	// persisted, and advancing over it would destroy it.
	c.onPersisted(&c3types.Inbound{MessageID: msgID})
	if got := c.offTrk.Committed(); got != 501 {
		t.Fatalf("MESSAGE LOSS: committed advanced past the unpersisted 502; got %d, want 501", got)
	}
}

// Item 1: on a durable Append FAILURE, the persist-failure path (onPersistFailed)
// must EVICT the update's poll-side dedup entry so the held-offset redelivery
// genuinely re-dispatches, while HOLDING the offset (loss-free). Without the
// eviction the redelivery is dedup-skipped and the "loss-free retry" never
// actually retries.
func TestDedupForget_AppendFailureReDispatchesAndHoldsOffset(t *testing.T) {
	c := &Channel{}
	c.offTrk = newOffsetTracker(600)
	c.dedup = newUpdateDedup(2000, 5*time.Minute)
	c.msgToUpdate = map[int64][]int64{}

	// First dispatch of update 601 (message_id 100): the poll loop records it in
	// the dedup, registers it in-flight, and dispatchMessage stages the seam.
	u := gotgbot.Update{UpdateId: 601, Message: textMsg("hi", 42)}
	if c.dedup.SeenOrAdd(&u) {
		t.Fatal("first SeenOrAdd of a fresh update must be false (recorded, dispatched)")
	}
	c.offTrk.Register(601)
	c.mu.Lock()
	c.seamStageLocked(u.Message.MessageId, 601)
	c.mu.Unlock()

	// The worker's Append FAILS → onPersistFailed fires. It must (a) NOT advance
	// the offset (loss-free hold) and (b) evict the poll-side dedup entry.
	c.onPersistFailed(&c3types.Inbound{MessageID: u.Message.MessageId})

	if got := c.offTrk.Committed(); got != 600 {
		t.Fatalf("Append failure must HOLD the offset (loss-free); committed=%d, want 600", got)
	}
	// The redelivery of the SAME update must NOT be dedup-skipped now — the entry
	// was forgotten, so SeenOrAdd returns false (genuine re-dispatch).
	u2 := gotgbot.Update{UpdateId: 601, Message: textMsg("hi", 42)}
	if c.dedup.SeenOrAdd(&u2) {
		t.Fatal("after onPersistFailed the same update must re-dispatch (dedup evicted), not skip")
	}
	// Seam drained by the failure callback (the redelivery re-stages it fresh).
	c.mu.Lock()
	_, leaked := c.msgToUpdate[u.Message.MessageId]
	c.mu.Unlock()
	if leaked {
		t.Fatal("onPersistFailed must pop the seam entry (redelivery re-stages fresh)")
	}
}

// Convergence guard for the Windows "spam loop" class (Bug B): a persist FAILURE
// holds the offset and re-dispatches, and then the retry's persist SUCCESS
// advances the committed offset OVER that update — so the loss-free retry is
// BOUNDED, not a permanent live-lock. On Windows a directory-fsync that always
// failed made queue Append() never succeed, so the offset never advanced and
// Telegram redelivered the same updates ~434x each. That syscall trigger is
// Windows-only (and now guarded), but this locks the platform-agnostic
// invariant it violated: one successful persist terminates the retry loop.
func TestPersistFailThenSuccess_ConvergesAndAdvancesOffset(t *testing.T) {
	c := &Channel{}
	c.offTrk = newOffsetTracker(600)
	c.dedup = newUpdateDedup(2000, 5*time.Minute)
	c.msgToUpdate = map[int64][]int64{}

	// First dispatch of update 601 (message_id 42): poll loop records + registers
	// + stages the seam.
	u := gotgbot.Update{UpdateId: 601, Message: textMsg("hi", 42)}
	if c.dedup.SeenOrAdd(&u) {
		t.Fatal("first SeenOrAdd of a fresh update must be false (recorded, dispatched)")
	}
	c.offTrk.Register(601)
	c.mu.Lock()
	c.seamStageLocked(u.Message.MessageId, 601)
	c.mu.Unlock()

	// Append FAILS → offset HELD at 600 (loss-free), dedup evicted, seam popped.
	c.onPersistFailed(&c3types.Inbound{MessageID: u.Message.MessageId})
	if got := c.offTrk.Committed(); got != 600 {
		t.Fatalf("after Append failure committed=%d, want 600 (held, loss-free)", got)
	}

	// Telegram redelivers the SAME update: not dedup-skipped (genuine
	// re-dispatch), and the poll loop re-registers + re-stages it.
	u2 := gotgbot.Update{UpdateId: 601, Message: textMsg("hi", 42)}
	if c.dedup.SeenOrAdd(&u2) {
		t.Fatal("redelivery of the same update must re-dispatch (dedup evicted), not skip")
	}
	c.offTrk.Register(601)
	c.mu.Lock()
	c.seamStageLocked(u2.Message.MessageId, 601)
	c.mu.Unlock()

	// This time Append SUCCEEDS → onPersisted MarkDone's 601 and the committed
	// offset advances OVER it. The retry loop terminates here; without a
	// terminating success this is the Windows spam loop.
	c.onPersisted(&c3types.Inbound{MessageID: u2.Message.MessageId})
	if got := c.offTrk.Committed(); got != 601 {
		t.Fatalf("after recovery committed=%d, want 601 (converged — offset advanced)", got)
	}
	// Seam fully drained after the success — no leak.
	c.mu.Lock()
	_, leaked := c.msgToUpdate[u2.Message.MessageId]
	c.mu.Unlock()
	if leaked {
		t.Fatal("seam entry must be drained after onPersisted")
	}
}

// Item 1 (dedup unit): forget(update_id) evicts exactly that update's entry and
// leaves others intact.
func TestUpdateDedup_ForgetEvictsOnlyThatUpdate(t *testing.T) {
	d := newUpdateDedup(2000, 5*time.Minute)
	a := gotgbot.Update{UpdateId: 701, Message: textMsg("a", 42)}
	b := gotgbot.Update{UpdateId: 702, Message: textMsg("b", 43)}
	d.SeenOrAdd(&a)
	d.SeenOrAdd(&b)

	d.forget(701)

	if d.SeenOrAdd(&a) {
		t.Fatal("forgotten update 701 must re-dispatch (SeenOrAdd false)")
	}
	// 702 was never forgotten — still deduped.
	b2 := gotgbot.Update{UpdateId: 702, Message: textMsg("b", 43)}
	if !d.SeenOrAdd(&b2) {
		t.Fatal("update 702 must still be deduped (forget(701) must not evict it)")
	}
}

// I-SEC: a /status from a NON-allowlisted sender must be DROPPED — no reply, no
// Emit, no global summary leak — and the update marked done (no stall). The gate
// runs BEFORE the /status intercept, so a stranger's "/status" hits the silent
// default-deny drop and never reaches HandleCommand.
func TestDispatchMessage_StatusFromStranger_DroppedNotAnswered(t *testing.T) {
	// cmdHandled=true would answer /status IF the intercept were reached — it must
	// NOT be, because the gate DROPS first.
	h := &fakeHost{decision: channel.GateInboundDrop, cmdHandled: true}
	c := makeChannel(h)
	c.offTrk = newOffsetTracker(200)
	c.msgToUpdate = map[int64][]int64{}

	c.offTrk.Register(201)
	// No recover needed: the gate drops before any SendReply, so the nil bot in
	// makeChannel is never touched.
	c.dispatchMessage(201, textMsg("/status", 99), false, nil)

	if got := h.emitCount(); got != 0 {
		t.Fatalf("a dropped /status must not Emit; got %d", got)
	}
	// HandleCommand must NOT have been called — the gate dropped first, so the
	// stranger never reaches the /status intercept (no reply, no summary leak).
	if h.handleCommandCalled() {
		t.Fatal("a non-allowlisted /status must NOT reach HandleCommand (no reply, no global summary leak)")
	}
	// The dropped update must be marked done so it doesn't wedge the offset.
	if got := c.offTrk.Committed(); got != 201 {
		t.Fatalf("dropped /status must MarkDone its update; committed=%d, want 201", got)
	}
}

// I-SEC companion: an allowlisted /status IS still answered (handled by the
// broker, never routed to an agent), and its update is marked done.
func TestDispatchMessage_StatusFromAllowlisted_StillAnswered(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow, cmdHandled: true}
	c := makeChannel(h)
	c.offTrk = newOffsetTracker(300)
	c.msgToUpdate = map[int64][]int64{}

	c.offTrk.Register(301)
	func() {
		defer func() { _ = recover() }() // tolerate nil-bot SendReply in the intercept
		c.dispatchMessage(301, textMsg("/status", 42), false, nil)
	}()

	if !h.handleCommandCalled() {
		t.Fatal("an allowlisted /status must reach HandleCommand (answered, not routed)")
	}
	if got := h.emitCount(); got != 0 {
		t.Fatalf("an answered /status must NOT be routed to an agent; Emit=%d, want 0", got)
	}
	if got := c.offTrk.Committed(); got != 301 {
		t.Fatalf("an answered /status must MarkDone its update; committed=%d, want 301", got)
	}
}
