package telegram

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
)

// Item A (shutdown-save): on loop exit the poll loop must persist the highest
// durably-committed offset, even when the final advance landed AFTER the
// per-batch save already ran for that iteration (the realistic gap: the broker's
// async persist callback MarkDone-s an in-flight update between the per-batch
// Save and shutdown). Without the final defer-Save, that last advance would be
// lost and a restart would re-deliver it.
//
// Construction: the bot delivers ONE allowed message update (update_id=5). It is
// Registered in-flight and Emitted, but no persist callback fires in this test,
// so committed stays 0 through the per-batch Save (it can't pass an in-flight
// update) — lastSaved stays 0 and the per-batch path persists nothing. The bot's
// SECOND call then BLOCKS until ctx is cancelled, parking the loop inside
// getUpdates so no per-batch Save can race. We externally MarkDone(5) (the
// worker's post-Append callback), advancing committed to 5, then cancel. Only the
// shutdown defer can persist 5 — the test fails without it.
func TestPollLoop_ShutdownSavesCommittedOffset(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())

	h := &fakeHost{} // default decision = GateInboundAllow; Emit returns true
	fb := &funcBotClient{}
	c := newConflictTestChannel(h, fb)
	// Seed committed at 4 (a prior resume point) so the delivered update_id=5 is the
	// next contiguous id and MarkDone(5) advances the committed prefix to 5.
	c.offTrk = newOffsetTracker(4)
	c.msgToUpdate = map[seamKey][]int64{}
	store, err := newOffsetStore("telegram")
	if err != nil {
		t.Fatalf("newOffsetStore: %v", err)
	}
	c.offsets = store

	parked := make(chan struct{})
	var parkedOnce bool
	fb.fn = func(call int) (json.RawMessage, error) {
		if call == 1 {
			// update_id is contiguous from the seeded committed (4) so MarkDone(5)
			// can advance the committed prefix to 5.
			upd := []gotgbot.Update{{
				UpdateId: 5,
				Message: &gotgbot.Message{
					MessageId: 500,
					Date:      time.Now().Unix(),
					Chat:      gotgbot.Chat{Id: -100, Type: "supergroup"},
					From:      &gotgbot.User{Id: 7, Username: "u"},
					Text:      "hello",
				},
			}}
			raw, _ := json.Marshal(upd)
			return raw, nil
		}
		// Call 2+: BLOCK until the loop's ctx is cancelled. Parks the loop inside
		// getUpdates so NO per-batch Save runs between our external MarkDone(5) and
		// cancel — making the shutdown defer the ONLY path that can persist the
		// final advance.
		if !parkedOnce {
			parkedOnce = true
			close(parked)
		}
		<-c.ctx.Done()
		return nil, c.ctx.Err()
	}

	done := startPollLoop(c)

	// Wait until the loop has consumed the first batch and parked in call 2.
	select {
	case <-parked:
	case <-time.After(2 * time.Second):
		c.cancel()
		awaitDone(t, done)
		t.Fatal("loop never reached the second (blocking) getUpdates; first batch not processed")
	}

	// The message (update 5) was Emitted and remains in-flight, so the per-batch
	// Save persisted nothing.
	if h.emitCount() < 1 {
		c.cancel()
		awaitDone(t, done)
		t.Fatal("message update was never emitted; in-flight→persist seam not exercised")
	}
	if got, _ := store.Load(); got != 0 {
		c.cancel()
		awaitDone(t, done)
		t.Fatalf("pre-shutdown persisted offset = %d, want 0 (in-flight update must not be saved)", got)
	}

	// Simulate the broker's async persist callback finishing the Append → committed
	// advances to 5, with the per-batch save already past (loop is parked).
	c.offTrk.MarkDone(5)
	if got := c.offTrk.Committed(); got != 5 {
		c.cancel()
		awaitDone(t, done)
		t.Fatalf("committed = %d, want 5 after MarkDone(5)", got)
	}

	// Shut down. Only the shutdown defer can persist the committed=5 advance.
	c.cancel()
	awaitDone(t, done)

	got, err := store.Load()
	if err != nil {
		t.Fatalf("Load after shutdown: %v", err)
	}
	if got != 5 {
		t.Fatalf("persisted offset after shutdown = %d, want 5 (shutdown defer-Save lost the final advance)", got)
	}
}

// Telegram update_ids do not begin at 1, and allowed-update filtering can leave
// numeric holes. A returned batch must advance over those holes while still
// holding behind the earliest OBSERVED update whose durable persist is pending.
// This exercises the full poll → register → seam → persist callback wiring from
// a fresh offset store, including out-of-order worker completion.
func TestPollLoop_FreshGappedBatchOutOfOrderPersist(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())

	h := &fakeHost{}
	fb := &funcBotClient{}
	c := newConflictTestChannel(h, fb)
	c.offTrk = newOffsetTracker(0)
	c.msgToUpdate = map[seamKey][]int64{}
	store, err := newOffsetStore("telegram")
	if err != nil {
		t.Fatalf("newOffsetStore: %v", err)
	}
	c.offsets = store

	const (
		firstID  = int64(483747726)
		secondID = int64(483747731)
		chatID   = int64(-100)
	)
	batch := []gotgbot.Update{
		{
			UpdateId: firstID,
			Message: &gotgbot.Message{
				MessageId: 501,
				Date:      time.Now().Unix(),
				Chat:      gotgbot.Chat{Id: chatID, Type: "supergroup"},
				From:      &gotgbot.User{Id: 7, Username: "u"},
				Text:      "first",
			},
		},
		{
			UpdateId: secondID,
			Message: &gotgbot.Message{
				MessageId: 502,
				Date:      time.Now().Unix(),
				Chat:      gotgbot.Chat{Id: chatID, Type: "supergroup"},
				From:      &gotgbot.User{Id: 7, Username: "u"},
				Text:      "second",
			},
		},
	}
	raw, _ := json.Marshal(batch)
	parked := make(chan struct{})
	fb.fn = func(call int) (json.RawMessage, error) {
		if call == 1 {
			return raw, nil
		}
		select {
		case <-parked:
		default:
			close(parked)
		}
		<-c.ctx.Done()
		return nil, c.ctx.Err()
	}

	done := startPollLoop(c)
	select {
	case <-parked:
	case <-time.After(2 * time.Second):
		c.cancel()
		awaitDone(t, done)
		t.Fatal("poll loop did not process the fresh gapped batch")
	}
	if got := h.emitCount(); got != 2 {
		c.cancel()
		awaitDone(t, done)
		t.Fatalf("emitted updates = %d, want 2", got)
	}

	// The later worker finishes first. The earlier observed in-flight update
	// must hold the offset even though the ids have a numeric gap.
	c.onPersisted(&c3types.Inbound{ChatID: chatID, MessageID: 502})
	if got := c.offTrk.Committed(); got != 0 {
		c.cancel()
		awaitDone(t, done)
		t.Fatalf("committed past first observed in-flight update = %d, want 0", got)
	}

	c.onPersisted(&c3types.Inbound{ChatID: chatID, MessageID: 501})
	if got := c.offTrk.Committed(); got != secondID {
		c.cancel()
		awaitDone(t, done)
		t.Fatalf("committed after both persists = %d, want %d", got, secondID)
	}

	c.cancel()
	awaitDone(t, done)
	if got, err := store.Load(); err != nil || got != secondID {
		t.Fatalf("persisted offset after shutdown = %d, %v; want %d, nil", got, err, secondID)
	}
}

// Stop is the production lifecycle boundary: it must not return while
// pollLoop's deferred final Save is still running. Hold the real offset store's
// mutex to make that Save late, then prove Stop waits until the durable write
// finishes.
func TestChannelStop_WaitsForLateFinalOffsetSave(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())

	h := &fakeHost{}
	fb := &funcBotClient{}
	c := newConflictTestChannel(h, fb)
	c.offTrk = newOffsetTracker(4)
	c.msgToUpdate = map[seamKey][]int64{}
	store, err := newOffsetStore("telegram")
	if err != nil {
		t.Fatalf("newOffsetStore: %v", err)
	}
	c.offsets = store

	upd := []gotgbot.Update{{
		UpdateId: 5,
		Message: &gotgbot.Message{
			MessageId: 500,
			Date:      time.Now().Unix(),
			Chat:      gotgbot.Chat{Id: -100, Type: "supergroup"},
			From:      &gotgbot.User{Id: 7, Username: "u"},
			Text:      "hello",
		},
	}}
	raw, _ := json.Marshal(upd)
	parked := make(chan struct{})
	requestCanceled := make(chan struct{})
	fb.fn = func(call int) (json.RawMessage, error) {
		if call == 1 {
			return raw, nil
		}
		select {
		case <-parked:
		default:
			close(parked)
		}
		<-c.ctx.Done()
		close(requestCanceled)
		return nil, c.ctx.Err()
	}

	c.pollDone = make(chan struct{})
	go func() {
		defer close(c.pollDone)
		c.pollLoop()
	}()
	select {
	case <-parked:
	case <-time.After(2 * time.Second):
		c.cancel()
		t.Fatal("poll loop did not reach blocking getUpdates")
	}
	c.offTrk.MarkDone(5)

	store.mu.Lock()
	storeLocked := true
	defer func() {
		if storeLocked {
			store.mu.Unlock()
		}
	}()
	stopReturned := make(chan error, 1)
	go func() { stopReturned <- c.Stop() }()

	select {
	case <-requestCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not cancel the active getUpdates")
	}
	select {
	case err := <-stopReturned:
		t.Fatalf("Stop returned before final offset Save completed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	store.mu.Unlock()
	storeLocked = false
	select {
	case err := <-stopReturned:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after final offset Save completed")
	}
	if got, err := store.Load(); err != nil || got != 5 {
		t.Fatalf("persisted offset when Stop returned = %d, %v; want 5, nil", got, err)
	}
}

func TestChannelStop_BoundsPollWaitAndHandlesUnstarted(t *testing.T) {
	var unstarted Channel
	if err := unstarted.Stop(); err != nil {
		t.Fatalf("unstarted Stop: %v", err)
	}

	c := &Channel{
		host:         &fakeHost{},
		pollDone:     make(chan struct{}), // deliberately never closed
		pollStopWait: 20 * time.Millisecond,
	}
	start := time.Now()
	if err := c.Stop(); err != nil {
		t.Fatalf("bounded Stop: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < c.pollStopWait {
		t.Fatalf("Stop returned before configured bound: %v < %v", elapsed, c.pollStopWait)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Stop hung past configured bound: %v", elapsed)
	}
}
