package telegram

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"
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
