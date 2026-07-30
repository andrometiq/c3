package telegram

import (
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

// The poll loop registers each accepted update as in-flight and only marks it
// done once the broker's persist callback fires (Append+fsync succeeded). An
// update whose Append is still in-flight must NOT let the committed offset pass
// it — otherwise a crash there loses the message (Telegram won't redeliver an
// already-acked offset). This exercises the exact Register → MarkDone(persist)
// seam the poll-loop wiring uses.
func TestPollOffsetWiring_NoAdvancePastUnpersisted(t *testing.T) {
	c := &Channel{}
	c.offTrk = newOffsetTracker(100)
	c.msgToUpdate = map[seamKey][]int64{}

	// The real persist callback the channel registers in Start (onPersisted).
	persist := c.onPersisted

	// One chat throughout: the seam is keyed by (chat_id, message_id), and the
	// callback must carry the SAME ChatID the stage used or it resolves nothing.
	const chatID = int64(-1001)

	// Batch of three accepted updates 101,102,103; stage (chat,msg)→update like
	// dispatchMessage does before Emit (FIFO seam).
	for _, p := range []struct{ msg, upd int64 }{{1, 101}, {2, 102}, {3, 103}} {
		c.offTrk.Register(p.upd)
		c.mu.Lock()
		c.seamStageLocked(chatID, p.msg, p.upd)
		c.mu.Unlock()
	}
	// 101 and 103 persist; 102 is still mid-STT/mid-Append (the "crash" point).
	persist(&c3types.Inbound{ChatID: chatID, MessageID: 1})
	persist(&c3types.Inbound{ChatID: chatID, MessageID: 3})
	if got := c.offTrk.Committed(); got != 101 {
		t.Fatalf("committed = %d, want 101 (must NOT pass unpersisted 102)", got)
	}
	// 102 finally persists → committed jumps to 103.
	persist(&c3types.Inbound{ChatID: chatID, MessageID: 2})
	if got := c.offTrk.Committed(); got != 103 {
		t.Fatalf("committed after 102 persisted = %d, want 103", got)
	}
}

// A gated/dropped/non-message/`/status` update is marked done immediately via
// markUpdateDone and must not block the completed observed prefix.
func TestPollOffsetWiring_MarkUpdateDoneUnblocks(t *testing.T) {
	c := &Channel{}
	c.offTrk = newOffsetTracker(200)
	c.offTrk.Register(201)
	c.offTrk.Register(202)
	c.markUpdateDone(202) // e.g. /status — handled, never persisted
	c.markUpdateDone(201)
	if got := c.offTrk.Committed(); got != 202 {
		t.Fatalf("committed = %d, want 202 (gated 202 must not block once 201 done)", got)
	}
}
