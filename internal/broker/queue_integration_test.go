package broker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/karthikeyan5/c3/internal/c3types"
	"github.com/karthikeyan5/c3/internal/queue"
)

// No session attached → the inbound is queued (not dropped) AND a held-count
// auto-reply is sent (reusing the 5-min fallback cooldown).
func TestForwardOrFallback_NoSession_QueuesAndHeldReply(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	in := &c3types.Inbound{Channel: "telegram", ChatID: -1001234567890, TopicID: &tid, MessageID: 1, Text: "hello", Timestamp: time.Now()}
	w.forwardOrFallback(context.Background(), in, 1)

	qrk := queue.RouteKey{Channel: "telegram", ChatID: -1001234567890, TopicID: &tid}
	if n, _ := b.Queue.Pending(qrk); n != 1 {
		t.Fatalf("no-session inbound should be queued; pending=%d, want 1", n)
	}
	if got := len(fc.sendRepliesSnapshot()); got != 1 {
		t.Fatalf("expected one held-count auto-reply, got %d sends", got)
	}
	// Second message within cooldown: queued silently (no second reply).
	in2 := &c3types.Inbound{Channel: "telegram", ChatID: -1001234567890, TopicID: &tid, MessageID: 2, Text: "again", Timestamp: time.Now()}
	w.forwardOrFallback(context.Background(), in2, 1)
	if n, _ := b.Queue.Pending(qrk); n != 2 {
		t.Fatalf("second inbound should also queue; pending=%d, want 2", n)
	}
	if got := len(fc.sendRepliesSnapshot()); got != 1 {
		t.Fatalf("second inbound within cooldown must NOT send a second reply; got %d sends", got)
	}
}

// Debounce (msg 6083): on an edit-capable channel a BURST of holds coalesces
// into ONE held-notice per route per the HeldNotices window — not a fresh notice
// per message (which flooded, esp. under the Windows offset-loop). Both messages
// still queue; the second notice is suppressed within the window; never an
// in-place edit. After the window elapses a new hold re-alerts (leading-edge),
// preserving #36's per-message re-alert intent, just rate-limited.
func TestForwardOrFallback_NoSession_EditCapable_DebouncesHeldNotices(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	fc := &fakeChannel{editMessages: true, replyReturnID: 7001}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	// Two unattached inbounds for the SAME route, back to back (within the window).
	for i := 1; i <= 2; i++ {
		in := &c3types.Inbound{Channel: "telegram", ChatID: -1001234567890, TopicID: &tid, MessageID: int64(i), Text: "hi", Timestamp: time.Now()}
		w.forwardOrFallback(context.Background(), in, 1)
	}

	qrk := queue.RouteKey{Channel: "telegram", ChatID: -1001234567890, TopicID: &tid}
	if n, _ := b.Queue.Pending(qrk); n != 2 {
		t.Fatalf("both inbounds should be queued; pending=%d, want 2", n)
	}
	// The burst coalesces to exactly ONE held-notice (the second is within the
	// debounce window) — never an in-place edit.
	sends := fc.sendRepliesSnapshot()
	if len(sends) != 1 {
		t.Fatalf("a back-to-back burst must coalesce to ONE held-notice; got %d sends", len(sends))
	}
	if edits := fc.editCallsSnapshot(); len(edits) != 0 {
		t.Fatalf("edit-capable held path must not edit in place; got %d edits", len(edits))
	}
	if !strings.Contains(sends[0].Text, "message") || !strings.Contains(sends[0].Text, "queued") {
		t.Fatalf("held-notice should carry a queued-count line; got %q", sends[0].Text)
	}

	// After the debounce window elapses, a new hold re-alerts. Backdate the
	// tracker's last-notice timestamp instead of sleeping the full window.
	b.HeldNotices.mu.Lock()
	b.HeldNotices.lastByKey[key] = time.Now().Add(-defaultHeldNoticeCooldown - time.Second)
	b.HeldNotices.mu.Unlock()
	in3 := &c3types.Inbound{Channel: "telegram", ChatID: -1001234567890, TopicID: &tid, MessageID: 3, Text: "hi", Timestamp: time.Now()}
	w.forwardOrFallback(context.Background(), in3, 1)
	if got := len(fc.sendRepliesSnapshot()); got != 2 {
		t.Fatalf("a hold after the debounce window must re-alert; got %d sends, want 2", got)
	}
}

func TestHeldReplyText_CarriesCount(t *testing.T) {
	got := heldReplyText(3)
	// Pin a specific count-bearing phrase, not a stray '3'.
	if !strings.Contains(got, "3 messages queued") {
		t.Fatalf("heldReplyText(3) = %q, want '3 messages queued'", got)
	}
	if !strings.Contains(heldReplyText(1), "1 message queued") {
		t.Fatalf("heldReplyText(1) should use the singular '1 message queued'; got %q", heldReplyText(1))
	}
}

func TestFlushInbounds_DedupesReplayedMessageID(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100}

	in := &c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 7, Text: "hi", Timestamp: time.Now()}
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})
	// Simulate a crash-recovery replay of the SAME message_id.
	w.flushInbounds(context.Background(), []*c3types.Inbound{{Channel: "telegram", ChatID: -100, MessageID: 7, Text: "hi", Timestamp: time.Now()}})

	if n, _ := b.Queue.Pending(qrk); n != 1 {
		t.Fatalf("replayed message_id should be deduped; pending=%d, want 1", n)
	}
}
