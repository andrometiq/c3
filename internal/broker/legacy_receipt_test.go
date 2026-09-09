package broker

import (
	"context"
	"testing"
	"time"
)

func TestLegacyLateAcceptanceKeepsDeclaredMilestone(t *testing.T) {
	// Only transcript-confirming legacy adapters have the observation deadline.
	// Other legacy hosts can spend longer accepting a submission (for example
	// an old adapter waiting for its host's foreground tool to complete).
	b, w, s, frames := shadowFixture(t)
	push := shadowPushOne(t, w, frames, time.Now())
	w.updateAttempt(push.DeliveryToken, func(a *attemptRecord) { a.Deadline = time.Now().Add(-time.Second) })
	s.ReceiptConfirming = false
	w.handleConsume(context.Background(), &ConsumeJob{Owner: s, Token: push.DeliveryToken, MessageID: push.Inbound.MessageID, Count: 1})
	if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 0 {
		t.Fatal("changed legacy accept-based timing")
	}
}

func TestNoticePendingIntersectsSurvivingAttemptIDs(t *testing.T) {
	b, w, s, frames := shadowFixture(t)
	push := shadowPushOne(t, w, frames, time.Now())
	if n := b.noticePending(w.key, s); n != 0 {
		t.Fatal("attempt counted as Held", n)
	}
	queued := inboundOn(w.key.ChatID, nil, 2, "still queued")
	if _, err := b.Queue.AppendTracked(queueRouteKey(w.key), queued); err != nil {
		t.Fatal(err)
	}
	if n := b.noticePending(w.key, s); n != 1 {
		t.Fatal("queued row hidden", n)
	}
	if _, err := b.Queue.RemoveRecordIDs(queueRouteKey(w.key), push.RecordIDs); err != nil {
		t.Fatal(err)
	}
	// The observation can lag the durable removal; its stale member must not
	// subtract an unrelated queued row from the notice.
	if n := b.noticePending(w.key, s); n != 1 {
		t.Fatal("stale attempt member hid a surviving queued row", n)
	}
}
