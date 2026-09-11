package broker

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHeldNoticeQueueReadErrorNeverClaimsHeld(t *testing.T) {
	for _, negotiated := range []bool{false, true} {
		for _, cached := range []int{0, 1} {
			t.Run(fmt.Sprintf("negotiated=%v/cached=%d", negotiated, cached), func(t *testing.T) { heldReadError(t, negotiated, cached) })
		}
	}
}

func heldReadError(t *testing.T, negotiated bool, cached int) {
	logs := &matrixLogBuffer{}
	prior := log.Writer()
	log.SetOutput(logs)
	t.Cleanup(func() { log.SetOutput(prior) })
	b := injectionFixture(t, true)
	b.Workers.Stop()
	topic := int64(42)
	key := MakeRouteKey(TestInjectChannel, TestInjectChatID, &topic)
	var s *Stub
	if negotiated {
		s, _ = negotiatedHolder(t, b, key, 1)
		s.delivery.mu.Lock()
		s.delivery.live.Channel.Eligible = false
		s.delivery.mu.Unlock()
	} else {
		s, _ = liveHolderFrames(t, b, key)
		s.AddRoute(key)
		s.MarkRouteConfirmed(key)
		s.SetRenderRoute("queue_only", "test host unavailable", true)
	}
	path := filepath.Join(filepath.Dir(b.Queue.RetentionDir()), queueRouteKey(key).File()+".jsonl")
	if cached > 0 {
		in := inboundOn(key.ChatID, &topic, 1, "waiting sample")
		in.Channel = key.Channel
		if _, err := b.Queue.AppendTracked(queueRouteKey(key), in); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(path, path+".saved"); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Queue.PeekTracked(queueRouteKey(key), -1); err == nil {
		t.Fatal("read error was not injected")
	}
	if got, err := b.noticePending(key, s); got != cached || err == nil {
		t.Fatalf("cached pending=%d, want %d", got, cached)
	}

	b.evaluateNotices(key, true)
	settleNotice(t, b, key)
	if strings.Contains(logs.text(), "TEST SINK reply") {
		t.Fatal("unreadable queue claimed Held", logs.text())
	}

	if !strings.Contains(logs.text(), "Held count read failed") || !strings.Contains(logs.text(), fmt.Sprintf("using cached pending=%d", cached)) {
		t.Fatal("read failure was not logged", logs.text())
	}
}

func TestLegacyLateAcceptanceKeepsDeclaredMilestone(t *testing.T) {
	// Shadow observation deadlines never change legacy acceptance semantics.
	// The injection matrix also pins the transcript-confirming inbox fallback.
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
	if n, err := b.noticePending(w.key, s); err != nil || n != 0 {
		t.Fatal("attempt counted as Held", n)
	}
	queued := inboundOn(w.key.ChatID, nil, 2, "still queued")
	if _, err := b.Queue.AppendTracked(queueRouteKey(w.key), queued); err != nil {
		t.Fatal(err)
	}
	if n, err := b.noticePending(w.key, s); err != nil || n != 1 {
		t.Fatal("queued row hidden", n)
	}
	if _, err := b.Queue.RemoveRecordIDs(queueRouteKey(w.key), push.RecordIDs); err != nil {
		t.Fatal(err)
	}
	// The observation can lag the durable removal; its stale member must not
	// subtract an unrelated queued row from the notice.
	if n, err := b.noticePending(w.key, s); err != nil || n != 1 {
		t.Fatal("stale attempt member hid a surviving queued row", n)
	}
}
