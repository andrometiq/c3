package broker

import (
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/queue"
)

func TestBacklogSummary_PeeksOldestWithoutConsuming(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	for i := int64(1); i <= 5; i++ {
		_ = b.Queue.Append(qrk, &c3types.Inbound{
			Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: i,
			Sender: c3types.Sender{Username: "k"}, Text: "msg", Timestamp: time.Now(),
		})
	}
	count, items := b.backlogSummary(key)
	if count != 5 {
		t.Fatalf("backlog count = %d, want 5", count)
	}
	if len(items) == 0 || len(items) > 3 {
		t.Fatalf("summary items = %d, want 1..3 (compact preview)", len(items))
	}
	if items[0].MessageID != 1 {
		t.Errorf("first summary item msg = %d, want 1 (oldest)", items[0].MessageID)
	}
	// Peek must NOT consume.
	if n, _ := b.Queue.Pending(qrk); n != 5 {
		t.Errorf("backlogSummary consumed the queue; pending=%d, want 5", n)
	}
}
