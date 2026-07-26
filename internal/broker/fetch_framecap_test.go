package broker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

// bigQueue fills one route with n records of roughly bytesEach, so the total
// comfortably exceeds one IPC frame. Text is where the size goes because that
// is where it goes in production: an STT transcript is not bounded by the
// channel's own message-length limit.
func bigQueue(t *testing.T, b *Broker, qrk queue.RouteKey, n, bytesEach int) {
	t.Helper()
	body := strings.Repeat("x", bytesEach)
	for i := 1; i <= n; i++ {
		in := &c3types.Inbound{
			Channel: "telegram", ChatID: qrk.ChatID, TopicID: qrk.TopicID,
			MessageID: int64(i), Text: body, Timestamp: time.Now(),
		}
		if err := b.Queue.Append(qrk, in); err != nil {
			t.Fatalf("seed append %d: %v", i, err)
		}
	}
}

// fetch_queue(all=true) returns ONE frame. If the batch is assembled without
// regard to the frame cap, WriteJSON refuses it — and on the ack path the
// messages have already been consumed, so they are gone and the user never sees
// them. The batch must therefore be bounded before the consume, not after.
func TestFetchAll_BoundedByFrameCap_AckDoesNotEatWhatItCannotSend(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}

	// ~12 MiB total against a 4 MiB frame — three frames' worth.
	const (
		count     = 24
		bytesEach = 512 * 1024
	)
	bigQueue(t, b, qrk, count, bytesEach)

	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	resultCh := make(chan FetchResult, 1)
	w.handleFetch(context.Background(), &FetchJob{All: true, Ack: true, ResultCh: resultCh})
	res := <-resultCh
	if res.Err != nil {
		t.Fatalf("fetch: %v", res.Err)
	}

	if len(res.Messages) == 0 {
		t.Fatal("returned nothing; a bounded batch must still make progress or the route never drains")
	}
	if len(res.Messages) >= count {
		t.Fatalf("returned all %d messages (~%d MiB) in one frame; the %d MiB cap was not applied",
			len(res.Messages), count*bytesEach/(1024*1024), ipc.MaxFrameSize/(1024*1024))
	}

	// The batch must actually fit the frame it will be written into — that is the
	// whole point, and it is what WriteJSON will now enforce on the way out.
	resp := ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: "t1", Messages: res.Messages, Remaining: res.Remaining}
	enc, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	if len(enc)+1 > ipc.MaxFrameSize {
		t.Fatalf("bounded batch still overflows: %d bytes for %d messages, cap %d",
			len(enc)+1, len(res.Messages), ipc.MaxFrameSize)
	}

	// The loss test: ack=true consumed exactly what it returned, no more. If the
	// bound had been applied after the consume, the difference would be missing
	// from both the response and the queue.
	pending, _ := b.Queue.Pending(qrk)
	if want := count - len(res.Messages); pending != want {
		t.Fatalf("MESSAGE LOSS: consumed %d but %d left queued, want %d left — the gap was consumed and never delivered",
			len(res.Messages), pending, want)
	}
	if res.Remaining != pending {
		t.Errorf("Remaining=%d but %d are actually queued; the caller uses this to know it must fetch again",
			res.Remaining, pending)
	}

	// And the route genuinely drains across repeated calls rather than stalling.
	for range 10 {
		if pending == 0 {
			break
		}
		ch := make(chan FetchResult, 1)
		w.handleFetch(context.Background(), &FetchJob{All: true, Ack: true, ResultCh: ch})
		r := <-ch
		if r.Err != nil {
			t.Fatalf("drain fetch: %v", r.Err)
		}
		if len(r.Messages) == 0 {
			t.Fatalf("drain stalled with %d still queued", pending)
		}
		pending, _ = b.Queue.Pending(qrk)
	}
	if pending != 0 {
		t.Errorf("route did not drain: %d still queued after repeated fetch_queue(all)", pending)
	}
}

// A queue that fits must be unaffected — the bound is a ceiling, not a chunker.
func TestFetchAll_SmallQueueStillReturnsEverythingInOneCall(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(915)
	key := MakeRouteKey("telegram", -101, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -101, TopicID: &tid}
	bigQueue(t, b, qrk, 5, 128)

	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	resultCh := make(chan FetchResult, 1)
	w.handleFetch(context.Background(), &FetchJob{All: true, Ack: true, ResultCh: resultCh})
	res := <-resultCh
	if res.Err != nil {
		t.Fatalf("fetch: %v", res.Err)
	}
	if len(res.Messages) != 5 {
		t.Fatalf("returned %d of 5; a queue far under the cap must come back whole", len(res.Messages))
	}
	if res.Remaining != 0 {
		t.Errorf("Remaining=%d, want 0", res.Remaining)
	}
}
