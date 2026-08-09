package broker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/queue"
)

// P0-1: a retranscribe whose message is no longer queued (it was delivered live and
// consumed) must NOT drop the transcript. It is delivered as a FRESH durable line
// tagged with the original message_id — so a caller that already timed out never
// loses the result — and it is stored EXACTLY ONCE (the no-claim append-if-absent
// path must not double-store what deliverTranscriptAsNewLine already appended).
func TestHandleRefreshText_MissDeliversTranscriptAsFreshLine(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}

	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	// Nothing queued for message 555 → refresh misses → late-deliver as a new line.
	resultCh := make(chan RefreshResult, 1)
	w.handleRefreshText(context.Background(), &RefreshTextJob{
		MessageID: 555, FileID: "vfile", Transcript: "the recovered transcript", ResultCh: resultCh,
	})
	res := <-resultCh
	if res.Refreshed || !res.AppendedNew {
		t.Fatalf("refresh miss should append a fresh line, got %+v", res)
	}

	lines, err := b.Queue.PeekTracked(qrk, -1)
	if err != nil {
		t.Fatal(err)
	}
	matches := 0
	for _, rec := range lines {
		if rec.Inbound.MessageID == 555 && strings.Contains(rec.Inbound.Text, "the recovered transcript") {
			matches++
			if !strings.Contains(rec.Inbound.Text, "Re-transcription") {
				t.Errorf("fresh line missing the re-transcription marker: %q", rec.Inbound.Text)
			}
		}
	}
	if matches != 1 {
		t.Fatalf("transcript must be stored exactly once (no double-append); got %d of %d lines", matches, len(lines))
	}
}
