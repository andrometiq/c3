package broker

import (
	"context"
	"testing"
	"time"
)

// The legacy refresh job no longer invents a second delivery path on a miss.
// VoiceScheduler + JobResolveVoice own that revision behavior; keeping it here
// would race the scheduler and recreate two transcript owners.
func TestHandleRefreshText_MissIsCleanNoOp(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	resultCh := make(chan RefreshResult, 1)
	w.handleRefreshText(context.Background(), &RefreshTextJob{
		MessageID: 555, FileID: "vfile", Transcript: "the recovered transcript", ResultCh: resultCh,
	})
	res := <-resultCh
	if res.Refreshed || res.AppendedNew || res.Err != nil {
		t.Fatalf("refresh miss should be a clean no-op, got %+v", res)
	}
	if n, _ := b.Queue.Pending(queueRouteKey(key)); n != 0 {
		t.Fatalf("legacy refresh miss appended %d row(s); JobResolveVoice must be the only revision owner", n)
	}
}
