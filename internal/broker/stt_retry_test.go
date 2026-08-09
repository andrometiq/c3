package broker

import (
	"context"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/queue"
)

// P0-2: the fetch-failure classifier is a fail-closed allowlist — only known
// transient network signatures retry; permanent failures never loop.
func TestIsNetworkTransient(t *testing.T) {
	transient := []string{
		"dial tcp: lookup api.telegram.org: Temporary failure in name resolution",
		"<urlopen error [Errno 101] Network is unreachable>",
		"read tcp 10.0.0.1:443: i/o timeout",
		"connection refused",
		"connection reset by peer",
		"no route to host",
		"lookup api.telegram.org: Name or service not known",
		"getaddrinfo: nodename nor servname provided, or not known",
		"HTTP Error 503: Service Unavailable",
		"HTTP Error 502: Bad Gateway",
		"HTTP Error 504: Gateway Timeout",
	}
	for _, s := range transient {
		if !isNetworkTransient(s) {
			t.Errorf("should classify as transient (retryable): %q", s)
		}
	}
	permanent := []string{
		"Bad Request: file is too big",
		"getFile failed (error_code=400): invalid file_id",
		"provider returned an empty transcript",
		"",
	}
	for _, s := range permanent {
		if isNetworkTransient(s) {
			t.Errorf("should NOT classify as transient (must not loop): %q", s)
		}
	}
}

// P0-2: parking is deduped by (route, message_id, file_id) and bounded.
func TestParkSTTRetry_DedupAndBounded(t *testing.T) {
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()
	route := MakeRouteKey("telegram", -100, nil)

	b.parkSTTRetry(route, 1, "f1")
	b.parkSTTRetry(route, 1, "f1") // exact dup — ignored
	b.parkSTTRetry(route, 2, "f2")
	b.parkSTTRetry(route, 3, "") // empty file_id — ignored (nothing to re-fetch)
	if got := len(b.sttRetries); got != 2 {
		t.Fatalf("dedup/empty-guard failed: %d parked, want 2", got)
	}

	for i := 0; i < maxSTTRetries+10; i++ {
		b.parkSTTRetry(route, int64(1000+i), "fx")
	}
	if got := len(b.sttRetries); got > maxSTTRetries {
		t.Fatalf("park list not bounded: %d > %d", got, maxSTTRetries)
	}
}

// P0-2: on a channel's fetch recovery, only that channel's parked entries are
// taken (submitted for retry); other channels' entries stay parked.
func TestOnChannelRecovered_DrainsOnlyThatChannel(t *testing.T) {
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	b.parkSTTRetry(MakeRouteKey("telegram", -100, nil), 1, "f1")
	b.parkSTTRetry(MakeRouteKey("other", -200, nil), 2, "f2")

	b.onChannelRecovered("telegram") // list mutation is synchronous; submit is async

	b.sttRetryMu.Lock()
	defer b.sttRetryMu.Unlock()
	if len(b.sttRetries) != 1 || b.sttRetries[0].route.Channel != "other" {
		t.Fatalf("recovered channel's entries not drained (or wrong channel drained): %+v", b.sttRetries)
	}
}

// P0-2 end-to-end: a retry that transcribes (network is back) runs STT in the pool
// and hands delivery to the route worker, landing as exactly one fresh durable line
// via the P0-1 late-delivery path.
func TestRunSTTRetry_SuccessDeliversLate(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(_ context.Context, _ c3types.VoicePayload) (string, error) {
		return "recovered transcript text", nil // network is back → STT succeeds
	})

	tid := int64(914)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	route := MakeRouteKey("telegram", -100, &tid)

	// runSTTRetry runs STT in the pool, then submits the delivery to the route worker
	// and waits for it — so once it returns the line is durably queued.
	b.runSTTRetry(sttRetryEntry{route: route, messageID: 777, fileID: "vf"})

	lines, err := b.Queue.PeekTracked(qrk, -1)
	if err != nil {
		t.Fatal(err)
	}
	matches := 0
	for _, rec := range lines {
		if rec.Inbound.MessageID == 777 && strings.Contains(rec.Inbound.Text, "recovered transcript text") && strings.Contains(rec.Inbound.Text, "Re-transcription") {
			matches++
		}
	}
	if matches != 1 {
		t.Fatalf("recovered transcript must be delivered as exactly one fresh line; got %d of %d", matches, len(lines))
	}
}
