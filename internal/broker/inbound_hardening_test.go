package broker

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/queue"
)

// captureLog redirects the default logger to a buffer for the duration of fn and
// returns everything written. Mirrors the pattern in dispatch_test.go.
func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	oldOut := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(oldOut)
		log.SetFlags(oldFlags)
	}()
	fn()
	return buf.String()
}

// Item E (caps never silent): an over-cap eviction must fire BOTH the broker.log
// "queue CAP" line AND a best-effort Telegram SendReply notice — the "caps never
// silent" guarantee. We trigger the cheap AGE branch of EvictOverCap (one stale
// message) rather than appending the 1000-line count cap.
func TestEvictIfOverCap_LogsAndNotifies(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}

	// One already-aged message (past MaxAge) → EvictOverCap drops it via the age
	// branch. A second fresh message proves only the stale one is dropped.
	_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 1, Text: "old", Timestamp: time.Now().Add(-queue.MaxAge - time.Hour)})
	_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 2, Text: "fresh", Timestamp: time.Now()})

	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	out := captureLog(t, func() { w.evictIfOverCap(qrk) })

	if !strings.Contains(out, "queue CAP") {
		t.Fatalf("over-cap eviction must log a 'queue CAP' line; log was:\n%s", out)
	}
	replies := fc.sendRepliesSnapshot()
	found := false
	for _, r := range replies {
		if strings.Contains(r.Text, "queue full") && strings.Contains(r.Text, "dropped") {
			found = true
		}
	}
	if !found {
		t.Fatalf("over-cap eviction must send a Telegram notice (caps never silent); replies=%+v", replies)
	}
}

// Item E (disk-full = persist failure → best-effort Telegram notice): a failed
// durable Append must fire notePersistFailure's Telegram SendReply notice AND
// must NOT advance the offset (no markPersisted). The no-poison + no-advance
// property is covered by TestFlushInbounds_AppendFailDoesNotPoisonDedup; this
// adds the missing NOTICE assertion.
func TestFlushInbounds_AppendFailSendsNotice(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	var persisted []int64
	b.SetPersistedCallback(func(in *c3types.Inbound) { persisted = append(persisted, in.MessageID) })

	// Force Append to fail: build a store, then remove its directory so OpenFile
	// fails (same technique as the no-poison test).
	brokenDir := t.TempDir()
	brokenStore, err := queue.NewStore(brokenDir)
	if err != nil {
		t.Fatal(err)
	}
	if rmErr := os.RemoveAll(brokenDir); rmErr != nil {
		t.Fatalf("setup: remove broken dir: %v", rmErr)
	}
	b.Queue = brokenStore

	in := &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 77, Text: "hi", Timestamp: time.Now()}
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})

	if len(persisted) != 0 {
		t.Fatalf("a failed Append must NOT advance the offset (markPersisted); persisted=%v", persisted)
	}
	replies := fc.sendRepliesSnapshot()
	found := false
	for _, r := range replies {
		if strings.Contains(r.Text, "Could not persist") {
			found = true
		}
	}
	if !found {
		t.Fatalf("a failed Append must send a best-effort persist-failure Telegram notice; replies=%+v", replies)
	}
}
