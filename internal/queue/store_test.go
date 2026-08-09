package queue

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	s, err := NewStore(QueueDir())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func msg(id int64, text string) *c3types.Inbound {
	return &c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: id, Text: text, Timestamp: time.Now()}
}

// freshKey returns a RouteKey for ONE logical topic route, minting a DISTINCT
// *int64 for TopicID on every call — exactly what the broker's queueRouteKey does
// per call (broker.go: t := k.TopicID; rk.TopicID = &t). Two of these are equal by
// File() value but are DISTINCT Go map keys, which is the whole point of the B fix:
// the status index must key by File() so per-call pointer churn can't accrue stale
// duplicate rows.
func freshKey() RouteKey {
	t := int64(914)
	return RouteKey{Channel: "telegram", ChatID: -100, TopicID: &t}
}

func TestAppendPeekConsumeAndDeleteOnEmpty(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 3; i++ {
		if err := s.Append(rk, msg(i, "m")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	// Peek does not advance.
	peek, err := s.Peek(rk, 2)
	if err != nil || len(peek) != 2 || peek[0].MessageID != 1 {
		t.Fatalf("peek = %+v err=%v", peek, err)
	}
	if n, _ := s.Pending(rk); n != 3 {
		t.Fatalf("pending after peek = %d, want 3", n)
	}
	// Consume advances.
	got, err := s.Consume(rk, 2)
	if err != nil || len(got) != 2 || got[1].MessageID != 2 {
		t.Fatalf("consume = %+v err=%v", got, err)
	}
	if n, _ := s.Pending(rk); n != 1 {
		t.Fatalf("pending after consume = %d, want 1", n)
	}
	// Drain the rest → files deleted.
	if _, err := s.Consume(rk, 10); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.Pending(rk); n != 0 {
		t.Fatalf("pending after drain = %d, want 0", n)
	}
	// A fresh append after delete-on-empty must restart at line 1.
	if err := s.Append(rk, msg(99, "again")); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Consume(rk, 1); len(got) != 1 || got[0].MessageID != 99 {
		t.Fatalf("re-append consume = %+v, want msg 99", got)
	}
}

func TestRecoverOnStartup_CursorBehindReplaysAtLeastOnce(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	s1, _ := NewStore(QueueDir())
	for i := int64(1); i <= 4; i++ {
		_ = s1.Append(rk, msg(i, "m"))
	}
	if _, err := s1.Consume(rk, 2); err != nil { // cursor = 2 persisted
		t.Fatal(err)
	}
	// Fresh store over the same dir simulates a restart.
	s2, err := NewStore(QueueDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.RecoverOnStartup(); err != nil {
		t.Fatal(err)
	}
	if n, _ := s2.Pending(rk); n != 2 {
		t.Fatalf("recovered pending = %d, want 2 (lines 3,4)", n)
	}
	got, _ := s2.Consume(rk, 2)
	if len(got) != 2 || got[0].MessageID != 3 {
		t.Fatalf("recovered consume = %+v, want msgs 3,4", got)
	}
}

func TestRecoverOnStartup_FullyConsumedPairDeleted(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	s1, _ := NewStore(QueueDir())
	_ = s1.Append(rk, msg(1, "m"))
	_ = s1.Append(rk, msg(2, "m"))
	// Simulate a crash AFTER persisting cursor=2 but BEFORE delete-on-empty by
	// writing the .cur to EOF directly via Consume, then dropping the in-memory
	// store and recovering.
	_, _ = s1.Consume(rk, 2)
	s2, _ := NewStore(QueueDir())
	if err := s2.RecoverOnStartup(); err != nil {
		t.Fatal(err)
	}
	if n, _ := s2.Pending(rk); n != 0 {
		t.Fatalf("fully-consumed route should recover to 0 pending, got %d", n)
	}
}

func TestEvictOverCap_DropsOldestAndAdjustsCursor(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	// Append cap+5 messages.
	for i := int64(1); i <= MaxMessages+5; i++ {
		_ = s.Append(rk, msg(i, "m"))
	}
	dropped, err := s.EvictOverCap(rk)
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 5 {
		t.Fatalf("dropped = %d, want 5", dropped)
	}
	if n, _ := s.Pending(rk); n != MaxMessages {
		t.Fatalf("pending after evict = %d, want %d", n, MaxMessages)
	}
	// Oldest survivor is message 6 (1..5 dropped).
	got, _ := s.Peek(rk, 1)
	if got[0].MessageID != 6 {
		t.Fatalf("oldest after evict = %d, want 6", got[0].MessageID)
	}
}

func TestEvictOverCap_RewriteFailureCannotHidePendingBehindOldCursor(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= MaxMessages+1; i++ {
		if err := s.Append(rk, msg(i, "m")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if got, err := s.Consume(rk, 900); err != nil || len(got) != 900 {
		t.Fatalf("consume 900 = %d, %v", len(got), err)
	}

	injected := errors.New("crash before JSONL rewrite")
	s.rewriteTestHook = func() error { return injected }
	if _, err := s.EvictOverCap(rk); !errors.Is(err, injected) {
		t.Fatalf("EvictOverCap error = %v, want injected rewrite failure", err)
	}

	// Re-open as after a process crash. JSONL is still the old long file, but
	// the lowered cursor is already durable, so every originally pending
	// record (901..1001) remains visible. Message 900 may replay; that is the
	// deliberate fail-toward-duplicate side of the contract.
	reopened, err := NewStore(QueueDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.RecoverOnStartup(); err != nil {
		t.Fatal(err)
	}
	pending, err := reopened.Peek(rk, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 102 || pending[0].MessageID != 900 || pending[len(pending)-1].MessageID != MaxMessages+1 {
		t.Fatalf("crash state pending = %d (%d..%d), want duplicate-safe 102 (900..1001)",
			len(pending), pending[0].MessageID, pending[len(pending)-1].MessageID)
	}
}

func TestEvictOverCap_DropsByAge(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	old := &c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 1, Text: "old", Timestamp: time.Now().Add(-MaxAge - time.Hour)}
	fresh := &c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 2, Text: "new", Timestamp: time.Now()}
	_ = s.Append(rk, old)
	_ = s.Append(rk, fresh)
	dropped, err := s.EvictOverCap(rk)
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 1 {
		t.Fatalf("age-evict dropped = %d, want 1", dropped)
	}
	got, _ := s.Peek(rk, 5)
	if len(got) != 1 || got[0].MessageID != 2 {
		t.Fatalf("after age-evict = %+v, want only msg 2", got)
	}
}

// FIX 1: Consume(rk, -1) is the "consume all" sentinel. A negative n must not
// reach make() as a capacity (which panics "makeslice: cap out of range"); it
// must drain every pending message and then honor the delete-on-empty contract.
func TestConsumeAll_NegativeN(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 3; i++ {
		if err := s.Append(rk, msg(i, "m")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	got, err := s.Consume(rk, -1) // must not panic on the make() cap hint
	if err != nil {
		t.Fatalf("consume-all: %v", err)
	}
	if len(got) != 3 || got[0].MessageID != 1 || got[2].MessageID != 3 {
		t.Fatalf("consume-all = %+v, want msgs 1,2,3", got)
	}
	if n, _ := s.Pending(rk); n != 0 {
		t.Fatalf("pending after consume-all = %d, want 0", n)
	}
	// delete-on-empty contract: both files gone once the cursor hits EOF.
	if _, err := os.Stat(filepath.Join(QueueDir(), rk.File()+".jsonl")); !os.IsNotExist(err) {
		t.Fatalf("jsonl should be deleted on empty, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(QueueDir(), rk.File()+".cur")); !os.IsNotExist(err) {
		t.Fatalf("cur should be deleted on empty, stat err = %v", err)
	}
}

// FIX 3: EvictOverCap must run its cap/age/cursor math on the corrupt-free real
// lines (rewrite() strips corrupt placeholders from the file). With a corrupt
// line present, the rewritten file's length must stay consistent with newCursor
// so the surviving messages are served exactly once — no double-serve, no skip.
func TestEvictOverCap_CorruptLineCursorConsistent(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	// Line layout: [old(age-evict)] [corrupt] [fresh msg2] [fresh msg3]
	old := &c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 1, Text: "old", Timestamp: time.Now().Add(-MaxAge - time.Hour)}
	_ = s.Append(rk, old)
	if err := appendRawLine(t, QueueDir(), rk, "{corrupt"); err != nil {
		t.Fatal(err)
	}
	_ = s.Append(rk, msg(2, "fresh"))
	_ = s.Append(rk, msg(3, "fresh"))

	dropped, err := s.EvictOverCap(rk)
	if err != nil {
		t.Fatal(err)
	}
	// Only the one old REAL line is dropped; the corrupt line is not counted as a
	// dropped message (it's stripped by rewrite, not "evicted").
	if dropped != 1 {
		t.Fatalf("dropped = %d, want 1 (old real line only)", dropped)
	}
	// After evict the corrupt placeholder is gone and msgs 2,3 remain pending.
	if n, _ := s.Pending(rk); n != 2 {
		t.Fatalf("pending after evict = %d, want 2 (msgs 2,3)", n)
	}
	// Draining must return EXACTLY msgs 2 and 3, in order, once each — proving the
	// rewritten-file length and the cursor agree (the bug double-served or skipped).
	got, err := s.Consume(rk, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].MessageID != 2 || got[1].MessageID != 3 {
		t.Fatalf("drain after corrupt-evict = %+v, want msgs 2,3 exactly once", got)
	}
	if n, _ := s.Pending(rk); n != 0 {
		t.Fatalf("pending after drain = %d, want 0", n)
	}
}

// TestRecoverOnStartup_SkipsCorruptLine covers the corrupt-at-startup path: a
// broker restart over a route whose file holds an unparseable line must rebuild the
// index from the REAL lines only, leave the still-pending pair in place, and serve
// exactly the real messages.
//
// The assertion is deliberately on StatusFor, which reads ONLY the in-memory index
// (no file I/O): it is zero unless RecoverOnStartup actually ran. An earlier version
// of this test never called RecoverOnStartup at all — it was a Peek test wearing a
// startup name, and it would have passed with the whole startup scan deleted.
func TestRecoverOnStartup_SkipsCorruptLine(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	s1, err := NewStore(QueueDir())
	if err != nil {
		t.Fatal(err)
	}
	_ = s1.Append(rk, msg(1, "ok"))
	// Manually append a corrupt line to the jsonl.
	if err := appendRawLine(t, QueueDir(), rk, "{not json"); err != nil {
		t.Fatal(err)
	}
	_ = s1.Append(rk, msg(3, "ok2"))

	// Restart: a fresh store over the same dir rebuilds its index from disk.
	s2, err := NewStore(QueueDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.RecoverOnStartup(); err != nil {
		t.Fatalf("RecoverOnStartup over a corrupt line errored: %v", err)
	}
	// The recovered index counts the two real messages, never the corrupt line.
	if st := s2.StatusFor(rk); st.Pending != 2 {
		t.Fatalf("recovered StatusFor.Pending = %d, want 2 (msgs 1,3; a corrupt line is not a message)", st.Pending)
	}
	// A cursor-behind route keeps its live pair (it is not fully consumed).
	if _, err := os.Stat(filepath.Join(dir, rk.File()+".jsonl")); err != nil {
		t.Fatalf("live jsonl must survive recovery of a cursor-behind route: %v", err)
	}
	// A peek that walks past the corrupt line must skip it, not error.
	got, err := s2.Peek(rk, 5)
	if err != nil {
		t.Fatalf("peek over corrupt line errored: %v", err)
	}
	if len(got) != 2 || got[0].MessageID != 1 || got[1].MessageID != 3 {
		t.Fatalf("peek skipping corrupt = %+v, want msgs 1,3", got)
	}
}

func TestStatusAll_ReportsPendingAndOldest(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	_ = s.Append(rk, msg(1, "m"))
	_ = s.Append(rk, msg(2, "m"))
	all := s.StatusAll()
	st, ok := all[rk]
	if !ok || st.Pending != 2 || st.OldestUnix == 0 {
		t.Fatalf("StatusAll[%v] = %+v ok=%v", rk, st, ok)
	}
}

// I7: StatusFor reads the in-memory index for ONE route (no file I/O), so a
// per-topic /status read is race-free against a concurrent worker. It must match
// by VALUE identity: RouteKey.TopicID is a *int64, so a query RouteKey carrying a
// DISTINCT pointer to the same topic value must still find the stored entry (a raw
// map lookup would miss). This also pins the latent pointer-key bug the fix avoids.
func TestStatusFor_IndexBackedAndPointerSafe(t *testing.T) {
	s := newStore(t)
	stored := int64(914)
	rk := RouteKey{Channel: "telegram", ChatID: -100, TopicID: &stored}
	_ = s.Append(rk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &stored, MessageID: 1, Text: "m", Timestamp: time.Now().Add(-time.Hour)})
	_ = s.Append(rk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &stored, MessageID: 2, Text: "m", Timestamp: time.Now()})

	// Query with a SEPARATE pointer to the same topic id (what the broker builds via
	// queueRouteKey on each call).
	queryTopic := int64(914)
	query := RouteKey{Channel: "telegram", ChatID: -100, TopicID: &queryTopic}
	st := s.StatusFor(query)
	if st.Pending != 2 {
		t.Fatalf("StatusFor.Pending = %d, want 2 (value-identity match across distinct *int64 pointers)", st.Pending)
	}
	if st.OldestUnix == 0 {
		t.Fatalf("StatusFor.OldestUnix = 0, want the oldest pending timestamp")
	}

	// A route with nothing queued returns the zero Status.
	none := RouteKey{Channel: "telegram", ChatID: -999}
	if got := s.StatusFor(none); got.Pending != 0 {
		t.Fatalf("StatusFor for an empty route = %+v, want zero Status", got)
	}

	// After draining, StatusFor reflects the index update (no stale count).
	if _, err := s.Consume(rk, -1); err != nil {
		t.Fatal(err)
	}
	if got := s.StatusFor(query); got.Pending != 0 {
		t.Fatalf("StatusFor after drain = %+v, want Pending 0", got)
	}
}

// TestStatusFor_DistinctPointersPerCall pins the B fix: production mints a FRESH
// *int64 RouteKey on EVERY Append/Consume (queueRouteKey), so a pointer-keyed index
// accrues a stale row per call and StatusFor returns a map-order-random count that
// never clears after drain. With the index keyed by File(), there is exactly ONE
// canonical row per route: the count is deterministic and drain clears it.
//
// On the UNFIXED tree this FAILS — after draining via yet another distinct pointer,
// refreshIndex deletes a key that was never the Append-time key, so the stale rows
// survive and StatusFor reports a nonzero (1/2/3) count for an empty route.
func TestStatusFor_DistinctPointersPerCall(t *testing.T) {
	s := newStore(t)
	// Append 3 messages, each routed by a freshKey() carrying a DISTINCT pointer
	// to the same topic value — three separate Go map keys for one logical route.
	for i := int64(1); i <= 3; i++ {
		if err := s.Append(freshKey(), msg(i, "m")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if st := s.StatusFor(freshKey()); st.Pending != 3 {
		t.Fatalf("StatusFor.Pending = %d, want 3 (one canonical row, no per-pointer stale duplicates)", st.Pending)
	}
	// Drain via ANOTHER distinct pointer; the index must clear to zero.
	if _, err := s.Consume(freshKey(), -1); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if st := s.StatusFor(freshKey()); st.Pending != 0 {
		t.Fatalf("StatusFor after drain = %d, want 0 (no stale duplicate survives)", st.Pending)
	}
}

// TestStatusAll_AfterDistinctPointerAppends_NoDuplicateRows pins that the
// cross-route summary collapses per-call pointer churn into a single row, and that
// the reconstructed RouteKey round-trips Channel/ChatID and the TopicID VALUE (so
// statusGlobal, which reads k.Channel/k.ChatID/k.TopicID, still renders correctly).
//
// On the UNFIXED tree this FAILS: the pointer-keyed index holds three distinct map
// entries for the one route, so StatusAll returns len 3.
func TestStatusAll_AfterDistinctPointerAppends_NoDuplicateRows(t *testing.T) {
	s := newStore(t)
	for i := int64(1); i <= 3; i++ {
		if err := s.Append(freshKey(), msg(i, "m")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	all := s.StatusAll()
	if len(all) != 1 {
		t.Fatalf("StatusAll len = %d, want 1 canonical row (no per-pointer duplicate rows)", len(all))
	}
	var gotKey RouteKey
	var gotStatus Status
	for k, v := range all {
		gotKey, gotStatus = k, v
	}
	if gotKey.Channel != "telegram" || gotKey.ChatID != -100 {
		t.Fatalf("reconstructed key = %+v, want Channel telegram / ChatID -100", gotKey)
	}
	if gotKey.TopicID == nil || *gotKey.TopicID != 914 {
		t.Fatalf("reconstructed TopicID = %v, want a pointer to value 914", gotKey.TopicID)
	}
	if gotStatus.Pending != 3 {
		t.Fatalf("status.Pending = %d, want 3", gotStatus.Pending)
	}
}

// -race coverage with a DETERMINISTIC post-condition: a single route worker
// interleaving appends + consumes must be race-free (all calls funnel through one
// goroutine, mirroring the worker's single-owner model) AND must never return a
// consumed message twice. Run under `go test -race`. Unlike a bare "it ran"
// test, this asserts (a) the final Pending equals appends-minus-successfully-
// consumed and (b) no MessageID is ever consumed twice, so a regression in the
// cursor/consume math is actually caught.
func TestStore_SingleOwnerSerializedConsumeIsExactlyOnce(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	type op struct {
		append bool
		id     int64
	}
	ops := make(chan op)
	doneCh := make(chan struct{})
	seen := map[int64]int{} // MessageID -> times consumed
	appended, consumed := 0, 0
	go func() { // the single owner goroutine — also owns `seen` (no shared access)
		defer close(doneCh)
		for o := range ops {
			if o.append {
				if err := s.Append(rk, msg(o.id, "m")); err == nil {
					appended++
				}
				continue
			}
			got, err := s.Consume(rk, 1)
			if err != nil {
				t.Errorf("consume: %v", err)
				continue
			}
			for _, m := range got {
				seen[m.MessageID]++
				consumed++
			}
		}
	}()
	var wg sync.WaitGroup
	var nextID int64
	for i := 0; i < 50; i++ {
		wg.Add(1)
		id := nextID + 1
		nextID = id
		go func(i int, id int64) { defer wg.Done(); ops <- op{append: i%2 == 0, id: id} }(i, id)
	}
	wg.Wait()
	close(ops)
	<-doneCh
	for id, n := range seen {
		if n != 1 {
			t.Errorf("message %d consumed %d times, want exactly once", id, n)
		}
	}
	if n, _ := s.Pending(rk); n != appended-consumed {
		t.Errorf("final pending = %d, want appended(%d) - consumed(%d) = %d", n, appended, consumed, appended-consumed)
	}
}

func appendRawLine(t *testing.T, dir string, rk RouteKey, raw string) error {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, rk.File()+".jsonl"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(raw + "\n")
	return err
}
