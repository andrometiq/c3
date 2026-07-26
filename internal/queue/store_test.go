package queue

import (
	"os"
	"path/filepath"
	"strconv"
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

func TestRecoverOnStartup_SkipsCorruptLine(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	_ = s.Append(rk, msg(1, "ok"))
	// Manually append a corrupt line to the jsonl.
	if err := appendRawLine(t, QueueDir(), rk, "{not json"); err != nil {
		t.Fatal(err)
	}
	_ = s.Append(rk, msg(3, "ok2"))
	// A peek that walks past the corrupt line must skip it, not error.
	got, err := s.Peek(rk, 5)
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

// TestRefreshText_UpdatesPendingLineInPlace asserts RefreshText rewrites exactly
// the still-pending line whose MessageID matches, leaving the others untouched,
// and that a later Peek returns the refreshed Text.
func TestRefreshText_UpdatesPendingLineInPlace(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 3; i++ {
		if err := s.Append(rk, msg(i, "old-"+strconv.FormatInt(i, 10))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	ok, err := s.RefreshText(rk, 2, "fixed transcript")
	if err != nil {
		t.Fatalf("RefreshText: %v", err)
	}
	if !ok {
		t.Fatal("RefreshText should report a hit for a pending message_id")
	}
	got, err := s.Peek(rk, 3)
	if err != nil {
		t.Fatalf("Peek: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("peek len = %d, want 3", len(got))
	}
	for _, in := range got {
		switch in.MessageID {
		case 2:
			if in.Text != "fixed transcript" {
				t.Fatalf("msg 2 text = %q, want refreshed", in.Text)
			}
		default:
			if in.Text != "old-"+strconv.FormatInt(in.MessageID, 10) {
				t.Fatalf("msg %d text = %q, want untouched", in.MessageID, in.Text)
			}
		}
	}
	// Pending count is unchanged by an in-place refresh.
	if n, _ := s.Pending(rk); n != 3 {
		t.Fatalf("pending after refresh = %d, want 3", n)
	}
}

// TestRefreshText_NoOpWhenNotQueued asserts RefreshText returns (false, nil) and
// rewrites nothing when the message_id is absent (never queued) or already
// consumed (behind the cursor).
func TestRefreshText_NoOpWhenNotQueued(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 3; i++ {
		if err := s.Append(rk, msg(i, "old-"+strconv.FormatInt(i, 10))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Absent message_id: no-op.
	ok, err := s.RefreshText(rk, 999, "irrelevant")
	if err != nil {
		t.Fatalf("RefreshText(absent): %v", err)
	}
	if ok {
		t.Fatal("RefreshText for an absent message_id must report no hit")
	}

	// Consume the first two; message_id 1 is now behind the cursor.
	if _, err := s.Consume(rk, 2); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	ok, err = s.RefreshText(rk, 1, "too late")
	if err != nil {
		t.Fatalf("RefreshText(consumed): %v", err)
	}
	if ok {
		t.Fatal("RefreshText for an already-consumed message_id must report no hit")
	}
	// The still-pending line (msg 3) keeps its original text.
	got, err := s.Peek(rk, 3)
	if err != nil {
		t.Fatalf("Peek: %v", err)
	}
	if len(got) != 1 || got[0].MessageID != 3 || got[0].Text != "old-3" {
		t.Fatalf("after no-op refresh, head = %+v, want msg 3 unchanged", got)
	}
}

// TestRefreshText_MissingFileNoOp asserts RefreshText on a route with no queue
// file is a clean (false, nil) no-op.
func TestRefreshText_MissingFileNoOp(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -777}
	ok, err := s.RefreshText(rk, 5, "x")
	if err != nil {
		t.Fatalf("RefreshText(missing): %v", err)
	}
	if ok {
		t.Fatal("RefreshText on an empty/missing queue must report no hit")
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

// Item E (RefreshText cursor-remap branch): a corrupt line that sits BEFORE the
// cursor (already consumed past) is stripped by RefreshText's rewrite, so the
// cursor must be remapped into the corrupt-free coordinate space (store.go
// ~330-369). After consuming past the corrupt line, refreshing a still-pending
// line must update exactly that line, keep the other pending line(s) intact, and
// leave Pending correct (no off-by-one from the dropped corrupt line).
func TestRefreshText_CursorRemapAfterCorruptBeforeCursor(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	// Layout: [msg1] [corrupt] [msg2] [msg3]. msg1 is appended first so the file
	// exists before appendRawLine writes the corrupt placeholder.
	if err := s.Append(rk, msg(1, "old-1")); err != nil {
		t.Fatalf("append 1: %v", err)
	}
	if err := appendRawLine(t, QueueDir(), rk, "{corrupt"); err != nil {
		t.Fatal(err)
	}
	for i := int64(2); i <= 3; i++ {
		if err := s.Append(rk, msg(i, "old-"+strconv.FormatInt(i, 10))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Consume 2 REAL messages: msg1 then (stepping over the corrupt line) msg2.
	// The cursor now sits PAST the corrupt line — corrupt is behind it, msg3 is the
	// sole still-pending line.
	got, err := s.Consume(rk, 2)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(got) != 2 || got[0].MessageID != 1 || got[1].MessageID != 2 {
		t.Fatalf("consume = %+v, want msg1,msg2", got)
	}
	if n, _ := s.Pending(rk); n != 1 {
		t.Fatalf("pending after consume = %d, want 1 (msg3)", n)
	}

	// Refresh the still-pending msg3. rewrite() drops the corrupt line that sat
	// BEFORE the cursor, so the cursor must be remapped into the corrupt-free space
	// (the store.go ~330-369 branch); a desync here would mis-serve or skip msg3.
	ok, err := s.RefreshText(rk, 3, "fixed transcript")
	if err != nil {
		t.Fatalf("RefreshText: %v", err)
	}
	if !ok {
		t.Fatal("RefreshText should hit the still-pending msg3")
	}

	// msg3 stays the sole pending line, refreshed, served exactly once — proving the
	// remapped cursor stays aligned with the rewritten file.
	if n, _ := s.Pending(rk); n != 1 {
		t.Fatalf("pending after refresh = %d, want 1", n)
	}
	drained, err := s.Consume(rk, -1)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(drained) != 1 || drained[0].MessageID != 3 {
		t.Fatalf("drain after refresh = %+v, want msg3 exactly once", drained)
	}
	if drained[0].Text != "fixed transcript" {
		t.Fatalf("msg3 text = %q, want refreshed 'fixed transcript'", drained[0].Text)
	}
	if n, _ := s.Pending(rk); n != 0 {
		t.Fatalf("pending after drain = %d, want 0", n)
	}
}
