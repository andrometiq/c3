package queue

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

// tsMsg builds a message with an explicit timestamp (for the MIN/MAX age tests).
func tsMsg(id int64, text string, ts time.Time) *c3types.Inbound {
	return &c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: id, Text: text, Timestamp: ts}
}

// drainedMsg builds a drained-in line (DrainedFrom set) for the B7 RefreshText
// skip test.
func drainedMsg(id int64, text, from string) *c3types.Inbound {
	return &c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: id, Text: text, DrainedFrom: from, Timestamp: time.Now()}
}

// peekIDs returns the MessageIDs of every pending line, oldest-first.
func peekIDs(t *testing.T, s *Store, rk RouteKey) []int64 {
	t.Helper()
	got, err := s.Peek(rk, -1)
	if err != nil {
		t.Fatalf("Peek: %v", err)
	}
	return idsOf(got)
}

func idsOf(in []c3types.Inbound) []int64 {
	ids := make([]int64, len(in))
	for i, m := range in {
		ids[i] = m.MessageID
	}
	return ids
}

// rawJSONL returns the on-disk .jsonl lines verbatim (for byte-for-byte checks).
func rawJSONL(t *testing.T, dir string, rk RouteKey) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, rk.File()+".jsonl"))
	if err != nil {
		t.Fatalf("read jsonl: %v", err)
	}
	var out []string
	for _, ln := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		if ln != "" {
			out = append(out, ln)
		}
	}
	return out
}

// trashEvicted reads every .trash/*.evicted.jsonl snapshot record (the RemoveIDs /
// EvictOverCap drop snapshots).
func trashEvicted(t *testing.T, dir string) []c3types.Inbound {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, trashDirName, "*.evicted.jsonl"))
	if err != nil {
		t.Fatalf("glob trash: %v", err)
	}
	var out []c3types.Inbound
	for _, m := range matches {
		data, rerr := os.ReadFile(m)
		if rerr != nil {
			t.Fatalf("read trash %s: %v", m, rerr)
		}
		for _, ln := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
			if ln == "" {
				continue
			}
			var in c3types.Inbound
			if jerr := json.Unmarshal([]byte(ln), &in); jerr != nil {
				t.Fatalf("unmarshal trash line %q: %v", ln, jerr)
			}
			out = append(out, in)
		}
	}
	return out
}

// TestRemoveIDs_MidRangeLeavesHeadAndTail: remove the 3 middle lines of 5, leaving
// head (1) + tail (5) — the mid-range removal Consume's prefix-only cursor cannot
// express.
func TestRemoveIDs_MidRangeLeavesHeadAndTail(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 5; i++ {
		if err := s.Append(rk, msg(i, "m")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	removed, err := s.RemoveIDs(rk, map[int64][]int{2: {1}, 3: {1}, 4: {1}})
	if err != nil {
		t.Fatalf("RemoveIDs: %v", err)
	}
	if want := []int64{2, 3, 4}; !reflect.DeepEqual(idsOf(removed), want) {
		t.Fatalf("removed = %v, want %v (file order)", idsOf(removed), want)
	}
	if want := []int64{1, 5}; !reflect.DeepEqual(peekIDs(t, s, rk), want) {
		t.Fatalf("pending = %v, want %v (head+tail)", peekIDs(t, s, rk), want)
	}
	if n, _ := s.Pending(rk); n != 2 {
		t.Fatalf("pending count = %d, want 2", n)
	}
}

// TestRemoveIDs_PrefixRemoval: removing the oldest prefix via RemoveIDs leaves the
// tail, same as Consume would but through the one primitive.
func TestRemoveIDs_PrefixRemoval(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 5; i++ {
		_ = s.Append(rk, msg(i, "m"))
	}
	removed, err := s.RemoveIDs(rk, map[int64][]int{1: {1}, 2: {1}})
	if err != nil {
		t.Fatalf("RemoveIDs: %v", err)
	}
	if want := []int64{1, 2}; !reflect.DeepEqual(idsOf(removed), want) {
		t.Fatalf("removed = %v, want %v", idsOf(removed), want)
	}
	if want := []int64{3, 4, 5}; !reflect.DeepEqual(peekIDs(t, s, rk), want) {
		t.Fatalf("pending = %v, want %v", peekIDs(t, s, rk), want)
	}
}

// TestRemoveIDs_RemoveAllRetiresPairAndSnapshots: removing every pending line
// retires the pair (files gone) and leaves a .trash snapshot of the removed lines.
func TestRemoveIDs_RemoveAllRetiresPairAndSnapshots(t *testing.T) {
	s := newStore(t)
	dir := QueueDir()
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 3; i++ {
		_ = s.Append(rk, msg(i, "m"))
	}
	removed, err := s.RemoveIDs(rk, map[int64][]int{1: {1}, 2: {1}, 3: {1}})
	if err != nil {
		t.Fatalf("RemoveIDs: %v", err)
	}
	if want := []int64{1, 2, 3}; !reflect.DeepEqual(idsOf(removed), want) {
		t.Fatalf("removed = %v, want %v", idsOf(removed), want)
	}
	if n, _ := s.Pending(rk); n != 0 {
		t.Fatalf("pending after remove-all = %d, want 0", n)
	}
	// retirePair fires: the live pair is gone.
	if _, err := os.Stat(filepath.Join(dir, rk.File()+".jsonl")); !os.IsNotExist(err) {
		t.Fatalf("jsonl should be gone after remove-all, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, rk.File()+".cur")); !os.IsNotExist(err) {
		t.Fatalf("cur should be gone after remove-all, stat err = %v", err)
	}
	// The .trash snapshot holds the removed lines.
	if got := idsOf(trashEvicted(t, dir)); !reflect.DeepEqual(got, []int64{1, 2, 3}) {
		t.Fatalf("trash snapshot ids = %v, want [1 2 3]", got)
	}
}

// TestRemoveIDs_UnknownIDNoOp: an id absent from pending removes nothing and
// mutates no file.
func TestRemoveIDs_UnknownIDNoOp(t *testing.T) {
	s := newStore(t)
	dir := QueueDir()
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 3; i++ {
		_ = s.Append(rk, msg(i, "m"))
	}
	before := rawJSONL(t, dir, rk)
	removed, err := s.RemoveIDs(rk, map[int64][]int{999: {1}})
	if err != nil {
		t.Fatalf("RemoveIDs: %v", err)
	}
	if len(removed) != 0 {
		t.Fatalf("removed = %v, want empty", idsOf(removed))
	}
	if want := []int64{1, 2, 3}; !reflect.DeepEqual(peekIDs(t, s, rk), want) {
		t.Fatalf("pending = %v, want %v (unchanged)", peekIDs(t, s, rk), want)
	}
	if after := rawJSONL(t, dir, rk); !reflect.DeepEqual(after, before) {
		t.Fatalf("file mutated on no-op: before=%v after=%v", before, after)
	}
	// An empty selection map is likewise a clean no-op.
	if r, err := s.RemoveIDs(rk, map[int64][]int{}); err != nil || len(r) != 0 {
		t.Fatalf("empty-sel RemoveIDs = (%v,%v), want (nil,nil)", idsOf(r), err)
	}
}

// TestRemoveIDs_AlreadyAbsentIdempotent: re-issuing a remove for an id already
// gone from pending is a converging no-op (crash-retry safety).
func TestRemoveIDs_AlreadyAbsentIdempotent(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 3; i++ {
		_ = s.Append(rk, msg(i, "m"))
	}
	if _, err := s.RemoveIDs(rk, map[int64][]int{2: {1}}); err != nil {
		t.Fatalf("RemoveIDs first: %v", err)
	}
	if want := []int64{1, 3}; !reflect.DeepEqual(peekIDs(t, s, rk), want) {
		t.Fatalf("pending after first = %v, want %v", peekIDs(t, s, rk), want)
	}
	// Re-issue: 2 is already gone ⇒ removes nothing, pending unchanged.
	removed, err := s.RemoveIDs(rk, map[int64][]int{2: {1}})
	if err != nil {
		t.Fatalf("RemoveIDs re-issue: %v", err)
	}
	if len(removed) != 0 {
		t.Fatalf("re-issue removed = %v, want empty (idempotent)", idsOf(removed))
	}
	if want := []int64{1, 3}; !reflect.DeepEqual(peekIDs(t, s, rk), want) {
		t.Fatalf("pending after re-issue = %v, want %v", peekIDs(t, s, rk), want)
	}
}

// TestRemoveIDs_DuplicateMessageIDByOccurrence: two pending lines share one
// MessageID (edited-message re-dispatch, A2). Selection is by OCCURRENCE
// ORDINAL: sel={id:{1}} removes ONLY the first occurrence, sel={id:{2}}
// removes ONLY the second (the first survives — the wrong-line removal a
// per-id count could not express), and sel={id:{1,2}} removes both.
func TestRemoveIDs_DuplicateMessageIDByOccurrence(t *testing.T) {
	// sel={7:{1}} → first occurrence only.
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	_ = s.Append(rk, msg(1, "head"))
	_ = s.Append(rk, msg(7, "first-7"))
	_ = s.Append(rk, msg(7, "second-7"))
	removed, err := s.RemoveIDs(rk, map[int64][]int{7: {1}})
	if err != nil {
		t.Fatalf("RemoveIDs sel={7:{1}}: %v", err)
	}
	if len(removed) != 1 || removed[0].MessageID != 7 || removed[0].Text != "first-7" {
		t.Fatalf("removed = %+v, want exactly the FIRST id-7 (text first-7)", removed)
	}
	got, _ := s.Peek(rk, -1)
	if len(got) != 2 || got[0].MessageID != 1 || got[1].MessageID != 7 || got[1].Text != "second-7" {
		t.Fatalf("pending = %+v, want [msg1, second-7]", got)
	}

	// sel={7:{2}} → the SECOND occurrence only; the first survives in place.
	s2 := newStore(t)
	rk2 := RouteKey{Channel: "telegram", ChatID: -100}
	_ = s2.Append(rk2, msg(1, "head"))
	_ = s2.Append(rk2, msg(7, "first-7"))
	_ = s2.Append(rk2, msg(7, "second-7"))
	removed2, err := s2.RemoveIDs(rk2, map[int64][]int{7: {2}})
	if err != nil {
		t.Fatalf("RemoveIDs sel={7:{2}}: %v", err)
	}
	if len(removed2) != 1 || removed2[0].MessageID != 7 || removed2[0].Text != "second-7" {
		t.Fatalf("removed = %+v, want exactly the SECOND id-7 (text second-7)", removed2)
	}
	got2, _ := s2.Peek(rk2, -1)
	if len(got2) != 2 || got2[0].MessageID != 1 || got2[1].MessageID != 7 || got2[1].Text != "first-7" {
		t.Fatalf("pending = %+v, want [msg1, first-7] (first occurrence KEPT)", got2)
	}

	// sel={7:{1,2}} → both occurrences, file order.
	s3 := newStore(t)
	rk3 := RouteKey{Channel: "telegram", ChatID: -100}
	_ = s3.Append(rk3, msg(7, "a"))
	_ = s3.Append(rk3, msg(7, "b"))
	removed3, err := s3.RemoveIDs(rk3, map[int64][]int{7: {1, 2}})
	if err != nil {
		t.Fatalf("RemoveIDs sel={7:{1,2}}: %v", err)
	}
	if len(removed3) != 2 || removed3[0].Text != "a" || removed3[1].Text != "b" {
		t.Fatalf("removed = %+v, want both id-7 in file order [a,b]", removed3)
	}
	if n, _ := s3.Pending(rk3); n != 0 {
		t.Fatalf("pending after sel={7:{1,2}} = %d, want 0", n)
	}
}

func TestAppendTracked_RecordIdentitySurvivesEveryRewrite(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, *Store, RouteKey)
		rewrite func(*testing.T, *Store, RouteKey)
	}{
		{
			name: "RefreshText",
			rewrite: func(t *testing.T, s *Store, rk RouteKey) {
				t.Helper()
				if ok, err := s.RefreshText(rk, 7, "first-refreshed"); err != nil || !ok {
					t.Fatalf("RefreshText = %v, %v", ok, err)
				}
			},
		},
		{
			name: "RemoveIDs",
			prepare: func(t *testing.T, s *Store, rk RouteKey) {
				t.Helper()
				if err := s.Append(rk, msg(99, "remove-me")); err != nil {
					t.Fatal(err)
				}
			},
			rewrite: func(t *testing.T, s *Store, rk RouteKey) {
				t.Helper()
				if _, err := s.RemoveIDs(rk, map[int64][]int{99: {1}}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "EvictOverCap",
			prepare: func(t *testing.T, s *Store, rk RouteKey) {
				t.Helper()
				old := msg(99, "evict-me")
				old.Timestamp = time.Now().Add(-MaxAge - time.Hour)
				if err := s.Append(rk, old); err != nil {
					t.Fatal(err)
				}
			},
			rewrite: func(t *testing.T, s *Store, rk RouteKey) {
				t.Helper()
				if dropped, err := s.EvictOverCap(rk); err != nil || dropped != 1 {
					t.Fatalf("EvictOverCap = %d, %v", dropped, err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newStore(t)
			rk := RouteKey{Channel: "telegram", ChatID: -100}
			if tt.prepare != nil {
				tt.prepare(t, s, rk)
			}
			firstID, err := s.AppendTracked(rk, msg(7, "first"))
			if err != nil {
				t.Fatal(err)
			}
			secondID, err := s.AppendTracked(rk, msg(7, "second"))
			if err != nil {
				t.Fatal(err)
			}
			if firstID == "" || secondID == "" || firstID == secondID {
				t.Fatalf("defect: tracked queue identities are not distinct and non-empty: first=%q second=%q", firstID, secondID)
			}

			tt.rewrite(t, s, rk)
			removed, err := s.RemoveRecordIDs(rk, []string{secondID})
			if err != nil {
				t.Fatal(err)
			}
			if len(removed) != 1 || removed[0].Text != "second" {
				t.Fatalf("defect: %s dropped the exact durable identity; removing the second duplicate got %+v", tt.name, removed)
			}
			left, err := s.Peek(rk, -1)
			if err != nil {
				t.Fatal(err)
			}
			if len(left) != 1 || left[0].MessageID != 7 || (left[0].Text != "first" && left[0].Text != "first-refreshed") {
				t.Fatalf("defect: exact removal after %s removed the wrong same-MessageID occurrence; left=%+v", tt.name, left)
			}
			if _, err := s.RemoveRecordIDs(rk, []string{firstID}); err != nil {
				t.Fatal(err)
			}
			if n, _ := s.Pending(rk); n != 0 {
				t.Fatalf("first durable identity no longer resolves after %s; pending=%d", tt.name, n)
			}
		})
	}
}

func TestAppendTracked_PrivateIdentityIsOldReaderCompatible(t *testing.T) {
	s := newStore(t)
	dir := QueueDir()
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	want := msg(7, "body")
	recordID, err := s.AppendTracked(rk, want)
	if err != nil {
		t.Fatal(err)
	}
	lines := rawJSONL(t, dir, rk)
	if len(lines) != 1 || !strings.Contains(lines[0], `"_c3_queue_id":"`+recordID+`"`) {
		t.Fatalf("tracked line does not carry its private durable identity: %v", lines)
	}
	var oldReader c3types.Inbound
	if err := json.Unmarshal([]byte(lines[0]), &oldReader); err != nil {
		t.Fatalf("old Inbound reader rejected additive queue identity: %v", err)
	}
	if oldReader.MessageID != want.MessageID || oldReader.Text != want.Text {
		t.Fatalf("old Inbound reader changed the public record: got %+v want msg=%d text=%q", oldReader, want.MessageID, want.Text)
	}
}

// TestRemoveIDs_CorruptLineSteppedOver: a corrupt line in pending is never removed,
// never counted, and is stripped by the rewrite; the surrounding real lines are
// handled correctly.
func TestRemoveIDs_CorruptLineSteppedOver(t *testing.T) {
	s := newStore(t)
	dir := QueueDir()
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	// Layout: [msg1][corrupt][msg2][msg3].
	_ = s.Append(rk, msg(1, "m"))
	if err := appendRawLine(t, dir, rk, "{corrupt"); err != nil {
		t.Fatal(err)
	}
	_ = s.Append(rk, msg(2, "m"))
	_ = s.Append(rk, msg(3, "m"))

	removed, err := s.RemoveIDs(rk, map[int64][]int{2: {1}})
	if err != nil {
		t.Fatalf("RemoveIDs: %v", err)
	}
	// Only the real msg2 is removed; the corrupt line is never in the removed set.
	if len(removed) != 1 || removed[0].MessageID != 2 {
		t.Fatalf("removed = %+v, want only msg2", removed)
	}
	// msg1 + msg3 survive; corrupt is stripped from the file.
	if want := []int64{1, 3}; !reflect.DeepEqual(peekIDs(t, s, rk), want) {
		t.Fatalf("pending = %v, want %v", peekIDs(t, s, rk), want)
	}
	if lines := rawJSONL(t, dir, rk); len(lines) != 2 {
		t.Fatalf("on-disk lines = %d, want 2 (corrupt stripped)", len(lines))
	}
	// Draining returns msgs 1,3 exactly once (file length + cursor agree).
	drained, err := s.Consume(rk, -1)
	if err != nil {
		t.Fatal(err)
	}
	if want := []int64{1, 3}; !reflect.DeepEqual(idsOf(drained), want) {
		t.Fatalf("drain = %v, want %v exactly once", idsOf(drained), want)
	}
}

// TestRemoveIDs_CursorRemapPreservesConsumed: with lines already consumed past a
// corrupt line, RemoveIDs preserves the consumed region byte-for-byte, remaps the
// cursor into the corrupt-free space, and keeps Pending correct.
func TestRemoveIDs_CursorRemapPreservesConsumed(t *testing.T) {
	s := newStore(t)
	dir := QueueDir()
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	// Layout: [msg1][corrupt][msg2][msg3][msg4].
	m1 := msg(1, "one")
	_ = s.Append(rk, m1)
	if err := appendRawLine(t, dir, rk, "{corrupt"); err != nil {
		t.Fatal(err)
	}
	m2 := msg(2, "two")
	_ = s.Append(rk, m2)
	_ = s.Append(rk, msg(3, "three"))
	_ = s.Append(rk, msg(4, "four"))

	// Consume 2 REAL messages (msg1, then stepping over corrupt, msg2). Cursor now
	// sits past the corrupt line; msg3,msg4 pending.
	if got, err := s.Consume(rk, 2); err != nil || len(got) != 2 || got[1].MessageID != 2 {
		t.Fatalf("consume = %+v err=%v, want msg1,msg2", got, err)
	}
	// Capture the exact consumed-line bytes before the remove, read from the FILE
	// rather than re-marshaled from m1/m2. The invariant under test is that
	// rewrite() does not perturb bytes already on disk; re-deriving the
	// expectation from the in-memory struct instead pins the encoder's current
	// output, so any additive field Append legitimately stamps on the way to disk
	// (the V record-version marker) breaks it while rewrite() is still perfectly
	// faithful. Reading the real lines tests the real thing and cannot rot.
	// Layout at this point: [msg1][corrupt][msg2][msg3][msg4].
	beforeLines := rawJSONL(t, dir, rk)
	if len(beforeLines) != 5 {
		t.Fatalf("pre-remove on-disk lines = %d, want 5", len(beforeLines))
	}
	wantM1, wantM2 := beforeLines[0], beforeLines[2]

	// Remove the pending msg3. rewrite() strips the corrupt line that sat BEFORE the
	// cursor, so the cursor must be remapped (old .cur=3 → new .cur=2).
	removed, err := s.RemoveIDs(rk, map[int64][]int{3: {1}})
	if err != nil {
		t.Fatalf("RemoveIDs: %v", err)
	}
	if len(removed) != 1 || removed[0].MessageID != 3 {
		t.Fatalf("removed = %+v, want only msg3", removed)
	}
	// Consumed region preserved byte-for-byte at the head of the rewritten file.
	lines := rawJSONL(t, dir, rk)
	if len(lines) != 3 {
		t.Fatalf("on-disk lines = %d, want 3 (msg1,msg2,msg4)", len(lines))
	}
	if lines[0] != string(wantM1) || lines[1] != string(wantM2) {
		t.Fatalf("consumed region not byte-identical:\n got  %q,%q\n want %q,%q", lines[0], lines[1], wantM1, wantM2)
	}
	// Only msg4 remains pending, served exactly once — cursor stayed aligned.
	if want := []int64{4}; !reflect.DeepEqual(peekIDs(t, s, rk), want) {
		t.Fatalf("pending = %v, want [4]", peekIDs(t, s, rk))
	}
	if drained, _ := s.Consume(rk, -1); len(drained) != 1 || drained[0].MessageID != 4 {
		t.Fatalf("drain = %+v, want msg4 exactly once", drained)
	}
}

// TestRemoveIDs_TrashSnapshotMatchesRemoved: the .trash snapshot content equals the
// removed lines (recoverability, INV-4).
func TestRemoveIDs_TrashSnapshotMatchesRemoved(t *testing.T) {
	s := newStore(t)
	dir := QueueDir()
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 5; i++ {
		_ = s.Append(rk, msg(i, "body-"+string(rune('0'+i))))
	}
	removed, err := s.RemoveIDs(rk, map[int64][]int{2: {1}, 4: {1}})
	if err != nil {
		t.Fatalf("RemoveIDs: %v", err)
	}
	snap := trashEvicted(t, dir)
	if !reflect.DeepEqual(idsOf(snap), idsOf(removed)) {
		t.Fatalf("trash ids %v != removed ids %v", idsOf(snap), idsOf(removed))
	}
	for i := range snap {
		if snap[i].Text != removed[i].Text || snap[i].MessageID != removed[i].MessageID {
			t.Fatalf("trash[%d] = %+v, want %+v", i, snap[i], removed[i])
		}
	}
}

// TestRemoveIDs_MinMaxAgesRecompute: MIN/MAX pending ages hold under out-of-order
// timestamps and recompute after RemoveIDs drops the oldest line (B5).
func TestRemoveIDs_MinMaxAgesRecompute(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	base := time.Now().Truncate(time.Second)
	t3h := base.Add(-3 * time.Hour)
	t1h := base.Add(-1 * time.Hour)
	t5h := base.Add(-5 * time.Hour)
	// Append OUT OF timestamp order: newest, middle, oldest.
	_ = s.Append(rk, tsMsg(1, "m", t3h))
	_ = s.Append(rk, tsMsg(2, "m", t1h))
	_ = s.Append(rk, tsMsg(3, "m", t5h)) // oldest, but LAST in file
	st := s.StatusFor(rk)
	if st.OldestUnix != t5h.Unix() {
		t.Fatalf("OldestUnix = %d, want MIN %d (t5h, tail line)", st.OldestUnix, t5h.Unix())
	}
	if st.NewestUnix != t1h.Unix() {
		t.Fatalf("NewestUnix = %d, want MAX %d (t1h)", st.NewestUnix, t1h.Unix())
	}
	// Remove the oldest line (id 3) → ages recompute over {t3h, t1h}.
	if _, err := s.RemoveIDs(rk, map[int64][]int{3: {1}}); err != nil {
		t.Fatalf("RemoveIDs: %v", err)
	}
	st = s.StatusFor(rk)
	if st.OldestUnix != t3h.Unix() {
		t.Fatalf("OldestUnix after remove = %d, want %d (t3h)", st.OldestUnix, t3h.Unix())
	}
	if st.NewestUnix != t1h.Unix() {
		t.Fatalf("NewestUnix after remove = %d, want %d (t1h)", st.NewestUnix, t1h.Unix())
	}
}

// TestRefreshText_SkipsDrainedInLine: RefreshText (organic STT refresh, matched by
// per-chat MessageID) must SKIP a drained-in line that collides on MessageID and
// refresh the organic one instead (B7). The drained line is placed FIRST so that,
// absent the guard, it would be the one matched.
func TestRefreshText_SkipsDrainedInLine(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	_ = s.Append(rk, drainedMsg(5, "frozen", "telegram__-100__42")) // moved-in, same id 5
	_ = s.Append(rk, msg(5, "organic-old"))                         // organic, same id 5

	ok, err := s.RefreshText(rk, 5, "new-transcript")
	if err != nil {
		t.Fatalf("RefreshText: %v", err)
	}
	if !ok {
		t.Fatal("RefreshText should hit the organic id-5 line")
	}
	got, err := s.Peek(rk, -1)
	if err != nil {
		t.Fatalf("Peek: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("pending = %d, want 2", len(got))
	}
	if got[0].DrainedFrom == "" || got[0].Text != "frozen" {
		t.Fatalf("drained line = %+v, want text unchanged 'frozen'", got[0])
	}
	if got[1].DrainedFrom != "" || got[1].Text != "new-transcript" {
		t.Fatalf("organic line = %+v, want refreshed 'new-transcript'", got[1])
	}
}
