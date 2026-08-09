package queue

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

// --- helpers for the torn-append / corrupt-line quarantine tests ---

// captureLog redirects the standard logger into a buffer for the duration of the
// test. The store's "never silently destroy queued bytes" guarantee is literally a
// statement about broker.log, so these tests have to read what was logged — a test
// that only checked the files would pass on a version that destroys the bytes
// quietly.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(old) })
	return &buf
}

// corruptTrash concatenates every .trash/<base>.<stamp>.corrupt.jsonl written so
// far, so a test can assert the RAW bytes of a quarantined line survived.
func corruptTrash(t *testing.T) string {
	t.Helper()
	var sb strings.Builder
	for _, n := range lsTrash(t) {
		if !strings.HasSuffix(n, ".corrupt.jsonl") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(QueueDir(), trashDirName, n))
		if err != nil {
			t.Fatalf("read %s: %v", n, err)
		}
		sb.Write(b)
	}
	return sb.String()
}

// writeTornTail writes a route's .jsonl with a PARTIAL final line — no trailing
// newline — exactly as a real ENOSPC/EFBIG or power-cut Append leaves it (Go's
// poll.FD.Write loops over short writes, so the bytes it managed to write persist
// while Append still reports failure). appendRawLine cannot produce this: it always
// terminates the line, which is why the whole suite missed this shape.
func writeTornTail(t *testing.T, rk RouteKey, partial string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(QueueDir(), rk.File()+".jsonl"), []byte(partial), 0600); err != nil {
		t.Fatalf("seed torn tail: %v", err)
	}
}

// TestAppend_TornTailDoesNotSwallowNextMessage is the regression for LOSS PATH 2.
// A torn write leaves a partial line with NO trailing newline; appending straight
// onto it concatenates the incoming record onto the fragment, the joined line no
// longer parses, and the message is destroyed even though Append returned nil (so
// the caller lets the Telegram offset advance past it — total, permanent loss).
//
// Two independent halves, both asserted here:
//
//	(i) the incoming record must land intact on its own line;
//	(ii) the fragment must not be destroyed silently — its RAW bytes go to .trash/
//	     and the removal is logged.
//
// On the unfixed store (i) fails immediately: Peek returns zero records.
func TestAppend_TornTailDoesNotSwallowNextMessage(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	// Exactly the shape a real Append produced under RLIMIT_FSIZE/ENOSPC: a record
	// cut off mid-string, no trailing newline.
	writeTornTail(t, rk, `{"Channel":"telegram","ChatID":-100,"MessageID":1,"Text":"HALF`)
	logbuf := captureLog(t)

	if err := s.Append(rk, msg(2, "SURVIVOR")); err != nil {
		t.Fatalf("append onto torn tail: %v", err)
	}

	// (i) The next message must be intact and deliverable.
	got, err := s.Peek(rk, 10)
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if len(got) != 1 || got[0].MessageID != 2 || got[0].Text != "SURVIVOR" {
		t.Fatalf("torn tail swallowed the next message: peek = %+v", got)
	}
	if !strings.Contains(logbuf.String(), "torn tail detected") {
		t.Fatalf("torn tail was healed silently; log = %q", logbuf.String())
	}

	// (ii) Stripping the fragment must retain its RAW bytes and say so.
	if _, err := s.RemoveIDs(rk, map[int64][]int{2: {1}}); err != nil {
		t.Fatalf("RemoveIDs: %v", err)
	}
	if !strings.Contains(corruptTrash(t), `"Text":"HALF`) {
		t.Fatalf("fragment destroyed with no .trash copy; trash = %v", lsTrash(t))
	}
	if !strings.Contains(logbuf.String(), "unparseable") {
		t.Fatalf("destruction was silent; log = %q", logbuf.String())
	}
}

// TestEvictOverCap_QuarantinesCorruptLine covers the quarantine hook on the
// cap/age eviction path — the rewrite that runs after EVERY successful Append
// (worker.go's evictIfOverCap), and therefore the one most likely to be the first
// to strip a corrupt line from a live queue.
func TestEvictOverCap_QuarantinesCorruptLine(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	// Layout: [old (age-evicted)] [corrupt] [fresh]. The age path triggers the same
	// rewrite as the count cap without appending 1000 messages.
	old := &c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 1, Text: "old", Timestamp: time.Now().Add(-MaxAge - time.Hour)}
	if err := s.Append(rk, old); err != nil {
		t.Fatal(err)
	}
	if err := appendRawLine(t, QueueDir(), rk, "{EVICT-TORN-BYTES"); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(rk, msg(2, "fresh")); err != nil {
		t.Fatal(err)
	}
	logbuf := captureLog(t)

	dropped, err := s.EvictOverCap(rk)
	if err != nil {
		t.Fatalf("EvictOverCap: %v", err)
	}
	if dropped != 1 {
		t.Fatalf("dropped = %d, want 1 (the old real line)", dropped)
	}
	if !strings.Contains(corruptTrash(t), "{EVICT-TORN-BYTES") {
		t.Fatalf("evict rewrite destroyed the corrupt line with no .trash copy; trash = %v", lsTrash(t))
	}
	if !strings.Contains(logbuf.String(), "unparseable") {
		t.Fatalf("evict rewrite destroyed the corrupt line silently; log = %q", logbuf.String())
	}
}

// TestEvictOverCap_AllCorruptQuarantinesBeforeRetire covers the OTHER quarantine
// hook in EvictOverCap: the len(real) == 0 branch, where every remaining line is
// unparseable and the rewrite empties the file outright. That branch retires a
// zero-byte pair, so without the quarantine the bytes leave with no copy anywhere.
func TestEvictOverCap_AllCorruptQuarantinesBeforeRetire(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	writeTornTail(t, rk, "{ONLY-CORRUPT-BYTES\n")
	logbuf := captureLog(t)

	dropped, err := s.EvictOverCap(rk)
	if err != nil {
		t.Fatalf("EvictOverCap: %v", err)
	}
	if dropped != 0 {
		t.Fatalf("dropped = %d, want 0 (a corrupt line is stripped, not evicted)", dropped)
	}
	if !strings.Contains(corruptTrash(t), "{ONLY-CORRUPT-BYTES") {
		t.Fatalf("all-corrupt rewrite destroyed the bytes with no .trash copy; trash = %v", lsTrash(t))
	}
	if !strings.Contains(logbuf.String(), "unparseable") {
		t.Fatalf("all-corrupt rewrite was silent; log = %q", logbuf.String())
	}
}

// TestTrashStamp_CorruptFilename pins that GC ages a quarantine file by its
// FILENAME stamp. Without the ".corrupt.jsonl" case, trashStamp falls through to
// the ".jsonl" arm, tries to ParseInt("corrupt"), fails, and the caller silently
// falls back to ModTime — so a quarantined file ages by when the filesystem last
// touched it rather than when it was written.
func TestTrashStamp_CorruptFilename(t *testing.T) {
	const stamp = int64(1785077928373988407)
	got, ok := trashStamp("telegram__-100__none.1785077928373988407.corrupt.jsonl")
	if !ok || got != stamp {
		t.Fatalf("trashStamp(corrupt file) = (%d, %v), want (%d, true)", got, ok, stamp)
	}
}

// TestConsume_LogsSteppedOverCorruptLine pins the never-silent line at the moment
// the loss becomes permanent: the cursor only moves forward, so once Consume steps
// past an unparseable line those bytes can never be delivered as a message. The
// cursor walks each corrupt line exactly once, so this logs once per line.
func TestConsume_LogsSteppedOverCorruptLine(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	if err := s.Append(rk, msg(1, "one")); err != nil {
		t.Fatal(err)
	}
	if err := appendRawLine(t, QueueDir(), rk, "{bad"); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(rk, msg(2, "two")); err != nil {
		t.Fatal(err)
	}
	logbuf := captureLog(t)

	got, err := s.Consume(rk, -1)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(got) != 2 || got[0].MessageID != 1 || got[1].MessageID != 2 {
		t.Fatalf("consume = %+v, want msgs 1,2 (corrupt stepped over, not returned)", got)
	}
	if !strings.Contains(logbuf.String(), "cursor stepped over 1 unparseable line(s)") {
		t.Fatalf("the cursor stepped past undeliverable bytes silently; log = %q", logbuf.String())
	}
}

// TestQuarantineCorrupt_RetentionDisabledStillLogs covers the degraded mode (item
// G): with no .trash/ to retain into, the copy is impossible — but "never silent"
// is NOT part of the degradation, so the destruction must still be logged and the
// rewrite must still succeed. Without the retentionDisabled branch, quarantine
// would try to write into a directory that does not exist and fail the whole
// voice resolve.
func TestQuarantineCorrupt_RetentionDisabledStillLogs(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	// A stray regular FILE on the .trash path → MkdirAll fails → retention disabled.
	if err := os.WriteFile(filepath.Join(dir, trashDirName), []byte("stray"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if !s.retentionDisabled {
		t.Fatal("retention should be disabled when .trash/ can't be created")
	}
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	recordID, err := s.AppendTracked(rk, msg(1, "stt pending"), "voice-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := appendRawLine(t, dir, rk, "{NO-TRASH-BYTES"); err != nil {
		t.Fatal(err)
	}
	logbuf := captureLog(t)

	ok, allDone, err := s.ResolveVoiceText(rk, recordID, "voice-1", "fixed transcript")
	if err != nil {
		t.Fatalf("ResolveVoiceText with retention disabled must still succeed: %v", err)
	}
	if !ok || !allDone {
		t.Fatal("ResolveVoiceText should finish the pending msg 1")
	}
	if !strings.Contains(logbuf.String(), "DESTROYING 1 unparseable line(s)") {
		t.Fatalf("retention-disabled destruction was silent; log = %q", logbuf.String())
	}
}
