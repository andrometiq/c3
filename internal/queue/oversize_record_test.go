package queue

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

func oversizeStore(t *testing.T) (*Store, string, RouteKey) {
	t.Helper()
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	tid := int64(77)
	return s, dir, RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
}

// A record larger than readLines' scanner cap makes the WHOLE route unreadable:
// Peek fails with "token too long", and pendingStats swallowed that error and
// returned 0 — so /status, the attach backlog summary and the held-notice count
// all reported an EMPTY route that was holding real messages. Silently, and
// permanently.
func TestPendingStats_UnreadableQueueIsNotReportedEmpty(t *testing.T) {
	s, dir, rk := oversizeStore(t)

	// Three ordinary records, then one legacy line past the 8 MiB scanner cap
	// (written raw: exactly the shape an unbounded producer left behind).
	for i := int64(1); i <= 3; i++ {
		if err := s.Append(rk, &c3types.Inbound{
			Channel: "telegram", ChatID: -100, TopicID: rk.TopicID,
			MessageID: i, Text: "hello", Timestamp: time.Now(),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	f, err := os.OpenFile(filepath.Join(dir, rk.File()+".jsonl"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("open jsonl: %v", err)
	}
	if _, err := f.Write(append([]byte(`{"Channel":"telegram","Text":"`+strings.Repeat("q", 9*1024*1024)+`"}`), '\n')); err != nil {
		t.Fatalf("write oversize line: %v", err)
	}
	f.Close()

	if _, perr := s.Peek(rk, -1); perr == nil {
		t.Fatal("fixture: Peek was expected to fail on the over-cap line")
	}

	n, _ := s.Pending(rk)
	if n == 0 {
		t.Fatal("an unreadable queue reported Pending 0 while holding 4 records — /status, the attach backlog summary and " +
			"the held-notice count all say the route is empty, permanently and silently, while Peek fails with \"token too long\"")
	}
	if n < 4 {
		t.Errorf("Pending reported %d of 4 records — an unreadable queue must not under-report what it is holding", n)
	}
}

// Append is the one gate every producer passes through (debounce-merge re-queue,
// a plugin replacing merged.Text, STT stdout), and it had no byte bound at all.
// An unbounded record wedges its own route: over the frame cap it can never be
// delivered, over the scanner cap it makes the route unreadable.
func TestAppend_BoundsTheRecordItStores(t *testing.T) {
	s, dir, rk := oversizeStore(t)

	const mark = "TRANSCRIPT-TAIL-MARKER"
	huge := strings.Repeat("w", 6*1024*1024) + mark
	err := s.Append(rk, &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: rk.TopicID,
		MessageID: 42, Text: huge, Timestamp: time.Now(),
	})
	// Rejecting is NOT an acceptable answer: the caller holds the Telegram offset
	// on an Append error, so a rejected record is redelivered forever and the route
	// stops receiving anything new.
	if err != nil {
		t.Fatalf("Append refused an oversize record (%v) — the producer will hold the offset and Telegram will redeliver the same record forever", err)
	}

	got, perr := s.Peek(rk, -1)
	if perr != nil {
		t.Fatalf("the stored record is not readable back: %v", perr)
	}
	if len(got) != 1 {
		t.Fatalf("peeked %d records, want 1", len(got))
	}
	if n := len(got[0].Text); n > MaxRecordBytes {
		t.Errorf("stored Text is %d bytes, past the %d-byte record bound — this record can never be delivered in one frame", n, MaxRecordBytes)
	}
	if got[0].MessageID != 42 {
		t.Errorf("identity lost: MessageID=%d, want 42", got[0].MessageID)
	}
	if !strings.Contains(got[0].Text, "truncated") {
		t.Errorf("the stored record does not say it was truncated; the agent would read a cut-off message as the whole message. Text tail: %q",
			tail(got[0].Text, 200))
	}

	// The bytes that were cut are not destroyed — the full original is retained.
	if !trashContains(t, dir, mark) {
		t.Error("the original oversize record was not retained under .trash/ — the truncated bytes are gone for good")
	}
}

// An ordinary record must be stored byte-for-byte; the bound is a ceiling, not a
// filter.
func TestAppend_OrdinaryRecordIsUntouched(t *testing.T) {
	s, _, rk := oversizeStore(t)
	body := strings.Repeat("m", 64*1024)
	if err := s.Append(rk, &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: rk.TopicID,
		MessageID: 7, Text: body, Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	got, err := s.Peek(rk, -1)
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if len(got) != 1 || got[0].Text != body {
		t.Fatalf("an ordinary record was altered on the way to disk (len %d, want %d)", len(got[0].Text), len(body))
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

func trashContains(t *testing.T, dir, needle string) bool {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, trashDirName, "*"))
	if err != nil {
		t.Fatalf("glob trash: %v", err)
	}
	for _, m := range matches {
		data, rerr := os.ReadFile(m)
		if rerr == nil && strings.Contains(string(data), needle) {
			return true
		}
	}
	return false
}
