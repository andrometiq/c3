package telegram

import (
	"strings"
	"testing"
)

// countLogsContaining returns how many rendered log lines contain sub.
func (h *fakeHost) countLogsContaining(sub string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, l := range h.logs {
		if strings.Contains(l, sub) {
			n++
		}
	}
	return n
}

// TestLogDedupSkip_InFlightRefetch_ThrottledToOncePerFrontier locks the
// re-poll/dedup-skip noise fix (ROADMAP live regression 2026-06-27).
//
// The persisted-offset tracker holds the getUpdates offset at Committed()+1 for
// loss-freedom, so Telegram re-draws any un-persisted frontier update on EVERY
// poll and the dedup map skips each redraw. That is expected, not a "recent
// duplicate", and must NOT be logged ~1/s. logDedupSkip logs such an in-flight
// re-fetch at most ONCE per distinct frontier id, while a genuine redelivery of
// an already-committed update keeps its per-occurrence line.
func TestLogDedupSkip_InFlightRefetch_ThrottledToOncePerFrontier(t *testing.T) {
	h := &fakeHost{}
	c := &Channel{host: h}
	c.offTrk = newOffsetTracker(100) // committed = 100
	c.offTrk.Register(101)           // 101 is the un-acked frontier (in-flight)

	// Five poll passes re-draw the same in-flight frontier update. Before the
	// fix this logged five "recent duplicate" lines (the ~1/s spam).
	var last int64
	for i := 0; i < 5; i++ {
		last = c.logDedupSkip(101, last)
	}
	if last != 101 {
		t.Fatalf("throttle cursor = %d, want 101", last)
	}
	if n := h.countLogsContaining("re-poll skip update="); n != 1 {
		t.Fatalf("in-flight re-fetch logged %d times, want exactly 1 (must not spam every poll)", n)
	}
	if n := h.countLogsContaining("recent duplicate"); n != 0 {
		t.Fatalf("an in-flight re-fetch must NOT be logged as a recent duplicate; got %d", n)
	}

	// A genuine redelivery of an already-committed update (id <= committed) is
	// rare and worth seeing, so it keeps its per-occurrence "recent duplicate".
	c.logDedupSkip(50, last)
	c.logDedupSkip(50, last)
	if n := h.countLogsContaining("recent duplicate"); n != 2 {
		t.Fatalf("genuine duplicate logged %d times, want 2 (per occurrence)", n)
	}

	// When the frontier advances to a NEW stuck id, that id logs once more — the
	// throttle is per-frontier-id, so a genuinely wedged update stays visible.
	c.offTrk.Register(102)
	last = c.logDedupSkip(102, last)
	if last != 102 {
		t.Fatalf("throttle cursor after new frontier = %d, want 102", last)
	}
	if n := h.countLogsContaining("re-poll skip update="); n != 2 {
		t.Fatalf("new frontier id must log once more; got %d re-poll lines, want 2", n)
	}
}

// TestLogDedupSkip_NoTracker_FallsBackToRecentDuplicate covers the legacy path
// (offTrk nil — the conflict/resilience unit configs): with no tracker there is
// no notion of an "in-flight frontier", so every dedup-skip keeps the classic
// "recent duplicate" line.
func TestLogDedupSkip_NoTracker_FallsBackToRecentDuplicate(t *testing.T) {
	h := &fakeHost{}
	c := &Channel{host: h} // offTrk stays nil

	var last int64
	last = c.logDedupSkip(500, last)
	last = c.logDedupSkip(500, last)
	if last != 0 {
		t.Fatalf("no-tracker path must not move the in-flight cursor; got %d", last)
	}
	if n := h.countLogsContaining("recent duplicate"); n != 2 {
		t.Fatalf("no-tracker dedup-skip logged %d times, want 2 (per occurrence)", n)
	}
	if n := h.countLogsContaining("re-poll skip"); n != 0 {
		t.Fatalf("no-tracker path must not emit re-poll-skip lines; got %d", n)
	}
}
