package telegram

import "testing"

func TestOffsetTracker_ObservedPrefixAdvance(t *testing.T) {
	tr := newOffsetTracker(0)
	tr.Register(1)
	tr.Register(2)
	tr.Register(3)
	// Out-of-order completion: 2 done first must NOT advance past the gap at 1.
	tr.MarkDone(2)
	if got := tr.Committed(); got != 0 {
		t.Fatalf("committed with gap at 1 = %d, want 0", got)
	}
	tr.MarkDone(1) // now 1,2 contiguous
	if got := tr.Committed(); got != 2 {
		t.Fatalf("committed after 1,2 done = %d, want 2", got)
	}
	tr.MarkDone(3)
	if got := tr.Committed(); got != 3 {
		t.Fatalf("committed after all done = %d, want 3", got)
	}
}

func TestOffsetTracker_FreshArbitraryFirstID(t *testing.T) {
	tr := newOffsetTracker(0)
	const first = int64(483747726)
	tr.Register(first)
	tr.MarkDone(first)
	if got := tr.Committed(); got != first {
		t.Fatalf("committed after arbitrary first update = %d, want %d", got, first)
	}
}

func TestOffsetTracker_NumericGapsDoNotBlockObservedPrefix(t *testing.T) {
	tr := newOffsetTracker(10)
	for _, id := range []int64{101, 105, 900} {
		tr.Register(id)
	}

	// Later observed updates can finish first, but the earliest observed
	// in-flight update still holds the loss-free frontier.
	tr.MarkDone(900)
	tr.MarkDone(105)
	if got := tr.Committed(); got != 10 {
		t.Fatalf("committed past observed in-flight 101 = %d, want 10", got)
	}

	// Once 101 finishes, numeric holes contain no delivered work and therefore
	// do not wedge the offset.
	tr.MarkDone(101)
	if got := tr.Committed(); got != 900 {
		t.Fatalf("committed across valid numeric gaps = %d, want 900", got)
	}
}

func TestOffsetTracker_GatedOrDroppedDoesNotBlock(t *testing.T) {
	tr := newOffsetTracker(10)
	tr.Register(11)
	tr.Register(12)
	tr.Register(13)
	// 12 is a gated/dropped/non-message update → MarkDone immediately.
	tr.MarkDone(12)
	tr.MarkDone(11)
	if got := tr.Committed(); got != 12 {
		t.Fatalf("committed = %d, want 12 (gated 12 must not block)", got)
	}
	// 13 still in-flight (mid-STT) holds the line.
	if got := tr.Committed(); got >= 13 {
		t.Fatalf("committed should not pass in-flight 13, got %d", got)
	}
	tr.MarkDone(13)
	if got := tr.Committed(); got != 13 {
		t.Fatalf("committed after 13 = %d, want 13", got)
	}
}

func TestOffsetTracker_CrashBeforePersistDoesNotAdvance(t *testing.T) {
	tr := newOffsetTracker(5)
	tr.Register(6) // accepted but its Append never completes (crash)
	if got := tr.Committed(); got != 5 {
		t.Fatalf("committed with in-flight 6 = %d, want 5 (no advance → Telegram redelivers)", got)
	}
}
