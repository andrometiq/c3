package telegram

import "sync"

// offsetTracker advances the Telegram offset only through the ordered update_ids
// C3 has actually observed and finished — durably persisted (Append+fsync
// succeeded), or a no-op (gated / dropped / non-message). Telegram update_ids
// start at an arbitrary positive number, and allowed-update filtering can leave
// numeric gaps, so arithmetic contiguity (committed+1) is not a valid frontier.
//
// An observed update whose message is still mid-STT/mid-persist stays in-flight,
// so the committed offset cannot pass it; if the broker crashes there, the
// offset never advanced and Telegram redelivers it (within 24h). Loss-free by
// construction.
//
// It is goroutine-safe: the poll loop Registers every update in a returned batch
// before dispatching any of them; MarkDone is called from both the poll loop
// (gated/dropped/non-message) and per-route worker goroutines (after
// Append+fsync); Committed is read by the poll loop before Save.
type offsetTracker struct {
	mu        sync.Mutex
	committed int64          // highest observed id whose ordered prefix is done
	pending   map[int64]bool // observed ids > committed; true=done, false=in-flight
}

// newOffsetTracker seeds committed from the persisted store (0 if none).
func newOffsetTracker(committed int64) *offsetTracker {
	return &offsetTracker{
		committed: committed,
		pending:   map[int64]bool{},
	}
}

// Register marks an accepted update_id as in-flight. Ids <= committed are
// ignored (already accounted for — e.g. a dedup-skip re-seen after restart).
func (t *offsetTracker) Register(id int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if id <= t.committed {
		return
	}
	if _, observed := t.pending[id]; !observed {
		t.pending[id] = false
	}
}

// MarkDone marks an update_id done and advances committed over the now-complete
// prefix of observed ids. Numeric gaps between observed ids are ignored.
//
// Safe to call for an id never Registered (some direct unit paths mark no-op
// updates done); production pre-registers the complete returned batch before
// dispatch so a fast completion can never advance past an observed-but-not-yet-
// registered update.
func (t *offsetTracker) MarkDone(id int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if id <= t.committed {
		return
	}
	t.pending[id] = true
	for len(t.pending) > 0 {
		var (
			next     int64
			nextDone bool
			found    bool
		)
		for observed, done := range t.pending {
			if !found || observed < next {
				next, nextDone, found = observed, done, true
			}
		}
		if !nextDone {
			return
		}
		t.committed = next
		delete(t.pending, next)
	}
}

// Committed returns the highest observed update_id whose ordered prefix is done.
// Persist this value; the next getUpdates offset is Committed()+1.
func (t *offsetTracker) Committed() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.committed
}
