package broker

import (
	"fmt"
	"sync"
	"time"
)

// fallbackTracker enforces the cooldown-fallback dedup rule from spec §4.4.3:
// when an inbound for (channel, chat, *topic) arrives but no stub holds the
// claim, the broker sends a single "no CLI attached" reply and records the
// timestamp. Subsequent inbounds for the same key within `cooldown` are
// silently dropped (no second fallback) until the window passes.
//
// Default cooldown is 300s (5 minutes); spec-configurable per channel via
// mappings.json:channels.<chan>.fallback_cooldown_s.
type fallbackTracker struct {
	mu        sync.Mutex
	lastByKey map[RouteKey]time.Time
	cooldown  time.Duration
}

// newFallbackTracker returns a tracker with the given cooldown.
func newFallbackTracker(cooldown time.Duration) *fallbackTracker {
	return &fallbackTracker{
		lastByKey: map[RouteKey]time.Time{},
		cooldown:  cooldown,
	}
}

// ShouldSend returns true and updates the timestamp if cooldown has elapsed
// since the last fallback for key. Returns false otherwise (caller should
// silently drop).
func (f *fallbackTracker) ShouldSend(key RouteKey) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	last := f.lastByKey[key]
	if time.Since(last) < f.cooldown {
		return false
	}
	f.lastByKey[key] = time.Now()
	return true
}

const defaultFallbackCooldown = 300 * time.Second

// defaultHeldNoticeCooldown preserves the web status-event cadence. Telegram
// uses backlog episodes instead of elapsed time.
const defaultHeldNoticeCooldown = 10 * time.Second

// fallbackText is the boilerplate reply sent on a no-claim inbound.
const fallbackText = "No CLI is currently attached to this topic. Run `c3-broker status` to see attached terminals, or open a CLI in the project directory and `attach`."

// heldReplyText is the "held, nothing lost" auto-reply sent when an inbound is
// still queued after scheduling. It reassures and carries the running
// count of queued messages. Telegram announces once per backlog episode.
//
// ONLY valid while the durable queue is live. When it is not, the reassurance is
// inaccurate about local storage — use
// heldDegradedText() instead (worker.go picks between them on Broker.Queue).
func heldReplyText(channelName string, n int) string {
	plural := "messages"
	if n == 1 {
		plural = "message"
	}
	return fmt.Sprintf("📨 Held — nothing lost. %d %s queued. Send /status to check.", n, plural)
}

// queueDisabledWarning is shared by startup, held notices, and status.
// Queue-disabled intake holds Telegram offsets; only restart with a working
// queue restores durable intake. Live handoffs do not acknowledge updates.
const queueDisabledWarning = "C3's durable queue is DISABLED for this run — inbound is held at Telegram and replayed when the broker restarts with a working queue; anything delivered live in the meantime will arrive again."

// degradedHoldLogPhrase is also quoted in docs/USAGE.md for log searches.
// TestDocsQuoteTheRealNotices keeps the documentation and code in sync.
const degradedHoldLogPhrase = "HELD AT TELEGRAM — durable queue disabled"

// heldDegradedText is what the auto-reply says INSTEAD of heldReplyText when the
// durable queue is disabled. It carries no count on purpose: nothing was queued,
// so there is nothing to count and nothing for `fetch_queue` to find later.
func heldDegradedText() string {
	return "⚠️ Held at Telegram — local queue unavailable.\n" + queueDisabledWarning + "\n\n\nSend /status to check."
}

func (f *fallbackTracker) remaining(key RouteKey) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return max(time.Millisecond, time.Until(f.lastByKey[key].Add(f.cooldown)))
}

// Telegram announces each durable backlog episode once. Exceptional warnings
// keep their separate cooldown; web keeps the ordinary notice cooldown.
func (f *fallbackTracker) reserveHeld(key RouteKey) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	last, hasSent := f.lastByKey[key]
	if (key.Channel == "telegram" && hasSent) || (key.Channel != "telegram" && time.Since(last) < f.cooldown) {
		return time.Time{}
	}
	stamp := time.Now()
	f.lastByKey[key] = stamp
	return stamp
}

// A failed send cannot cancel a newer episode's reservation.
func (f *fallbackTracker) cancelHeld(key RouteKey, stamp time.Time) {
	if key.Channel != "telegram" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !stamp.IsZero() && f.lastByKey[key] == stamp {
		delete(f.lastByKey, key)
	}
}

func (f *fallbackTracker) clearEpisode(key RouteKey) {
	if key.Channel != "telegram" {
		return
	}
	f.mu.Lock()
	delete(f.lastByKey, key)
	f.mu.Unlock()
}
