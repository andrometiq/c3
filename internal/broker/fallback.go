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

// defaultHeldNoticeCooldown throttles the edit-capable "held — nothing lost"
// auto-reply: at most one held-notice per route per this window, so a burst of
// held messages coalesces into a single notice instead of a per-message flood
// (msg 6083). Short (unlike the 5-min fallback) so a genuinely new message after
// a quiet gap still re-alerts promptly.
const defaultHeldNoticeCooldown = 10 * time.Second

// fallbackText is the boilerplate reply sent on a no-claim inbound.
const fallbackText = "No CLI is currently attached to this topic. Run `c3-broker status` to see attached terminals, or open a CLI in the project directory and `attach`."

// heldReplyText is the "held, nothing lost" auto-reply sent when an inbound is
// queued because no session is attached. It reassures and carries the running
// count of queued messages. Cadence is the existing 5-min fallback cooldown.
func heldReplyText(n int) string {
	plural := "messages"
	if n == 1 {
		plural = "message"
	}
	return fmt.Sprintf("📨 Held — nothing lost.\n%d %s queued.\n\n\nSend /status to check.", n, plural)
}
