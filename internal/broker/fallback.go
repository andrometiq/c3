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
//
// ONLY valid while the durable queue is live. When it is not, the reassurance is
// a lie told at the exact moment the message is destroyed — use
// heldDegradedText() instead (worker.go picks between them on Broker.Queue).
func heldReplyText(n int) string {
	plural := "messages"
	if n == 1 {
		plural = "message"
	}
	return fmt.Sprintf("📨 Held — nothing lost.\n%d %s queued.\n\n\nSend /status to check.", n, plural)
}

// queueDisabledWarning is the ONE sentence every degraded-mode operator surface
// says — the startup announcement (broker.go announceQueueDegraded), the
// held-notice below, and `/status` (status_command.go) — so the three can never
// drift apart.
//
// "Degraded mode" is exactly Broker.Queue == nil: queue.NewStore failed at
// startup and the broker chose to keep running (broker.go New). In that mode
// flushInbounds still marks every inbound persisted — it must, or the source
// update_id wedges in-flight forever and ALL inbound re-polls forever — so the
// update is acked to Telegram with nothing written anywhere. Telegram never
// redelivers an acked update, so every message arriving with no session attached
// is destroyed for the whole run. Keeping that silent was the defect; these
// surfaces are the fix.
const queueDisabledWarning = "C3's durable queue is DISABLED for this run — it failed to open at startup, so messages that arrive while no session is attached are NOT saved and cannot be recovered."

// degradedDropLogPhrase is the phrase every dropped-message log line carries in
// degraded mode, and the exact string docs/USAGE.md tells operators to grep
// broker.log for. A const rather than a literal so the doc and the log cannot
// drift into naming different things (TestDocsQuoteTheRealNotices pins it).
const degradedDropLogPhrase = "DROPPED — durable queue disabled"

// heldDegradedText is what the auto-reply says INSTEAD of heldReplyText when the
// durable queue is disabled. It carries no count on purpose: nothing was queued,
// so there is nothing to count and nothing for `fetch_queue` to find later.
func heldDegradedText() string {
	return "⚠️ NOT held — that message was dropped.\n" + queueDisabledWarning + "\n\n\nSend /status to check."
}
