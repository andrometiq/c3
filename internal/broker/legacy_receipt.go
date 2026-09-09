package broker

import "time"

// Legacy transcript adapters use the same 15-second receipt window as the
// broker's attempt observation. Once it expires, a late receipt cannot retire
// the row that has returned to fetch. Accept-based legacy adapters retain their
// historical timing: their host submission can legitimately take much longer.
// Called on the worker, under withConfirmedHolder, after exact correlation.
func (w *RouteWorker) legacyReceiptCurrent(owner *Stub, token string) bool {
	if owner == nil || !owner.ReceiptConfirming {
		return true
	}
	for _, a := range w.broker.attempts.lookup(token, time.Now()) {
		if a.Route == w.key {
			return a.Holder.Stub == owner && a.Outcome == "open"
		}
	}
	// The bounded legacy observation can be absent. Exact covered-row
	// correlation remains authoritative in that compatibility case; only an
	// observed expired/closed attempt can reject the otherwise valid receipt.
	return true
}

// A route notice is a read-only view of surviving queued identities. Legacy
// pushes keep their rows durable until ack, so a raw Pending count includes
// messages still attempting. Intersect with actual surviving ids, rather than
// subtracting a stale batch size after a drain, eviction or partial retirement.
func (b *Broker) noticePending(key RouteKey, owner *Stub) int {
	rows, err := b.Queue.PeekTracked(queueRouteKey(key), -1)
	if err != nil {
		return 0
	}
	hidden := map[string]bool{}
	for _, a := range b.attempts.snapshot(time.Now()) {
		if a.Route == key && a.Holder.Stub == owner && a.Outcome == "open" {
			for _, member := range a.Members {
				hidden[member.ID] = true
			}
		}
	}
	count := 0
	for _, row := range rows {
		if !hidden[row.RecordID] {
			count++
		}
	}
	return count
}
