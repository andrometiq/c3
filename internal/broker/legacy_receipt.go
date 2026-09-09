package broker

import (
	"log"
	"time"
)

// A route notice is a read-only view of surviving queued identities. Legacy
// pushes keep their rows durable until ack, so a raw Pending count includes
// messages still attempting. Intersect with actual surviving ids, rather than
// subtracting a stale batch size after a drain, eviction or partial retirement.
func (b *Broker) noticePending(key RouteKey, owner *Stub) (int, error) {
	rows, err := b.Queue.PeekTracked(queueRouteKey(key), -1)
	if err != nil {
		count := b.Queue.StatusFor(queueRouteKey(key)).Pending
		log.Printf("Held count read failed route=%s: %v; using cached pending=%d", routeKeyStr(key), err, count)
		return count, err
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
	return count, nil
}
