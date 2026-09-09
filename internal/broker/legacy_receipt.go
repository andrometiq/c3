package broker

import (
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
	"log"
	"time"
)

// A route notice is a read-only view of surviving queued identities. Legacy
// pushes keep their rows durable until ack, so a raw Pending count includes
// messages still attempting. Intersect with actual surviving ids, rather than
// subtracting a stale batch size after a drain, eviction or partial retirement.
func (b *Broker) noticePending(key RouteKey, owner *Stub) (int, error) {
	if b.Queue == nil {
		return 0, nil
	}
	rows, err := b.queuedRows(key)
	if err != nil {
		count := b.Queue.StatusFor(queueRouteKey(key)).Pending
		log.Printf("Held count read failed route=%s: %v; using cached pending=%d", routeKeyStr(key), err, count)
		return count, err
	}
	return len(rows), nil
}

// Use exact surviving revisions for every notice and /status. Receipt evidence
// remains evidence even if durable retirement failed and released the attempt.
func (b *Broker) queuedRows(key RouteKey) ([]queue.TrackedInbound, error) {
	if b.Queue == nil {
		return nil, nil
	}
	rows, err := b.Queue.PeekTracked(queueRouteKey(key), -1)
	if err != nil {
		return nil, err
	}
	hidden := map[string][]string{}
	for _, a := range b.attempts.snapshot(time.Now()) {
		if a.Route == key && (b.noticeAttemptOpen(a) || a.Evidence || a.Outcome == "confirmed") {
			for _, m := range a.Members {
				hidden[m.ID] = append(hidden[m.ID], m.Revision)
			}
		}
	}
	recorded := b.recorded.surviving(key, rows)
	out := rows[:0]
	for _, row := range rows {
		excluded := recorded[row.RecordID]
		for _, revision := range hidden[row.RecordID] {
			if revision == "" || revision == rowRevision(row) {
				excluded = true
				break
			}
		}
		if !excluded {
			out = append(out, row)
		}
	}
	return out, nil
}

// The legacy table's 15-second shadow deadline is not a host termination.
// Legacy adapters can still be trying their own inbox fallback on the same
// token. Keep that observation excluded until a terminal queue-only report,
// receipt, release or identity reconciliation. Only the current route holder
// can keep a legacy observation in flight; topic switches never hide backlog.
func (b *Broker) noticeAttemptOpen(a attemptRecord) bool {
	if a.Negotiated {
		return a.Outcome == "open"
	}
	holder, _ := b.Routes.Holder(a.Route)
	if holder == nil || holder != a.Holder.Stub || holder.ConnID != a.Holder.ConnID || !holder.IsAlive() {
		return false
	}
	if a.Outcome == "open" {
		return true
	}
	return !a.Negotiated && a.Outcome == "expired" && a.Reason == "unobserved" && a.Holder.Stub != nil && a.Holder.Stub.IsAlive() && a.Holder.Stub.RenderRoute().State != ipc.RenderQueueOnly
}
