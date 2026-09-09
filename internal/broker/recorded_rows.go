package broker

import (
	"sync"

	"github.com/Andrometiq/c3/internal/queue"
)

// Receipt evidence outlives bounded attempt history. Keep only an identity and
// revision for each surviving recorded row, never message payloads or tokens.
// This follows the broker lifetime (restart can replay), including worker exits.
// Successful removal or a new content revision releases the marker.
type recordedRows struct {
	mu     sync.Mutex
	routes map[RouteKey]map[string]string
}

func (r *recordedRows) remember(key RouteKey, members []attemptMember) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.routes == nil {
		r.routes = map[RouteKey]map[string]string{}
	}
	if r.routes[key] == nil {
		r.routes[key] = map[string]string{}
	}
	for _, m := range members {
		if m.ID != "" {
			r.routes[key][m.ID] = m.Revision
		}
	}
}
func (r *recordedRows) forget(key RouteKey, ids []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range ids {
		delete(r.routes[key], id)
	}
	if len(r.routes[key]) == 0 {
		delete(r.routes, key)
	}
}
func (r *recordedRows) surviving(key RouteKey, rows []queue.TrackedInbound) map[string]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	kept := map[string]bool{}
	for _, row := range rows {
		revision, ok := r.routes[key][row.RecordID]
		if ok && (revision == "" || revision == rowRevision(row)) {
			kept[row.RecordID] = true
		}
	}
	for id := range r.routes[key] {
		if !kept[id] {
			delete(r.routes[key], id)
		}
	}
	if len(r.routes[key]) == 0 {
		delete(r.routes, key)
	}
	return kept
}

// Legacy receipts name identities without content revisions. Bind available
// rows at the authorized acknowledgment; if storage is unreadable, retain the
// identity until an explicit enrichment/removal invalidates that evidence.
func (w *RouteWorker) rememberLegacyReceipt(ids []string) {
	members := attemptMembers(ids)
	rows, err := w.broker.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if err == nil {
		wanted := map[string]int{}
		for i, m := range members {
			wanted[m.ID] = i
		}
		for _, row := range rows {
			if i, ok := wanted[row.RecordID]; ok {
				members[i].Revision = rowRevision(row)
			}
		}
	}
	w.broker.recorded.remember(w.key, members)
}
