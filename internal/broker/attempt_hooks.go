package broker

import (
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

// Only observation hooks live here. Reads use this baseline's non-mutating
// PeekTracked, on the owning route worker. Read failure leaves the outcome open;
// it must never affect a queue operation's result or manufacture retirement.
type shadowRows struct {
	ids []string
	ok  bool
}

func (w *RouteWorker) shadowRows() shadowRows {
	if w.broker == nil || w.broker.Queue == nil {
		return shadowRows{}
	}
	rows, err := w.broker.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if err != nil {
		return shadowRows{}
	}
	out := shadowRows{ok: true}
	for _, row := range rows {
		if row.RecordID != "" {
			out.ids = append(out.ids, row.RecordID)
		}
	}
	return out
}

func (w *RouteWorker) shadowRemoval(before shadowRows, token, cause string) {
	after := w.shadowRows()
	if !before.ok || !after.ok {
		return
	}
	live := make(map[string]bool, len(after.ids))
	for _, id := range after.ids {
		live[id] = true
	}
	var removed []string
	for _, id := range before.ids {
		if !live[id] {
			removed = append(removed, id)
		}
	}
	w.broker.recorded.forget(w.key, removed)
	now := time.Now()
	if token != "" {
		w.broker.attempts.confirm(token, w.key, removed, cause, now)
	}
	if w.routeNegotiated() {
		w.retirePendingRecords(removed)
	}
	w.broker.attempts.drop(w.key, removed, cause, token, now)
	w.reconcileAttempt()
}

func (w *RouteWorker) shadowPush(token string, holder *Stub, ids []string) {
	w.broker.attempts.open(attemptRecord{
		Token: token, Route: w.key, Transport: "channel",
		Holder: shadowHolder(holder), Members: attemptMembers(ids),
	}, time.Now())
}

func (w *RouteWorker) shadowLegacyFetch(holder *Stub, msgs []c3types.Inbound) {
	var ids []string
	for _, in := range msgs {
		if in.ConsumedRecordID != "" {
			ids = append(ids, in.ConsumedRecordID)
		}
	}
	if len(ids) == 0 {
		return
	}
	w.broker.recorded.forget(w.key, ids)
	// retireConsumedRecords already paired the actual returned rows and IDs.
	now := time.Now()
	token := w.broker.mintDeliveryToken()
	w.broker.attempts.drop(w.key, ids, "legacy_consume", "", now)
	w.broker.attempts.open(attemptRecord{Token: token, Route: w.key, Transport: "fetch",
		Holder: shadowHolder(holder), Members: attemptMembers(ids)}, now)
	w.broker.attempts.confirm(token, w.key, ids, "legacy_consume", now)
}

func (w *RouteWorker) shadowHolderDeath() {
	if w.broker == nil {
		return
	}
	now := time.Now()
	for _, a := range w.broker.attempts.snapshot(now) {
		if a.Route == w.key && a.Outcome == "open" {
			w.broker.attempts.release(a.Holder.Stub, "holder_death", now)
		}
	}
}
