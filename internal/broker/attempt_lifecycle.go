package broker

import (
	"context"
	"log"
	"slices"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

func (w *RouteWorker) releaseAttemptSlot(s *Stub, token string) {
	if !s.negotiated() {
		return
	}
	d := s.delivery
	d.mu.Lock()
	if d.slot == token {
		d.slot = ""
	}
	waiting := slices.Clone(d.waiting)
	d.mu.Unlock()
	for _, key := range waiting {
		w.broker.wakeDelivery(key)
	}
}
func (w *RouteWorker) exhaustAttempt(s *Stub) {
	if !s.negotiated() {
		return
	}
	d := s.delivery
	d.mu.Lock()
	d.route(w.key).Exhausted = time.Now()
	d.mu.Unlock()
	log.Print("attempt exhausted: no receipt on channel; route paused")
}
func (w *RouteWorker) finishAttempt(token, outcome, reason string) {
	var owner *Stub
	w.updateAttempt(token, func(a *attemptRecord) {
		if a.Outcome == "open" {
			a.Outcome = outcome
			a.Reason = reason
			owner = a.Holder.Stub
		}
	})
	if owner == nil {
		return
	}
	if outcome == "failed" || outcome == "expired" {
		w.exhaustAttempt(owner)
	}
	w.releaseAttemptSlot(owner, token)
}

// P4/G3: partial reconciliation never recreates removed identities; an empty
// attempt proves nothing and cannot rearm a route.
func (w *RouteWorker) reconcileAttempt() {
	a := w.liveAttempt()
	if a == nil {
		return
	}
	rows, err := w.broker.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if err != nil {
		return
	}
	live := map[string]string{}
	for _, r := range rows {
		live[r.RecordID] = rowRevision(r)
	}
	w.updateAttempt(a.Token, func(a *attemptRecord) {
		a.Members = slices.DeleteFunc(a.Members, func(m attemptMember) bool { return live[m.ID] != m.Revision })
	})
	for _, current := range w.broker.attempts.lookup(a.Token, time.Now()) {
		if len(current.Members) == 0 {
			w.finishAttempt(a.Token, "released", "members removed")
		}
	}
}
func (w *RouteWorker) handleAttemptResult(job *attemptResultJob) {
	if job == nil {
		return
	}
	accepted := false
	w.broker.Routes.withConfirmedHolder(w.key, job.Owner, func() {
		a := w.liveAttempt()
		if a == nil || a.Token != job.Msg.Token || a.Holder.Stub != job.Owner || a.Holder.ConnID != job.Owner.ConnID || a.Holder.ClaimGeneration != job.Owner.claimGeneration(w.key) || !time.Now().Before(a.Deadline) || len(a.Members) == 0 || a.Evidence {
			return
		}
		accepted = true
		if job.Msg.Outcome == "confirmed" {
			w.updateAttempt(a.Token, func(a *attemptRecord) { a.Evidence = true })
		} else {
			w.finishAttempt(a.Token, "failed", job.Msg.Reason)
		}
	})
	if !accepted {
		attemptNoop()
		return
	}
	w.retireAttempt()
}
func (w *RouteWorker) retireAttempt() {
	a := w.liveAttempt()
	if a == nil || !a.Evidence {
		return
	}
	w.broker.Routes.withConfirmedHolder(w.key, a.Holder.Stub, func() {
		if a.Holder.ClaimGeneration != a.Holder.Stub.claimGeneration(w.key) {
			return
		}
		w.reconcileAttempt()
		a = w.liveAttempt()
		if a == nil {
			return
		}
		_, err := w.broker.Queue.RemoveRecordIDs(queueRouteKey(w.key), memberIDs(a.Members))
		if err != nil {
			w.updateAttempt(a.Token, func(a *attemptRecord) { a.RemovalTries++ })
			log.Print("attempt retirement retry: storage write failed; evidence retained")
			if a.RemovalTries >= 2 {
				log.Print("attempt retirement released: storage retry limit reached")
				w.finishAttempt(a.Token, "failed", "storage failure")
			}
			return
		}
		// Retire recovery left by an earlier legacy holder only after the
		// durable removal succeeds. Reconcile its shadow identities as well.
		ids := memberIDs(a.Members)
		w.retirePendingRecords(ids)
		w.broker.attempts.drop(w.key, ids, "negotiated_confirm", a.Token, time.Now())
		w.updateAttempt(a.Token, func(a *attemptRecord) { a.Retired = memberIDs(a.Members) })
		s := a.Holder.Stub
		d := s.delivery
		d.mu.Lock()
		d.proven = true
		d.route(w.key).Confirmed = time.Now()
		d.mu.Unlock()
		w.finishAttempt(a.Token, "confirmed", "transcript")
		w.broker.rearmDelivery(s)
	})
}
func (w *RouteWorker) tickAttempt(ctx context.Context) {
	a := w.liveAttempt()
	if a != nil {
		s, _ := w.broker.Routes.Holder(w.key)
		if s != a.Holder.Stub && s.negotiated() && s.delivery == a.Holder.Stub.delivery && time.Now().Before(a.Deadline) {
			return
		}
		if s != a.Holder.Stub || !s.IsAlive() || !s.RouteConfirmed(w.key) || s.claimGeneration(w.key) != a.Holder.ClaimGeneration {
			w.finishAttempt(a.Token, "released", "holder changed")
		} else if a.Evidence {
			w.retireAttempt()
		} else if !time.Now().Before(a.Deadline) {
			w.finishAttempt(a.Token, "expired", "deadline")
		}
	}
	w.scheduleAttempt(ctx, false)
}
func (w *RouteWorker) adoptAttempt(job *attemptAdoptJob) {
	defer close(job.Done)
	a := w.liveAttempt()
	if a == nil || a.Holder.Stub != job.Old {
		return
	}
	if !job.Same || a.Holder.ClaimGeneration != job.Old.claimGeneration(w.key) || !time.Now().Before(a.Deadline) {
		w.finishAttempt(a.Token, "released", "reconnect changed or expired")
		return
	}
	ok, _ := w.broker.Routes.withConfirmedHolder(w.key, job.Next, func() {
		holder := shadowHolder(job.Next)
		holder.ClaimGeneration = job.Next.claimGeneration(w.key)
		w.updateAttempt(a.Token, func(a *attemptRecord) { a.Holder = holder; a.Adopted = true })
	})
	if !ok {
		w.finishAttempt(a.Token, "released", "claim interrupted")
	}
}

func (w *RouteWorker) enableAttemptTimer() {
	if w.attemptTick == nil {
		w.attemptTick = time.NewTicker(100 * time.Millisecond)
		w.attemptC = w.attemptTick.C
	}
}

// Display uses the reusable bounded writer too, independent of the notice
// cooldown. A confirmation refreshes age even when the semantic state is stable.
func (w *RouteWorker) publishAttemptState(s *Stub) {
	if w.attemptCtx == nil || !s.deliveryReady.Load() {
		return
	}
	r := s.RenderRouteFor(w.key)
	r.Held = 0
	if w.attemptDisplay == r && w.attemptDisplayConn == s.ConnID {
		return
	}
	conn, _ := s.ConnValue().(*ipc.Conn)
	if conn == nil {
		return
	}
	w.startAttemptWriter(w.attemptCtx)
	var topic *int64
	if w.key.HasTopic {
		id := w.key.TopicID
		topic = &id
	}
	msg := ipc.RenderStateMsg{Op: ipc.OpRenderState, Channel: w.key.Channel, ChatID: w.key.ChatID, TopicID: topic, RenderRoute: r}
	select {
	case w.attemptWrites <- attemptWrite{Conn: conn, Frame: msg}:
		w.attemptDisplay = r
		w.attemptDisplayConn = s.ConnID
	default:
	}
}
