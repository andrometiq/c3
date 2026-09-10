package broker

import (
	"context"
	"log"
	"slices"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

const maxAttemptRemovalTries = 3

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
func (w *RouteWorker) exhaustAttempt(s *Stub, reason string) {
	if !s.negotiated() {
		return
	}
	d := s.delivery
	d.mu.Lock()
	d.route(w.key).Exhausted = time.Now()
	d.route(w.key).ExhaustionReason = ""
	if reason == "storage failure" {
		d.route(w.key).ExhaustionReason = "receipt recorded; queue retirement failed; retries on reconnect, attach or new messages after 60 s"
	}
	d.route(w.key).clearCycle()
	d.mu.Unlock()
	if reason == "storage failure" {
		log.Print("attempt exhausted: receipt recorded; queue retirement failed; route paused")
	} else {
		log.Print("attempt exhausted: no receipt on channel or inbox; route paused")
	}
}
func (w *RouteWorker) finishAttempt(token, outcome, reason string) {
	var finished *attemptRecord
	w.updateAttempt(token, func(a *attemptRecord) {
		if a.Outcome == "open" {
			a.Outcome, a.Reason = outcome, reason
			snapshot := cloneAttempt(a)
			finished = &snapshot
		}
	})
	if finished != nil {
		w.afterAttemptFinished(*finished)
	}
}

// The terminal record is already published. Logging and scheduling follow it;
// neither may leave observers seeing retired rows in an open attempt.
func (w *RouteWorker) afterAttemptFinished(a attemptRecord) {
	owner, transport, members := a.Holder.Stub, a.Transport, a.Members
	token, outcome, reason := a.Token, a.Outcome, a.Reason
	if owner == nil {
		return
	}
	w.logTestAttempt(token, outcome)
	log.Printf("attempt finished transport=%s outcome=%s", transport, outcome)
	if transport == "fetch" {
		return
	}

	d := owner.delivery
	d.mu.Lock()
	state := d.route(w.key)
	fallback := (outcome == "failed" || outcome == "expired") && state.Exhausted.IsZero() && len(members) > 0 && state.nextTransport(owner.acceptedLive()) != ""
	if fallback {
		state.CycleMembers = members
	} else {
		state.clearCycle()
	}
	d.mu.Unlock()
	if fallback {
		return
	} // P6: "A cycle holds the slot until it terminates".
	if outcome == "failed" || outcome == "expired" {
		w.exhaustAttempt(owner, reason)
	}
	w.releaseAttemptSlot(owner, token)
}

// P4/G3: partial reconciliation never recreates removed identities; an empty
// attempt proves nothing and cannot rearm a route.
func (w *RouteWorker) reconcileAttempt() error {
	if len(w.openAttempts()) == 0 {
		return nil
	}
	rows, err := w.broker.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if err != nil {
		return err
	}
	live := map[string]string{}
	for _, r := range rows {
		live[r.RecordID] = rowRevision(r)
	}
	for _, a := range w.openAttempts() {
		w.updateAttempt(a.Token, func(a *attemptRecord) {
			a.Members = slices.DeleteFunc(a.Members, func(m attemptMember) bool { return live[m.ID] != m.Revision })
		})
		if current := w.attempt(a.Token); current != nil && len(current.Members) == 0 {
			w.finishAttempt(a.Token, "released", "members removed")
		}
	}
	return nil
}

func (w *RouteWorker) handleAttemptResult(job *attemptResultJob) {
	if job == nil {
		return
	}
	accepted := false
	w.broker.Routes.withConfirmedHolder(w.key, job.Owner, func() {
		a := w.attempt(job.Msg.Token)
		if a == nil || a.Token != job.Msg.Token || a.Holder.Stub != job.Owner || a.Holder.ConnID != job.Owner.ConnID || a.Holder.ClaimGeneration != job.Owner.claimGeneration(w.key) || !time.Now().Before(a.Deadline) || len(a.Members) == 0 || a.Evidence {
			return
		}
		accepted = true
		if job.Msg.Outcome == "confirmed" {
			w.broker.recorded.remember(w.key, a.Members)
			w.updateAttempt(a.Token, func(a *attemptRecord) { a.Evidence = true })
			log.Printf("attempt confirmed token=%s route=%s ms=%d", a.Token, routeKeyStr(w.key), time.Since(a.Started).Milliseconds())
		} else {
			w.finishAttempt(a.Token, "failed", job.Msg.Reason)
		}
	})
	if !accepted {
		attemptNoop()
		return
	}
	w.retireAttemptToken(job.Msg.Token)
}
func (w *RouteWorker) retireAttemptToken(token string) {
	a := w.attempt(token)
	if a == nil || !a.Evidence {
		return
	}
	w.broker.Routes.withConfirmedHolder(w.key, a.Holder.Stub, func() {
		if a.Holder.ClaimGeneration != a.Holder.Stub.claimGeneration(w.key) {
			return
		}
		err := w.reconcileAttempt()
		var removed []c3types.Inbound
		if err == nil {
			a = w.attempt(token)
			if a == nil {
				return
			}
			// Hold the attempt-table lock across durable removal and terminal
			// publication. Once the queue index reports retirement, an attempt
			// lookup must wait for the matching confirmed outcome and identities.
			w.updateAttempt(a.Token, func(record *attemptRecord) {
				removed, err = w.broker.Queue.RemoveRecordIDs(queueRouteKey(w.key), memberIDs(record.Members))
				if err != nil {
					return
				}
				record.Retired = memberIDs(record.Members)
				record.Outcome, record.Reason = "confirmed", "transcript"
				if record.Transport == "fetch" {
					record.Reason = "tool result"
				}
				snapshot := cloneAttempt(record)
				a = &snapshot
			})
		}
		if err != nil {
			tries := a.RemovalTries + 1
			w.updateAttempt(a.Token, func(a *attemptRecord) { a.RemovalTries = tries })
			log.Print("attempt retirement retry: storage write failed; evidence retained")
			if tries >= maxAttemptRemovalTries {
				log.Print("attempt retirement released: storage retry limit reached")
				w.finishAttempt(a.Token, "failed", "storage failure")
			}
			return
		}
		// Retire recovery left by an earlier legacy holder only after the
		// durable removal succeeds. Reconcile its shadow identities as well.
		ids := memberIDs(a.Members)
		w.broker.recorded.forget(w.key, ids)
		w.retirePendingRecords(ids)
		w.broker.attempts.drop(w.key, ids, "negotiated_confirm", a.Token, time.Now())
		s := a.Holder.Stub
		if a.Transport == "fetch" {
			w.afterAttemptFinished(*a)
			log.Printf("attempt retired n=%d token=%s", len(removed), a.Token)
			if a.FetchGroup != nil && a.FetchGroup.rearmed.CompareAndSwap(false, true) {
				w.broker.rearmDelivery(s)
			}
			return
		}
		d := s.delivery
		d.mu.Lock()
		d.proven = true
		d.route(w.key).Confirmed = time.Now()
		d.route(w.key).Transport = a.Transport
		d.mu.Unlock()
		w.afterAttemptFinished(*a)
		log.Printf("attempt retired n=%d token=%s", len(removed), a.Token)
		w.broker.rearmDelivery(s)
	})
}
func (w *RouteWorker) tickAttempt(ctx context.Context) {
	for _, a := range w.openAttempts() {
		s, _ := w.broker.Routes.Holder(w.key)
		if a.Transport != "fetch" && s != a.Holder.Stub && s != nil && s.delivery == a.Holder.Stub.delivery && !s.deliveryRefused.Load() && time.Now().Before(a.Deadline) {
			continue
		}
		if s != a.Holder.Stub || !s.IsAlive() || !s.RouteConfirmed(w.key) || s.claimGeneration(w.key) != a.Holder.ClaimGeneration {
			w.finishAttempt(a.Token, "released", "holder changed")
		} else if a.Evidence {
			w.retireAttemptToken(a.Token)
		} else if !time.Now().Before(a.Deadline) {
			w.finishAttempt(a.Token, "expired", "deadline")
		}
	}
	w.scheduleAttempt(ctx, false)
}
func (w *RouteWorker) adoptAttempt(job *attemptAdoptJob) {
	defer close(job.Done)
	for _, a := range w.openAttempts() {
		if a.Holder.Stub != job.Old {
			continue
		}
		if a.Transport == "fetch" || !job.Same || a.Holder.ClaimGeneration != job.Old.claimGeneration(w.key) || !time.Now().Before(a.Deadline) {
			w.finishAttempt(a.Token, "released", "reconnect changed or expired")
			continue
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
