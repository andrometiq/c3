package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"sync/atomic"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// One token, one table record per route. This shared flag makes a successful
// nonempty group rearm once; it owns no rows, timers, or retirement authority.
type attemptFetchGroup struct {
	token   string
	rearmed atomic.Bool
}

func (w *RouteWorker) openAttempts() []attemptRecord {
	var out []attemptRecord
	for _, a := range w.broker.attempts.snapshot(time.Now()) {
		if a.Negotiated && a.Route == w.key && a.Outcome == "open" {
			out = append(out, a)
		}
	}
	return out
}

func (w *RouteWorker) attempt(token string) *attemptRecord {
	for _, a := range w.broker.attempts.lookup(token, time.Now()) {
		if a.Negotiated && a.Route == w.key && a.Outcome == "open" {
			return &a
		}
	}
	return nil
}

func attemptFetchResponse(id, token string, messages []c3types.Inbound, members []ipc.FetchReceiptMember, remaining int) ipc.FetchQueueResp {
	r := ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: id, Messages: messages, Remaining: remaining}
	if len(members) > 0 {
		r.LeaseToken, r.Members = token, members
		r.ReceiptTrailer = ipc.FetchReceiptTrailer(token, members)
	}
	return r
}

// Reservation uses the same cancellation lease, drain barrier, confirmed-holder
// gate and route worker as consume. Identity preparation is inside that gate.
// No durable row is removed until the host confirms the complete group trailer.
func (w *RouteWorker) handleAttemptFetch(ctx context.Context, job *FetchJob) {
	result := FetchResult{}
	if !job.Ack || !job.Owner.acceptsDeliveryMode("fetch_receipt") {
		result.Err = fmt.Errorf("fetch receipt reservation requires accepted fetch_receipt mode")
		job.ResultCh <- result
		return
	}
	if !job.Lease.beginConsume() {
		result.Err = fmt.Errorf("fetch reservation cancelled")
		job.ResultCh <- result
		return
	}
	func() {
		defer job.Lease.finishConsume()
		w.broker.drains.mu.Lock()
		defer w.broker.drains.mu.Unlock()
		if _, draining := w.broker.drains.inFlight[queueRouteKey(w.key).File()]; draining {
			result.Err = fmt.Errorf("fetch reservation deferred while drain snapshot is active")
			return
		}
		authorized, reason := w.broker.Routes.withConfirmedHolder(w.key, job.Owner, func() {
			if err := w.broker.Queue.EnsureIdentities(queueRouteKey(w.key)); err != nil {
				result.Err = err
				return
			}
			if err := w.reconcileAttempt(); err != nil {
				result.Err = err
				return
			}
			rows, err := w.visibleAttemptRows(-1)
			if err != nil {
				result.Err = err
				return
			}
			result.Remaining = len(rows)
			limit := len(rows)
			if !job.All && job.Limit >= 0 {
				limit = min(limit, job.Limit)
			}
			var members []attemptMember
			fits := func(messages []c3types.Inbound, receipts []ipc.FetchReceiptMember, reserve int) bool {
				encoded, err := json.Marshal(attemptFetchResponse(job.RespID, job.ReceiptGroup.token, messages, receipts, len(rows)))
				return err == nil && len(encoded)+reserve+1 <= ipc.MaxFrameSize
			}
			for _, row := range rows[:limit] {
				member := ipc.FetchReceiptMember{RecordID: row.RecordID, Revision: rowRevision(row)}
				in := row.Inbound
				if !fits([]c3types.Inbound{in}, []ipc.FetchReceiptMember{member}, 0) {
					// The original stays durable until this replacement is receipted.
					in = oversizeNotice(in, encodedSize(in), "", false)
				}
				messages := append(slices.Clone(result.Messages), in)
				receipts := append(slices.Clone(result.Members), member)
				if !fits(messages, receipts, job.FrameReserve) {
					break
				}
				result.Messages, result.Members = messages, receipts
				members = append(members, attemptMember{ID: member.RecordID, Revision: member.Revision})
			}
			if len(members) == 0 {
				return
			}
			holder := shadowHolder(job.Owner)
			holder.ClaimGeneration = job.Owner.claimGeneration(w.key)
			w.broker.attempts.open(attemptRecord{Negotiated: true, FetchGroup: job.ReceiptGroup, Token: job.ReceiptGroup.token,
				Route: w.key, Transport: "fetch", Holder: holder, Members: members}, time.Now())
			if w.attempt(job.ReceiptGroup.token) == nil {
				result.Messages, result.Members = nil, nil
				result.Err = fmt.Errorf("fetch reservation admission full")
				return
			}
			result.Remaining -= len(members)
			w.enableAttemptTimer()
			w.logTestAttempt(job.ReceiptGroup.token, "reserved")
			log.Printf("attempt reserved transport=fetch members=%d budget_ms=60000", len(members))
		})
		if !authorized {
			result.SkipReason = reason
		}
	}()
	if result.Err == nil && result.SkipReason == "" && len(result.Members) > 0 {
		w.scheduleAttempt(ctx, false)
	}
	job.ResultCh <- result
}
