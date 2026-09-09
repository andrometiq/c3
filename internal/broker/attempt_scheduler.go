package broker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

type attemptResultJob struct {
	Owner *Stub
	Msg   ipc.AttemptResultMsg
}
type attemptAdoptJob struct {
	Old, Next *Stub
	Same      bool
	Done      chan struct{}
}
type attemptWrite struct {
	Conn     *ipc.Conn
	Frame    any
	Deadline time.Time
	Token    string
}
type attemptWriteResult struct {
	Token string
	Err   error
}
type attemptFrame struct {
	ipc.DeliverMsg
	Deadline time.Time `json:"-"`
}

func (f attemptFrame) PrepareFrame() any {
	f.DeadlineMS = max(0, time.Until(f.Deadline).Milliseconds())
	return f.DeliverMsg
}
func rowRevision(r queue.TrackedInbound) string {
	b, _ := json.Marshal(r)
	return fmt.Sprintf("%x", sha256.Sum256(b))
}
func (w *RouteWorker) liveAttempt() *attemptRecord {
	for _, a := range w.broker.attempts.snapshot(time.Now()) {
		if a.Negotiated && a.Route == w.key && a.Outcome == "open" && a.Transport != "fetch" {
			return &a
		}
	}
	return nil
}
func (w *RouteWorker) updateAttempt(token string, fn func(*attemptRecord)) {
	t := &w.broker.attempts
	t.mu.Lock()
	defer t.mu.Unlock()
	if a := t.entries[attemptKey{token, w.key}]; a != nil {
		fn(a)
	}
}
func (w *RouteWorker) startAttemptWriter(ctx context.Context) {
	if w.attemptWrites != nil {
		return
	}
	// Only the writer belongs to the run loop's lifetime. Legacy chained
	// voice echoes keep the worker context until Stop or parent cancellation.
	ctx, w.attemptWriteCancel = context.WithCancel(ctx)
	w.attemptWrites = make(chan attemptWrite, 2)
	w.attemptWritten = make(chan attemptWriteResult, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-w.attemptWrites:
				deadline := time.Now().Add(time.Second)
				if !req.Deadline.IsZero() {
					deadline = minTime(req.Deadline, deadline)
				}
				wc, cancel := context.WithDeadline(ctx, deadline)
				err := req.Conn.WriteJSONContext(wc, req.Frame)
				cancel()
				if req.Token == "" {
					continue
				}
				select {
				case w.attemptWritten <- attemptWriteResult{req.Token, err}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// P6: "Cycle = one selected batch (bounded to the IPC frame)."
// This function only reserves; every terminal decision runs on this worker too.
func (w *RouteWorker) scheduleAttempt(ctx context.Context, inbound bool) {
	if w.broker == nil || w.broker.Queue == nil {
		return
	}
	w.attemptCtx = ctx
	w.reconcileAttempt()
	if a := w.liveAttempt(); a != nil {
		w.evaluateAttemptHeld(a.Holder.Stub)
		return
	}
	s, _ := w.broker.Routes.Holder(w.key)
	if !s.negotiated() || !s.RouteConfirmed(w.key) {
		return
	}
	d := s.delivery
	var identityErr error
	authorized, _ := w.broker.Routes.withConfirmedHolder(w.key, s, func() { identityErr = w.broker.Queue.EnsureIdentities(queueRouteKey(w.key)) })
	if !authorized || identityErr != nil {
		return
	}
	rows, err := w.visibleAttemptRows(-1)
	if err != nil {
		return
	}
	eligible := make([]queue.TrackedInbound, 0, len(rows))
	for _, r := range rows {
		if r.RecordID != "" && len(r.VoicePending) == 0 && r.Origin != "drain" && r.SourceRecordID == "" && r.Inbound.DrainedFrom == "" {
			eligible = append(eligible, r)
		}
	}
	d.mu.Lock()
	state := d.route(w.key)
	if inbound && !state.Exhausted.IsZero() && time.Since(state.Exhausted) >= 60*time.Second {
		state.Exhausted = time.Time{}
	}
	// P6: fallback covers only the surviving revisions of the selected batch.
	if state.CycleToken != "" {
		eligible = slices.DeleteFunc(eligible, func(r queue.TrackedInbound) bool {
			return !slices.Contains(state.CycleMembers, attemptMember{ID: r.RecordID, Revision: rowRevision(r)})
		})
	}
	transport := state.nextTransport(s.acceptedLive())
	ready := s.deliveryReady.Load() && transport != "" && state.Exhausted.IsZero() && len(eligible) > 0 && s.IsConnected()
	if !ready {
		d.waiting = slices.DeleteFunc(d.waiting, func(k RouteKey) bool { return k == w.key })
		oldToken := state.CycleToken
		state.clearCycle()
		d.mu.Unlock()
		w.releaseAttemptSlot(s, oldToken)
		w.evaluateAttemptHeld(s)
		return
	}
	if !slices.Contains(d.waiting, w.key) {
		d.waiting = append(d.waiting, w.key)
	}
	if !d.proven && !(state.CycleToken != "" && d.slot == state.CycleToken) && (d.slot != "" || d.waiting[0] != w.key) {
		d.mu.Unlock()
		w.evaluateAttemptHeld(s)
		return
	}
	token := w.broker.mintDeliveryToken()
	d.waiting = slices.DeleteFunc(d.waiting, func(k RouteKey) bool { return k == w.key })
	d.slot = token
	d.mu.Unlock()
	var batch []*c3types.Inbound
	var members []attemptMember
	frame := ipc.DeliverMsg{Op: ipc.OpDeliver, Token: token, Transport: transport, DeadlineMS: 15000}
	for _, row := range eligible {
		in := row.Inbound
		candidate := append(slices.Clone(batch), &in)
		frame.Inbound = *mergeBatch(candidate)
		data, e := json.Marshal(frame)
		if e != nil || len(data)+1 > ipc.MaxFrameSize {
			if len(batch) == 0 && e == nil {
				notice := oversizeNotice(in, len(data), w.broker.Queue.RetentionDir(), true)
				batch = []*c3types.Inbound{&notice}
				members = []attemptMember{{ID: row.RecordID, Revision: rowRevision(row)}}
			}
			break
		}
		batch = candidate
		members = append(members, attemptMember{ID: row.RecordID, Revision: rowRevision(row)})
	}
	if len(batch) == 0 {
		w.releaseAttemptSlot(s, token)
		w.exhaustAttempt(s)
		return
	}
	frame.Inbound = *mergeBatch(batch)
	encoded, encodeErr := json.Marshal(frame)
	if encodeErr != nil || len(encoded)+1 > ipc.MaxFrameSize {
		w.releaseAttemptSlot(s, token)
		w.exhaustAttempt(s)
		return
	}
	d.mu.Lock()
	state.CycleToken = token
	state.CycleMembers = slices.Clone(members)
	if transport == "channel" {
		state.TriedChannel = true
	} else {
		state.TriedInbox = true
	}
	d.mu.Unlock()
	now := time.Now()
	holder := shadowHolder(s)
	holder.ClaimGeneration = s.claimGeneration(w.key)
	a := attemptRecord{Negotiated: true, Token: token, Route: w.key, Transport: transport, Holder: holder, Members: members}
	w.broker.drains.mu.Lock()
	_, draining := w.broker.drains.inFlight[queueRouteKey(w.key).File()]
	if !draining {
		w.broker.Routes.withConfirmedHolder(w.key, s, func() {
			if s.claimGeneration(w.key) == holder.ClaimGeneration && s.deliveryReady.Load() {
				w.broker.attempts.open(a, now)
			}
		})
	}
	w.broker.drains.mu.Unlock()
	if got := w.liveAttempt(); got == nil {
		d.mu.Lock()
		state.clearCycle()
		d.mu.Unlock()
		w.releaseAttemptSlot(s, token)
		return
	}
	conn, _ := s.ConnValue().(*ipc.Conn)
	if conn == nil {
		w.finishAttempt(token, "released", "disconnected")
		return
	}
	w.logTestAttempt(token, "reserved")
	log.Printf("attempt reserved transport=%s members=%d budget_ms=15000", transport, len(members))
	w.startAttemptWriter(ctx)
	select {
	case w.attemptWrites <- attemptWrite{Conn: conn, Frame: attemptFrame{frame, now.Add(15 * time.Second)}, Deadline: now.Add(15 * time.Second), Token: token}:
	default:
		w.finishAttempt(token, "failed", "write admission full")
	}
	w.evaluateAttemptHeld(s)
}
