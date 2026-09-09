package broker

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

type negotiatedRoute struct {
	Exhausted time.Time
	Confirmed time.Time
	Held      int
}
type negotiatedSession struct {
	mu      sync.Mutex
	offer   ipc.DeliveryOffer
	live    ipc.DeliveryLive
	proven  bool
	slot    string
	waiting []RouteKey
	routes  map[RouteKey]*negotiatedRoute
}

func (s *Stub) negotiated() bool { return s != nil && s.delivery != nil }
func (s *Stub) claimGeneration(key RouteKey) uint64 {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	return s.claimGenerations[key]
}
func (b *Broker) configureDelivery(s *Stub, raw json.RawMessage) {
	offer := ipc.ParseDeliveryOffer(raw)
	if b.Queue == nil || offer == nil || !offer.Live.Channel.Eligible {
		return
	}
	s.delivery = &negotiatedSession{offer: *offer, live: offer.Live, routes: map[RouteKey]*negotiatedRoute{}}
}
func (d *negotiatedSession) route(key RouteKey) *negotiatedRoute {
	r := d.routes[key]
	if r == nil {
		r = &negotiatedRoute{}
		d.routes[key] = r
	}
	return r
}
func (b *Broker) rearmDelivery(s *Stub) {
	if !s.negotiated() {
		return
	}
	d := s.delivery
	d.mu.Lock()
	for _, r := range d.routes {
		r.Exhausted = time.Time{}
	}
	d.mu.Unlock()
	for _, key := range s.Routes() {
		b.wakeDelivery(key)
	}
}
func (b *Broker) wakeDelivery(key RouteKey) {
	if b.Workers != nil {
		b.Workers.Submit(key, Job{Kind: JobAttemptWake})
	}
}
func (b *Broker) handleDeliveryReport(s *Stub, raw []byte) {
	if !s.negotiated() {
		return
	}
	var msg ipc.DeliveryReportMsg
	var fields map[string]json.RawMessage
	if ipc.StrictJSON(raw, &msg) != nil || json.Unmarshal(raw, &fields) != nil || !ipc.ValidDeliveryLive(fields["live"]) {
		return
	}
	d := s.delivery
	d.mu.Lock()
	changed := d.live != msg.Live
	d.live = msg.Live
	d.mu.Unlock()
	if changed {
		b.rearmDelivery(s)
	}
}
func attemptNoop() {
	log.Print("attempt result ignored: authority, deadline or open membership mismatch")
}
func (b *Broker) handleAttemptResult(s *Stub, raw []byte) {
	if !s.negotiated() {
		attemptNoop()
		return
	}
	var msg ipc.AttemptResultMsg
	if ipc.StrictJSON(raw, &msg) != nil || (msg.Outcome != "confirmed" && msg.Outcome != "failed") {
		attemptNoop()
		return
	}
	for _, a := range b.attempts.lookup(msg.Token, time.Now()) {
		if a.Negotiated && a.Holder.Stub == s && a.Holder.ConnID == s.ConnID {
			if b.Workers != nil && b.Workers.Submit(a.Route, Job{Kind: JobAttemptResult, AttemptResult: &attemptResultJob{Owner: s, Msg: msg}}) {
				return
			}
		}
	}
	attemptNoop()
}

// P1: "Reconnect that changes mode or declared milestones: release every attempt
// of the old connection first". The worker validates adoption under the claim gate.
func (b *Broker) reconnectDelivery(old, next *Stub) {
	same := old.negotiated() && next.negotiated() && old.delivery.offer == next.delivery.offer && isPIDAlive(old.PID)

	for _, a := range b.attempts.snapshot(time.Now()) {
		if a.Negotiated && a.Holder.Stub == old && a.Outcome == "open" && b.Workers != nil {
			done := make(chan struct{})
			if b.Workers.Submit(a.Route, Job{Kind: JobAttemptAdopt, AttemptAdopt: &attemptAdoptJob{Old: old, Next: next, Same: same, Done: done}}) {
				select {
				case <-done:
				case <-time.After(workerJobTimeout):
				}
			}
		}
	}
}

func (b *Broker) releaseDelivery(s *Stub) {
	if !s.negotiated() || b.Workers == nil {
		return
	}
	for _, key := range s.Routes() {
		done := make(chan struct{})
		if b.Workers.Submit(key, Job{Kind: JobAttemptAdopt, AttemptAdopt: &attemptAdoptJob{Old: s, Done: done}}) {
			select {
			case <-done:
			case <-time.After(workerJobTimeout):
			}
		}
	}
}

// Publish immutable negotiation together with the stub, so concurrent status
// readers never observe a half-configured hello or race a reconnect adoption.
func (b *Broker) registerDeliveryHello(hello ipc.HelloMsg, conn *ipc.Conn, old *Stub) *Stub {
	return b.Stubs.Register(hello.CLI, hello.PID, hello.CWD, conn, func(s *Stub) {
		b.configureDelivery(s, hello.Delivery)
		s.ReceiptConfirming = hello.RenderState != ""
		s.SetRenderRoute(hello.RenderState, hello.RenderReason, hello.CannotRenderChannels)
		if old.negotiated() && s.negotiated() && old.delivery.offer == s.delivery.offer && isPIDAlive(old.PID) {
			s.delivery = old.delivery
		}
	})
}

// Called with Routes.mu held at every actual release. AddRoute alone cannot
// distinguish release/reclaim of the same stub from an uninterrupted claim.
func (s *Stub) invalidateDeliveryClaim(key RouteKey) {
	if !s.negotiated() {
		return
	}
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	s.claimSequence++
	if s.claimGenerations == nil {
		s.claimGenerations = map[RouteKey]uint64{}
	}
	s.claimGenerations[key] = s.claimSequence
}
