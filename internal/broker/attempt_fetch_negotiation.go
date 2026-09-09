package broker

import (
	"encoding/json"
	"log"
	"slices"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

func deliveryAcceptance(offer ipc.DeliveryOffer) *ipc.DeliveryAcceptance {
	modes := []string{}
	if offer.Receipts == "transcript" {
		modes = append(modes, "channel", "inbox")
	}
	if offer.Fetch == "receipt" {
		modes = append(modes, "fetch_receipt")
	}
	return &ipc.DeliveryAcceptance{Version: 1, Modes: modes}
}

func (s *Stub) acceptsDeliveryMode(mode string) bool {
	return s.negotiated() && s.deliveryModes.Load().HasMode(mode)
}

func sameAcceptedModes(a, b *Stub) bool {
	x, y := a.deliveryModes.Load(), b.deliveryModes.Load()
	return x != nil && y != nil && slices.Equal(x.Modes, y.Modes)
}

// Call with delivery.mu held. Facts for unconfirmed modes cannot schedule a
// transport or make the route appear live, even after a capability report.
func (s *Stub) acceptedLive() ipc.DeliveryLive {
	live := s.delivery.live
	if !s.acceptsDeliveryMode("channel") {
		live.Channel = ipc.DeliveryEligibility{Reason: "channel mode not accepted"}
	}
	if !s.acceptsDeliveryMode("inbox") {
		live.Inbox = ipc.DeliveryEligibility{Reason: "inbox mode not accepted"}
	}
	return live
}

// Pin 0: hello_ack is an offer of modes, not permission to send deliver. Only
// this first, valid adapter confirmation activates the immutable connection set.
func (b *Broker) handleDeliveryReport(s *Stub, raw []byte) {
	if s == nil || s.delivery == nil || s.deliveryRefused.Load() {
		return
	}
	var msg ipc.DeliveryReportMsg
	var fields map[string]json.RawMessage
	if ipc.StrictJSON(raw, &msg) != nil || json.Unmarshal(raw, &fields) != nil {
		return
	}
	if accepted, present := fields["accepted"]; present {
		valid := string(accepted) != "null"
		ack := deliveryAcceptance(s.delivery.offer)
		for i, mode := range msg.Accepted {
			valid = valid && ack.HasMode(mode) && !slices.Contains(msg.Accepted[:i], mode)
		}
		slices.Sort(msg.Accepted)
		if s.negotiated() {
			valid = valid && slices.Equal(s.deliveryModes.Load().Modes, msg.Accepted)
		}
		if !valid {
			log.Print("delivery negotiation protocol error: unaccepted mode; legacy connection")
			b.releaseDelivery(s)
			s.deliveryReady.Store(false)
			s.deliveryRefused.Store(true)
			if old := s.deliveryPrevious; old != nil {
				b.reconnectDelivery(old, s)
			}
			return
		}
		if !s.negotiated() {
			if ipc.ValidDeliveryLive(fields["live"]) {
				s.delivery.mu.Lock()
				s.delivery.live = msg.Live
				s.delivery.mu.Unlock()
			}
			s.deliveryModes.Store(&ipc.DeliveryAcceptance{Version: 1, Modes: slices.Clone(msg.Accepted)})
			if old := s.deliveryPrevious; old != nil {
				b.reconnectDelivery(old, s)
				if !sameAcceptedModes(old, s) {
					d := s.delivery
					d.mu.Lock()
					d.proven, d.slot, d.waiting, d.routes = false, "", nil, map[RouteKey]*negotiatedRoute{}
					d.mu.Unlock()
				}
			}
			s.deliveryReady.Store(true)
			b.rearmDelivery(s)
		}
	}
	if !s.negotiated() || !ipc.ValidDeliveryLive(fields["live"]) {
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

func (b *Broker) handleFetchConfirm(s *Stub, raw []byte) {
	var msg ipc.FetchConfirmReq
	if !s.acceptsDeliveryMode("fetch_receipt") || ipc.StrictJSON(raw, &msg) != nil {
		attemptNoop()
		return
	}
	// The alias must never turn a live token into fetch evidence.
	for _, a := range b.attempts.lookup(msg.LeaseToken, time.Now()) {
		if a.Negotiated && a.Transport == "fetch" && a.Holder.Stub == s {
			frame, _ := json.Marshal(ipc.AttemptResultMsg{Op: ipc.OpAttemptResult, Token: msg.LeaseToken, Outcome: "confirmed"})
			b.handleAttemptResult(s, frame)
			return
		}
	}
	attemptNoop()
}
