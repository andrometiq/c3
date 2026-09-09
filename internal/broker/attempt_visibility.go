package broker

import (
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

func (w *RouteWorker) routeNegotiated() bool {
	s, _ := w.broker.Routes.Holder(w.key)
	return s.negotiated() || len(w.openAttempts()) > 0
}

// G1: "rows inside a negotiated live attempt are invisible to fetch."
func (w *RouteWorker) visibleAttemptRows(n int) ([]queue.TrackedInbound, error) {
	if !w.routeNegotiated() {
		return w.broker.Queue.PeekTracked(queueRouteKey(w.key), n)
	}
	rows, err := w.broker.Queue.PeekTracked(queueRouteKey(w.key), -1)
	hidden := map[string]bool{}
	for _, a := range w.openAttempts() {
		for _, m := range a.Members {
			hidden[m.ID] = true
		}
	}
	out := make([]queue.TrackedInbound, 0, len(rows))
	for _, r := range rows {
		if !hidden[r.RecordID] && (n < 0 || len(out) < n) {
			out = append(out, r)
		}
	}
	return out, err
}
func (b *Broker) attemptingCount(key RouteKey) int {
	n := 0
	for _, a := range b.attempts.snapshot(time.Now()) {
		if a.Negotiated && a.Route == key && a.Outcome == "open" {
			n += len(a.Members)
		}
	}
	return n
}
func (w *RouteWorker) evaluateAttemptHeld(s *Stub) {
	if !s.negotiated() {
		return
	}
	rows, err := w.visibleAttemptRows(-1)
	if err != nil {
		return
	}
	d := s.delivery
	d.mu.Lock()
	d.route(w.key).Held = len(rows)
	d.mu.Unlock()
	w.publishAttemptState(s)
	w.broker.notifyRenderRoute(s)
}
func (s *Stub) deliveryRoute(key RouteKey) ipc.RenderRoute {
	d := s.delivery
	d.mu.Lock()
	defer d.mu.Unlock()
	state := d.route(key)
	live := s.acceptedLive()
	r := ipc.RenderRoute{State: "waiting", Held: state.Held}
	if !state.Confirmed.IsZero() {
		r.State = "live_" + state.Transport
		r.Transport = state.Transport
		r.Confirmed = state.Confirmed
	}
	if (!live.Channel.Eligible && !live.Inbox.Eligible) ||
		(state.Transport == "channel" && !live.Channel.Eligible) ||
		(state.Transport == "inbox" && !live.Inbox.Eligible) {
		r.State = "pull_only"
		r.Reason = live.Channel.Reason
		if state.Transport == "inbox" {
			r.Reason = live.Inbox.Reason
		}
	}
	if !state.Exhausted.IsZero() {
		r.State = "pull_only"
		r.Reason = "no receipt on channel or inbox; retries on reconnect, attach or new messages after 60 s"
	}
	return r
}
func (s *Stub) RenderRouteFor(key RouteKey) ipc.RenderRoute {
	if s.negotiated() {
		return s.deliveryRoute(key)
	}
	return s.RenderRoute()
}
