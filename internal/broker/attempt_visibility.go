package broker

import (
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

func (w *RouteWorker) routeNegotiated() bool {
	s, _ := w.broker.Routes.Holder(w.key)
	return s.negotiated() || w.liveAttempt() != nil
}

// G1: "rows inside a negotiated live attempt are invisible to fetch."
func (w *RouteWorker) visibleAttemptRows(n int) ([]queue.TrackedInbound, error) {
	if !w.routeNegotiated() {
		return w.broker.Queue.PeekTracked(queueRouteKey(w.key), n)
	}
	rows, err := w.broker.Queue.PeekTracked(queueRouteKey(w.key), -1)
	hidden := map[string]bool{}
	if a := w.liveAttempt(); a != nil {
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
	r := ipc.RenderRoute{State: "waiting", Held: state.Held}
	if !state.Confirmed.IsZero() {
		r.State = "live_channel"
		r.Confirmed = state.Confirmed
	}
	if !d.live.Channel.Eligible {
		r.State = "pull_only"
		r.Reason = d.live.Channel.Reason
	}
	if !state.Exhausted.IsZero() {
		r.State = "pull_only"
		r.Reason = "no receipt on channel; retries on reconnect, attach or new messages after 60 s"
	}
	return r
}
func (s *Stub) RenderRouteFor(key RouteKey) ipc.RenderRoute {
	if s.negotiated() {
		return s.deliveryRoute(key)
	}
	return s.RenderRoute()
}
