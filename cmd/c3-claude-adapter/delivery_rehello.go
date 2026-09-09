package main

import (
	"log"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

const deliveryRehelloInterval = 10 * time.Second

type deliveryRehelloState struct {
	facts            ipc.DeliveryLive
	fetch            bool
	pending, closing bool
	last             time.Time
}

// F: "at most once per fact change, never more than once per 10 s, never
// while a legacy push is in flight (wait for its ack/expiry)". The shared
// observer only closes the old socket; brokerReader owns reconnect and hello.
func (a *adapter) pollDeliveryRehello(facts ipc.DeliveryLive, now time.Time, fetchEligible ...bool) {
	a.liveMu.Lock()
	conn := a.currentConn()
	r := &a.deliveryRehello
	if a.upgrade.quiescing.Load() || a.deliveryAccepted.Load() || conn == nil || r.closing {
		a.liveMu.Unlock()
		return
	}
	fetch := len(fetchEligible) > 0 && fetchEligible[0]
	if fetch && !r.fetch {
		r.pending = true
	}
	r.fetch = fetch
	if facts != r.facts {
		becameEligible := (!r.facts.Channel.Eligible && facts.Channel.Eligible) ||
			(!r.facts.Inbox.Eligible && facts.Inbox.Eligible)
		r.facts = facts
		r.pending = r.pending || becameEligible
	}
	if !facts.Channel.Eligible && !facts.Inbox.Eligible && !fetch {
		r.pending = false
	}
	if !r.pending || a.liveActive != 0 || (!r.last.IsZero() && now.Sub(r.last) < deliveryRehelloInterval) {
		a.liveMu.Unlock()
		return
	}
	r.last, r.pending, r.closing = now, false, true
	a.liveMu.Unlock()
	log.Print("delivery eligibility available: reconnecting to offer delivery")
	// The closing latch and liveActive admission share liveMu: a new legacy
	// push cannot start between the idle check and closing this connection.
	_ = conn.Close()
}
