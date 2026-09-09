package main

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

func TestDeliveryRehelloFactsFlapBounded(t *testing.T) {
	// F: "at most once per fact change, never more than once per 10 s".
	a, _ := adapterWithConn(t)
	off := ipc.DeliveryLive{}
	on := ipc.DeliveryLive{Channel: ipc.DeliveryEligibility{Eligible: true}}
	now := time.Now()
	a.pollDeliveryRehello(off, now)
	if a.deliveryRehello.closing {
		t.Fatal("unchanged unavailable facts re-helloed")
	}
	a.pollDeliveryRehello(on, now)
	if !a.deliveryRehello.closing {
		t.Fatal("eligible change did not reconnect")
	}
	first := a.deliveryRehello.last
	// A legacy broker declines negotiation. The new hello snapshots eligible
	// facts, so neither an unchanged fact nor a poll can loop reconnects.
	peer, client := net.Pipe()
	defer peer.Close()
	defer client.Close()
	a.bmu.Lock()
	a.conn = ipc.NewConn(client)
	a.bmu.Unlock()
	a.deliveryRehello.closing = false
	for i := 1; i < 10; i++ {
		a.pollDeliveryRehello(off, now.Add(time.Duration(i)*time.Second))
		a.pollDeliveryRehello(on, now.Add(time.Duration(i)*time.Second))
		if a.deliveryRehello.closing || a.deliveryRehello.last != first {
			t.Fatal("facts flap caused repeated re-hello within 10 s")
		}
	}
	a.pollDeliveryRehello(on, now.Add(10*time.Second))
	if !a.deliveryRehello.closing || a.deliveryRehello.last != now.Add(10*time.Second) {
		t.Fatal("coalesced change lost after cooldown")
	}
	// Without a new transition, even time advancing cannot rearm again.
	a.deliveryRehello.closing = false
	a.pollDeliveryRehello(on, now.Add(time.Minute))
	if a.deliveryRehello.closing {
		t.Fatal("same fact retried")
	}
}

func TestDeliveryRehelloWaitsForLegacyAckOrExpiry(t *testing.T) {
	// F: "never while a legacy push is in flight (wait for its ack/expiry)".
	for _, outcome := range []string{"ack", "expiry"} {
		t.Run(outcome, func(t *testing.T) {
			a, out, frames := liveFixture(t, ipc.RenderCapable)
			a.liveTimeout = 300 * time.Millisecond
			pushLive(t, a, "legacy-in-flight")
			facts := ipc.DeliveryLive{Channel: ipc.DeliveryEligibility{Eligible: true}}
			a.pollDeliveryRehello(facts, time.Now())
			a.liveMu.Lock()
			active, closing := a.liveActive, a.deliveryRehello.closing
			a.liveMu.Unlock()
			if active == 0 || closing {
				t.Fatal("re-hello interrupted legacy push")
			}
			if outcome == "ack" {
				appendChannelReceipt(t, a.livePath(), "legacy-in-flight")
				raw := nextLiveFrame(t, frames)
				var ack ipc.InboundDeliveredMsg
				if json.Unmarshal(raw, &ack) != nil || ack.Op != ipc.OpInboundDelivered || !ack.OK {
					t.Fatal(string(raw))
				}
			}
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				a.liveMu.Lock()
				active = a.liveActive
				a.liveMu.Unlock()
				if active == 0 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if active != 0 {
				t.Fatal("legacy attempt did not terminate")
			}
			a.pollDeliveryRehello(facts, time.Now())
			a.liveMu.Lock()
			closing = a.deliveryRehello.closing
			a.liveMu.Unlock()
			if !closing {
				t.Fatal("deferred re-hello was lost")
			}
			// This tests admission independently of the socket already being closed.
			before := len(out.Bytes())
			a.pushWithReadback(context.Background(), ipc.InboundMsg{DeliveryToken: "late-push"}, map[string]any{"content": "late", "meta": map[string]any{}})
			if len(out.Bytes()) != before {
				t.Fatal("new legacy push started during re-hello")
			}
		})
	}
}
