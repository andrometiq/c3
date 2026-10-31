package main

import (
	"context"
	"encoding/json"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// Model the shipped strict decoder, including its original field order.
type oldEventEligibility struct {
	Eligible bool   `json:"eligible"`
	Reason   string `json:"reason,omitempty"`
}
type oldEventLive struct {
	Channel oldEventEligibility `json:"channel"`
	Inbox   oldEventEligibility `json:"inbox"`
}
type oldEventReport struct {
	AcceptedCapabilities []string     `json:"accepted_capabilities,omitempty"`
	ReceiptShapeDrift    string       `json:"receipt_shape_drift,omitempty"`
	Op                   ipc.Op       `json:"op"`
	Live                 oldEventLive `json:"live"`
	Accepted             []string     `json:"accepted,omitzero"`
}

func TestEventFactMixedVersionRecovery(t *testing.T) {
	isolateAdapterTest(t)
	for _, accepts := range []bool{false, true} {
		name := "old_broker"
		if accepts {
			name = "new_broker"
		}
		t.Run(name, func(t *testing.T) {
			a, output, _ := liveFixture(t, ipc.RenderQueueOnly)
			a.deliveryHostInitialized.Store(true)
			state := ipc.RenderQueueOnly
			a.deliveryHostRoute = func() ipc.RenderRoute { return ipc.RenderRoute{State: state} }
			client, server := net.Pipe()
			t.Cleanup(func() { client.Close(); server.Close() })
			a.bmu.Lock()
			a.conn = ipc.NewConn(client)
			a.helloPending = true
			a.bmu.Unlock()
			peer := ipc.NewConn(server)
			frames := make(chan []byte, 16)
			errs := make(chan error, 1)
			go func() {
				raw, err := peer.ReadFrame()
				if err != nil {
					errs <- err
					return
				}
				frames <- raw
				ack := ipc.HelloAckMsg{Op: ipc.OpHelloAck, Delivery: &ipc.DeliveryAcceptance{Version: 1, Modes: []string{"channel", "inbox", "fetch_receipt"}}}
				if accepts {
					ack.AcceptedCapabilities = []string{"event_push_renders:1"}
				}
				if err := peer.WriteJSON(ack); err != nil {
					errs <- err
					return
				}
				for {
					raw, err := peer.ReadFrame()
					if err != nil {
						return
					}
					frames <- raw
				}
			}()
			if err := a.hello(); err != nil {
				t.Fatal(err)
			}
			var hello ipc.HelloMsg
			raw := nextLiveFrame(t, frames)
			if err := json.Unmarshal(raw, &hello); err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(hello.Capabilities, "event_push_renders:1") || strings.Contains(string(hello.Delivery), "event_push_renders") {
				t.Fatalf("hello=%s", raw)
			}
			confirmation := nextLiveFrame(t, frames)
			if !accepts && string(confirmation) != `{"op":"delivery_report","accepted":["channel","inbox","fetch_receipt"]}` {
				t.Fatalf("old confirmation changed: %s", confirmation)
			}
			if accepts && !strings.Contains(string(confirmation), `"accepted_capabilities":["event_push_renders:1"]`) {
				t.Fatalf("missing confirmation: %s", confirmation)
			}
			if !a.fetchReceiptAccepted() {
				t.Fatal("fetch receipts were not negotiated while host unavailable")
			}
			// Recover without rehello, then downgrade to probing and recover again.
			for i, next := range []string{ipc.RenderCapable, ipc.RenderProbing, ipc.RenderCapable} {
				state = next
				a.pollDeliveries()
				// Probing and Capable have identical old-schema facts, so old brokers
				// receive no redundant report when only the unsupported fact changes.
				if !accepts && i > 0 {
					continue
				}
				raw = nextLiveFrame(t, frames)
				if !accepts {
					var report oldEventReport
					if err := ipc.StrictJSON(raw, &report); err != nil {
						t.Fatalf("old decoder rejected recovery: %s: %v", raw, err)
					}
					want, _ := json.Marshal(report)
					if string(raw) != string(want) || !report.Live.Channel.Eligible {
						t.Fatalf("old schema changed: %s", raw)
					}
				} else {
					var report ipc.DeliveryReportMsg
					if err := ipc.StrictJSON(raw, &report); err != nil || report.Live.Channel.EventPushRenders != (next == ipc.RenderCapable) {
						t.Fatalf("facts=%s err=%v", raw, err)
					}
				}
			}
			// The old strict parser accepted the recovery report, enabling delivery.
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			msg, _ := json.Marshal(ipc.DeliverMsg{Op: ipc.OpDeliver, Token: "recovered", Transport: "channel", DeadlineMS: 15000, Inbound: c3types.Inbound{Text: "ordinary recovery"}})
			a.handleDeliver(ctx, msg)
			waitDeliveryWritten(t, a, "recovered")
			if !strings.Contains(string(output.Bytes()), "ordinary recovery") {
				t.Fatal("ordinary message did not render")
			}

			// Even a later hello from an already-capable connection stays old-schema.
			if strings.Contains(string(a.deliveryOffer()), "event_push_renders") {
				t.Fatal("rehello leaked the fact")
			}
			select {
			case err := <-errs:
				t.Fatal(err)
			default:
			}
			if !accepts {
				select {
				case raw := <-frames:
					if strings.Contains(string(raw), "event_push_renders") {
						t.Fatalf("leaked: %s", raw)
					}
				case <-time.After(20 * time.Millisecond):
				}
			}
		})
	}
}

func TestEventFactAcceptanceBoundToConnection(t *testing.T) {
	isolateAdapterTest(t)
	a, _, frames := liveFixture(t, ipc.RenderCapable)
	a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderCapable}
	a.deliveryHostInitialized.Store(true)
	accepted := a.currentConn()
	a.eventPushFactsConn.Store(accepted)
	replacement := ipc.NewConn(nil)
	if !a.deliveryReportFacts(accepted).Channel.EventPushRenders {
		t.Fatal("accepted connection lost facts")
	}
	if a.deliveryReportFacts(replacement).Channel.EventPushRenders {
		t.Fatal("replacement inherited acceptance")
	}
	a.eventPushFactsConn.Store(replacement)
	if a.deliveryReportFacts(accepted).Channel.EventPushRenders {
		t.Fatal("old connection inherited replacement acceptance")
	}
	// A paused old poll can overwrite the shared last-facts cache after hello.
	// Identical facts still need publication on the current connection.
	a.eventPushFactsConn.Store(nil)
	facts := a.deliveryReportFacts(accepted)
	a.liveMu.Lock()
	a.deliveryLastFacts = facts
	a.deliveryLastFactsConn = replacement
	a.liveMu.Unlock()
	a.pollDeliveries()
	raw := nextLiveFrame(t, frames)
	var report oldEventReport
	if err := ipc.StrictJSON(raw, &report); err != nil || !report.Live.Channel.Eligible {
		t.Fatalf("replacement recovery report=%s err=%v", raw, err)
	}
	if a.deliveryReportFacts(nil).Channel.EventPushRenders {
		t.Fatal("unknown connection matched")
	}
}

func TestEventFactInitialCapableConfirmation(t *testing.T) {
	isolateAdapterTest(t)
	a, _, frames, _ := negotiatedAdapter(t)
	conn := a.currentConn()
	a.eventPushFactsConn.Store(conn)
	if strings.Contains(string(a.deliveryOffer()), "event_push_renders") {
		t.Fatal("hello leaked unaccepted fact")
	}
	if err := a.confirmDeliveryAcceptance(conn, &ipc.DeliveryAcceptance{Version: 1, Modes: []string{"channel"}}); err != nil {
		t.Fatal(err)
	}
	raw := nextLiveFrame(t, frames)
	var report ipc.DeliveryReportMsg
	if err := ipc.StrictJSON(raw, &report); err != nil || !report.Live.Channel.EventPushRenders || !slices.Contains(report.AcceptedCapabilities, "event_push_renders:1") {
		t.Fatalf("initial report=%s err=%v", raw, err)
	}
}
