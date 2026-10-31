package broker

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/ipc"
)

func TestEventFactCapabilityNegotiation(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		offer, confirm, renders bool
	}{
		{"new_adapter_capable", true, true, true},
		{"new_adapter_probing", true, true, false},
		{"old_adapter", false, false, false},
		{"unoffered_claim", false, true, true},
		{"unconfirmed_claim", true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, w, holder, frames, ctx := negotiatedFixture(t)
			hello := ipc.HelloMsg{}
			if tc.offer {
				hello.Capabilities = []string{"event_push_renders:1"}
			}
			ack := b.buildHelloAck(hello, holder)
			if slices.Contains(ack.AcceptedCapabilities, "event_push_renders:1") != tc.offer {
				t.Fatalf("ack=%+v", ack)
			}
			live := ipc.DeliveryLive{Channel: ipc.DeliveryEligibility{Eligible: true, EventPushRenders: tc.renders}}
			report := ipc.DeliveryReportMsg{Op: ipc.OpDeliveryReport, Accepted: []string{"channel", "inbox"}, Live: live}
			if tc.confirm {
				report.AcceptedCapabilities = []string{"event_push_renders:1"}
			}
			raw, _ := json.Marshal(report)
			b.handleDeliveryReport(holder, raw)
			want := tc.offer && tc.confirm && tc.renders
			if eventPushRenders(holder) != want {
				t.Fatalf("renders=%t want=%t", eventPushRenders(holder), want)
			}
			if want {
				return
			} // Direct event delivery is covered by TestFlushEventNegotiatedChannel.
			w.flushEvent(ctx, callbackInbound(w.key))
			frame := nextDeliver(t, frames)
			if frame.Inbound.IsEvent() || !strings.Contains(frame.Inbound.Text, "[button tap]") {
				t.Fatalf("not converted: %+v", frame)
			}
			if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 1 {
				t.Fatalf("pending=%d", n)
			}
			w.handleAttemptResult(resultFor(holder, frame, "confirmed"))
			if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 0 {
				t.Fatalf("pending after receipt=%d", n)
			}
		})
	}
}

// The first confirmation is deliberately withheld: hello_ack has transferred
// the claim, but the legacy Capable state cannot authorize a direct event.
func TestEventReconnectBeforeDeliveryConfirmation(t *testing.T) {
	for _, tc := range []struct{ name, offer, report string }{
		{"pending", `{"version":1,"live":{"channel":{"eligible":false},"inbox":{"eligible":true}},"receipts":"transcript","fetch":"receipt"}`, ""},
		{"malformed", `{"version":1}`, ""},
		{"null", `null`, ""},
		{"refused", channelOffer, `{"op":"delivery_report","accepted":["unknown"]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, w, old, _ := shadowFixture(t)
			b.Workers.Stop()
			old.PID = os.Getpid()
			b.configureDelivery(old, []byte(channelOffer))
			b.handleDeliveryReport(old, []byte(`{"op":"delivery_report","accepted":["channel","inbox"]}`))
			old.MarkDisconnected()
			peer, done := peerPair(t, b)
			t.Cleanup(done)
			hello := ipc.HelloMsg{Op: ipc.OpHello, CLI: old.CLI, PID: old.PID, CWD: old.CWD,
				RenderState: ipc.RenderCapable, Delivery: json.RawMessage(tc.offer), Capabilities: []string{"event_push_renders:1"}}
			if err := peer.WriteJSON(hello); err != nil {
				t.Fatal(err)
			}
			raw, err := peer.ReadFrame()
			if err != nil {
				t.Fatal(err)
			}
			var ack ipc.HelloAckMsg
			if err := json.Unmarshal(raw, &ack); err != nil {
				t.Fatal(err)
			}
			holder, claimed := b.Routes.Holder(w.key)
			if !claimed || holder == old || !holder.HasRoute(w.key) || !holder.RouteConfirmed(w.key) {
				t.Fatal("reconnect did not transfer confirmed claim")
			}
			if holder.negotiated() || holder.RenderRoute().State != ipc.RenderCapable || !holder.deliveryOffered {
				t.Fatal("expected offered but unconfirmed delivery with legacy Capable state")
			}
			if tc.report != "" {
				b.handleDeliveryReport(holder, []byte(tc.report))
				if !holder.deliveryRefused.Load() {
					t.Fatal("expected refusal")
				}
			}
			frames := make(chan ipc.InboundMsg, 20)
			go func() {
				for {
					raw, err := peer.ReadFrame()
					if err != nil {
						return
					}
					var frame ipc.InboundMsg
					if json.Unmarshal(raw, &frame) == nil && frame.Op == ipc.OpInbound {
						frames <- frame
					}
				}
			}()
			w.eventDispatchTestHook = func(stage string) {
				if stage == "before_write" {
					t.Error("direct event reached the write boundary before confirmation")
				}
			}
			w.flushEvent(context.Background(), callbackInbound(w.key))
			rows, err := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
			if err != nil || len(rows) != 1 || rows[0].Inbound.IsEvent() || !strings.Contains(rows[0].Inbound.Text, "[button tap]") {
				t.Fatalf("event not converted durably: rows=%+v err=%v", rows, err)
			}
			select {
			case frame := <-frames:
				t.Fatalf("unexpected push before confirmation: %+v", frame)
			default:
			}
		})
	}
}
