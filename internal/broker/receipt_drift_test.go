package broker

import (
	"encoding/json"
	"testing"

	"github.com/Andrometiq/c3/internal/ipc"
)

func TestReceiptDriftSessionStatus(t *testing.T) {
	clearFetchTestEnvironment(t)
	b := newTestBroker(t, mfWithTelegram())
	t.Cleanup(b.Shutdown)
	s := b.registerDeliveryHello(ipc.HelloMsg{CLI: "claude", PID: 123, CWD: "/work", Delivery: json.RawMessage(channelOffer)}, nil, nil)
	report := []byte(`{"op":"delivery_report","receipt_shape_drift":"2.1.266"}`)
	b.handleDeliveryReport(s, report)
	if s.receiptShapeDrift != "" {
		t.Fatal("unconfirmed negotiation honored report")
	}
	b.handleDeliveryReport(s, []byte(`{"op":"delivery_report","accepted":["channel"]}`))
	before := s.delivery.live
	b.handleDeliveryReport(s, report)
	if s.delivery.live != before {
		t.Fatal("diagnostics changed delivery facts")
	}
	peer, done := peerPair(t, b)
	defer done()
	pingerHelloAck(t, peer, "c3-broker-cli", 999, "/status")
	reply := askSessions(t, peer, 0, "")
	if len(reply.Sessions) != 1 || reply.Sessions[0].ReceiptShapeDrift != "2.1.266" {
		t.Fatalf("status=%+v", reply)
	}
	// Hello carries an already-alarmed session through a broker restart.
	next := b.registerDeliveryHello(ipc.HelloMsg{CLI: "claude", PID: 123, CWD: "/work", ReceiptShapeDrift: "2.1.266"}, nil, s)
	if next.receiptShapeDrift != "2.1.266" {
		t.Fatal("hello lost drift diagnostic")
	}
	legacy := ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "live push not confirmed"}
	next.SetRenderRoute(legacy.State, legacy.Reason, true)
	next.renderProbeSent = true
	raw, _ := json.Marshal(ipc.RenderStateMsg{Op: ipc.OpRenderState, ReceiptShapeDrift: "2.1.266"})
	b.handleRenderState(next, raw)
	if next.RenderRoute() != legacy || !next.renderProbeSent {
		t.Fatal("legacy diagnostic changed route")
	}
	next.SetStableSessionID("old")
	next.SetStableSessionID("new")
	if next.receiptShapeDrift != "" {
		t.Fatal("new session inherited alarm")
	}
}
