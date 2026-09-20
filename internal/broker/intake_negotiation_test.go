package broker

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/intake"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

const intakeReceiptOffer = `{"version":1,"live":{"channel":{"eligible":false},"inbox":{"eligible":false}},"receipts":"none","fetch":"receipt"}`
const intakeConfirmReport = `{"op":"delivery_report","accepted":["fetch_receipt"],"accepted_capabilities":["intake_metadata:1"]}`

func TestIntakeNegotiationWireAndFetch(t *testing.T) {
	for _, tc := range []struct {
		name       string
		caps       []string
		report     string
		active     bool
		negotiated bool
		refused    bool
	}{
		{"complete", []string{"acme:1", "intake_metadata:1", "intake_metadata:1"}, intakeConfirmReport, true, true, false},
		{"no hello capability", nil, `{"op":"delivery_report","accepted":["fetch_receipt"]}`, false, true, false},
		{"unoffered claim", nil, intakeConfirmReport, false, true, false},
		{"unknown hello capability", []string{"acme:1"}, intakeConfirmReport, false, true, false},
		{"no confirmation", []string{"intake_metadata:1"}, `{"op":"delivery_report","accepted":["fetch_receipt"]}`, false, true, false},
		{"null confirmation", []string{"intake_metadata:1"}, `{"op":"delivery_report","accepted":["fetch_receipt"],"accepted_capabilities":null}`, false, true, false},
		{"unknown confirmation", []string{"intake_metadata:1"}, `{"op":"delivery_report","accepted":["fetch_receipt"],"accepted_capabilities":["acme:1"]}`, false, true, false},
		{"no fetch receipt", []string{"intake_metadata:1"}, `{"op":"delivery_report","accepted":[],"accepted_capabilities":["intake_metadata:1"]}`, false, true, false},
		{"no accepted modes", []string{"intake_metadata:1"}, `{"op":"delivery_report","accepted_capabilities":["intake_metadata:1"]}`, false, false, false},
		{"null accepted modes", []string{"intake_metadata:1"}, `{"op":"delivery_report","accepted":null,"accepted_capabilities":["intake_metadata:1"]}`, false, false, true},
		{"malformed confirmation", []string{"intake_metadata:1"}, `{"op":"delivery_report","accepted":["fetch_receipt"],"accepted_capabilities":"intake_metadata:1"}`, false, false, false},
		{"unaccepted delivery mode", []string{"intake_metadata:1"}, `{"op":"delivery_report","accepted":["fetch_receipt","channel"],"accepted_capabilities":["intake_metadata:1"]}`, false, false, true},
		{"capability in delivery modes", []string{"intake_metadata:1"}, `{"op":"delivery_report","accepted":["fetch_receipt","intake_metadata:1"],"accepted_capabilities":["intake_metadata:1"]}`, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearFetchTestEnvironment(t)
			b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
			t.Cleanup(b.Shutdown)
			peer, closePeer := peerPair(t, b)
			t.Cleanup(closePeer)
			hello := ipc.HelloMsg{Op: ipc.OpHello, CLI: "acme", PID: os.Getpid(), CWD: "/work",
				Capabilities: tc.caps, Delivery: json.RawMessage(intakeReceiptOffer)}
			if err := peer.WriteJSON(hello); err != nil {
				t.Fatal(err)
			}
			raw := nextWireOp(t, peer, ipc.OpHelloAck)
			var ack ipc.HelloAckMsg
			if err := json.Unmarshal(raw, &ack); err != nil {
				t.Fatal(err)
			}
			var wantCaps []string
			if slices.Contains(tc.caps, "intake_metadata:1") {
				wantCaps = []string{"intake_metadata:1"}
			}
			if !slices.Equal(ack.AcceptedCapabilities, wantCaps) || !ack.Delivery.HasMode("fetch_receipt") {
				t.Fatalf("hello_ack=%s", raw)
			}
			if len(wantCaps) == 0 && strings.Contains(string(raw), `"accepted_capabilities"`) {
				t.Fatalf("non-negotiating hello_ack gained a field: %s", raw)
			}
			s, ok := b.Stubs.Get(ack.ConnID)
			if !ok || s.intakeMetadataActive() {
				t.Fatal("hello/hello_ack activated intake without confirmation")
			}
			key := MakeRouteKey("telegram", -100, nil)
			b.Routes.Claim(key, s)
			s.AddRoute(key)
			s.MarkRouteConfirmed(key)
			if _, err := b.Queue.AppendTrackedIntake(queueRouteKey(key), &c3types.Inbound{Text: "hello"}, intakeFetchSource(),
				intake.AttachmentsState{{Index: 0, STT: intake.STTDone, Transcript: "spoken"}}); err != nil {
				t.Fatal(err)
			}
			fetch := func(ack bool) []byte {
				t.Helper()
				if err := peer.WriteJSON(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "q", All: true, Ack: ack, Lease: json.RawMessage("true")}); err != nil {
					t.Fatal(err)
				}
				return nextWireOp(t, peer, ipc.OpFetchQueueResult)
			}
			legacy := `{"op":"fetch_queue_result","id":"q","messages":[` + strings.TrimSuffix(intakeInboundGolden, "}") + `,"V":1}],"remaining":0}`
			if raw := fetch(false); string(raw) != legacy {
				t.Fatalf("pre-confirmation fetch=%s want=%s", raw, legacy)
			}
			if err := peer.WriteJSON(json.RawMessage(tc.report)); err != nil {
				t.Fatal(err)
			}
			// The fetch response also synchronizes with the preceding report.
			raw = fetch(tc.active)
			if s.intakeMetadataActive() != tc.active || s.negotiated() != tc.negotiated || s.deliveryRefused.Load() != tc.refused {
				t.Fatalf("active=%v negotiated=%v refused=%v", s.intakeMetadataActive(), s.negotiated(), s.deliveryRefused.Load())
			}
			if !tc.active {
				if string(raw) != legacy {
					t.Fatalf("inactive fetch=%s want=%s", raw, legacy)
				}
				if s.acceptsDeliveryMode("fetch_receipt") {
					raw = fetch(true)
					var resp ipc.FetchQueueResp
					if err := json.Unmarshal(raw, &resp); err != nil || resp.Err != "" || len(resp.Messages) != 1 || len(resp.Members) != 1 || resp.LeaseToken == "" {
						t.Fatalf("legacy receipt fetch=%s err=%v", raw, err)
					}
					if resp.BlockedOn != nil {
						t.Fatalf("inactive receipt fetch gained blocked_on: %s", raw)
					}
					baseline, err := json.Marshal(resp)
					if err != nil || string(raw) != string(baseline) {
						t.Fatalf("legacy receipt bytes changed: %s err=%v", raw, err)
					}
				}
				return
			}
			var resp struct {
				ipc.FetchQueueResp
				Messages []json.RawMessage `json:"messages"`
			}
			if err := json.Unmarshal(raw, &resp); err != nil || resp.Err != "" || len(resp.Messages) != 1 || len(resp.Members) != 1 || resp.LeaseToken == "" {
				t.Fatalf("receipt fetch=%s err=%v", raw, err)
			}
			want := strings.Replace(intakeMemberGolden, `,"intake":`, `,"V":1,"intake":`, 1)
			if string(resp.Messages[0]) != want || resp.ReceiptTrailer != ipc.FetchReceiptTrailer(resp.LeaseToken, resp.Members) {
				t.Fatalf("negotiated fetch did not carry intake and receipts: %s", raw)
			}
			if b.Queue.StatusFor(queueRouteKey(key)).Pending != 1 {
				t.Fatal("unconfirmed receipt fetch consumed the row")
			}
		})
	}
}

func TestIntakeNegotiationReadyPrefixAcceptance(t *testing.T) {
	clearFetchTestEnvironment(t)
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	t.Cleanup(b.Shutdown)
	peer, closePeer := peerPair(t, b)
	t.Cleanup(closePeer)
	hello := ipc.HelloMsg{Op: ipc.OpHello, CLI: "acme", PID: os.Getpid(), CWD: "/work",
		Capabilities: []string{"intake_metadata:1"}, Delivery: json.RawMessage(intakeReceiptOffer)}
	if err := peer.WriteJSON(hello); err != nil {
		t.Fatal(err)
	}
	raw := nextWireOp(t, peer, ipc.OpHelloAck)
	var ack ipc.HelloAckMsg
	if err := json.Unmarshal(raw, &ack); err != nil || !ack.Delivery.HasMode("fetch_receipt") || !slices.Equal(ack.AcceptedCapabilities, []string{"intake_metadata:1"}) {
		t.Fatalf("hello_ack=%s err=%v", raw, err)
	}
	s, ok := b.Stubs.Get(ack.ConnID)
	if !ok || s.intakeMetadataActive() {
		t.Fatal("hello/hello_ack activated intake without confirmation")
	}
	key := MakeRouteKey("telegram", -100, nil)
	b.Routes.Claim(key, s)
	s.AddRoute(key)
	s.MarkRouteConfirmed(key)
	ready := appendReadyPrefixRow(t, b, key, false, false, false)
	blocked := appendReadyPrefixRow(t, b, key, false, false, true)
	appendReadyPrefixRow(t, b, key, false, false, false)
	fetch := func() []byte {
		t.Helper()
		if err := peer.WriteJSON(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "q", Mode: "ready_prefix",
			All: true, Ack: true, Lease: json.RawMessage("true")}); err != nil {
			t.Fatal(err)
		}
		return nextWireOp(t, peer, ipc.OpFetchQueueResult)
	}
	if raw := fetch(); string(raw) != `{"op":"fetch_queue_result","id":"q","err":"fetch_queue: ready_prefix requires the negotiated intake_metadata capability"}` {
		t.Fatalf("pre-confirmation ready_prefix=%s", raw)
	}
	if err := peer.WriteJSON(json.RawMessage(intakeConfirmReport)); err != nil {
		t.Fatal(err)
	}
	raw = fetch() // Synchronizes with the preceding confirmation report.
	if !s.intakeMetadataActive() || !s.acceptsDeliveryMode("fetch_receipt") {
		t.Fatal("complete handshake did not activate intake and receipt fetch")
	}
	resp := assertReadyPrefixWire(t, raw, []string{readyPrefixMemberGolden()},
		[]queue.TrackedInbound{ready}, 2, blocked.RecordID, "stt_pending", true)
	if len(resp.Members) != 1 || resp.Members[0].RecordID != ready.RecordID {
		t.Fatal("receipt did not identify exactly the returned ready head")
	}
	assertReadyPrefixWire(t, fetch(), nil, nil, 3, ready.RecordID, "reserved", true)
	if b.Queue.StatusFor(queueRouteKey(key)).Pending != 3 {
		t.Fatal("unconfirmed ready_prefix fetch consumed rows")
	}
}

func TestIntakeNegotiationRequiresEchoAndPreservesActivation(t *testing.T) {
	clearFetchTestEnvironment(t)
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	t.Cleanup(b.Shutdown)
	hello := ipc.HelloMsg{CLI: "acme", PID: os.Getpid(), CWD: "/work", Capabilities: []string{"intake_metadata:1"}, Delivery: json.RawMessage(intakeReceiptOffer)}
	s := b.registerDeliveryHello(hello, nil, nil)
	b.handleDeliveryReport(s, []byte(intakeConfirmReport))
	if !s.acceptsDeliveryMode("fetch_receipt") || s.intakeMetadataActive() {
		t.Fatal("confirmation without a broker echo activated intake")
	}
	ack := b.buildHelloAck(hello, s)
	if !slices.Equal(ack.AcceptedCapabilities, []string{"intake_metadata:1"}) || s.intakeMetadataActive() {
		t.Fatal("echo retroactively activated an earlier confirmation")
	}
	b.handleDeliveryReport(s, []byte(`{"op":"delivery_report","accepted_capabilities":["intake_metadata:1"]}`))
	if s.intakeMetadataActive() {
		t.Fatal("capability-only report activated intake")
	}
	b.handleDeliveryReport(s, []byte(intakeConfirmReport))
	if !s.intakeMetadataActive() {
		t.Fatal("complete handshake did not activate intake")
	}
	for _, report := range []string{intakeConfirmReport, `{"op":"delivery_report","accepted":["fetch_receipt"]}`, `{"op":"delivery_report","accepted":["fetch_receipt"],"accepted_capabilities":["acme:1"]}`} {
		b.handleDeliveryReport(s, []byte(report))
		if !s.intakeMetadataActive() || !s.acceptsDeliveryMode("fetch_receipt") {
			t.Fatal("well-formed re-report deactivated negotiation")
		}
	}
}

func TestIntakeNegotiationReconnectResets(t *testing.T) {
	for _, caps := range [][]string{nil, {"intake_metadata:1"}} {
		t.Run(strings.Join(caps, ","), func(t *testing.T) {
			clearFetchTestEnvironment(t)
			b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
			t.Cleanup(b.Shutdown)
			hello := ipc.HelloMsg{CLI: "acme", PID: os.Getpid(), CWD: "/work", Capabilities: []string{"intake_metadata:1"}, Delivery: json.RawMessage(intakeReceiptOffer)}
			old := b.registerDeliveryHello(hello, nil, nil)
			b.buildHelloAck(hello, old)
			b.handleDeliveryReport(old, []byte(intakeConfirmReport))
			if !old.intakeMetadataActive() {
				t.Fatal("old connection never activated")
			}
			hello.Capabilities = caps
			next := b.registerDeliveryHello(hello, nil, old)
			if next == old || next.ConnID == old.ConnID || next.delivery != old.delivery || next.deliveryPrevious != old {
				t.Fatal("fixture did not exercise shared delivery reconnect")
			}
			next.stubMu.Lock()
			echoed := slices.Clone(next.helloAcceptedCaps)
			next.stubMu.Unlock()
			if next.intakeMetadataActive() || len(echoed) != 0 || next.negotiated() {
				t.Fatal("fresh connection inherited negotiation")
			}
			key := MakeRouteKey("telegram", -100, nil)
			b.Routes.Claim(key, next)
			next.AddRoute(key)
			next.MarkRouteConfirmed(key)
			if _, err := b.Queue.AppendTrackedIntake(queueRouteKey(key), &c3types.Inbound{Text: "hello"}, intakeFetchSource(),
				intake.AttachmentsState{{Index: 0, STT: intake.STTDone, Transcript: "spoken"}}); err != nil {
				t.Fatal(err)
			}
			fetch := func() []byte {
				return intakeFetchWire(t, b, next, ipc.FetchQueueReq{ID: "q", All: true})
			}
			legacy := `{"op":"fetch_queue_result","id":"q","messages":[` + strings.TrimSuffix(intakeInboundGolden, "}") + `,"V":1}],"remaining":0}`
			if raw := fetch(); string(raw) != legacy {
				t.Fatalf("reconnect inherited intake in fetch: %s", raw)
			}
			b.reconnectDelivery(old, next)
			ack := b.buildHelloAck(hello, next)
			if next.intakeMetadataActive() || !slices.Equal(ack.AcceptedCapabilities, caps) {
				t.Fatal("reconnect or echo activated intake")
			}
			b.handleDeliveryReport(next, []byte(intakeConfirmReport))
			if !next.acceptsDeliveryMode("fetch_receipt") || next.intakeMetadataActive() != (len(caps) > 0) {
				t.Fatal("reconnect did not require its own capability handshake")
			}
			want := legacy
			if len(caps) > 0 {
				want = `{"op":"fetch_queue_result","id":"q","messages":[` + strings.Replace(intakeMemberGolden, `,"intake":`, `,"V":1,"intake":`, 1) + `],"remaining":0}`
			}
			if raw := fetch(); string(raw) != want {
				t.Fatalf("reconnect fetch=%s want=%s", raw, want)
			}
		})
	}
}
