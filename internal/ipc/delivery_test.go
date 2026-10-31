package ipc

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

func TestNegotiatedWireGolden(t *testing.T) {
	// P2: "Wire, pinned (protocol v1, additive; only sent/honoured once accepted)".
	for _, tc := range []struct {
		name   string
		value  any
		golden string
	}{
		{"offer", DeliveryOffer{Version: 1, Live: DeliveryLive{Channel: DeliveryEligibility{Eligible: true}, Inbox: DeliveryEligibility{Reason: "unavailable"}}, Receipts: "transcript", Fetch: "receipt"}, `{"version":1,"live":{"channel":{"eligible":true},"inbox":{"eligible":false,"reason":"unavailable"}},"receipts":"transcript","fetch":"receipt"}`},
		{"inbox-offer", DeliveryOffer{Version: 1, Live: DeliveryLive{Channel: DeliveryEligibility{Reason: "no channel"}, Inbox: DeliveryEligibility{Eligible: true}}, Receipts: "transcript", Fetch: "receipt"}, `{"version":1,"live":{"channel":{"eligible":false,"reason":"no channel"},"inbox":{"eligible":true}},"receipts":"transcript","fetch":"receipt"}`},
		{"inbox-accept", DeliveryAcceptance{Version: 1, Modes: []string{"channel", "inbox"}}, `{"version":1,"modes":["channel","inbox"]}`},
		{"accept", DeliveryAcceptance{Version: 1, Modes: []string{"channel"}}, `{"version":1,"modes":["channel"]}`},
		{"result", AttemptResultMsg{Op: OpAttemptResult, Token: "t", Outcome: "confirmed"}, `{"op":"attempt_result","token":"t","outcome":"confirmed","reason":""}`},
		{"report", DeliveryReportMsg{Op: OpDeliveryReport, Live: DeliveryLive{Channel: DeliveryEligibility{Eligible: true}}}, `{"op":"delivery_report","live":{"channel":{"eligible":true},"inbox":{"eligible":false}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.value)
			if err != nil || string(got) != tc.golden {
				t.Fatalf("got %s err=%v want %s", got, err, tc.golden)
			}
		})
	}
	raw, _ := json.Marshal(DeliverMsg{Op: OpDeliver, Token: "t", Transport: "channel", DeadlineMS: 14999, Inbound: c3types.Inbound{Text: "hello"}})
	if string(raw) != `{"op":"deliver","token":"t","transport":"channel","deadline_ms":14999,"inbound":{"Channel":"","ChatID":0,"TopicID":null,"MessageID":0,"Sender":{"UserID":0,"Username":""},"Text":"hello","Attachments":null,"ReplyTo":null,"Timestamp":"0001-01-01T00:00:00Z"}}` {
		t.Fatalf("deliver: %s", raw)
	}
	hello, _ := json.Marshal(HelloMsg{Op: OpHello, Delivery: json.RawMessage(`{"version":1}`), Capabilities: []string{"claude/channel"}})
	if !strings.Contains(string(hello), `"delivery":{"version":1}`) || !strings.Contains(string(hello), `"capabilities":["claude/channel"]`) {
		t.Fatal(string(hello))
	}
	ack, _ := json.Marshal(HelloAckMsg{Op: OpHelloAck, Delivery: &DeliveryAcceptance{Version: 1, Modes: []string{"channel"}}})
	if !strings.Contains(string(ack), `"delivery":{"version":1,"modes":["channel"]}`) {
		t.Fatal(string(ack))
	}
}

type delayedDelivery struct {
	Deadline   time.Time `json:"-"`
	DeadlineMS int64     `json:"deadline_ms"`
}

func (d delayedDelivery) PrepareFrame() any {
	d.DeadlineMS = time.Until(d.Deadline).Milliseconds()
	return struct {
		DeadlineMS int64 `json:"deadline_ms"`
	}{d.DeadlineMS}
}
func TestDeliverDeadlineMeasuredAfterAdmission(t *testing.T) {
	// G4: "remaining observation budget measured immediately before the frame is written".
	conn, peer := newPipePair(t)
	defer conn.Close()
	defer peer.Close()
	conn.wmu <- struct{}{}
	done := make(chan error, 1)
	deadline := time.Now().Add(time.Second)
	go func() {
		done <- conn.WriteJSONContext(context.Background(), delayedDelivery{Deadline: deadline, DeadlineMS: 1000})
	}()
	time.Sleep(80 * time.Millisecond)
	<-conn.wmu
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var frame struct {
		DeadlineMS int64 `json:"deadline_ms"`
	}
	json.Unmarshal(raw, &frame)
	if frame.DeadlineMS >= 950 || frame.DeadlineMS <= 0 {
		t.Fatalf("stale write budget: %s", raw)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDeliverInboxWireGolden(t *testing.T) {
	// P2: "deliver ... transport: channel|inbox"; the new token is broker-authored.
	raw, _ := json.Marshal(DeliverMsg{Op: OpDeliver, Token: "inbox-new", Transport: "inbox", DeadlineMS: 15000, Inbound: c3types.Inbound{Text: "hello"}})
	want := `{"op":"deliver","token":"inbox-new","transport":"inbox","deadline_ms":15000,"inbound":{"Channel":"","ChatID":0,"TopicID":null,"MessageID":0,"Sender":{"UserID":0,"Username":""},"Text":"hello","Attachments":null,"ReplyTo":null,"Timestamp":"0001-01-01T00:00:00Z"}}`
	if string(raw) != want {
		t.Fatal(string(raw))
	}
}
func TestNegotiatedInboxRenderHistory(t *testing.T) {
	// P8: "live: <transport>, confirmed <age>"; exhaustion keeps transport history.
	for _, state := range []string{"live_inbox", "pull_only"} {
		r := RenderRoute{State: state, Confirmed: time.Now(), Transport: "inbox"}
		raw, _ := json.Marshal(r)
		var decoded RenderRoute
		if json.Unmarshal(raw, &decoded) != nil {
			t.Fatal(string(raw))
		}
		want := "live: inbox, confirmed"
		if state == "pull_only" {
			want = "was inbox, confirmed"
		}
		if !strings.Contains(decoded.Text(), want) {
			t.Fatal(decoded.Text())
		}
	}
}

func TestEventPushRenderingFact(t *testing.T) {
	for _, tc := range []struct {
		value          string
		valid, renders bool
	}{
		{"", true, false}, {`,"event_push_renders":false`, true, false}, {`,"event_push_renders":true`, true, true}, {`,"event_push_renders":"true"`, false, false},
	} {
		raw := []byte(`{"version":1,"live":{"channel":{"eligible":true` + tc.value + `},"inbox":{"eligible":false}},"receipts":"transcript","fetch":"receipt"}`)
		offer := ParseDeliveryOffer(raw)
		if (offer != nil) != tc.valid {
			t.Fatalf("offer=%+v raw=%s", offer, raw)
		}
		if offer != nil && offer.Live.Channel.EventPushRenders != tc.renders {
			t.Fatalf("offer=%+v", offer)
		}
	}
	// Older brokers strictly parse the old shape and decline extended offers;
	// the existing hello legacy render state remains their fallback.
	type oldEligibility struct {
		Eligible bool   `json:"eligible"`
		Reason   string `json:"reason,omitempty"`
	}
	var oldLive struct {
		Channel oldEligibility `json:"channel"`
		Inbox   oldEligibility `json:"inbox"`
	}
	if StrictJSON([]byte(`{"channel":{"eligible":true,"event_push_renders":true},"inbox":{"eligible":false}}`), &oldLive) == nil {
		t.Fatal("old schema unexpectedly accepted extended facts")
	}
	raw, err := json.Marshal(DeliveryLive{Channel: DeliveryEligibility{Eligible: true}})
	if err != nil || strings.Contains(string(raw), "event_push_renders") {
		t.Fatalf("false fact must be omitted: %s %v", raw, err)
	}
}
