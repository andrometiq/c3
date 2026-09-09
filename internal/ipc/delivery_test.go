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
