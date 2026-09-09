package broker

import (
	"encoding/json"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

// A slow operator log must not expose a half-published terminal transition.
// The default logger serializes Write calls, so only the logger touches seen.
type blockedRetirementLog struct {
	match   string
	seen    int
	entered chan struct{}
	release chan struct{}
}

func (l *blockedRetirementLog) Write(p []byte) (int, error) {
	if strings.Contains(string(p), l.match) {
		l.seen++
		if l.seen == 2 {
			close(l.entered)
			<-l.release
		}
	}
	return len(p), nil
}

func TestFetchReceiptMultiRouteIPCConfirmBeforeRetirementLogCompletes(t *testing.T) {
	for _, alias := range []bool{false, true} {
		t.Run(map[bool]string{false: "attempt_result", true: "fetch_confirm"}[alias], func(t *testing.T) {
			clearFetchTestEnvironment(t)
			b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
			t.Cleanup(b.Shutdown)
			key := MakeRouteKey("telegram", -100, nil)
			s, _ := negotiatedHolder(t, b, key, 1)
			receiptHolder(s)
			topic := int64(2)
			other := MakeRouteKey("telegram", -100, &topic)
			s.AddRoute(other)
			s.MarkRouteConfirmed(other)
			b.Routes.Claim(other, s)
			for _, route := range []RouteKey{key, other} {
				if err := b.Queue.Append(queueRouteKey(route), inboundOn(-100, nil, 1, "queued")); err != nil {
					t.Fatal(err)
				}
			}
			response := b.fetchSelectedRoutes(s, ipc.FetchQueueReq{ID: "q", Ack: true, All: true, Lease: json.RawMessage("true")}, []RouteKey{key, other})
			if response.Err != "" || len(response.Members) != 2 {
				t.Fatal(response)
			}
			sink := &blockedRetirementLog{
				match:   "attempt retired n=1 token=" + response.LeaseToken,
				entered: make(chan struct{}),
				release: make(chan struct{}),
			}
			previous := log.Writer()
			log.SetOutput(sink)
			t.Cleanup(func() {
				close(sink.release)
				log.SetOutput(previous)
			})
			if alias {
				raw, _ := json.Marshal(ipc.FetchConfirmReq{Op: ipc.OpFetchConfirm, LeaseToken: response.LeaseToken})
				b.handleFetchConfirm(s, raw)
			} else {
				raw, _ := json.Marshal(ipc.AttemptResultMsg{Op: ipc.OpAttemptResult, Token: response.LeaseToken, Outcome: "confirmed"})
				b.handleAttemptResult(s, raw)
			}
			select {
			case <-sink.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("second route did not reach retirement logging")
			}
			if pending := b.Queue.StatusFor(queueRouteKey(key)).Pending + b.Queue.StatusFor(queueRouteKey(other)).Pending; pending != 0 {
				t.Fatalf("retirement logged with %d rows still pending", pending)
			}
			attempts := b.attempts.lookup(response.LeaseToken, time.Now())
			if len(attempts) != 2 {
				t.Fatal(attempts)
			}
			for _, attempt := range attempts {
				if attempt.Outcome != "confirmed" || len(attempt.Retired) != 1 {
					t.Fatalf("queue is empty but attempt publication is incomplete: %+v", attempt)
				}
			}
		})
	}
}
