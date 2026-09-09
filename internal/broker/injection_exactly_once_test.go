package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

type matrixLogBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *matrixLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}
func (b *matrixLogBuffer) text() string { b.mu.Lock(); defer b.mu.Unlock(); return b.Buffer.String() }

// A fake host accepts the wire write while its foreground tool is still
// running, and deliberately withholds the transcript receipt. Both wire
// generations use the exact same injection entrypoint as the live harness.
func TestInjectedMidTurnExactlyOnce(t *testing.T) {
	for _, mode := range []string{"channel", "inbox", "legacy"} {
		for _, count := range []int{1, 2} {
			for _, late := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/count%d/late%v", mode, count, late), func(t *testing.T) {
					logs := &matrixLogBuffer{}
					prior := log.Writer()
					log.SetOutput(logs)
					t.Cleanup(func() { log.SetOutput(prior) })
					b := injectionFixture(t, true)
					topic := int64(42)
					key := MakeRouteKey(TestInjectChannel, -1, &topic)
					var s *Stub
					var negotiated <-chan ipc.DeliverMsg
					var legacy <-chan ipc.InboundMsg
					if mode == "legacy" {
						s, legacy = liveHolderFrames(t, b, key)
						s.ReceiptConfirming = true
						s.AddRoute(key)
						s.MarkRouteConfirmed(key)
						s.SetRenderRoute(ipc.RenderCapable, "", true)
					} else {
						s, negotiated = negotiatedHolder(t, b, key, 1)
						if mode == "inbox" {
							enableInbox(s, false)
						} else {
							enableInbox(s, true)
						}
					}
					response := b.Inject(ipc.TestInjectReq{Topic: topic, Text: "generic mid-turn sample", Count: count})
					if !response.Accepted {
						t.Fatal(response)
					}
					token := ""
					messageID := int64(0)
					if mode == "legacy" {
						f := waitInboundPush(t, legacy)
						token = f.DeliveryToken
						messageID = f.Inbound.MessageID
						if f.Covered != count || f.Pending != 0 {
							t.Fatalf("legacy counts include attempting rows: %+v", f)
						}
					} else {
						f := nextDeliver(t, negotiated)
						token = f.Token
						if f.Transport != mode {
							t.Fatal(f)
						}
					}
					b.notifyRenderRoute(s)
					// Several scheduler ticks while the host is mid-turn must
					// leave one open attempt, durable rows and no fallback.
					time.Sleep(250 * time.Millisecond)
					attempts := b.attempts.lookup(token, time.Now())
					if len(attempts) != 1 || attempts[0].Outcome != "open" || len(attempts[0].Members) != count || len(b.attempts.snapshot(time.Now())) != 1 {
						t.Fatalf("mid-turn attempts: %+v", attempts)
					}
					if n, _ := b.Queue.Pending(queueRouteKey(key)); n != count {
						t.Fatal("write retired rows before receipt", n)
					}
					if mode != "legacy" && s.RenderRouteFor(key).Held != 0 {
						t.Fatal("Held includes attempting rows")
					}
					for _, line := range strings.Split(logs.text(), "\n") {
						if strings.Contains(line, "TEST SINK") && strings.Contains(line, "Held —") {
							t.Fatal("false Held while every row is attempting:", line)
						}
					}
					receipt := func() {
						var frame any = ipc.AttemptResultMsg{Op: ipc.OpAttemptResult, Token: token, Outcome: "confirmed"}
						if mode == "legacy" {
							frame = ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, DeliveryToken: token, UpdateID: messageID, Count: count, OK: true}
						}
						raw, _ := json.Marshal(frame)
						if mode == "legacy" {
							b.handleInboundDelivered(s, raw)
						} else {
							b.handleAttemptResult(s, raw)
						}
					}
					fetch := func(ack bool) FetchResult {
						t.Helper()
						result, err := b.fetchHeldRoute(s, key, ipc.FetchQueueReq{All: true, Ack: ack}, 0, 0)
						if err != nil || result.Err != nil {
							t.Fatalf("fetch: %+v %v", result, err)
						}
						return result
					}
					if late {
						// The fake legacy adapter has only a channel path. A
						// negotiated adapter also reports inbox unavailable
						// for this expiry branch so expiry returns rows to fetch.
						if mode != "legacy" {
							s.delivery.mu.Lock()
							s.delivery.live.Inbox.Eligible = false
							s.delivery.mu.Unlock()
						}
						b.attempts.mu.Lock()
						b.attempts.entries[attemptKey{token, key}].Deadline = time.Now().Add(-time.Second)
						b.attempts.mu.Unlock()
						if mode == "legacy" {
							s.SetRenderRoute(ipc.RenderQueueOnly, "host receipt window expired", true)
						}
						waitForVoiceCondition(t, "attempt expiry", func() bool { return b.attempts.lookup(token, time.Now())[0].Outcome == "expired" })
						receipt()
						// Serialized fetch is also the barrier after receipt.
						if got := fetch(false); len(got.Messages) != count {
							t.Fatal("late receipt consumed queued rows", got)
						}
						if a := b.attempts.lookup(token, time.Now())[0]; len(a.Retired) != 0 {
							t.Fatal("late receipt retired", a)
						}
						if got := fetch(true); len(got.Messages) != count {
							t.Fatal("expired rows were not fetchable once", got)
						}
						receipt()
						if got := fetch(true); len(got.Messages) != 0 {
							t.Fatal("late receipt resurrected fetched rows", got)
						}
					} else {
						receipt()
						waitForFetchPending(t, b, queueRouteKey(key), 0, "receipt retirement")
						waitForVoiceCondition(t, "attempt confirmation", func() bool { return b.attempts.lookup(token, time.Now())[0].Outcome == "confirmed" })
						retired := b.attempts.lookup(token, time.Now())[0]
						if retired.Outcome != "confirmed" || len(retired.Retired) != count {
							t.Fatal(retired)
						}
						receipt()
						if got := fetch(true); len(got.Messages) != 0 {
							t.Fatal("duplicate receipt resurrected rows", got)
						}
						if a := b.attempts.lookup(token, time.Now())[0]; len(a.Retired) != count {
							t.Fatal("retired twice", a)
						}
					}
					select {
					case f := <-negotiated:
						t.Fatal("fallback/duplicate delivery", f)
					default:
					}
					select {
					case f := <-legacy:
						t.Fatal("duplicate legacy delivery", f)
					default:
					}
				})
			}
		}
	}
}
