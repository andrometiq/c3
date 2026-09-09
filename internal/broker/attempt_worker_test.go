package broker

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

func TestNegotiatedWorkerDeadlineHeldOnceAndLiveness(t *testing.T) {
	// P6/P8: "Active attempts hold the route worker alive"; Held after termination excludes attempting rows.
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	t.Cleanup(b.Shutdown)
	b.Workers.idle = 20 * time.Millisecond
	b.HeldNotices = newFallbackTracker(time.Millisecond)
	key := MakeRouteKey("telegram", -100, nil)
	s, frames := negotiatedHolder(t, b, key, 1)
	in := inboundOn(-100, nil, 1, "deadline")
	if !b.Workers.Submit(key, Job{Kind: JobInbound, Inbound: in}) {
		t.Fatal("submit")
	}
	f := nextDeliver(t, frames)
	b.Workers.mu.Lock()
	w := b.Workers.workers[key]
	b.Workers.mu.Unlock()
	time.Sleep(60 * time.Millisecond)
	select {
	case <-w.Done():
		t.Fatal("worker exited with active attempt")
	default:
	}
	w.updateAttempt(f.Token, func(a *attemptRecord) { a.Deadline = time.Now().Add(-time.Second) })
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && s.RenderRouteFor(key).State != "pull_only" {
		time.Sleep(time.Millisecond)
	}
	if s.RenderRouteFor(key).State != "pull_only" {
		t.Fatal("worker did not expire attempt")
	}
	select {
	case <-w.Done():
	case <-time.After(time.Second):
		t.Fatal("terminal attempt kept worker alive")
	}
	fc.mu.Lock()
	held := 0
	for _, reply := range fc.replyCalls {
		if strings.Contains(reply.Text, "Held —") {
			held++
		}
	}
	fc.mu.Unlock()
	if held != 1 {
		t.Fatalf("Held notices=%d want 1", held)
	}
}
func TestNegotiatedAttemptResultDispatchedToWorker(t *testing.T) {
	// P3: "the broker's token record names the route ... dispatched to that route worker".
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	t.Cleanup(b.Shutdown)
	key := MakeRouteKey("telegram", -100, nil)
	s, frames := negotiatedHolder(t, b, key, 1)
	b.Workers.Submit(key, Job{Kind: JobInbound, Inbound: inboundOn(-100, nil, 1, "receipt")})
	f := nextDeliver(t, frames)
	raw, _ := json.Marshal(ipc.AttemptResultMsg{Op: ipc.OpAttemptResult, Token: f.Token, Outcome: "confirmed"})
	b.handleAttemptResult(s, raw)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		a := b.attempts.lookup(f.Token, time.Now())[0]
		if a.Outcome == "confirmed" {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("worker never retired confirmed attempt")
}
func TestNegotiatedProcessDeathReleasesImmediately(t *testing.T) {
	// P6: "Process death ... releases every attempt immediately".
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	t.Cleanup(b.Shutdown)
	key := MakeRouteKey("telegram", -100, nil)
	s, frames := negotiatedHolder(t, b, key, 1)
	b.Workers.Submit(key, Job{Kind: JobInbound, Inbound: inboundOn(-100, nil, 1, "death")})
	f := nextDeliver(t, frames)
	s.MarkDisconnected()
	b.releaseDelivery(s)
	a := b.attempts.lookup(f.Token, time.Now())[0]
	if a.Outcome != "released" || len(a.Retired) != 0 {
		t.Fatalf("death release: %+v", a)
	}
}
func TestNegotiatedReadbackDoesNotWaitForPushAdmission(t *testing.T) {
	// P6: "receipt processing never waits behind a push".
	b, w, s, frames, ctx := negotiatedFixture(t)
	negotiatedAppend(t, w, 1, "first")
	w.scheduleAttempt(ctx, false)
	f := nextDeliver(t, frames)
	// Writer completion stays queued; the route can process evidence directly
	// without waiting to receive that completion or obtain the IPC write lock.
	w.handleAttemptResult(resultFor(s, f, "confirmed"))
	if b.attempts.lookup(f.Token, time.Now())[0].Outcome != "confirmed" {
		t.Fatal("receipt depended on writer completion")
	}
}

func nextWireOp(t *testing.T, peer *ipc.Conn, want ipc.Op) []byte {
	t.Helper()
	for {
		raw, err := peer.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		op, _ := ipc.PeekOp(raw)
		if op == want {
			return raw
		}
	}
}
func TestNegotiatedSocketReconnectAdoptsWithoutResend(t *testing.T) {
	// P1/P6: "same living process ... uninterrupted claim ... transferring the claim generation".
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "adoption", true: "changed_offer_releases"}[changed], func(t *testing.T) {
			t.Setenv("C3_QUEUE_DIR", t.TempDir())
			b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
			t.Cleanup(b.Shutdown)
			first, closeFirst := peerPair(t, b)
			t.Cleanup(closeFirst)
			hello := ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: os.Getpid(), CWD: "/work", Delivery: json.RawMessage(channelOffer)}
			first.WriteJSON(hello)
			raw := nextWireOp(t, first, ipc.OpHelloAck)
			var ack ipc.HelloAckMsg
			json.Unmarshal(raw, &ack)
			old, _ := b.Stubs.Get(ack.ConnID)
			key := MakeRouteKey("telegram", -100, nil)
			b.Routes.Claim(key, old)
			old.AddRoute(key)
			old.MarkRouteConfirmed(key)
			b.Workers.Submit(key, Job{Kind: JobInbound, Inbound: inboundOn(-100, nil, 1, "reconnect")})
			raw = nextWireOp(t, first, ipc.OpDeliver)
			var f ipc.DeliverMsg
			json.Unmarshal(raw, &f)
			closeFirst()
			if changed {
				hello.Delivery = json.RawMessage(strings.Replace(channelOffer, `"fetch":"receipt"`, `"fetch":"consume"`, 1))
			}
			second, closeSecond := peerPair(t, b)
			t.Cleanup(closeSecond)
			second.WriteJSON(hello)
			raw = nextWireOp(t, second, ipc.OpHelloAck)
			json.Unmarshal(raw, &ack)
			a := b.attempts.lookup(f.Token, time.Now())[0]
			if changed {
				if a.Outcome != "released" {
					t.Fatalf("changed offer retained attempt: %+v", a)
				}
				return
			}
			if !a.Adopted || a.Holder.ConnID != ack.ConnID || a.Token != f.Token {
				t.Fatalf("adoption: %+v", a)
			}
			second.WriteJSON(ipc.AttemptResultMsg{Op: ipc.OpAttemptResult, Token: f.Token, Outcome: "confirmed"})
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				if b.attempts.lookup(f.Token, time.Now())[0].Outcome == "confirmed" {
					return
				}
				time.Sleep(time.Millisecond)
			}
			t.Fatal("adopted receipt not retired")
		})
	}
}
