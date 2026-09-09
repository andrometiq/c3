package broker

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

const channelOffer = `{"version":1,"live":{"channel":{"eligible":true},"inbox":{"eligible":false}},"receipts":"transcript","fetch":"receipt"}`

func negotiatedFixture(t *testing.T) (*Broker, *RouteWorker, *Stub, <-chan ipc.DeliverMsg, context.Context) {
	t.Helper()
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	b.Workers.Stop()
	t.Cleanup(b.Shutdown)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	key := MakeRouteKey("telegram", -100, nil)
	s, frames := negotiatedHolder(t, b, key, 1)
	w := &RouteWorker{key: key, broker: b}
	t.Cleanup(func() {
		if w.attemptTick != nil {
			w.attemptTick.Stop()
		}
	})
	return b, w, s, frames, ctx
}
func negotiatedHolder(t *testing.T, b *Broker, key RouteKey, id uint64) (*Stub, <-chan ipc.DeliverMsg) {
	t.Helper()
	a, c := net.Pipe()
	t.Cleanup(func() { a.Close(); c.Close() })
	s := &Stub{CLI: "claude", PID: os.Getpid(), CWD: "/work", ConnID: id, Conn: ipc.NewConn(c)}
	b.configureDelivery(s, []byte(channelOffer))
	s.deliveryReady.Store(true)
	b.Routes.Claim(key, s)
	s.AddRoute(key)
	s.MarkRouteConfirmed(key)
	frames := make(chan ipc.DeliverMsg, 20)
	go func() {
		conn := ipc.NewConn(a)
		for {
			raw, err := conn.ReadFrame()
			if err != nil {
				return
			}
			var f ipc.DeliverMsg
			if json.Unmarshal(raw, &f) == nil && f.Op == ipc.OpDeliver {
				frames <- f
			}
		}
	}()
	return s, frames
}
func negotiatedAppend(t *testing.T, w *RouteWorker, id int64, text string) string {
	t.Helper()
	in := inboundOn(w.key.ChatID, nil, id, text)
	in.Channel = w.key.Channel
	record, err := w.broker.Queue.AppendTracked(queueRouteKey(w.key), in)
	if err != nil {
		t.Fatal(err)
	}
	return record
}
func nextDeliver(t *testing.T, frames <-chan ipc.DeliverMsg) ipc.DeliverMsg {
	t.Helper()
	select {
	case f := <-frames:
		return f
	case <-time.After(2 * time.Second):
		t.Fatal("no deliver frame")
		return ipc.DeliverMsg{}
	}
}
func resultFor(s *Stub, f ipc.DeliverMsg, outcome string) *attemptResultJob {
	return &attemptResultJob{Owner: s, Msg: ipc.AttemptResultMsg{Op: ipc.OpAttemptResult, Token: f.Token, Outcome: outcome}}
}
func TestNegotiatedLifecycle(t *testing.T) {
	// P3/P4/P6: "One serialised terminal transition per attempt inside the route worker".
	for _, outcome := range []string{"confirmed", "failed", "deadline", "late", "removed"} {
		t.Run(outcome, func(t *testing.T) {
			b, w, s, frames, ctx := negotiatedFixture(t)
			id := negotiatedAppend(t, w, 1, "hello")
			w.scheduleAttempt(ctx, false)
			f := nextDeliver(t, frames)
			if f.Token == "" || f.DeadlineMS <= 0 || f.DeadlineMS > 15000 {
				t.Fatalf("frame: %+v", f)
			}
			if len(w.pendingAck) != 0 || len(w.coveredByPush) != 0 || len(s.pushOrder) != 0 {
				t.Fatal("negotiated push populated legacy retirement identities")
			}
			switch outcome {
			case "confirmed", "failed":
				w.handleAttemptResult(resultFor(s, f, outcome))
			case "deadline", "late":
				w.updateAttempt(f.Token, func(a *attemptRecord) { a.Deadline = time.Now().Add(-time.Second) })
				if outcome == "late" {
					w.handleAttemptResult(resultFor(s, f, "confirmed"))
				}
				w.tickAttempt(ctx)
			case "removed":
				b.Queue.RemoveRecordIDs(queueRouteKey(w.key), []string{id})
				w.reconcileAttempt()
				w.handleAttemptResult(resultFor(s, f, "confirmed"))
			}
			a := b.attempts.lookup(f.Token, time.Now())[0]
			n, _ := b.Queue.Pending(queueRouteKey(w.key))
			if outcome == "confirmed" {
				if n != 0 || a.Outcome != "confirmed" || len(a.Retired) != 1 || s.RenderRouteFor(w.key).State != "live_channel" {
					t.Fatalf("confirmed %+v n=%d", a, n)
				}
			} else if outcome == "removed" {
				if n != 0 || a.Outcome != "released" || s.delivery.proven {
					t.Fatalf("removed %+v", a)
				}
			} else {
				if n != 1 || s.RenderRouteFor(w.key).State != "pull_only" || a.Outcome == "open" {
					t.Fatalf("exhausted %+v n=%d", a, n)
				}
			}
			w.scheduleAttempt(ctx, false)
			if s.delivery.slot != "" {
				t.Fatal("terminal attempt retained admission")
			}
		})
	}
	t.Run("late_confirmed_still_open", func(t *testing.T) {
		// Follow-up B/P6: a late confirmation on a still-open record is a uniform no-op.
		b, w, s, frames, ctx := negotiatedFixture(t)
		id := negotiatedAppend(t, w, 1, "late")
		w.scheduleAttempt(ctx, false)
		f := nextDeliver(t, frames)
		deadline := time.Now().Add(-time.Second)
		w.updateAttempt(f.Token, func(a *attemptRecord) { a.Deadline = deadline })
		if a := w.liveAttempt(); a == nil || a.Outcome != "open" {
			t.Fatal("test requires a still-open record after its deadline")
		}
		logs := captureProtoLog(t)
		w.handleAttemptResult(resultFor(s, f, "confirmed"))
		a := b.attempts.lookup(f.Token, time.Now())[0]
		if a.Outcome != "open" || a.Evidence || len(a.Retired) != 0 || a.Deadline != deadline {
			t.Fatalf("late result changed the still-open attempt: %+v", a)
		}
		rows, err := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
		if err != nil || len(rows) != 1 || rows[0].RecordID != id {
			t.Fatalf("late result changed durable rows: %+v, %v", rows, err)
		}
		const refusal = "attempt result ignored: authority, deadline or open membership mismatch\n"
		if got := logs.String(); strings.Count(got, refusal) != 1 {
			t.Fatalf("want one uniform refusal line, got %q", got)
		}
	})

}
func TestNegotiatedAuthorityMisses(t *testing.T) {
	// P3: "A copied token on another connection consumes nothing even if that connection now holds the route."
	for _, miss := range []string{"other_connection", "unconfirmed", "generation", "claim_interrupted", "released", "unknown_token", "legacy_ack"} {
		t.Run(miss, func(t *testing.T) {
			b, w, s, frames, ctx := negotiatedFixture(t)
			negotiatedAppend(t, w, 1, "hello")
			w.scheduleAttempt(ctx, false)
			f := nextDeliver(t, frames)
			owner := s
			switch miss {
			case "other_connection":
				b.Routes.Release(w.key, s.ConnID)
				owner, _ = negotiatedHolder(t, b, w.key, 2)
			case "unconfirmed":
				s.stubMu.Lock()
				s.confirmed[w.key] = false
				s.stubMu.Unlock()
			case "generation":
				s.RemoveRoute(w.key)
				s.AddRoute(w.key)
				s.MarkRouteConfirmed(w.key)
			case "claim_interrupted":
				b.Routes.Release(w.key, s.ConnID)
				b.Routes.Claim(w.key, s)
			case "released":
				b.Routes.Release(w.key, s.ConnID)
			case "unknown_token":
				f.Token = "forged"
			case "legacy_ack":
				raw, _ := json.Marshal(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, OK: true, Count: 1, UpdateID: 1, DeliveryToken: f.Token})
				b.handleInboundDelivered(s, raw)
			}
			if miss != "legacy_ack" {
				w.handleAttemptResult(resultFor(owner, f, "confirmed"))
			}
			if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 1 {
				t.Fatal("authority miss consumed")
			}
			if a := w.liveAttempt(); a == nil || a.Evidence {
				t.Fatal("authority miss consumed token/evidence")
			}
		})
	}
}
func TestNegotiatedFetchBacklogHeldExclusion(t *testing.T) {
	// G1/P8: "rows inside a negotiated live attempt are invisible to fetch" and counts.
	b, w, s, frames, ctx := negotiatedFixture(t)
	negotiatedAppend(t, w, 1, "attempting")
	w.scheduleAttempt(ctx, false)
	nextDeliver(t, frames)
	negotiatedAppend(t, w, 2, "queued")
	for _, ack := range []bool{false, true} {
		ch := make(chan FetchResult, 1)
		w.handleFetch(ctx, &FetchJob{All: true, Ack: ack, Owner: s, ResultCh: ch})
		r := <-ch
		if r.Err != nil || len(r.Messages) != 1 || r.Messages[0].Text != "queued" || r.Remaining != 0 {
			t.Fatalf("fetch ack=%v: %+v", ack, r)
		}
	}
	ch := make(chan BacklogResult, 1)
	w.handleBacklog(ctx, &BacklogJob{PeekN: 3, ResultCh: ch})
	if r := <-ch; r.Total != 0 {
		t.Fatal(r)
	}
	if b.attemptingCount(w.key) != 1 {
		t.Fatal("attempt visibility lost")
	}
	w.evaluateAttemptHeld(s)
	if s.RenderRouteFor(w.key).Held != 0 {
		t.Fatal("Held includes attempt")
	}
}
func TestNegotiatedRearmEvents(t *testing.T) {
	// P6: "Exhaustion sets the 60 s clock; ordinary inbound before it neither retries nor resets it".
	for _, event := range []string{"inbound30", "inbound60", "attach", "reconnect", "confirmed_sibling", "report_same", "report_changed"} {
		t.Run(event, func(t *testing.T) {
			_, w, s, frames, ctx := negotiatedFixture(t)
			negotiatedAppend(t, w, 1, "hello")
			w.scheduleAttempt(ctx, false)
			f := nextDeliver(t, frames)
			w.handleAttemptResult(resultFor(s, f, "failed"))
			s.delivery.mu.Lock()
			stamp := time.Now().Add(-30 * time.Second)
			if event == "inbound60" {
				stamp = time.Now().Add(-61 * time.Second)
			}
			s.delivery.route(w.key).Exhausted = stamp
			s.delivery.mu.Unlock()
			switch event {
			case "attach", "reconnect", "confirmed_sibling":
				w.broker.rearmDelivery(s)
			case "report_same", "report_changed":
				live := s.delivery.live
				if event == "report_changed" {
					live.Inbox.Reason = "changed"
				}
				raw, _ := json.Marshal(ipc.DeliveryReportMsg{Op: ipc.OpDeliveryReport, Live: live})
				w.broker.handleDeliveryReport(s, raw)
			}
			w.scheduleAttempt(ctx, strings.HasPrefix(event, "inbound"))
			retry := event != "inbound30" && event != "report_same"
			if (w.liveAttempt() != nil) != retry {
				t.Fatalf("retry=%v state=%+v", retry, s.RenderRouteFor(w.key))
			}
			if !retry && s.delivery.route(w.key).Exhausted != stamp {
				t.Fatal("early event reset exhaustion clock")
			}
		})
	}
}
func TestNegotiatedAdmissionFIFO(t *testing.T) {
	// P6: "on release the slot goes to the longest-waiting route with queued live-eligible rows".
	b, w, s, frames, ctx := negotiatedFixture(t)
	siblings := []*RouteWorker{w}
	for _, id := range []int64{2, 3} {
		key := MakeRouteKey("telegram", -100, &id)
		s.AddRoute(key)
		s.MarkRouteConfirmed(key)
		b.Routes.Claim(key, s)
		siblings = append(siblings, &RouteWorker{key: key, broker: b})
	}
	for i, r := range siblings {
		negotiatedAppend(t, r, int64(i+1), "hello")
		r.scheduleAttempt(ctx, false)
	}
	f := nextDeliver(t, frames)
	if siblings[1].liveAttempt() != nil || siblings[2].liveAttempt() != nil {
		t.Fatal("multiple unproven attempts")
	}
	w.handleAttemptResult(resultFor(s, f, "failed"))
	siblings[2].scheduleAttempt(ctx, false)
	if siblings[2].liveAttempt() != nil {
		t.Fatal("newer route took slot")
	}
	siblings[1].scheduleAttempt(ctx, false)
	nextDeliver(t, frames)
	if siblings[1].liveAttempt() == nil {
		t.Fatal("oldest route did not take slot")
	}
}
func TestNegotiatedReconcileRevisionAndDrainOrigin(t *testing.T) {
	// P4/G3: "reconcile that member only; the attempt continues for the rest".
	b, w, s, frames, ctx := negotiatedFixture(t)
	first := negotiatedAppend(t, w, 1, "first")
	second := negotiatedAppend(t, w, 2, "second")
	w.scheduleAttempt(ctx, false)
	f := nextDeliver(t, frames)
	b.Queue.RemoveRecordIDs(queueRouteKey(w.key), []string{first})
	w.reconcileAttempt()
	if a := w.liveAttempt(); a == nil || len(a.Members) != 1 || a.Members[0].ID != second {
		t.Fatalf("partial reconcile: %+v", a)
	}
	w.handleAttemptResult(resultFor(s, f, "confirmed"))
	in := inboundOn(-100, nil, 3, "drained")
	b.Queue.AppendDrainedTracked(queueRouteKey(w.key), in, "")
	w.scheduleAttempt(ctx, false)
	if w.liveAttempt() != nil {
		t.Fatal("drain import live scheduled")
	}
	rows, _ := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if len(rows) != 1 || rows[0].Origin != "drain" {
		t.Fatal("missing durable provenance")
	}
}
func TestNegotiatedRetirementStorageFailure(t *testing.T) {
	// P4: "keep the confirmation evidence and retry removal a bounded number of times".
	b, w, s, frames, ctx := negotiatedFixture(t)
	negotiatedAppend(t, w, 1, "hello")
	w.scheduleAttempt(ctx, false)
	f := nextDeliver(t, frames)
	// Blocking the retention directory fails before the queue rewrite on every platform.
	dir := b.Queue.RetentionDir()
	os.RemoveAll(dir)
	if err := os.WriteFile(dir, []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	w.handleAttemptResult(resultFor(s, f, "confirmed"))
	for tries := 1; tries <= 3; tries++ {
		if tries > 1 {
			w.retireAttempt()
		}
		a := b.attempts.lookup(f.Token, time.Now())[0]
		wantOutcome := "open"
		if tries == 3 {
			wantOutcome = "failed"
		}
		if a.RemovalTries != tries || a.Outcome != wantOutcome || !a.Evidence || len(a.Retired) != 0 {
			t.Fatalf("removal try %d: want %s with evidence and no retirement, got %+v", tries, wantOutcome, a)
		}
		if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 1 {
			t.Fatal("failed retirement removed row")
		}
	}
	w.retireAttempt()
	if a := b.attempts.lookup(f.Token, time.Now())[0]; a.RemovalTries != 3 || a.Reason != "storage failure" {
		t.Fatalf("retirement retried after the three-try limit: %+v", a)
	}

}
func TestNegotiatedOfferAcceptance(t *testing.T) {
	// P1: "A degraded broker (Queue == nil) never accepts"; malformed offers stay legacy.
	b, _, _, _, _ := negotiatedFixture(t)
	for _, raw := range []string{`null`, `{}`, `{"version":2}`, strings.Replace(channelOffer, `"eligible":true`, `"eligible":null`, 1), strings.Replace(channelOffer, `"version":1`, `"version":1,"unknown":true`, 1)} {
		s := &Stub{}
		b.configureDelivery(s, []byte(raw))
		if s.negotiated() {
			t.Fatalf("accepted %s", raw)
		}
	}
	b.Queue = nil
	s := &Stub{}
	b.configureDelivery(s, []byte(channelOffer))
	if s.negotiated() {
		t.Fatal("degraded acceptance")
	}
}
func TestNegotiatedReconnectAdoption(t *testing.T) {
	// P6: "same living process ... adopts live attempts ... nothing is resent".
	for _, adopt := range []bool{true, false} {
		t.Run(map[bool]string{true: "adopt", false: "death_or_changed_offer"}[adopt], func(t *testing.T) {
			b, w, s, frames, ctx := negotiatedFixture(t)
			negotiatedAppend(t, w, 1, "hello")
			w.scheduleAttempt(ctx, false)
			f := nextDeliver(t, frames)
			if !adopt {
				b.Routes.Release(w.key, s.ConnID)
			}
			next, _ := negotiatedHolder(t, b, w.key, 2)
			if adopt {
				next.delivery = s.delivery
			}
			done := make(chan struct{})
			w.adoptAttempt(&attemptAdoptJob{Old: s, Next: next, Same: adopt, Done: done})
			<-done
			w.handleAttemptResult(resultFor(next, f, "confirmed"))
			n, _ := b.Queue.Pending(queueRouteKey(w.key))
			if (n == 0) != adopt {
				t.Fatalf("adoption=%v rows=%d", adopt, n)
			}
		})
	}
}
func TestNegotiatedIdentityBackfillMutation(t *testing.T) {
	b, w, _, frames, ctx := negotiatedFixture(t)
	raw, _ := json.Marshal(c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 1, Text: "old"})
	dir := filepath.Dir(b.Queue.RetentionDir())
	os.WriteFile(filepath.Join(dir, queueRouteKey(w.key).File()+".jsonl"), append(raw, '\n'), 0600)
	w.scheduleAttempt(ctx, false)
	nextDeliver(t, frames)
	if a := w.liveAttempt(); a == nil || a.Members[0].ID == "" {
		t.Fatal("old queued row not assigned an identity")
	}
}
