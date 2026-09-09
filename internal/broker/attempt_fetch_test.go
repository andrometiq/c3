package broker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

func clearFetchTestEnvironment(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "C3_") || strings.HasPrefix(key, "CLAUDE_CODE_") {
			t.Setenv(key, "")
		}
	}
	for _, key := range []string{"C3_QUEUE_DIR", "XDG_CONFIG_HOME", "XDG_STATE_HOME"} {
		t.Setenv(key, t.TempDir())
	}
}

func receiptHolder(s *Stub) {
	s.deliveryModes.Store(&ipc.DeliveryAcceptance{Version: 1, Modes: []string{"fetch_receipt"}})
	s.delivery.mu.Lock()
	s.delivery.live = ipc.DeliveryLive{Channel: ipc.DeliveryEligibility{Reason: "no channel"}, Inbox: ipc.DeliveryEligibility{Reason: "no inbox"}}
	s.delivery.mu.Unlock()
}

func reserveFetch(t *testing.T, w *RouteWorker, s *Stub, group *attemptFetchGroup, limit int) FetchResult {
	t.Helper()
	ch := make(chan FetchResult, 1)
	w.handleFetch(context.Background(), &FetchJob{Owner: s, Ack: true, Limit: limit, All: limit < 0, RespID: "q", ReceiptGroup: group, Lease: newFetchLease(), ResultCh: ch})
	r := <-ch
	if r.Err != nil || r.SkipReason != "" {
		t.Fatalf("reservation: %+v", r)
	}
	return r
}

func fetchResult(s *Stub, token string) *attemptResultJob {
	return &attemptResultJob{Owner: s, Msg: ipc.AttemptResultMsg{Op: ipc.OpAttemptResult, Token: token, Outcome: "confirmed"}}
}

func TestFetchGroupLifecycleIndependentRoutes(t *testing.T) {
	clearFetchTestEnvironment(t)
	for _, change := range []string{"none", "holder", "generation"} {
		t.Run(change, func(t *testing.T) {
			b, w, s, _, ctx := negotiatedFixture(t)
			receiptHolder(s)
			topic := int64(2)
			key := MakeRouteKey("telegram", -100, &topic)
			s.AddRoute(key)
			s.MarkRouteConfirmed(key)
			b.Routes.Claim(key, s)
			other := &RouteWorker{key: key, broker: b}
			t.Cleanup(func() {
				if other.attemptTick != nil {
					other.attemptTick.Stop()
				}
				if other.attemptWriteCancel != nil {
					other.attemptWriteCancel()
				}
			})
			id := negotiatedAppend(t, w, 1, "first")
			otherID := negotiatedAppend(t, other, 2, "second")
			group := &attemptFetchGroup{token: b.mintDeliveryToken()}
			for _, worker := range []*RouteWorker{w, other} {
				r := reserveFetch(t, worker, s, group, -1)
				if len(r.Members) != 1 || len(r.Messages) != 1 || r.Remaining != 0 {
					t.Fatal(r)
				}
				if worker.liveAttempt() != nil || s.delivery.slot != "" {
					t.Fatal("fetch took live admission")
				}
				if a := worker.attempt(group.token); a == nil || a.Transport != "fetch" || a.Deadline.Sub(a.Started) != 60*time.Second {
					t.Fatal(a)
				}
				if rows, _ := worker.visibleAttemptRows(-1); len(rows) != 0 {
					t.Fatal("reservation remains fetchable")
				}
			}
			before := s.RenderRouteFor(w.key)
			if change == "holder" {
				b.Routes.Release(key, s.ConnID)
				negotiatedHolder(t, b, key, 2)
			}
			if change == "generation" {
				b.Routes.Release(key, s.ConnID)
				b.Routes.Claim(key, s)
			}
			w.handleAttemptResult(fetchResult(s, group.token))
			// The first nonempty route rearms the group. A later route must not
			// rearm exhaustion that happened after that accepted confirmation.
			paused := MakeRouteKey("fixture", 9, nil)
			stamp := time.Now()
			s.delivery.mu.Lock()
			s.delivery.route(paused).Exhausted = stamp
			s.delivery.mu.Unlock()
			other.handleAttemptResult(fetchResult(s, group.token))
			s.delivery.mu.Lock()
			stillPaused := s.delivery.route(paused).Exhausted == stamp
			s.delivery.mu.Unlock()
			if !stillPaused {
				t.Fatal("group rearmed more than once")
			}
			other.tickAttempt(ctx)
			if got := b.attempts.lookup(group.token, time.Now()); len(got) != 2 || !slices.Equal(got[0].Retired, []string{id}) {
				t.Fatal(got)
			}
			if got := s.RenderRouteFor(w.key); got.State != before.State || got.Confirmed != before.Confirmed || s.delivery.proven {
				t.Fatal("fetch changed live proof", got)
			}
			rows, _ := b.Queue.PeekTracked(queueRouteKey(key), -1)
			if change == "none" {
				if len(rows) != 0 {
					t.Fatal(rows)
				}
			} else if len(rows) != 1 || rows[0].RecordID != otherID {
				t.Fatal("changed holder consumed", rows)
			}
			// A copied token is correlation only, even on the current holder.
			current, _ := b.Routes.Holder(key)
			other.handleAttemptResult(fetchResult(current, group.token))
			w.handleAttemptResult(fetchResult(s, group.token))
			if !group.rearmed.Load() {
				t.Fatal("nonempty group did not rearm")
			}
		})
	}
}

func TestFetchGroupRevisionEvictionDrainAndExpiry(t *testing.T) {
	clearFetchTestEnvironment(t)
	for _, event := range []string{"voice", "eviction", "drain", "expiry", "failed", "oversize"} {
		t.Run(event, func(t *testing.T) {
			b, w, s, _, ctx := negotiatedFixture(t)
			receiptHolder(s)
			in := inboundOn(-100, nil, 1, "pending voice")
			if event == "eviction" {
				in.Timestamp = time.Now().Add(-queue.MaxAge - time.Hour)
			}
			id, err := b.Queue.AppendTracked(queueRouteKey(w.key), in, "voice-file")
			if err != nil {
				t.Fatal(err)
			}
			if event == "oversize" {
				raw, _ := json.Marshal(map[string]any{"Channel": "telegram", "ChatID": -100, "MessageID": 1, "Text": strings.Repeat("x", ipc.MaxFrameSize), "_c3_queue_id": id})
				path := filepath.Join(filepath.Dir(b.Queue.RetentionDir()), queueRouteKey(w.key).File()+".jsonl")
				if err := os.WriteFile(path, append(raw, '\n'), 0600); err != nil {
					t.Fatal(err)
				}
			}
			second := negotiatedAppend(t, w, 2, "survivor")
			group := &attemptFetchGroup{token: b.mintDeliveryToken()}
			r := reserveFetch(t, w, s, group, -1)
			if len(r.Members) != 2 || r.Members[0].RecordID != id {
				t.Fatal(r)
			}
			if event == "oversize" && (!strings.Contains(r.Messages[0].Text, "still queued") || len(r.Messages[0].Text) > 2000) {
				t.Fatal("oversize replacement missing")
			}
			// New queued arrivals remain independently fetchable and live-eligible.
			queued := negotiatedAppend(t, w, 3, "queued")
			peek := make(chan DrainPeekResult, 1)
			w.handleDrainPeek(&DrainPeekJob{ResultCh: peek})
			if snap := <-peek; len(snap.Tracked) != 1 || snap.Tracked[0].RecordID != queued {
				t.Fatal("drain included attempt", snap)
			}
			switch event {
			case "voice":
				b.Queue.ResolveVoiceText(queueRouteKey(w.key), id, "voice-file", "enriched")
				w.reconcileAttempt()
			case "eviction":
				w.evictIfOverCap(queueRouteKey(w.key))
			case "drain":
				ch := make(chan DrainRemoveResult, 1)
				w.handleDrainRemove(&DrainRemoveJob{RecordIDs: []string{id}, ResultCh: ch})
				<-ch
			case "expiry":
				w.updateAttempt(group.token, func(a *attemptRecord) { a.Deadline = time.Now().Add(-time.Second) })
				w.handleAttemptResult(fetchResult(s, group.token))
				w.tickAttempt(ctx)
			case "failed":
				job := fetchResult(s, group.token)
				job.Msg.Outcome = "failed"
				w.handleAttemptResult(job)
			}
			if event != "expiry" && event != "failed" {
				w.handleAttemptResult(fetchResult(s, group.token))
			}
			rows, _ := w.visibleAttemptRows(-1)
			want := 1
			if event == "voice" {
				want = 2
			}
			if event == "expiry" || event == "failed" {
				want = 3
			}
			if len(rows) != want {
				t.Fatalf("rows=%+v want=%d", rows, want)
			}
			if event == "voice" && rows[0].Inbound.Text != "enriched" {
				t.Fatal("obsolete voice receipt consumed enriched row")
			}
			a := b.attempts.lookup(group.token, time.Now())[0]
			if event == "voice" || event == "eviction" || event == "drain" {
				if !slices.Equal(a.Retired, []string{second}) {
					t.Fatal(a)
				}
			}
			if event == "expiry" || event == "failed" {
				next := &attemptFetchGroup{token: b.mintDeliveryToken()}
				r = reserveFetch(t, w, s, next, -1)
				if len(r.Messages) != 3 {
					t.Fatal("released rows not visible exactly once", r)
				}
				if r = reserveFetch(t, w, s, &attemptFetchGroup{token: b.mintDeliveryToken()}, -1); len(r.Messages) != 0 {
					t.Fatal("duplicate reservation", r)
				}
			}
		})
	}
}

func TestFetchPeekIdentityAndReservationAdmission(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, w, s, _, ctx := negotiatedFixture(t)
	receiptHolder(s)
	path := filepath.Join(filepath.Dir(b.Queue.RetentionDir()), queueRouteKey(w.key).File()+".jsonl")
	raw, _ := json.Marshal(inboundOn(-100, nil, 1, "legacy row"))
	raw = append(raw, '\n')
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	for _, ack := range []bool{false, true} {
		ch := make(chan FetchResult, 1)
		w.handleFetch(ctx, &FetchJob{Owner: s, All: true, Ack: ack, ResultCh: ch})
		r := <-ch
		if ack && r.Err == nil {
			t.Fatal("ack without lease not refused")
		}
		after, _ := os.ReadFile(path)
		if !slices.Equal(raw, after) || len(w.openAttempts()) != 0 {
			t.Fatal("peek/refusal mutated queue")
		}
	}
	group := &attemptFetchGroup{token: b.mintDeliveryToken()}
	b.drains.tryAcquire(queueRouteKey(w.key).File())
	ch := make(chan FetchResult, 1)
	w.handleFetch(ctx, &FetchJob{Owner: s, Ack: true, All: true, ReceiptGroup: group, ResultCh: ch})
	if r := <-ch; r.Err == nil {
		t.Fatal("reserved while drain active")
	}
	after, _ := os.ReadFile(path)
	if !slices.Equal(raw, after) {
		t.Fatal("drain barrier backfilled identity")
	}
	b.drains.release(queueRouteKey(w.key).File())
	r := reserveFetch(t, w, s, group, -1)
	rows, _ := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if len(rows) != 1 || rows[0].RecordID == "" || r.Members[0].RecordID != rows[0].RecordID {
		t.Fatal("identity not durable")
	}
}

func TestFetchReceiptMultiRouteIPCConfirm(t *testing.T) {
	clearFetchTestEnvironment(t)
	for _, alias := range []bool{false, true} {
		t.Run(map[bool]string{false: "attempt_result", true: "fetch_confirm"}[alias], func(t *testing.T) {
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
			for _, key := range []RouteKey{key, other} {
				if err := b.Queue.Append(queueRouteKey(key), inboundOn(-100, nil, 1, "held")); err != nil {
					t.Fatal(err)
				}
			}
			r := b.fetchSelectedRoutes(s, ipc.FetchQueueReq{ID: "q", Ack: true, All: true, Lease: json.RawMessage("true")}, []RouteKey{key, other})
			if r.Err != "" || len(r.Messages) != 2 || len(r.Members) != 2 || r.LeaseToken == "" || r.ReceiptTrailer != ipc.FetchReceiptTrailer(r.LeaseToken, r.Members) {
				t.Fatal(r)
			}
			var raw []byte
			if alias {
				raw, _ = json.Marshal(ipc.FetchConfirmReq{Op: ipc.OpFetchConfirm, LeaseToken: r.LeaseToken})
				b.handleFetchConfirm(s, raw)
			} else {
				raw, _ = json.Marshal(ipc.AttemptResultMsg{Op: ipc.OpAttemptResult, Token: r.LeaseToken, Outcome: "confirmed"})
				b.handleAttemptResult(s, raw)
			}
			waitForVoiceCondition(t, "group confirmation", func() bool {
				return b.Queue.StatusFor(queueRouteKey(key)).Pending+b.Queue.StatusFor(queueRouteKey(other)).Pending == 0
			})
			for _, a := range b.attempts.lookup(r.LeaseToken, time.Now()) {
				if a.Outcome != "confirmed" || len(a.Retired) != 1 {
					t.Fatal(a)
				}
			}
		})
	}
}

func TestNegotiationRequiresAdapterConfirmation(t *testing.T) {
	clearFetchTestEnvironment(t)
	for _, accepted := range []string{"channel", "never", "fetch_receipt"} {
		t.Run(accepted, func(t *testing.T) {
			b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
			t.Cleanup(b.Shutdown)
			peer, closePeer := peerPair(t, b)
			t.Cleanup(closePeer)
			offer := strings.Replace(channelOffer, `"fetch":"receipt"`, `"fetch":"consume"`, 1)
			offer = strings.Replace(offer, `"inbox":{"eligible":false}`, `"inbox":{"eligible":true}`, 1)
			peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "fixture", PID: os.Getpid(), CWD: "/work", Delivery: json.RawMessage(offer)})
			var ack ipc.HelloAckMsg
			json.Unmarshal(nextWireOp(t, peer, ipc.OpHelloAck), &ack)
			if !slices.Equal(ack.Delivery.Modes, []string{"channel", "inbox"}) {
				t.Fatal(ack)
			}
			s, _ := b.Stubs.Get(ack.ConnID)
			if s.negotiated() {
				t.Fatal("hello_ack activated negotiation")
			}
			if accepted != "never" {
				peer.WriteJSON(map[string]any{"op": "delivery_report", "accepted": []string{accepted}})
				waitForVoiceCondition(t, "report", func() bool { return s.negotiated() || s.deliveryRefused.Load() })
			}
			key := MakeRouteKey("telegram", -100, nil)
			b.Routes.Claim(key, s)
			s.AddRoute(key)
			s.MarkRouteConfirmed(key)
			b.Workers.Submit(key, Job{Kind: JobInbound, Inbound: inboundOn(-100, nil, 1, "hello")})
			if accepted == "channel" {
				raw := nextWireOp(t, peer, ipc.OpDeliver)
				var f ipc.DeliverMsg
				json.Unmarshal(raw, &f)
				if f.Transport != "channel" {
					t.Fatal(f)
				}
				peer.WriteJSON(ipc.AttemptResultMsg{Op: ipc.OpAttemptResult, Token: f.Token, Outcome: "failed"})
				waitForVoiceCondition(t, "channel exhausted", func() bool { return s.RenderRouteFor(key).State == "pull_only" })
				for _, a := range b.attempts.snapshot(time.Now()) {
					if a.Transport == "inbox" {
						t.Fatal("unconfirmed inbox scheduled")
					}
				}
			} else {
				nextWireOp(t, peer, ipc.OpInbound)
				for _, a := range b.attempts.snapshot(time.Now()) {
					if a.Negotiated {
						t.Fatal("unconfirmed adapter minted attempt")
					}
				}
			}
		})
	}
}

func TestFetchMixedLegacyConsumeReceiptWire(t *testing.T) {
	clearFetchTestEnvironment(t)
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	t.Cleanup(b.Shutdown)
	for i, kind := range []string{"legacy", "consume", "receipt", "pull_consume"} {
		peer, closePeer := peerPair(t, b)
		t.Cleanup(closePeer)
		hello := ipc.HelloMsg{Op: ipc.OpHello, CLI: kind, PID: os.Getpid(), CWD: "/work", CannotRenderChannels: true}
		if kind != "legacy" {
			hello.Delivery = json.RawMessage(strings.Replace(strings.Replace(channelOffer, `"fetch":"receipt"`, `"fetch":"`+kind+`"`, 1), `"channel":{"eligible":true}`, `"channel":{"eligible":false}`, 1))
		}
		if kind == "pull_consume" {
			hello.Delivery = json.RawMessage(`{"version":1,"live":{"channel":{"eligible":false},"inbox":{"eligible":false}},"receipts":"none","fetch":"consume"}`)
		}
		peer.WriteJSON(hello)
		var ack ipc.HelloAckMsg
		json.Unmarshal(nextWireOp(t, peer, ipc.OpHelloAck), &ack)
		s, _ := b.Stubs.Get(ack.ConnID)
		if kind != "legacy" {
			modes := []string{"channel", "inbox"}
			if kind == "pull_consume" {
				modes = []string{}
			}
			if kind == "receipt" {
				modes = append(modes, "fetch_receipt")
			}
			peer.WriteJSON(map[string]any{"op": "delivery_report", "accepted": modes})
			waitForVoiceCondition(t, "confirmation", s.negotiated)
		}
		topic := int64(i + 20)
		key := MakeRouteKey("telegram", -100, &topic)
		b.Routes.Claim(key, s)
		s.AddRoute(key)
		s.MarkRouteConfirmed(key)
		b.Queue.Append(queueRouteKey(key), inboundOn(-100, &topic, 1, "held"))
		request := ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "q", All: true, Ack: true}
		if kind == "receipt" {
			peer.WriteJSON(request)
			var refused ipc.FetchQueueResp
			json.Unmarshal(nextWireOp(t, peer, ipc.OpFetchQueueResult), &refused)
			if refused.Err == "" || b.Queue.StatusFor(queueRouteKey(key)).Pending != 1 {
				t.Fatal("receipt consume not refused", refused)
			}
			request.Lease = json.RawMessage("true")
		}
		peer.WriteJSON(request)
		raw := nextWireOp(t, peer, ipc.OpFetchQueueResult)
		var r ipc.FetchQueueResp
		json.Unmarshal(raw, &r)
		if r.Err != "" || len(r.Messages) != 1 {
			t.Fatal(string(raw))
		}
		if kind == "receipt" {
			if r.LeaseToken == "" || b.Queue.StatusFor(queueRouteKey(key)).Pending != 1 {
				t.Fatal("receipt consumed on return")
			}
			peer.WriteJSON(ipc.FetchConfirmReq{Op: ipc.OpFetchConfirm, LeaseToken: r.LeaseToken})
			waitForVoiceCondition(t, "wire alias", func() bool { return b.Queue.StatusFor(queueRouteKey(key)).Pending == 0 })
		} else if r.LeaseToken != "" || len(r.Members) != 0 || r.ReceiptTrailer != "" || b.Queue.StatusFor(queueRouteKey(key)).Pending != 0 {
			t.Fatal("baseline fetch changed", r)
		}
		if kind != "receipt" {
			peer.WriteJSON(ipc.FetchConfirmReq{Op: ipc.OpFetchConfirm, LeaseToken: "unknown"})
			raw := nextWireOp(t, peer, ipc.OpError)
			if string(raw) != `{"op":"error","err":"op not implemented yet: fetch_confirm"}` {
				t.Fatal(string(raw))
			}
		}

	}
}

func TestFetchGroupsExcludeLiveAndHeld(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, w, s, frames, ctx := negotiatedFixture(t)
	receiptHolder(s)
	negotiatedAppend(t, w, 1, "fetching")
	r := reserveFetch(t, w, s, &attemptFetchGroup{token: b.mintDeliveryToken()}, -1)
	negotiatedAppend(t, w, 2, "live next")
	s.deliveryModes.Store(&ipc.DeliveryAcceptance{Version: 1, Modes: []string{"channel", "fetch_receipt"}})
	s.delivery.mu.Lock()
	s.delivery.live.Channel = ipc.DeliveryEligibility{Eligible: true}
	s.delivery.mu.Unlock()
	w.scheduleAttempt(ctx, false)
	f := nextAttemptDelivery(t, w, frames)
	if f.Inbound.Text != "live next" {
		t.Fatal("live resent fetch member", f)
	}
	if n, err := b.noticePending(w.key, s); err != nil || n != 0 {
		t.Fatal("Held counted attempts", n, err)
	}
	w.handleAttemptResult(fetchResult(s, r.Members[0].RecordID)) // a record id is not a token
	if b.Queue.StatusFor(queueRouteKey(w.key)).Pending != 2 {
		t.Fatal("copied identity consumed")
	}
}

func TestFetchGroupReconnectAndEmptyRetirement(t *testing.T) {
	clearFetchTestEnvironment(t)
	for _, event := range []string{"reconnect", "empty", "storage", "read_failure"} {
		t.Run(event, func(t *testing.T) {
			b, w, s, _, _ := negotiatedFixture(t)
			receiptHolder(s)
			id := negotiatedAppend(t, w, 1, "held")
			group := &attemptFetchGroup{token: b.mintDeliveryToken()}
			reserveFetch(t, w, s, group, -1)
			switch event {
			case "reconnect":
				next, _ := negotiatedHolder(t, b, w.key, 2)
				done := make(chan struct{})
				w.adoptAttempt(&attemptAdoptJob{Old: s, Next: next, Same: true, Done: done})
				<-done
			case "empty":
				b.Queue.RemoveRecordIDs(queueRouteKey(w.key), []string{id})
				w.reconcileAttempt()
			case "read_failure":
				path := filepath.Join(filepath.Dir(b.Queue.RetentionDir()), queueRouteKey(w.key).File()+".jsonl")
				if err := os.Rename(path, path+".saved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "storage":
				dir := b.Queue.RetentionDir()
				os.RemoveAll(dir)
				os.WriteFile(dir, []byte("blocked"), 0600)
			}
			w.handleAttemptResult(fetchResult(s, group.token))
			if event == "storage" || event == "read_failure" {
				w.retireAttemptToken(group.token)
				w.retireAttemptToken(group.token)
			}
			a := b.attempts.lookup(group.token, time.Now())[0]
			if a.Outcome == "open" || a.Outcome == "confirmed" || group.rearmed.Load() || len(a.Retired) != 0 {
				t.Fatal(a)
			}
			if event != "empty" && b.Queue.StatusFor(queueRouteKey(w.key)).Pending != 1 {
				t.Fatal("failed confirmation lost row")
			}
		})
	}
}

func TestFetchReceiptFrameCapIncludesTrailerAndMultipleRoutes(t *testing.T) {
	clearFetchTestEnvironment(t)
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	t.Cleanup(b.Shutdown)
	key := MakeRouteKey("telegram", -100, nil)
	s, _ := negotiatedHolder(t, b, key, 1)
	receiptHolder(s)
	var routes []RouteKey
	for i := int64(0); i < 3; i++ {
		key := MakeRouteKey("telegram", -100, &i)
		s.AddRoute(key)
		s.MarkRouteConfirmed(key)
		b.Routes.Claim(key, s)
		routes = append(routes, key)
		for id := int64(1); id <= 2; id++ {
			b.Queue.Append(queueRouteKey(key), inboundOn(-100, &i, id, strings.Repeat("x", 900000)))
		}
	}
	r := b.fetchSelectedRoutes(s, ipc.FetchQueueReq{ID: strings.Repeat("q", 1024), All: true, Ack: true, Lease: json.RawMessage("true")}, routes)
	raw, err := json.Marshal(r)
	if err != nil || len(raw)+1 > ipc.MaxFrameSize || len(r.Messages) == 0 || len(r.Messages) >= 6 || r.Remaining != 6-len(r.Messages) || len(r.Members) != len(r.Messages) {
		t.Fatalf("bytes=%d messages=%d remaining=%d err=%v", len(raw), len(r.Messages), r.Remaining, err)
	}
	count := 0
	for _, a := range b.attempts.lookup(r.LeaseToken, time.Now()) {
		count += len(a.Members)
	}
	if count != len(r.Messages) {
		t.Fatal("reserved outside returned frame", count)
	}
}

func TestFetchAttemptKeepsWorkerAliveUntilExpiry(t *testing.T) {
	clearFetchTestEnvironment(t)
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	t.Cleanup(b.Shutdown)
	b.Workers.idle = 10 * time.Millisecond
	key := MakeRouteKey("telegram", -100, nil)
	s, _ := negotiatedHolder(t, b, key, 1)
	receiptHolder(s)
	b.Queue.Append(queueRouteKey(key), inboundOn(-100, nil, 1, "held"))
	r := b.fetchSelectedRoutes(s, ipc.FetchQueueReq{ID: "q", Ack: true, All: true, Lease: json.RawMessage("true")}, []RouteKey{key})
	if r.Err != "" || r.LeaseToken == "" {
		t.Fatal(r)
	}
	b.Workers.mu.Lock()
	w := b.Workers.workers[key]
	b.Workers.mu.Unlock()
	select {
	case <-w.Done():
		t.Fatal("worker exited with fetch attempt")
	case <-time.After(50 * time.Millisecond):
	}
	w.updateAttempt(r.LeaseToken, func(a *attemptRecord) { a.Deadline = time.Now().Add(-time.Second) })
	waitForVoiceCondition(t, "fetch expiry", func() bool { return b.attempts.lookup(r.LeaseToken, time.Now())[0].Outcome == "expired" })
	select {
	case <-w.Done():
	case <-time.After(time.Second):
		t.Fatal("expired fetch retained worker")
	}
	if b.Queue.StatusFor(queueRouteKey(key)).Pending != 1 {
		t.Fatal("expiry consumed row")
	}
}
