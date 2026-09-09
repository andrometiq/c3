package broker

import (
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

func TestNegotiatedVoiceRevisionInvalidatesOneMember(t *testing.T) {
	// P4: "Voice enrichment immediately invalidates that row's membership in any open attempt".
	b, w, s, frames, ctx := negotiatedFixture(t)
	in := inboundOn(-100, nil, 1, "pending voice")
	id, err := b.Queue.AppendTracked(queueRouteKey(w.key), in, "voice-file")
	if err != nil {
		t.Fatal(err)
	}
	second := negotiatedAppend(t, w, 2, "text")
	w.scheduleAttempt(ctx, false)
	f := nextDeliver(t, frames)
	if a := w.liveAttempt(); len(a.Members) != 1 || a.Members[0].ID != second {
		t.Fatal("pending voice was live eligible")
	}
	// Construct an old placeholder membership to pin revision invalidation even
	// for adopted/in-flight rows created before enrichment scheduling changed.
	rows, _ := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
	w.updateAttempt(f.Token, func(a *attemptRecord) {
		a.Members = append(a.Members, attemptMember{ID: id, Revision: rowRevision(rows[0])})
	})
	resolved, _, err := b.Queue.ResolveVoiceText(queueRouteKey(w.key), id, "voice-file", "enriched")
	if err != nil || !resolved {
		t.Fatal(err)
	}
	w.reconcileAttempt()
	a := w.liveAttempt()
	if a == nil || len(a.Members) != 1 || a.Members[0].ID != second {
		t.Fatalf("enrichment invalidation: %+v", a)
	}
	w.handleAttemptResult(resultFor(s, f, "confirmed"))
	w.scheduleAttempt(ctx, false)
	next := nextDeliver(t, frames)
	if next.Inbound.Text != "enriched" {
		t.Fatalf("new revision not queued: %+v", next)
	}
}
func TestNegotiatedDrainSnapshotBeforeReservation(t *testing.T) {
	// G3: "including a drain snapshot taken before reservation".
	b, w, _, frames, ctx := negotiatedFixture(t)
	negotiatedAppend(t, w, 1, "draining")
	key := queueRouteKey(w.key).File()
	b.drains.tryAcquire(key)
	w.scheduleAttempt(ctx, false)
	if w.liveAttempt() != nil {
		t.Fatal("reserved before drain snapshot")
	}
	ch := make(chan DrainPeekResult, 1)
	w.handleDrainPeek(&DrainPeekJob{ResultCh: ch})
	if len((<-ch).Pending) != 1 {
		t.Fatal("missing drain snapshot")
	}
	b.drains.release(key)
	w.scheduleAttempt(ctx, false)
	nextDeliver(t, frames)
}
func TestNegotiatedEvictionClosesEmptyAttempt(t *testing.T) {
	// P4: "an attempt left empty closes, releases admission and worker liveness".
	b, w, s, frames, ctx := negotiatedFixture(t)
	in := inboundOn(-100, nil, 1, "expired retention")
	in.Timestamp = time.Now().Add(-15 * 24 * time.Hour)
	b.Queue.AppendTracked(queueRouteKey(w.key), in)
	w.scheduleAttempt(ctx, false)
	f := nextDeliver(t, frames)
	w.evictIfOverCap(queueRouteKey(w.key))
	if w.liveAttempt() != nil || s.delivery.slot != "" {
		t.Fatal("eviction kept empty attempt or slot")
	}
	w.handleAttemptResult(resultFor(s, f, "confirmed"))
	if s.delivery.proven {
		t.Fatal("empty eviction receipt proved transport")
	}
}
func TestNegotiatedOversizeNoticeRetiresAfterReceipt(t *testing.T) {
	// Model/P4: "oversize set-aside ... only after a confirmed attempt of the replacement notice".
	b, w, s, frames, ctx := negotiatedFixture(t)
	record := map[string]any{"Channel": "telegram", "ChatID": -100, "MessageID": 1, "Text": strings.Repeat("x", ipc.MaxFrameSize), "_c3_queue_id": "oversize-row"}
	raw, _ := json.Marshal(record)
	dir := filepath.Dir(b.Queue.RetentionDir())
	if err := os.WriteFile(filepath.Join(dir, queueRouteKey(w.key).File()+".jsonl"), append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	w.scheduleAttempt(ctx, false)
	f := nextDeliver(t, frames)
	if len(f.Inbound.Text) >= ipc.MaxFrameSize {
		t.Fatal("oversize original was pushed")
	}
	rows, _ := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if len(rows) != 1 {
		t.Fatal("set aside before receipt")
	}
	w.handleAttemptResult(resultFor(s, f, "confirmed"))
	rows, _ = b.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if len(rows) != 0 {
		t.Fatal("confirmed replacement did not retire original")
	}
}
func TestNegotiatedMixedLegacyPaths(t *testing.T) {
	// R7: "Non-negotiated sessions keep the old path untouched. Never two delivery owners for one session."
	b, w, s, frames, ctx := negotiatedFixture(t)
	key := MakeRouteKey("telegram", -200, nil)
	legacy, pushes := liveHolderFrames(t, b, key)
	legacy.AddRoute(key)
	legacy.MarkRouteConfirmed(key)
	lw := &RouteWorker{key: key, broker: b}
	in := inboundOn(-200, nil, 2, "legacy")
	id, err := b.Queue.AppendTracked(queueRouteKey(key), in)
	if err != nil {
		t.Fatal(err)
	}
	lw.forwardOrFallbackCovering(ctx, in, []*c3types.Inbound{in}, 1, []string{id}, true)
	push := waitInboundPush(t, pushes)
	negotiatedAppend(t, w, 1, "negotiated")
	w.scheduleAttempt(ctx, false)
	f := nextDeliver(t, frames)
	lw.handleConsume(ctx, &ConsumeJob{Owner: legacy, MessageID: 2, Token: push.DeliveryToken, Count: 1})
	w.handleAttemptResult(resultFor(s, f, "confirmed"))
	requireShadow(t, b, push.DeliveryToken, "confirmed", "live_ack")
	if len(lw.pendingAck) != 1 || len(w.pendingAck) != 0 {
		t.Fatal("recovery ledger cutover affected legacy")
	}
	if n, _ := b.Queue.Pending(queueRouteKey(key)); n != 0 {
		t.Fatal("legacy retirement failed")
	}
	if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 0 {
		t.Fatal("negotiated retirement failed")
	}
}
func TestNegotiatedFetchLeaseRefusal(t *testing.T) {
	// G2: "fetch: consume with ack:true, lease:true is refused without mutation".
	b, w, s, _, _ := negotiatedFixture(t)
	negotiatedAppend(t, w, 1, "held")
	for _, declared := range []string{"consume", "receipt"} {
		for _, lease := range []string{"true", "false", "null"} {
			s.delivery.offer.Fetch = declared
			a, c := net.Pipe()
			peer, conn := ipc.NewConn(a), ipc.NewConn(c)
			done := make(chan struct{})
			go func() {
				b.handleFetchQueue(conn, s, []byte(`{"op":"fetch_queue","id":"q","ack":true,"lease":`+lease+`}`))
				close(done)
			}()
			raw, err := peer.ReadFrame()
			if err != nil {
				t.Fatal(err)
			}
			a.Close()
			c.Close()
			<-done
			var response ipc.FetchQueueResp
			json.Unmarshal(raw, &response)
			if response.ID != "q" || response.Err == "" {
				t.Fatalf("refusal: %s", raw)
			}
			if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 1 {
				t.Fatal("lease refusal mutated")
			}
		}
	}
}
func TestNegotiatedHelloWireAndProtocolGate(t *testing.T) {
	// P1/P3: negotiation precedes the protocol refusal switch.
	for _, offer := range []string{channelOffer, `42`, `{"version":9}`, `{"live":null}`} {
		t.Run(offer, func(t *testing.T) {
			t.Setenv("C3_QUEUE_DIR", t.TempDir())
			b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
			t.Cleanup(b.Shutdown)
			peer, closePeer := peerPair(t, b)
			t.Cleanup(closePeer)
			if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: os.Getpid(), CWD: "/work", Delivery: json.RawMessage(offer), ProtocolVersion: 99}); err != nil {
				t.Fatal(err)
			}
			raw, err := peer.ReadFrame()
			if err != nil {
				t.Fatal(err)
			}
			var ack ipc.HelloAckMsg
			json.Unmarshal(raw, &ack)
			if ack.Op != ipc.OpHelloAck || (ack.Delivery != nil) != (offer == channelOffer) {
				t.Fatalf("acceptance=%s", raw)
			}
			if ack.Delivery != nil {
				peer.WriteJSON(ipc.AttemptResultMsg{Op: ipc.OpAttemptResult, Token: "unknown", Outcome: "confirmed"})
				raw, err = peer.ReadFrame()
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(raw), "compatibility window") {
					t.Fatalf("missing protocol refusal: %s", raw)
				}
			}
		})
	}
}

func TestNegotiatedSiblingConfirmationRearmsExhaustedRoute(t *testing.T) {
	b, w, s, frames, ctx := negotiatedFixture(t)
	negotiatedAppend(t, w, 1, "first")
	w.scheduleAttempt(ctx, false)
	f := nextDeliver(t, frames)
	w.handleAttemptResult(resultFor(s, f, "failed"))
	topic := int64(2)
	key := MakeRouteKey("telegram", -100, &topic)
	s.AddRoute(key)
	s.MarkRouteConfirmed(key)
	b.Routes.Claim(key, s)
	sibling := &RouteWorker{key: key, broker: b}
	negotiatedAppend(t, sibling, 2, "sibling")
	sibling.scheduleAttempt(ctx, false)
	other := nextDeliver(t, frames)
	sibling.handleAttemptResult(resultFor(s, other, "confirmed"))
	w.scheduleAttempt(ctx, false)
	nextDeliver(t, frames)
	if w.liveAttempt() == nil {
		t.Fatal("confirmed sibling did not rearm exhausted route")
	}
}
func TestNegotiatedDisplayHistoryAndFetchDoesNotProve(t *testing.T) {
	_, w, s, frames, ctx := negotiatedFixture(t)
	negotiatedAppend(t, w, 1, "first")
	w.scheduleAttempt(ctx, false)
	f := nextDeliver(t, frames)
	w.handleAttemptResult(resultFor(s, f, "confirmed"))
	prior := s.RenderRouteFor(w.key).Confirmed
	negotiatedAppend(t, w, 2, "second")
	w.scheduleAttempt(ctx, false)
	f = nextDeliver(t, frames)
	w.handleAttemptResult(resultFor(s, f, "failed"))
	route := s.RenderRouteFor(w.key)
	if route.Confirmed != prior || !strings.Contains(route.Text(), "was channel") {
		t.Fatal(route.Text())
	}
	ch := make(chan FetchResult, 1)
	w.handleFetch(ctx, &FetchJob{All: true, Ack: true, Owner: s, ResultCh: ch})
	<-ch
	if now := s.RenderRouteFor(w.key); now.State != route.State || now.Confirmed != prior {
		t.Fatal("fetch altered route proof")
	}
	raw, _ := json.Marshal(ipc.DeliveryReportMsg{Op: ipc.OpDeliveryReport, Live: ipc.DeliveryLive{Channel: ipc.DeliveryEligibility{Reason: "transcript unavailable"}}})
	w.broker.handleDeliveryReport(s, raw)
	if now := s.RenderRouteFor(w.key); now.State != "pull_only" || now.Reason != "transcript unavailable" {
		t.Fatal(now)
	}
}

func TestNegotiatedBackfillAllLegacyRowsPreservesFIFO(t *testing.T) {
	// P4/P6/G1: FIFO among live-eligible rows; drain provenance survives backfill,
	// and unchanged consume fetch can remove old rows by their assigned identities.
	b, w, s, frames, ctx := negotiatedFixture(t)
	var data []byte
	for i, text := range []string{"first", "second", "drained"} {
		in := inboundOn(-100, nil, int64(i+1), text)
		if i == 2 {
			in.DrainedFrom = "old-route"
		}
		raw, _ := json.Marshal(in)
		data = append(data, raw...)
		data = append(data, '\n')
	}
	path := filepath.Join(filepath.Dir(b.Queue.RetentionDir()), queueRouteKey(w.key).File()+".jsonl")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	negotiatedAppend(t, w, 4, "newest")
	w.scheduleAttempt(ctx, false)
	f := nextDeliver(t, frames)
	a := w.liveAttempt()
	if a == nil || len(a.Members) != 3 {
		t.Fatalf("backfill skipped live-eligible rows: %+v", a)
	}
	if strings.Index(f.Inbound.Text, "first") >= strings.Index(f.Inbound.Text, "second") || strings.Index(f.Inbound.Text, "second") >= strings.Index(f.Inbound.Text, "newest") {
		t.Fatal(f.Inbound.Text)
	}
	w.handleAttemptResult(resultFor(s, f, "confirmed"))
	ch := make(chan FetchResult, 1)
	w.handleFetch(ctx, &FetchJob{All: true, Ack: true, Owner: s, ResultCh: ch})
	r := <-ch
	if r.Err != nil || len(r.Messages) != 1 || r.Messages[0].Text != "drained" || r.Messages[0].ConsumedRecordID == "" || r.Remaining != 0 {
		t.Fatalf("drain legacy consume: %+v", r)
	}
}
func TestNegotiatedFetchBackfillsOnlyOnAdmittedConsume(t *testing.T) {
	b, w, s, _, ctx := negotiatedFixture(t)
	raw, _ := json.Marshal(inboundOn(-100, nil, 1, "old"))
	path := filepath.Join(filepath.Dir(b.Queue.RetentionDir()), queueRouteKey(w.key).File()+".jsonl")
	os.WriteFile(path, append(raw, '\n'), 0600)
	ch := make(chan FetchResult, 1)
	w.handleFetch(ctx, &FetchJob{All: true, Owner: s, ResultCh: ch})
	<-ch
	rows, _ := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if rows[0].RecordID != "" {
		t.Fatal("peek mutated identity")
	}
	lease := newFetchLease()
	lease.cancel()
	w.handleFetch(ctx, &FetchJob{All: true, Ack: true, Lease: lease, Owner: s, ResultCh: ch})
	<-ch
	rows, _ = b.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if rows[0].RecordID != "" {
		t.Fatal("cancelled consume mutated identity")
	}
	w.handleFetch(ctx, &FetchJob{All: true, Ack: true, Owner: s, ResultCh: ch})
	r := <-ch
	if r.Err != nil || len(r.Messages) != 1 || r.Messages[0].ConsumedRecordID == "" || r.Remaining != 0 {
		t.Fatalf("old consume: %+v", r)
	}
}
func TestNegotiatedBacklogSchedulesRecoveredRows(t *testing.T) {
	// P6: "Reconnect/backlog rows are scheduled automatically."
	_, w, _, frames, ctx := negotiatedFixture(t)
	negotiatedAppend(t, w, 1, "recovered")
	ch := make(chan BacklogResult, 1)
	w.handleBacklog(ctx, &BacklogJob{PeekN: 3, ResultCh: ch})
	r := <-ch
	nextDeliver(t, frames)
	if r.Total != 0 || len(r.Preview) != 0 {
		t.Fatal("backlog counted newly attempting rows")
	}
	if w.attemptTick != nil {
		w.attemptTick.Stop()
	}
}

func TestNegotiatedRetirementClearsEarlierLegacyRecovery(t *testing.T) {
	// R1/P4: "Recovery state is never discarded before the mutation succeeds";
	// transcript-confirmed removal must not be resurrected by a legacy ledger.
	for _, seam := range []string{"confirmed", "eviction"} {
		t.Run(seam, func(t *testing.T) {
			b, w, s, frames, ctx := negotiatedFixture(t)
			in := inboundOn(-100, nil, 1, "legacy recovery")
			if seam == "eviction" {
				in.Timestamp = time.Now().Add(-15 * 24 * time.Hour)
			}
			id, err := b.Queue.AppendTracked(queueRouteKey(w.key), in)
			if err != nil {
				t.Fatal(err)
			}
			old := &Stub{ConnID: 99}
			w.shadowPush("old-legacy-token", old, []string{id})
			w.trackPendingAck([]*c3types.Inbound{in}, id)
			w.scheduleAttempt(ctx, false)
			f := nextDeliver(t, frames)
			if seam == "confirmed" {
				w.handleAttemptResult(resultFor(s, f, "confirmed"))
			} else {
				w.evictIfOverCap(queueRouteKey(w.key))
			}
			if len(w.pendingAck) != 0 {
				t.Fatal("removed row retained legacy recovery payload")
			}
			if a := b.attempts.lookup("old-legacy-token", time.Now())[0]; len(a.Members) != 0 {
				t.Fatal("legacy shadow retained removed membership")
			}
			w.flushPendingAck("holder exited")
			if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 0 {
				t.Fatal("holder death resurrected a retired row")
			}
		})
	}
}
