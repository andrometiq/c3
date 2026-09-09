package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

type noticeReplies interface{ sendRepliesSnapshot() []c3types.ReplyArgs }

func waitNoticeReplies(t *testing.T, fc noticeReplies, n int) []c3types.ReplyArgs {
	t.Helper()
	waitForVoiceCondition(t, "notice send", func() bool { return len(fc.sendRepliesSnapshot()) >= n })
	return fc.sendRepliesSnapshot()
}
func exceptionalReplies(replies []c3types.ReplyArgs) []c3types.ReplyArgs {
	var out []c3types.ReplyArgs
	for _, r := range replies {
		if !strings.Contains(r.Text, "Live route:") {
			out = append(out, r)
		}
	}
	return out
}
func noticeFixture(t *testing.T, negotiated bool) (*Broker, *RouteWorker, *Stub, *fakeChannel) {
	t.Helper()
	clearFetchTestEnvironment(t)
	b, w, s, _, _ := negotiatedFixture(t)
	fc, _ := b.Channel("telegram")
	if !negotiated {
		s.deliveryReady.Store(false)
		s.SetRenderRoute(ipc.RenderQueueOnly, "host unavailable", true)
	} else {
		s.delivery.mu.Lock()
		s.delivery.live = ipc.DeliveryLive{Channel: ipc.DeliveryEligibility{Reason: "host unavailable"}}
		s.delivery.mu.Unlock()
	}
	return b, w, s, fc.(*fakeChannel)
}
func settleNotice(t *testing.T, b *Broker, key RouteKey) {
	t.Helper()
	waitForVoiceCondition(t, "notice loop idle", func() bool {
		b.notices.mu.Lock()
		defer b.notices.mu.Unlock()
		return b.notices.routes[key] == nil || !b.notices.routes[key].pending
	})
}

func TestNoticeHeldCooldownAndSendTimeCount(t *testing.T) {
	for _, negotiated := range []bool{false, true} {
		for _, action := range []string{"queued", "attempting", "confirmed", "receipt observed", "fetch group"} {
			t.Run(fmt.Sprintf("negotiated=%v/%s", negotiated, action), func(t *testing.T) {
				b, w, s, fc := noticeFixture(t, negotiated)
				b.HeldNotices = newFallbackTracker(40 * time.Millisecond)
				b.HeldNotices.ShouldSend(w.key)
				first := negotiatedAppend(t, w, 1, "one")
				b.evaluateNotices(w.key, true)
				second := negotiatedAppend(t, w, 2, "two")
				rows, _ := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
				if action != "queued" {
					transport := "channel"
					if action == "fetch group" {
						transport = "fetch"
					}
					members := []attemptMember{{ID: first, Revision: rowRevision(rows[0])}, {ID: second, Revision: rowRevision(rows[1])}}
					b.attempts.open(attemptRecord{Negotiated: negotiated, Token: "test-token", Route: w.key, Transport: transport, Holder: shadowHolder(s), Members: members}, time.Now())
					if action == "confirmed" || action == "receipt observed" {
						w.updateAttempt("test-token", func(a *attemptRecord) { a.Evidence = true; a.Outcome = "failed" })
					}
				}
				// A route line is separately delayed. Force the sender to finish via a
				// previously announced semantic state so only the Held emitter is tested.
				b.notices.mu.Lock()
				b.notices.routes[w.key].announced = b.noticeRoute(w.key).Semantic()
				b.notices.mu.Unlock()
				if action == "queued" {
					got := waitNoticeReplies(t, fc, 1)
					if got[0].Text != heldReplyText("telegram", 2)+"\n"+b.noticeRoute(w.key).Text() {
						t.Fatal(got)
					}
					for i := 0; i < 3; i++ {
						b.evaluateNotices(w.key, true)
					}
					settleNotice(t, b, w.key)
					if len(fc.sendRepliesSnapshot()) != 1 {
						t.Fatal("unchanged held rows repeated")
					}
				} else {
					settleNotice(t, b, w.key)
					if got := fc.sendRepliesSnapshot(); len(got) != 0 {
						t.Fatalf("false Held: %+v", got)
					}
				}
			})
		}
	}
}

func TestNoticeSemanticStabilityAndFlapCancellation(t *testing.T) {
	for _, negotiated := range []bool{false, true} {
		t.Run(fmt.Sprint(negotiated), func(t *testing.T) {
			b, w, s, fc := noticeFixture(t, negotiated)
			b.notices.window = 40 * time.Millisecond
			set := func(state, reason string) {
				if negotiated {
					s.delivery.mu.Lock()
					r := s.delivery.route(w.key)
					r.Confirmed = time.Time{}
					r.Exhausted = time.Time{}
					s.delivery.live.Channel = ipc.DeliveryEligibility{Eligible: state == "A", Reason: reason}
					s.delivery.mu.Unlock()
				} else {
					st := ipc.RenderCapable
					if state != "A" {
						st = ipc.RenderQueueOnly
					}
					s.SetRenderRoute(st, reason, true)
				}
				b.notifyRenderRoute(s)
			}
			set("A", "")
			waitNoticeReplies(t, fc, 1)
			settleNotice(t, b, w.key)
			set("B", "first")
			b.notices.mu.Lock()
			first := b.notices.routes[w.key].since
			b.notices.mu.Unlock()
			set("B", "second")
			b.notices.mu.Lock()
			second := b.notices.routes[w.key].since
			b.notices.mu.Unlock()
			if !second.After(first) {
				t.Fatal("reason change did not restart stable window")
			}
			set("A", "")
			settleNotice(t, b, w.key)
			if len(fc.sendRepliesSnapshot()) != 1 {
				t.Fatal("flap emitted a line")
			}
			set("B", "stable")
			waitNoticeReplies(t, fc, 2)
			settleNotice(t, b, w.key)
			for _, r := range fc.sendRepliesSnapshot() {
				if strings.Contains(r.Text, "Held") || strings.Count(r.Text, "Live route:") != 1 || strings.Contains(r.Text, "\n") {
					t.Fatal(r.Text)
				}
			}
			r := ipc.RenderRoute{State: "live_inbox", Confirmed: time.Now(), Held: 1}
			before := r.Semantic()
			r.Confirmed = r.Confirmed.Add(time.Hour)
			r.Held = 999
			if r.Semantic() != before {
				t.Fatal("age/count changed semantic state")
			}
			if routeNoticeWindow != 60*time.Second || defaultHeldNoticeCooldown != 10*time.Second {
				t.Fatal("production timer contract changed")
			}
		})
	}
}

func TestNoticeNeverPullOnlyDuringLiveAttempt(t *testing.T) {
	for _, negotiated := range []bool{false, true} {
		t.Run(fmt.Sprint(negotiated), func(t *testing.T) {
			b, w, s, _ := noticeFixture(t, negotiated)
			id := negotiatedAppend(t, w, 1, "offered")
			b.attempts.open(attemptRecord{Negotiated: negotiated, Token: "live", Route: w.key, Transport: "inbox", Holder: shadowHolder(s), Members: attemptMembers([]string{id})}, time.Now())
			text := b.noticeRoute(w.key).Text()
			if strings.Contains(text, "queue-only") || strings.Contains(text, "pull-only") {
				t.Fatal(text)
			}
			if n, err := b.noticePending(w.key, s); err != nil || n != 0 {
				t.Fatal(n, err)
			}
		})
	}
}

func TestNoticeChannelTimeoutFallbackConfirmationScenario(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, w, s, frames, ctx := negotiatedFixture(t)
	b.notices.window = 50 * time.Millisecond
	s.delivery.mu.Lock()
	s.delivery.live.Inbox.Eligible = true
	s.delivery.mu.Unlock()
	negotiatedAppend(t, w, 1, "screenshot scenario")
	w.scheduleAttempt(ctx, true)
	first := nextDeliver(t, frames)
	w.updateAttempt(first.Token, func(a *attemptRecord) { a.Deadline = time.Now().Add(-time.Second) })
	w.tickAttempt(ctx)
	fallback := nextDeliver(t, frames)
	if fallback.Transport != "inbox" || fallback.Token == first.Token {
		t.Fatal(fallback)
	}
	w.handleAttemptResult(resultFor(s, fallback, "confirmed"))
	w.scheduleAttempt(ctx, false)
	ch, _ := b.Channel("telegram")
	fc := ch.(*fakeChannel)
	got := waitNoticeReplies(t, fc, 1)
	settleNotice(t, b, w.key)
	if len(got) != 1 || strings.Contains(got[0].Text, "Held") || !strings.Contains(got[0].Text, "Live route: live: inbox, confirmed") {
		t.Fatal(got)
	}
}

func TestEvictionNoticeAgeAndCountGoldens(t *testing.T) {
	for _, tc := range []struct {
		age, count, held int
		retention        bool
		want             string
	}{
		{2, 0, 1, true, "⚠️ 2 message(s) expired after 90 days (recoverable from trash for 90 days); 1 message(s) remain held."},
		{0, 3, 1000, true, "⚠️ queue full — dropped 3 oldest; 1000 message(s) remain held."},
		{1, 1, 7, true, "⚠️ 1 message(s) expired after 90 days (recoverable from trash for 90 days); queue full — dropped 1 oldest; 7 message(s) remain held."},
		{1, 0, 0, false, "⚠️ 1 message(s) expired after 90 days (trash retention unavailable); 0 message(s) remain held."},
	} {
		t.Run(tc.want, func(t *testing.T) {
			if got := evictionNotice(tc.age, tc.count, tc.held, nil, tc.retention); got != tc.want {
				t.Fatal(got)
			}
		})
	}
}

func TestNoticeNegotiatedOperatorLogGoldens(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, w, s, frames, ctx := negotiatedFixture(t)
	logs := &matrixLogBuffer{}
	prior := log.Writer()
	log.SetOutput(logs)
	t.Cleanup(func() { log.SetOutput(prior) })
	negotiatedAppend(t, w, 42, "private message body")
	w.scheduleAttempt(ctx, false)
	f := nextDeliver(t, frames)
	waitForVoiceCondition(t, "delivered log", func() bool { return strings.Contains(logs.text(), "delivered chan=") })
	w.handleAttemptResult(resultFor(s, f, "confirmed"))
	got := logs.text()
	for _, line := range []string{deliveredLog(w.key, 42, s, "channel", f.Token), "attempt confirmed token=" + f.Token + " route=-100/dm ms=", "attempt retired n=1 token=" + f.Token, "attempt reserved transport=channel members=1 budget_ms=15000", "attempt finished transport=channel outcome=confirmed"} {
		if !strings.Contains(got, line) {
			t.Errorf("missing %q in %s", line, got)
		}
	}
	if strings.Contains(got, "private message body") {
		t.Fatal("delivery log exposed body")
	}
	for _, transport := range []string{"channel", "inbox", "fetch"} {
		want := fmt.Sprintf("delivered chan=telegram topic=- msg=42 to cli=claude conn=1 transport=%s token=TOKEN", transport)
		if line := deliveredLog(w.key, 42, s, transport, "TOKEN"); line != want {
			t.Fatal(line)
		}
	}
	_ = b
}

func TestStatusNoticeSameQueuedRowsAndHistoryGolden(t *testing.T) {
	for _, negotiated := range []bool{false, true} {
		t.Run(fmt.Sprint(negotiated), func(t *testing.T) {
			b, w, s, _ := noticeFixture(t, negotiated)
			s.Build = "test-build"
			a := negotiatedAppend(t, w, 1, "attempting")
			negotiatedAppend(t, w, 2, "waiting")
			b.attempts.open(attemptRecord{Token: "secret", Route: w.key, Transport: "fetch", Holder: shadowHolder(s), Members: attemptMembers([]string{a}), Negotiated: negotiated}, time.Now())
			if negotiated {
				s.delivery.mu.Lock()
				r := s.delivery.route(w.key)
				r.Transport = "inbox"
				r.Confirmed = time.Now().Add(-90 * time.Second)
				r.Exhausted = time.Now()
				s.delivery.mu.Unlock()
			}
			count, err := b.noticePending(w.key, s)
			if err != nil || count != 1 {
				t.Fatal(count, err)
			}
			got := b.statusForTopic(w.key.Channel, w.key.ChatID, nil)
			route := "Live route: queue-only (host unavailable)."
			if negotiated {
				route = "Live route: pull-only (no receipt on channel or inbox; retries on reconnect, attach or new messages after 60 s), was inbox, confirmed 1m30s ago."
			}
			want := "📊 general · 1 queued (oldest <1m) · Claude Code attached · build test-build · " + route + " · broker up"
			if got != want {
				t.Fatalf("got %q\nwant %q", got, want)
			}
			if strings.Contains(b.statusGlobal(), "secret") || strings.Contains(got, "secret") {
				t.Fatal("status exposed token")
			}
		})
	}
}

func TestNoticeHeldWorkerEvents(t *testing.T) {
	for _, negotiated := range []bool{false, true} {
		for _, event := range []string{"enqueue", "termination", "ownership", "capability", "enrichment", "drain import"} {
			t.Run(fmt.Sprintf("%v/%s", negotiated, event), func(t *testing.T) {
				clearFetchTestEnvironment(t)
				fc := &fakeChannel{}
				b := brokerWithChannel(t, mfWithTelegram(), fc)
				t.Cleanup(b.Shutdown)
				key := MakeRouteKey("telegram", -100, nil)
				s, _ := negotiatedHolder(t, b, key, 1)
				if negotiated {
					receiptHolder(s)
				} else {
					s.deliveryReady.Store(false)
					s.SetRenderRoute(ipc.RenderQueueOnly, "host unavailable", true)
				}
				in := inboundOn(-100, nil, 1, "held row")
				barrier := make(chan BacklogResult, 1)
				b.Workers.Submit(key, Job{Kind: JobBacklog, Backlog: &BacklogJob{ResultCh: barrier}})
				<-barrier
				var id string
				var pending []string
				if event == "enrichment" {
					pending = []string{"voice-file"}
				}
				if event != "enqueue" && event != "drain import" {
					var err error
					if id, err = b.Queue.AppendTracked(queueRouteKey(key), in, pending...); err != nil {
						t.Fatal(err)
					}
				}
				switch event {
				case "enqueue":
					b.Workers.Submit(key, Job{Kind: JobInbound, Inbound: in})
				case "termination":
					b.Workers.Submit(key, Job{Kind: JobConsume, Consume: &ConsumeJob{Owner: s, Token: "unknown", Count: 1}})
				case "ownership":
					b.withRouteSet(s, ipc.AttachedMsg{OK: true, Channel: key.Channel, ChatID: key.ChatID})
				case "capability":
					if negotiated {
						b.handleDeliveryReport(s, []byte(`{"op":"delivery_report","live":{"channel":{"eligible":false,"reason":"changed"},"inbox":{"eligible":false,"reason":"changed"}}}`))
					} else {
						raw, _ := json.Marshal(ipc.RenderStateMsg{Op: ipc.OpRenderState, RenderRoute: ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "changed"}})
						b.handleRenderState(s, raw)
					}
				case "enrichment":
					b.Voice.wg.Add(1)
					b.Workers.Submit(key, Job{Kind: JobResolveVoice, ResolveVoice: &ResolveVoiceJob{Key: voiceScheduleKey{route: key, messageID: 1, fileID: "voice-file"}, Targets: []voiceResolveTarget{{recordID: id}}, Inbound: *in, FileID: "voice-file", SegmentText: "enriched", Success: true}})
				case "drain import":
					done := make(chan DrainAppendResult, 1)
					b.Workers.Submit(key, Job{Kind: JobDrainAppend, DrainAppend: &DrainAppendJob{From: "source", Messages: []DrainAppendMessage{{Inbound: *in, SourceRecordID: "source-row"}}, ResultCh: done}})
					<-done
				}
				got := waitNoticeReplies(t, fc, 1)
				if !strings.Contains(got[0].Text, "📨 Held — nothing lost. 1 message queued.") {
					t.Fatal(got)
				}
			})
		}
	}
}

// Keep the retention constant tied to the actual notice, not a copied limit.
func TestNoticeRetentionTracksStore(t *testing.T) {
	if queue.MaxAge != 90*24*time.Hour || queue.TrashTTL != 90*24*time.Hour {
		t.Fatal("update retention notice goldens with the store contract")
	}
}

func TestNoticeSenderSerializesAcrossOwnershipAndFlaps(t *testing.T) {
	for _, negotiated := range []bool{false, true} {
		for _, scenario := range []string{"new state", "back to sent", "replacement"} {
			t.Run(fmt.Sprintf("%v/%s", negotiated, scenario), func(t *testing.T) {
				b, w, s, fc := noticeFixture(t, negotiated)
				blocked := &firstNoticeBlockedChannel{fakeChannel: fc, entered: make(chan c3types.ReplyArgs, 8), release: make(chan struct{})}
				b.chMu.Lock()
				b.channels[w.key.Channel] = &channelRegistration{Channel: blocked}
				b.chMu.Unlock()
				b.notices.window = time.Millisecond
				released := false
				t.Cleanup(func() {
					if !released {
						close(blocked.release)
					}
				})
				set := func(reason string) {
					if negotiated {
						s.delivery.mu.Lock()
						s.delivery.live.Channel.Reason = reason
						s.delivery.mu.Unlock()
					} else {
						s.SetRenderRoute(ipc.RenderQueueOnly, reason, true)
					}
					b.notifyRenderRoute(s)
				}
				set("A")
				select {
				case <-blocked.entered:
				case <-time.After(time.Second):
					t.Fatal("sender never entered")
				}
				set("B")
				if scenario == "back to sent" {
					set("A")
				}
				if scenario == "replacement" {
					// Same route, a new stub: the previous loop still owns the send reservation.
					b.Routes.Release(w.key, s.ConnID)
					next := &Stub{CLI: s.CLI, PID: s.PID, CWD: s.CWD, ConnID: s.ConnID + 1, Conn: s.ConnValue(), delivery: s.delivery}
					if negotiated {
						next.deliveryModes.Store(s.deliveryModes.Load())
						next.deliveryReady.Store(true)
					} else {
						next.SetRenderRoute(ipc.RenderQueueOnly, "B", true)
					}
					b.Routes.Claim(w.key, next)
					next.AddRoute(w.key)
					next.MarkRouteConfirmed(w.key)
					s = next
					b.notifyRenderRoute(s)
				}
				select {
				case r := <-blocked.entered:
					t.Fatalf("overlapping sender: %s", r.Text)
				case <-time.After(20 * time.Millisecond):
				}
				close(blocked.release)
				released = true
				settleNotice(t, b, w.key)
				want := 2
				if scenario == "back to sent" {
					want = 1
				}
				replies := fc.sendRepliesSnapshot()
				if len(replies) != want {
					t.Fatalf("got %+v want %d", replies, want)
				}
				if want == 2 && !strings.Contains(replies[1].Text, "B") {
					t.Fatal(replies)
				}
			})
		}
	}
}

func TestEvictNoticeCountCapUsesRemainingQueuedRows(t *testing.T) {
	for _, negotiated := range []bool{false, true} {
		t.Run(fmt.Sprint(negotiated), func(t *testing.T) {
			b, w, s, fc := noticeFixture(t, negotiated)
			var data strings.Builder
			for i := 0; i <= queue.MaxMessages; i++ {
				row := map[string]any{"Channel": "telegram", "ChatID": -100, "MessageID": i + 1, "Text": "sample", "_c3_queue_id": fmt.Sprintf("row-%d", i)}
				raw, err := json.Marshal(row)
				if err != nil {
					t.Fatal(err)
				}
				data.Write(raw)
				data.WriteByte('\n')
			}
			path := filepath.Join(filepath.Dir(b.Queue.RetentionDir()), queueRouteKey(w.key).File()+".jsonl")
			if err := os.WriteFile(path, []byte(data.String()), 0600); err != nil {
				t.Fatal(err)
			}
			rows, err := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
			if err != nil {
				t.Fatal(err)
			}
			last := rows[len(rows)-1]
			b.attempts.open(attemptRecord{Negotiated: negotiated, Token: "open-fetch", Route: w.key, Transport: "fetch", Holder: shadowHolder(s), Members: []attemptMember{{ID: last.RecordID, Revision: rowRevision(last)}}}, time.Now())

			w.evictIfOverCap(queueRouteKey(w.key))
			got := fc.sendRepliesSnapshot()
			if len(got) != 1 || got[0].Text != "⚠️ queue full — dropped 1 oldest; 999 message(s) remain held." {
				t.Fatal(got)
			}
		})
	}
}

func TestNoticeLegacyFallbackConfirmationScenario(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, w, s, frames := shadowFixture(t)
	b.Workers.Stop()
	b.notices.window = 40 * time.Millisecond
	s.SetRenderRoute(ipc.RenderProbing, "awaiting confirmation", true)
	f := shadowPushOne(t, w, frames, time.Now())
	w.updateAttempt(f.DeliveryToken, func(a *attemptRecord) { a.Deadline = time.Now().Add(-time.Second) })
	s.SetRenderRoute(ipc.RenderProbing, "inbox awaiting confirmation", true)
	b.notifyRenderRoute(s)
	s.SetRenderRoute(ipc.RenderCrossSession, "", true)
	w.handleConsume(context.Background(), &ConsumeJob{Owner: s, MessageID: f.Inbound.MessageID, Token: f.DeliveryToken, Count: 1})
	w.scheduleAttempt(context.Background(), false)
	ch, _ := b.Channel("telegram")
	fc := ch.(*fakeChannel)
	got := waitNoticeReplies(t, fc, 1)
	settleNotice(t, b, w.key)
	if len(got) != 1 || got[0].Text != "Live route: cross-session." {
		t.Fatal(got)
	}
}

func TestNoticeAcceptMilestoneAndLegacyRenderGoldens(t *testing.T) {
	for _, tc := range []struct {
		route ipc.RenderRoute
		want  string
	}{
		{ipc.RenderRoute{State: ipc.RenderCapable}, "Live route: channel."},
		{ipc.RenderRoute{State: ipc.RenderProbing}, "Live route: probing."},
		{ipc.RenderRoute{State: ipc.RenderCrossSession}, "Live route: cross-session."},
		{ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "host unavailable"}, "Live route: queue-only (host unavailable)."},
		{ipc.RenderRoute{State: "waiting"}, "Live route: waiting."},
		{ipc.RenderRoute{State: "live_channel", Confirmed: time.Now(), AcceptedBy: "Codex"}, "Live route: live: channel, accepted by Codex 0s ago."},
		{ipc.RenderRoute{State: "pull_only", Reason: "unavailable", Transport: "inbox", Confirmed: time.Now(), AcceptedBy: "Codex"}, "Live route: pull-only (unavailable), was inbox, accepted by Codex 0s ago."},
	} {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.route.Text(); got != tc.want {
				t.Fatal(got)
			}
		})
	}
}

func TestNoticeSnapshotSchedulesBeforeCounting(t *testing.T) {
	clearFetchTestEnvironment(t)
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	t.Cleanup(b.Shutdown)
	key := MakeRouteKey("telegram", -100, nil)
	_, frames := negotiatedHolder(t, b, key, 1)
	in := inboundOn(-100, nil, 1, "awaiting scheduling")
	if _, err := b.Queue.AppendTracked(queueRouteKey(key), in); err != nil {
		t.Fatal(err)
	}
	result := make(chan BacklogResult, 1)
	b.Workers.Submit(key, Job{Kind: JobBacklog, Backlog: &BacklogJob{Notice: true, ResultCh: result}})
	got := <-result
	if got.Err != nil || len(got.NoticeRows) != 0 {
		t.Fatal("notice inspected rows before scheduling", got)
	}
	nextDeliver(t, frames)
}

func TestNoticeLegacyReceiptEvidenceSurvivesRemovalFailure(t *testing.T) {
	for _, emptyToken := range []bool{false, true} {
		t.Run(fmt.Sprint(emptyToken), func(t *testing.T) {
			clearFetchTestEnvironment(t)
			b, w, s, frames := shadowFixture(t)
			b.Workers.Stop()
			f := shadowPushOne(t, w, frames, time.Now())
			dir := b.Queue.RetentionDir()
			if err := os.RemoveAll(dir); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(dir, []byte("blocked"), 0600); err != nil {
				t.Fatal(err)
			}
			token := f.DeliveryToken
			if emptyToken {
				token = ""
			}
			w.handleConsume(context.Background(), &ConsumeJob{Owner: s, MessageID: f.Inbound.MessageID, Token: token, Count: 1})
			if n, err := b.noticePending(w.key, s); n != 0 || err != nil {
				t.Fatal("accepted row called Held", n, err)
			}
			if a := b.attempts.lookup(f.DeliveryToken, time.Now())[0]; !a.Evidence {
				t.Fatal("receipt evidence lost")
			}
		})
	}
}

func TestExceptionalRecoveryNoticeDoesNotInventProcessing(t *testing.T) {
	got := unprocessedNoticeText(2, "The session exited")
	want := "⚠️ The session exited. C3 restored 2 messages whose processing was not confirmed. Fetch them from the queue; delivery may repeat."
	if got != want {
		t.Fatalf("got %q", got)
	}
}

func TestFetchAttemptOperatorLogGoldens(t *testing.T) {
	clearFetchTestEnvironment(t)
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	t.Cleanup(b.Shutdown)
	key := MakeRouteKey("telegram", -100, nil)
	s, _ := negotiatedHolder(t, b, key, 1)
	receiptHolder(s)
	in := inboundOn(-100, nil, 42, "private fetch body")
	if _, err := b.Queue.AppendTracked(queueRouteKey(key), in); err != nil {
		t.Fatal(err)
	}
	logs := &matrixLogBuffer{}
	prior := log.Writer()
	log.SetOutput(logs)
	t.Cleanup(func() { log.SetOutput(prior) })
	a, c := net.Pipe()
	t.Cleanup(func() { a.Close(); c.Close() })
	done := make(chan struct{})
	raw, _ := json.Marshal(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "fetch", Ack: true, All: true, Lease: json.RawMessage("true")})
	go func() { b.handleFetchQueue(ipc.NewConn(c), s, raw); close(done) }()
	frame, err := ipc.NewConn(a).ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	<-done
	var resp ipc.FetchQueueResp
	if err := json.Unmarshal(frame, &resp); err != nil || resp.LeaseToken == "" {
		t.Fatal(string(frame), err)
	}
	raw, _ = json.Marshal(ipc.AttemptResultMsg{Op: ipc.OpAttemptResult, Token: resp.LeaseToken, Outcome: "confirmed"})
	b.handleAttemptResult(s, raw)
	waitForVoiceCondition(t, "fetch retirement log", func() bool { return strings.Contains(logs.text(), "attempt retired n=1 token="+resp.LeaseToken) })
	for _, line := range []string{deliveredLog(key, 42, s, "fetch", resp.LeaseToken), "attempt confirmed token=" + resp.LeaseToken + " route=-100/dm ms=", "attempt reserved transport=fetch members=1 budget_ms=60000", "attempt finished transport=fetch outcome=confirmed"} {
		if !strings.Contains(logs.text(), line) {
			t.Error("missing", line, logs.text())
		}
	}
	if strings.Contains(logs.text(), "private fetch body") {
		t.Fatal("logged content")
	}
}

func TestStatusClaimBuildAndPerRouteHistory(t *testing.T) {
	b, w, s, _ := noticeFixture(t, true)
	s.Build = "test-build"
	s.delivery.mu.Lock()
	r := s.delivery.route(w.key)
	r.Transport = "inbox"
	r.Confirmed = time.Now()
	r.Exhausted = time.Now()
	s.delivery.mu.Unlock()
	sibling := MakeRouteKey("telegram", -100, ptrI64Val(2))
	b.Routes.Claim(sibling, s)
	s.AddRoute(sibling)
	s.MarkRouteConfirmed(sibling)
	a, c := net.Pipe()
	t.Cleanup(func() { a.Close(); c.Close() })
	done := make(chan struct{})
	go func() { b.handleListClaims(ipc.NewConn(c)); close(done) }()
	frame, err := ipc.NewConn(a).ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	<-done
	var resp ipc.ClaimsListMsg
	if err := json.Unmarshal(frame, &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Claims) != 2 {
		t.Fatal(resp)
	}
	for _, claim := range resp.Claims {
		if claim.HolderBuild != "test-build" {
			t.Fatal("claim lost build", claim)
		}
		if !claim.HasTopic {
			if claim.ConfirmedTransport != "inbox" || claim.ConfirmedAt.IsZero() {
				t.Fatal("lost route history", claim)
			}
		} else if !claim.ConfirmedAt.IsZero() {
			t.Fatal("session proof leaked into sibling route", claim)
		}
	}
}

func TestDocsQuotePhase5NoticeContracts(t *testing.T) {
	key := MakeRouteKey("telegram", -100, ptrI64Val(281))
	for _, doc := range []struct {
		path   string
		quotes []string
	}{
		{"../../docs/DEBUGGING.md", []string{deliveredLog(key, 42, &Stub{CLI: "claude", ConnID: 7}, "channel", "TOKEN"), "attempt confirmed token=TOKEN route=-100/281 ms=100", "attempt retired n=1 token=TOKEN", "/reload-plugins"}},
		{"../../docs/ADAPTERS.md", []string{"| Negotiated route state | Meaning and history |", "60-second stable-state timer", "accepted by <host>", "holder_build"}},
		{"../../DECISIONS.md", []string{"D038: Notices derive from delivery state (phase 5)", "One broker-owned sender per route", "state cancels a pending change"}},
	} {
		body, err := os.ReadFile(doc.path)
		if err != nil {
			t.Fatal(err)
		}
		for _, quote := range doc.quotes {
			if !strings.Contains(string(body), quote) {
				t.Errorf("%s missing %q", doc.path, quote)
			}
		}
	}
}

func TestNoticeWaitingForAlternateTransportKeepsAcceptedHistory(t *testing.T) {
	_, w, s, _ := noticeFixture(t, true)
	s.CLI = "codex"
	s.delivery.mu.Lock()
	s.delivery.offer.Receipts = "accept"
	r := s.delivery.route(w.key)
	r.Confirmed = time.Now()
	r.Transport = "channel"
	s.delivery.live.Inbox.Eligible = true
	s.delivery.mu.Unlock()
	got := s.RenderRouteFor(w.key).Text()
	want := "Live route: waiting, was channel, accepted by Codex 0s ago."
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestNoticeSnapshotHonorsDrainIdentityBarrier(t *testing.T) {
	b, w, _, _ := noticeFixture(t, true)
	route := queueRouteKey(w.key)
	path := filepath.Join(filepath.Dir(b.Queue.RetentionDir()), route.File()+".jsonl")
	if err := os.WriteFile(path, []byte("{\"Channel\":\"telegram\",\"ChatID\":-100,\"MessageID\":1,\"Text\":\"legacy row\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	b.drains.tryAcquire(route.File())
	defer b.drains.release(route.File())
	result := make(chan BacklogResult, 1)
	w.handleBacklog(context.Background(), &BacklogJob{Notice: true, ResultCh: result})
	got := <-result
	if got.Err != nil || len(got.NoticeRows) != 1 || got.NoticeRows[0].RecordID != "" {
		t.Fatal("notice query mutated frozen identity", got)
	}
}
