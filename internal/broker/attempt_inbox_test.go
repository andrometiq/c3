package broker

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

func enableInbox(s *Stub, channel bool) {
	s.delivery.mu.Lock()
	defer s.delivery.mu.Unlock()
	s.delivery.live.Channel.Eligible = channel
	s.delivery.live.Inbox = ipc.DeliveryEligibility{Eligible: true}
}

func TestAttemptInboxFallbackSurvivingBatchAndLateChannel(t *testing.T) {
	clearFetchTestEnvironment(t)
	// P6: "Cycle = one selected batch"; P2 case (c): late receipts are no-ops.
	b, w, s, frames, ctx := negotiatedFixture(t)
	enableInbox(s, true)
	removed := negotiatedAppend(t, w, 1, "removed")
	survivor := negotiatedAppend(t, w, 2, "survivor")
	w.scheduleAttempt(ctx, false)
	channel := nextAttemptDelivery(t, w, frames)
	old := w.liveAttempt()
	if channel.Transport != "channel" {
		t.Fatal(channel)
	}
	b.Queue.RemoveRecordIDs(queueRouteKey(w.key), []string{removed})
	newest := negotiatedAppend(t, w, 3, "new arrival")
	w.updateAttempt(channel.Token, func(a *attemptRecord) { a.Deadline = time.Now().Add(-time.Second) })
	w.tickAttempt(ctx)
	inbox := nextAttemptDelivery(t, w, frames)
	a := w.liveAttempt()
	if inbox.Transport != "inbox" || inbox.Token == channel.Token || inbox.DeadlineMS < 14000 || a.Deadline.Sub(a.Started) != 15*time.Second || !a.Deadline.After(old.Deadline) {
		t.Fatalf("fallback: %+v %+v", inbox, a)
	}
	if len(a.Members) != 1 || a.Members[0].ID != survivor || strings.Contains(inbox.Inbound.Text, "new arrival") {
		t.Fatal("fallback changed batch", a)
	}
	w.handleAttemptResult(resultFor(s, channel, "confirmed"))
	if s.delivery.proven || w.liveAttempt().Token != inbox.Token || !s.delivery.route(w.key).Exhausted.IsZero() {
		t.Fatal("late channel changed authority or proof")
	}
	w.handleAttemptResult(resultFor(s, inbox, "confirmed"))
	rows, _ := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if len(rows) != 1 || rows[0].RecordID != newest {
		t.Fatal("retired outside surviving batch", rows)
	}
	retired := b.attempts.lookup(inbox.Token, time.Now())[0].Retired
	if !slices.Equal(retired, []string{survivor}) {
		t.Fatal(retired)
	}
	if !strings.Contains(s.RenderRouteFor(w.key).Text(), "live: inbox, confirmed") {
		t.Fatal(s.RenderRouteFor(w.key))
	}
	w.scheduleAttempt(ctx, false)
	if f := nextAttemptDelivery(t, w, frames); f.Transport != "channel" || f.Inbound.Text != "new arrival" {
		t.Fatal("new batch preference", f)
	}
}

func TestAttemptInboxCycleKeepsAdmissionAndExhausts(t *testing.T) {
	clearFetchTestEnvironment(t)
	// P6: "A cycle holds the slot until it terminates"; each transport once.
	b, w, s, frames, ctx := negotiatedFixture(t)
	enableInbox(s, true)
	negotiatedAppend(t, w, 1, "first")
	w.scheduleAttempt(ctx, false)
	channel := nextAttemptDelivery(t, w, frames)
	topic := int64(2)
	key := MakeRouteKey("telegram", -100, &topic)
	s.AddRoute(key)
	s.MarkRouteConfirmed(key)
	b.Routes.Claim(key, s)
	sibling := &RouteWorker{key: key, broker: b}
	negotiatedAppend(t, sibling, 2, "sibling")
	sibling.scheduleAttempt(ctx, false)
	w.handleAttemptResult(resultFor(s, channel, "failed"))
	sibling.scheduleAttempt(ctx, false)
	if sibling.liveAttempt() != nil || s.delivery.slot != channel.Token {
		t.Fatal("fallback lost admission")
	}
	w.scheduleAttempt(ctx, false)
	inbox := nextAttemptDelivery(t, w, frames)
	w.handleAttemptResult(resultFor(s, inbox, "failed"))
	state := s.RenderRouteFor(w.key)
	if state.State != "pull_only" || !strings.Contains(state.Text(), "no receipt on channel or inbox; retries on") || s.delivery.slot != "" {
		t.Fatal(state)
	}
	exhausted := s.delivery.route(w.key).Exhausted
	for _, f := range []ipc.DeliverMsg{channel, inbox} {
		w.handleAttemptResult(resultFor(s, f, "confirmed"))
	}
	if s.delivery.proven || s.delivery.route(w.key).Exhausted != exhausted {
		t.Fatal("late receipt rearmed exhaustion")
	}
	w.scheduleAttempt(ctx, true)
	if w.liveAttempt() != nil {
		t.Fatal("exhausted cycle retried")
	}
	sibling.scheduleAttempt(ctx, false)
	if sibling.liveAttempt() == nil {
		t.Fatal("exhaustion did not release admission")
	}
}

func TestNegotiatedInboxFirstReportAndHistory(t *testing.T) {
	clearFetchTestEnvironment(t)
	// P1/P2: "channel OR inbox is eligible". P8: "Fetch never changes" display.
	b, w, s, frames, ctx := negotiatedFixture(t)
	raw := `{"version":1,"live":{"channel":{"eligible":false,"reason":"no channel"},"inbox":{"eligible":true}},"receipts":"transcript","fetch":"receipt"}`
	b.configureDelivery(s, []byte(raw))
	if !s.negotiated() || s.RenderRouteFor(w.key).State != "waiting" {
		t.Fatal("inbox-only did not negotiate")
	}
	negotiatedAppend(t, w, 1, "first")
	w.scheduleAttempt(ctx, false)
	f := nextAttemptDelivery(t, w, frames)
	if f.Transport != "inbox" {
		t.Fatal(f)
	}
	w.handleAttemptResult(resultFor(s, f, "confirmed"))
	prior := s.RenderRouteFor(w.key)
	negotiatedAppend(t, w, 2, "second")
	w.scheduleAttempt(ctx, false)
	f = nextAttemptDelivery(t, w, frames)
	w.handleAttemptResult(resultFor(s, f, "failed"))
	state := s.RenderRouteFor(w.key)
	if state.Confirmed != prior.Confirmed || !strings.Contains(state.Text(), "was inbox") {
		t.Fatal(state.Text())
	}
	ch := make(chan FetchResult, 1)
	w.handleFetch(ctx, &FetchJob{All: true, Ack: true, Owner: s, ResultCh: ch})
	<-ch
	if got := s.RenderRouteFor(w.key); got.Confirmed != prior.Confirmed || got.State != state.State {
		t.Fatal("fetch changed display", got)
	}
	stamp := s.delivery.route(w.key).Exhausted
	report := func(live ipc.DeliveryLive) {
		raw, _ := json.Marshal(ipc.DeliveryReportMsg{Op: ipc.OpDeliveryReport, Live: live})
		b.handleDeliveryReport(s, raw)
	}
	report(s.delivery.live)
	if s.delivery.route(w.key).Exhausted != stamp {
		t.Fatal("unchanged report rearmed")
	}
	live := s.delivery.live
	live.Inbox = ipc.DeliveryEligibility{Reason: "socket disappeared"}
	report(live)
	if got := s.RenderRouteFor(w.key); got.State != "pull_only" || got.Reason != "socket disappeared" || !strings.Contains(got.Text(), "was inbox") {
		t.Fatal(got)
	}
	live.Inbox = ipc.DeliveryEligibility{Eligible: true}
	report(live)
	negotiatedAppend(t, w, 3, "socket returned")
	w.scheduleAttempt(ctx, false)
	if nextAttemptDelivery(t, w, frames).Transport != "inbox" {
		t.Fatal("capability change did not rearm")
	}
}

func TestAttemptInboxReconnectAdoptsWithoutResend(t *testing.T) {
	clearFetchTestEnvironment(t)
	// P6: "adopts live attempts ... nothing is resent to restore observation".
	b, w, s, frames, ctx := negotiatedFixture(t)
	enableInbox(s, false)
	negotiatedAppend(t, w, 1, "inbox")
	w.scheduleAttempt(ctx, false)
	f := nextAttemptDelivery(t, w, frames)
	next, nextFrames := negotiatedHolder(t, b, w.key, 2)
	next.delivery = s.delivery
	done := make(chan struct{})
	w.adoptAttempt(&attemptAdoptJob{Old: s, Next: next, Same: true, Done: done})
	<-done
	w.handleAttemptResult(resultFor(s, f, "confirmed"))
	if w.liveAttempt() == nil {
		t.Fatal("old connection retained authority")
	}
	w.handleAttemptResult(resultFor(next, f, "confirmed"))
	if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 0 {
		t.Fatal("adopted inbox failed retirement")
	}
	if !b.attempts.lookup(f.Token, time.Now())[0].Adopted {
		t.Fatal("missing adoption")
	}
	select {
	case f := <-nextFrames:
		t.Fatal("resent", f)
	default:
	}
}

func TestNegotiatedHelloAckForwardModes(t *testing.T) {
	clearFetchTestEnvironment(t)
	// Maintainer ruling: any supported subset may be acknowledged; a known,
	// offered mode must intersect the ack, while unknown modes are ignored.
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	t.Cleanup(b.Shutdown)
	peer, closePeer := peerPair(t, b)
	t.Cleanup(closePeer)
	if err := peer.WriteJSON(ipc.HelloMsg{Build: "test", Op: ipc.OpHello, CLI: "claude", PID: os.Getpid(), CWD: "/work", Delivery: json.RawMessage(channelOffer)}); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var ack ipc.HelloAckMsg
	if json.Unmarshal(raw, &ack) != nil || !ack.Delivery.AcceptsOffer(ipc.ParseDeliveryOffer([]byte(channelOffer))) {
		t.Fatal(string(raw))
	}
	for _, tc := range []struct {
		modes []string
		want  bool
	}{
		{[]string{"channel"}, true}, {[]string{"inbox", "channel"}, true},
		{[]string{"future", "channel"}, true}, {[]string{"inbox"}, false},
		{[]string{"future"}, false}, {nil, false},
	} {
		ack.Delivery.Modes = tc.modes
		if got := ack.Delivery.AcceptsOffer(ipc.ParseDeliveryOffer([]byte(channelOffer))); got != tc.want {
			t.Fatalf("modes=%v accepted=%v", tc.modes, got)
		}
	}
}

// These fixtures drive the worker synchronously; drain its writer completion
// just as run does, so three-attempt scenarios cannot park the bounded writer.
func nextAttemptDelivery(t *testing.T, w *RouteWorker, frames <-chan ipc.DeliverMsg) ipc.DeliverMsg {
	t.Helper()
	f := nextDeliver(t, frames)
	select {
	case result := <-w.attemptWritten:
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("writer completion missing")
	}
	return f
}

func TestNegotiatedInboxTopicStatus(t *testing.T) {
	clearFetchTestEnvironment(t)
	// P8: the operator's /status uses the confirmed transport on this route.
	b, w, s, frames, ctx := negotiatedFixture(t)
	enableInbox(s, false)
	negotiatedAppend(t, w, 1, "inbox")
	w.scheduleAttempt(ctx, false)
	f := nextAttemptDelivery(t, w, frames)
	w.handleAttemptResult(resultFor(s, f, "confirmed"))
	resumeStatusWorker(t, b)
	if got := b.statusForTopic(w.key.Channel, w.key.ChatID, nil); !strings.Contains(got, "live: inbox, confirmed") {
		t.Fatal(got)
	}
}
