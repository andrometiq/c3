package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

func TestNoticeLegacyReleasedObservationBecomesHeld(t *testing.T) {
	for _, expired := range []bool{false, true} {
		for _, replacement := range []bool{false, true} {
			t.Run(fmt.Sprintf("expired=%v/replacement=%v", expired, replacement), func(t *testing.T) {
				clearFetchTestEnvironment(t)
				b, w, old, frames := shadowFixture(t)
				b.Workers.Stop()
				f := shadowPushOne(t, w, frames, time.Now())
				if expired {
					w.updateAttempt(f.DeliveryToken, func(a *attemptRecord) { a.Deadline = time.Now().Add(-time.Second) })
				}
				if n, err := b.noticePending(w.key, old); err != nil || n != 0 {
					t.Fatal("current owner's attempt not hidden", n, err)
				}
				// Match tryClaim's topic-switch path: release/remove only this route;
				// the old process remains alive and owns a different route.
				b.Routes.Release(w.key, old.ConnID)
				old.RemoveRoute(w.key)
				other := MakeRouteKey(w.key.Channel, w.key.ChatID, ptrI64Val(9))
				b.Routes.Claim(other, old)
				old.AddRoute(other)
				old.MarkRouteConfirmed(other)
				if replacement {
					next := &Stub{CLI: "claude", PID: os.Getpid(), CWD: "/replacement", ConnID: 2, Conn: struct{}{}}
					if _, ok := b.Routes.Claim(w.key, next); !ok {
						t.Fatal("replacement claim failed")
					}
					next.SetRenderRoute(ipc.RenderQueueOnly, "host unavailable", true)
					next.AddRoute(w.key)
					next.MarkRouteConfirmed(w.key)
				}
				if !old.IsAlive() {
					t.Fatal("fixture killed old holder")
				}
				if n, err := b.noticePending(w.key, nil); err != nil || n != 1 {
					t.Fatalf("abandoned row hidden after ownership change: %d %v", n, err)
				}
				b.evaluateNotices(w.key, true)
				ch, _ := b.Channel(w.key.Channel)
				replies := waitNoticeReplies(t, ch.(*fakeChannel), 1)
				if !strings.Contains(replies[0].Text, "1 message queued") || !strings.Contains(replies[0].Text, "Held") {
					t.Fatal(replies)
				}
			})
		}
	}
}

type failHeldChannel struct {
	*fakeChannel
	muFailure sync.Mutex
	fail      bool
	attempted chan struct{}
}

func (c *failHeldChannel) SendReply(a c3types.ReplyArgs) (int64, error) {
	c.muFailure.Lock()
	fail := c.fail
	c.muFailure.Unlock()
	if strings.Contains(a.Text, "Held —") && fail {
		select {
		case c.attempted <- struct{}{}:
		default:
		}
		return 0, errors.New("injected send failure")
	}
	return c.fakeChannel.SendReply(a)
}
func TestNoticeHeldFailureRearmsUnchangedRows(t *testing.T) {
	for _, negotiated := range []bool{false, true} {
		for _, failure := range []string{"send", "snapshot"} {
			t.Run(fmt.Sprintf("%v/%s", negotiated, failure), func(t *testing.T) {
				b, w, _, fc := noticeFixture(t, negotiated)
				b.HeldNotices = newFallbackTracker(30 * time.Millisecond)
				b.HeldNotices.ShouldSend(w.key)
				negotiatedAppend(t, w, 1, "queued")
				ch := &failHeldChannel{fakeChannel: fc, fail: true, attempted: make(chan struct{}, 1)}
				b.chMu.Lock()
				b.channels[w.key.Channel] = &channelRegistration{Channel: ch}
				b.chMu.Unlock()
				// The route was already announced, so a route-only retry cannot pass.
				b.notices.mu.Lock()
				b.updateNoticeLocked(w.key, true)
				r := b.notices.routes[w.key]
				r.announced = r.route.Semantic()
				r.pending = false
				b.notices.mu.Unlock()
				path := filepath.Join(filepath.Dir(b.Queue.RetentionDir()), queueRouteKey(w.key).File()+".jsonl")
				if failure == "snapshot" {
					if err := os.Rename(path, path+".saved"); err != nil {
						t.Fatal(err)
					}
					if err := os.Mkdir(path, 0700); err != nil {
						t.Fatal(err)
					}
				}
				b.evaluateNotices(w.key, false)
				if failure == "send" {
					select {
					case <-ch.attempted:
					case <-time.After(time.Second):
						t.Fatal("send not attempted")
					}
				}
				settleNotice(t, b, w.key)
				if failure == "snapshot" {
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
					if err := os.Rename(path+".saved", path); err != nil {
						t.Fatal(err)
					}
				}
				ch.muFailure.Lock()
				ch.fail = false
				ch.muFailure.Unlock()
				b.evaluateNotices(w.key, true)
				replies := waitNoticeReplies(t, fc, 1)
				if len(replies) != 1 || !strings.Contains(replies[0].Text, "Held — nothing lost. 1 message queued.") {
					t.Fatal(replies)
				}
			})
		}
	}
}

// This callback runs on the route worker. Hold it in the middle of a simulated
// rewrite, where JSONL and cursor cannot be read as a consistent pair.
type statusRewriteChannel struct {
	*fakeChannel
	path    string
	entered chan struct{}
	release chan struct{}
}

func (c *statusRewriteChannel) SendReply(a c3types.ReplyArgs) (int64, error) {
	if err := os.Rename(c.path, c.path+".saved"); err != nil {
		return 0, err
	}
	if err := os.Mkdir(c.path, 0700); err != nil {
		return 0, err
	}
	close(c.entered)
	<-c.release
	if err := os.Remove(c.path); err != nil {
		return 0, err
	}
	if err := os.Rename(c.path+".saved", c.path); err != nil {
		return 0, err
	}
	return c.fakeChannel.SendReply(a)
}
func TestStatusSnapshotWaitsForWorkerRewrite(t *testing.T) {
	for _, global := range []bool{false, true} {
		t.Run(fmt.Sprint(global), func(t *testing.T) {
			clearFetchTestEnvironment(t)
			fc := &fakeChannel{}
			b := brokerWithChannel(t, mfWithTelegram(), fc)
			t.Cleanup(b.Shutdown)
			key := MakeRouteKey("telegram", -100, ptrI64Val(281))
			for i, stamp := range []time.Time{time.Now().Add(-2 * time.Hour), time.Now().Add(-72 * time.Hour)} {
				in := inboundOn(-100, &key.TopicID, int64(i+1), "held")
				in.Timestamp = stamp
				if err := b.Queue.Append(queueRouteKey(key), in); err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(filepath.Dir(b.Queue.RetentionDir()), queueRouteKey(key).File()+".jsonl")
			ch := &statusRewriteChannel{fakeChannel: fc, path: path, entered: make(chan struct{}), release: make(chan struct{})}
			b.chMu.Lock()
			b.channels[key.Channel] = &channelRegistration{Channel: ch}
			b.chMu.Unlock()
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(ch.release) }) })
			result := make(chan OutboundResult, 1)
			b.Workers.Submit(key, Job{Kind: JobOutbound, Outbound: &OutboundJob{Tool: "reply", Args: map[string]any{"text": "pause"}, ResultCh: result}})
			select {
			case <-ch.entered:
			case <-time.After(time.Second):
				t.Fatal("worker did not enter rewrite")
			}
			status := make(chan string, 1)
			go func() {
				if global {
					status <- b.statusGlobal()
				} else {
					status <- b.statusForTopic(key.Channel, key.ChatID, &key.TopicID)
				}
			}()
			select {
			case got := <-status:
				t.Fatalf("status bypassed worker during rewrite: %s", got)
			case <-time.After(40 * time.Millisecond):
			}
			release.Do(func() { close(ch.release) })
			if out := <-result; out.Err != nil {
				t.Fatal(out.Err)
			}
			select {
			case got := <-status:
				if !strings.Contains(got, "oldest 3d") || strings.Contains(got, "unavailable") {
					t.Fatal(got)
				}
			case <-time.After(time.Second):
				t.Fatal("status did not finish")
			}
		})
	}
}
func TestStatusOldestUsesMinimumTimestamp(t *testing.T) {
	clearFetchTestEnvironment(t)
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	t.Cleanup(b.Shutdown)
	key := MakeRouteKey("telegram", -100, ptrI64Val(281))
	for i, stamp := range []time.Time{time.Time{}, time.Now().Add(-2 * time.Hour), time.Now().Add(-72 * time.Hour)} {
		in := inboundOn(-100, &key.TopicID, int64(i+1), "held")
		in.Timestamp = stamp
		if err := b.Queue.Append(queueRouteKey(key), in); err != nil {
			t.Fatal(err)
		}
	}
	for _, text := range []string{b.statusForTopic(key.Channel, key.ChatID, &key.TopicID), b.statusGlobal()} {
		if !strings.Contains(text, "oldest 3d") {
			t.Fatal("oldest must be minimum, not file position", text)
		}
	}
}

func TestNoticeRecordedRowSurvivesAttemptHistoryEviction(t *testing.T) {
	for _, negotiated := range []bool{false, true} {
		for _, global := range []bool{false, true} {
			t.Run(fmt.Sprintf("negotiated=%v/global=%v", negotiated, global), func(t *testing.T) {
				clearFetchTestEnvironment(t)
				var b *Broker
				var w *RouteWorker
				var s *Stub
				var token string
				if negotiated {
					var frames <-chan ipc.DeliverMsg
					var ctx context.Context
					b, w, s, frames, ctx = negotiatedFixture(t)
					negotiatedAppend(t, w, 1, "recorded")
					w.scheduleAttempt(ctx, false)
					f := nextDeliver(t, frames)
					token = f.Token
				} else {
					var frames <-chan ipc.InboundMsg
					b, w, s, frames = shadowFixture(t)
					b.Workers.Stop()
					f := shadowPushOne(t, w, frames, time.Now())
					token = f.DeliveryToken
				}
				dir := b.Queue.RetentionDir()
				if err := os.RemoveAll(dir); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(dir, []byte("blocked"), 0600); err != nil {
					t.Fatal(err)
				}
				if negotiated {
					w.handleAttemptResult(fetchResult(s, token))
					w.retireAttemptToken(token)
					w.retireAttemptToken(token)
				} else {
					w.handleConsume(context.Background(), &ConsumeJob{Owner: s, MessageID: 1, Token: token, Count: 1})
					w.updateAttempt(token, func(a *attemptRecord) { a.Deadline = time.Now().Add(-time.Second) })
				}
				if n, err := b.noticePending(w.key, s); n != 0 || err != nil {
					t.Fatal("receipt not excluded before pressure", n, err)
				}
				limit := attemptRouteCap
				if global {
					limit = attemptGlobalCap
				}
				for i := 0; i < limit; i++ {
					key := w.key
					if global {
						key = MakeRouteKey(w.key.Channel, int64(i+100), nil)
					}
					next := fmt.Sprintf("pressure-%d", i)
					b.attempts.open(attemptRecord{Token: next, Route: key, Holder: shadowHolder(s), Transport: "fetch", Members: attemptMembers([]string{next})}, time.Now())
					b.attempts.fail(next, s, "test pressure", time.Now())
				}
				if len(b.attempts.lookup(token, time.Now())) != 0 {
					t.Fatal("fixture did not evict original attempt")
				}
				if n, err := b.noticePending(w.key, s); n != 0 || err != nil {
					t.Fatalf("recorded row became Held after history eviction: %d %v", n, err)
				}
			})
		}
	}
}

// Manual attempt fixtures stop the pool while driving lifecycle transitions.
// Restore a live worker for the actual serialized status read; assertions stay
// identical to the production-facing status goldens.
func resumeStatusWorker(t *testing.T, b *Broker) {
	t.Helper()
	b.Workers.mu.Lock()
	stopped := b.Workers.stopped
	b.Workers.mu.Unlock()
	if !stopped {
		t.Fatal("fixture already has a running pool")
	}
	// Finish any sender from the manually driven lifecycle before replacing
	// the fixture pool. Advance only its test clock; no notice is discarded.
	b.notices.mu.Lock()
	for key, r := range b.notices.routes {
		if r.pending {
			b.updateNoticeLocked(key, false)
			r.since = time.Now().Add(-routeNoticeWindow)
			select {
			case r.changed <- struct{}{}:
			default:
			}
		}
	}
	b.notices.mu.Unlock()
	waitForVoiceCondition(t, "manual fixture notices finished", func() bool {
		b.notices.mu.Lock()
		defer b.notices.mu.Unlock()
		for _, r := range b.notices.routes {
			if r.pending {
				return false
			}
		}
		return true
	})
	b.Workers = NewWorkerPool(b.ctx, time.Hour, b)
}

func TestStatusCommandDoesNotBlockPollBehindWorker(t *testing.T) {
	clearFetchTestEnvironment(t)
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	t.Cleanup(b.Shutdown)
	key := MakeRouteKey("telegram", -100, ptrI64Val(281))
	ch := &firstNoticeBlockedChannel{fakeChannel: fc, entered: make(chan c3types.ReplyArgs, 4), release: make(chan struct{})}
	b.chMu.Lock()
	b.channels[key.Channel] = &channelRegistration{Channel: ch}
	b.chMu.Unlock()
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(ch.release) }) })
	result := make(chan OutboundResult, 1)
	b.Workers.Submit(key, Job{Kind: JobOutbound, Outbound: &OutboundJob{Tool: "reply", Args: map[string]any{"text": "busy"}, ResultCh: result}})
	select {
	case <-ch.entered:
	case <-time.After(time.Second):
		t.Fatal("worker not blocked")
	}
	called := make(chan bool, 1)
	go func() {
		text, handled := NewBrokerHost(b, "telegram").HandleCommand(&c3types.Inbound{Channel: key.Channel, ChatID: key.ChatID, TopicID: &key.TopicID, Text: "/status"})
		called <- handled && text == ""
	}()
	select {
	case valid := <-called:
		if !valid {
			t.Fatal("status did not use asynchronous reply")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("status blocked the poll goroutine on a worker")
	}
	release.Do(func() { close(ch.release) })
	if out := <-result; out.Err != nil {
		t.Fatal(out.Err)
	}
	replies := waitNoticeReplies(t, fc, 2)
	if !strings.Contains(replies[1].Text, "📊 c3 · 0 queued") {
		t.Fatal(replies)
	}
}

func statusCommandReply(t *testing.T, host *BrokerHost, fc *fakeChannel, in *c3types.Inbound) (string, bool) {
	t.Helper()
	before := len(fc.sendRepliesSnapshot())
	text, handled := host.HandleCommand(in)
	if !handled {
		return text, false
	}
	if text != "" {
		t.Fatal("status must not wait on a worker on the poll goroutine")
	}
	waitForVoiceCondition(t, "status command reply", func() bool {
		for _, r := range fc.sendRepliesSnapshot()[before:] {
			if strings.HasPrefix(r.Text, "📊 ") {
				return true
			}
		}
		return false
	})
	var replies []c3types.ReplyArgs
	for _, r := range fc.sendRepliesSnapshot()[before:] {
		if strings.HasPrefix(r.Text, "📊 ") {
			replies = append(replies, r)
		}
	}
	if len(replies) != 1 {
		t.Fatalf("expected one status reply: %+v", replies)
	}
	r := replies[0]
	if MakeRouteKey(r.Channel, r.ChatID, r.TopicID) != MakeRouteKey(in.Channel, in.ChatID, in.TopicID) {
		t.Fatal("status replied to wrong route", r)
	}
	return r.Text, true
}

func TestRecordedMarkersFollowSurvivingRevisions(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, w, s, _, ctx := negotiatedFixture(t)
	receiptHolder(s)
	in := inboundOn(w.key.ChatID, nil, 1, "pending voice")
	id, err := b.Queue.AppendTracked(queueRouteKey(w.key), in, "voice-file")
	if err != nil {
		t.Fatal(err)
	}
	second := negotiatedAppend(t, w, 2, "recorded text")
	group := &attemptFetchGroup{token: b.mintDeliveryToken()}
	reserveFetch(t, w, s, group, -1)
	dir := b.Queue.RetentionDir()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir, []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	w.handleAttemptResult(fetchResult(s, group.token))
	w.retireAttemptToken(group.token)
	w.retireAttemptToken(group.token)
	if n, err := b.noticePending(w.key, s); n != 0 || err != nil {
		t.Fatal(n, err)
	}
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if resolved, _, err := b.Queue.ResolveVoiceText(queueRouteKey(w.key), id, "voice-file", "enriched"); err != nil || !resolved {
		t.Fatal(resolved, err)
	}
	rows, err := b.queuedRows(w.key)
	if err != nil || len(rows) != 1 || rows[0].RecordID != id {
		t.Fatal("new revision was hidden", rows, err)
	}
	b.recorded.mu.Lock()
	n := len(b.recorded.routes[w.key])
	b.recorded.mu.Unlock()
	if n != 1 {
		t.Fatal("old revision marker was not pruned", n)
	}
	before := w.shadowRows()
	if _, err := b.Queue.RemoveRecordIDs(queueRouteKey(w.key), []string{second}); err != nil {
		t.Fatal(err)
	}
	w.shadowRemoval(before, "", "drain")
	b.recorded.mu.Lock()
	n = len(b.recorded.routes)
	b.recorded.mu.Unlock()
	if n != 0 {
		t.Fatal("removed row marker was retained", n)
	}
	// The surviving new voice revision can be fetched and confirmed normally.
	next := &attemptFetchGroup{token: b.mintDeliveryToken()}
	result := reserveFetch(t, w, s, next, -1)
	if len(result.Messages) != 1 || result.Messages[0].Text != "enriched" {
		t.Fatal(result)
	}
	w.handleAttemptResult(fetchResult(s, next.token))
	w.scheduleAttempt(ctx, false)
	b.recorded.mu.Lock()
	n = len(b.recorded.routes)
	b.recorded.mu.Unlock()
	if n != 0 {
		t.Fatal("successful retirement retained markers", n)
	}
}

func TestLegacyReleaseClosesExpiredObservation(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, w, s, frames := shadowFixture(t)
	b.Workers.Stop()
	f := shadowPushOne(t, w, frames, time.Now())
	w.updateAttempt(f.DeliveryToken, func(a *attemptRecord) { a.Deadline = time.Now().Add(-time.Second) })
	b.attempts.release(s, "holder_death", time.Now())
	a := b.attempts.lookup(f.DeliveryToken, time.Now())[0]
	if a.Outcome != "released" || a.Reason != "holder_death" {
		t.Fatal("expired observation survived release", a)
	}
}
