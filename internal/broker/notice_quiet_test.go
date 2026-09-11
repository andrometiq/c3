package broker

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

type heldReadbackRecorder struct {
	*readbackRecorderChannel
	countsMu sync.Mutex
	counts   []int
}

func (c *heldReadbackRecorder) SendReadbackWithHeld(args c3types.ReadbackArgs, count int) (int64, error) {
	c.countsMu.Lock()
	c.counts = append(c.counts, count)
	c.countsMu.Unlock()
	return c.SendReadback(args)
}

func TestQuietVoicePendingUsesOnlyQuotedReadback(t *testing.T) {
	clearFetchTestEnvironment(t)
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	t.Cleanup(b.Shutdown)
	rc := &heldReadbackRecorder{readbackRecorderChannel: &readbackRecorderChannel{fakeChannel: fc}}
	b.chMu.Lock()
	b.channels["telegram"] = &channelRegistration{Channel: rc}
	b.chMu.Unlock()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	b.Plugins.OnVoiceReceived(func(ctx context.Context, _ c3types.VoicePayload) (string, error) {
		close(entered)
		select {
		case <-release:
			return "The real transcript.", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	key, in, _ := schedulerVoice(51, "voice-file")
	in.ReplyTo = &c3types.ReplyContext{MessageID: 999}
	w := newRouteWorker(b.ctx, key, time.Hour, b)
	t.Cleanup(w.Stop)
	w.flushInbounds(b.ctx, []*c3types.Inbound{&in})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("STT did not start")
	}
	settleNotice(t, b, key)
	if got := fc.sendRepliesSnapshot(); len(got) != 0 {
		t.Fatalf("pending STT sent bot messages: %+v", got)
	}
	once.Do(func() { close(release) })
	readbacks := rc.waitReadbacks(t, 1)
	waitForVoiceCondition(t, "readback finished", func() bool {
		b.notices.mu.Lock()
		defer b.notices.mu.Unlock()
		return b.notices.routes[key].readbacks == 0
	})
	b.evaluateNotices(key, true)
	settleNotice(t, b, key)
	if len(readbacks) != 1 || len(fc.sendRepliesSnapshot()) != 0 {
		t.Fatalf("wanted exactly one bot readback: readbacks=%+v replies=%+v", readbacks, fc.sendRepliesSnapshot())
	}
	if readbacks[0].ReplyTo == nil || *readbacks[0].ReplyTo != in.MessageID || readbacks[0].Transcript != "The real transcript." {
		t.Fatal(readbacks)
	}
	rc.countsMu.Lock()
	defer rc.countsMu.Unlock()
	if len(rc.counts) != 1 || rc.counts[0] != 1 {
		t.Fatalf("readback must carry genuine backlog count: %v", rc.counts)
	}
}

func TestQuietHeldEpisodeQuotesLocalSourceAndRearmsAfterEmpty(t *testing.T) {
	clearFetchTestEnvironment(t)
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	t.Cleanup(b.Shutdown)
	key := drainSrc()
	w := newRouteWorker(b.ctx, key, time.Hour, b)
	t.Cleanup(w.Stop)
	for i := int64(1); i <= 4; i++ {
		in := inboundOn(key.ChatID, &key.TopicID, i, "held")
		in.ReplyTo = &c3types.ReplyContext{MessageID: 999}
		w.forwardOrFallback(b.ctx, in, 1)
		waitNoticeReplies(t, fc, 1)
		settleNotice(t, b, key)
		// Cross more than one former cooldown window without a long wall-clock wait.
		b.HeldNotices.mu.Lock()
		b.HeldNotices.lastByKey[key] = time.Now().Add(-time.Duration(i+1) * defaultHeldNoticeCooldown)
		b.HeldNotices.mu.Unlock()
	}
	replies := fc.sendRepliesSnapshot()
	if len(replies) != 1 || replies[0].ReplyTo == nil || *replies[0].ReplyTo != 1 || strings.Contains(replies[0].Text, "Live route:") {
		t.Fatalf("episode notices: %+v", replies)
	}
	if n, _ := b.Queue.Pending(queueRouteKey(key)); n != 4 {
		t.Fatalf("queued=%d", n)
	}
	fetched := make(chan FetchResult, 1)
	w.handleFetch(b.ctx, &FetchJob{Ack: true, All: true, ResultCh: fetched})
	result := <-fetched
	if result.Err != nil || len(result.Messages) != 4 {
		t.Fatalf("fetch failed: %+v", result)
	}
	settleNotice(t, b, key)
	w.forwardOrFallback(b.ctx, inboundOn(key.ChatID, &key.TopicID, 5, "new episode"), 1)
	replies = waitNoticeReplies(t, fc, 2)
	if replies[1].ReplyTo == nil || *replies[1].ReplyTo != 5 {
		t.Fatalf("new episode quote: %+v", replies[1])
	}
}

func TestQuietHeldSuppressionDoesNotSilenceStorageOrDegradedWarnings(t *testing.T) {
	clearFetchTestEnvironment(t)
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	t.Cleanup(b.Shutdown)
	key := drainSrc()
	w := newRouteWorker(b.ctx, key, time.Hour, b)
	t.Cleanup(w.Stop)
	b.HeldNotices.reserveHeld(key)
	in := inboundOn(key.ChatID, &key.TopicID, 1, "test")
	w.notePersistFailure(in)
	if got := fc.sendRepliesSnapshot(); len(got) != 1 || !strings.Contains(got[0].Text, "storage error") || strings.Contains(got[0].Text, "nothing lost") {
		t.Fatal(got)
	}
	b.Queue = nil
	b.Fallbacks = newFallbackTracker(0)
	w.forwardOrFallback(b.ctx, in, 1)
	replies := fc.sendRepliesSnapshot()
	if len(replies) != 2 || replies[1].Text != heldDegradedText() {
		t.Fatal(replies)
	}
}
