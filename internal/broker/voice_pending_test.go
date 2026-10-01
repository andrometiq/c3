package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/version"
)

// Phase 4 (F4): a voice note held for minutes is visible on the status line
// (health.json) and, on Telegram, gets one "taking longer" reply.

func requirePendingSummary(t *testing.T, s *VoiceScheduler, wantOldest time.Time, wantRetrying bool) {
	t.Helper()
	oldest, retrying := s.PendingSummary()
	if !oldest.Equal(wantOldest) || retrying != wantRetrying {
		t.Fatalf("PendingSummary = (%v, %v); want (%v, %v)", oldest, retrying, wantOldest, wantRetrying)
	}
}

// Idle: the file is byte-identical to the shape before the voice fields existed.
func TestWriteHealthFile_IdleVoiceIsByteIdentical(t *testing.T) {
	hf := filepath.Join(t.TempDir(), "health.json")
	t.Setenv("C3_HEALTH_FILE", hf)
	b := newHealthTestBroker(t)
	b.WriteHealthFile()
	raw, err := os.ReadFile(hf)
	if err != nil {
		t.Fatal(err)
	}
	got := readHealthFile(t, hf)
	want := fmt.Sprintf(`{"broker_pid":%d,"written_unix":%d,"channels":{},"version":%q}`, os.Getpid(), got.WrittenUnix, version.Current())
	if string(raw) != want {
		t.Fatalf("idle health.json =\n%s\nwant\n%s", raw, want)
	}

	b.Voice = nil
	b.WriteHealthFile() // nil-safe
	if got := readHealthFile(t, hf); got.VoicePendingSinceUnix != 0 || got.VoiceRetrying {
		t.Fatalf("nil scheduler wrote voice fields: %+v", got)
	}
}

// A first attempt still running after 3 minutes is pending (not retrying), and
// health.json carries its arrival.
func TestVoicePending_LongFirstAttemptIsPending(t *testing.T) {
	hf := filepath.Join(t.TempDir(), "health.json")
	t.Setenv("C3_HEALTH_FILE", hf)
	b, scheduler, clock := schedulerHarness(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var started atomic.Int64
	scheduler.runAttempt = func(context.Context, voiceAttempt) voiceAttemptResult {
		started.Add(1)
		<-release
		return voiceAttemptResult{transient: true}
	}
	arrived := clock.Now()
	route, in, att := schedulerVoice(9001, "voice-long")
	if !scheduler.ScheduleAuto(route, "record-long", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	waitForVoiceCondition(t, "attempt start", func() bool { return started.Load() == 1 })
	clock.Advance(3 * time.Minute)

	requirePendingSummary(t, scheduler, arrived, false)
	b.WriteHealthFile()
	got := readHealthFile(t, hf)
	if got.VoicePendingSinceUnix != arrived.Unix() || got.VoiceRetrying {
		t.Fatalf("health.json voice fields = %d/%v; want %d/false", got.VoicePendingSinceUnix, got.VoiceRetrying, arrived.Unix())
	}
}

// A note queued behind two busy runners counts from its arrival, not from
// when an attempt finally starts.
func TestVoicePending_QueuedBehindBusyRunnersCountsFromArrival(t *testing.T) {
	_, scheduler, clock := schedulerHarness(t)
	scheduler.submit = schedulerCompletingSubmit(scheduler, nil)
	releaseBusy := make(chan struct{})
	releaseQueued := make(chan struct{})
	t.Cleanup(func() { close(releaseQueued) })
	queuedStarted := make(chan time.Time, 1)
	var started atomic.Int64
	scheduler.runAttempt = func(_ context.Context, attempt voiceAttempt) voiceAttemptResult {
		started.Add(1)
		if attempt.key.fileID == "voice-queued" {
			queuedStarted <- clock.Now()
			<-releaseQueued
		} else {
			<-releaseBusy
		}
		return voiceAttemptResult{success: true, transcript: "ok", segmentText: "[Transcribed voice]: ok"}
	}
	busyArrived := clock.Now()
	for i, fileID := range []string{"voice-busy-1", "voice-busy-2"} {
		route, in, att := schedulerVoice(int64(9100+i), fileID)
		if !scheduler.ScheduleAuto(route, "record-"+fileID, in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
			t.Fatal("schedule rejected")
		}
	}
	waitForVoiceCondition(t, "both runners busy", func() bool { return started.Load() == 2 })
	clock.Advance(time.Minute)
	queuedArrived := clock.Now()
	route, in, att := schedulerVoice(9110, "voice-queued")
	if !scheduler.ScheduleAuto(route, "record-queued", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	clock.Advance(3 * time.Minute)
	time.Sleep(10 * time.Millisecond)
	if started.Load() != 2 {
		t.Fatalf("queued note started early: attempts=%d", started.Load())
	}
	requirePendingSummary(t, scheduler, busyArrived, false)

	close(releaseBusy)
	select {
	case startedAt := <-queuedStarted:
		if !startedAt.After(queuedArrived) {
			t.Fatalf("queued note started at %v, not after waiting from %v", startedAt, queuedArrived)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("queued note never started")
	}
	waitForVoiceCondition(t, "busy notes to resolve", func() bool {
		oldest, _ := scheduler.PendingSummary()
		return oldest.Equal(queuedArrived)
	})
}

// After a proven failure the oldest note reads as retrying.
func TestVoicePending_AfterFailureIsRetrying(t *testing.T) {
	_, scheduler, clock := schedulerHarness(t)
	scheduler.jitter = func(d time.Duration) time.Duration { return d }
	scheduler.runAttempt = func(context.Context, voiceAttempt) voiceAttemptResult {
		return voiceAttemptResult{transient: true, detail: "network is unreachable"}
	}
	arrived := clock.Now()
	route, in, att := schedulerVoice(9201, "voice-retrying")
	key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
	if !scheduler.ScheduleAuto(route, "record-retrying", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	waitParked(t, scheduler, key, clock.Now().Add(voiceRetryBase))
	requirePendingSummary(t, scheduler, arrived, true)
}

// A note whose transcription finished but whose resolve is being retried is
// not counted.
func TestVoicePending_ResolutionRetryNotCounted(t *testing.T) {
	_, scheduler, _ := schedulerHarness(t)
	scheduler.resolveDelay = time.Hour
	scheduler.runAttempt = func(context.Context, voiceAttempt) voiceAttemptResult {
		return voiceAttemptResult{success: true, transcript: "done", segmentText: "[Transcribed voice]: done"}
	}
	var submits atomic.Int64
	scheduler.submit = func(RouteKey, Job) bool { submits.Add(1); return false }
	route, in, att := schedulerVoice(9301, "voice-resolving")
	key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
	if !scheduler.ScheduleAuto(route, "record-resolving", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	waitForVoiceCondition(t, "failed resolve submission", func() bool { return submits.Load() == 1 })
	waitForVoiceCondition(t, "resolve retry state", func() bool {
		state, _, ok := schedulerEntrySnapshot(scheduler, key)
		return ok && state == voiceResolveReady
	})
	requirePendingSummary(t, scheduler, time.Time{}, false)
}

func slowNoticeReplies(g *gateChannel) []c3types.ReplyArgs {
	var out []c3types.ReplyArgs
	for _, reply := range g.sendRepliesSnapshot() {
		if reply.Text == voiceSlowNoticeText {
			out = append(out, reply)
		}
	}
	return out
}

// The reply goes out once per note, on its first transient park at least 2
// minutes after arrival — including the handler-unavailable park, which
// fetches nothing — and quotes the voice message.
func TestVoiceSlowNotice_OnceAfterTwoMinutes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) *gateChannel
	}{
		{"fetch transient", func(*testing.T) *gateChannel {
			return newGateChannel(0, &channel.AttachmentTransientError{Err: errors.New("dial tcp: connection refused")})
		}},
		{"handler missing", func(*testing.T) *gateChannel { return newGateChannel(100, nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := tc.setup(t)
			b, scheduler, clock, _ := fetchHarness(t, g)
			if tc.name == "handler missing" {
				b.Plugins.OnVoiceReady(func() bool { return false })
			}
			rec := (&recordingSTT{}).register(b)
			scheduler.retryBase = voiceSlowNoticeAfter - time.Second
			route, in, att := schedulerVoice(9401, "voice-slow")
			key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
			if !scheduler.ScheduleAuto(route, "record-slow", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
				t.Fatal("schedule rejected")
			}

			// First park at +0: too early.
			waitParked(t, scheduler, key, clock.Now().Add(scheduler.retryBase))
			// Second park at +1m59s: still too early.
			clock.Advance(scheduler.retryBase)
			waitParked(t, scheduler, key, clock.Now().Add(2*scheduler.retryBase))
			time.Sleep(20 * time.Millisecond)
			if got := slowNoticeReplies(g); len(got) != 0 {
				t.Fatalf("reply sent before 2 minutes: %+v", got)
			}
			// Third park at +5m57s: the reply.
			clock.Advance(2 * scheduler.retryBase)
			waitForVoiceCondition(t, "slow-note reply", func() bool { return len(slowNoticeReplies(g)) == 1 })
			reply := slowNoticeReplies(g)[0]
			if reply.ChatID != in.ChatID || reply.ReplyTo == nil || *reply.ReplyTo != in.MessageID {
				t.Fatalf("reply = %+v; want a quote of chat %d msg %d", reply, in.ChatID, in.MessageID)
			}
			// Later parks (backoff now at its cap) never repeat it.
			waitParked(t, scheduler, key, clock.Now().Add(scheduler.retryCap))
			clock.Advance(scheduler.retryCap)
			waitForVoiceCondition(t, "a fourth park", func() bool {
				_, due, ok := schedulerEntrySnapshot(scheduler, key)
				return ok && due.After(clock.Now())
			})
			time.Sleep(20 * time.Millisecond)
			if got := slowNoticeReplies(g); len(got) != 1 {
				t.Fatalf("slow-note replies = %d; want exactly 1", len(got))
			}
			if rec.count() != 0 {
				t.Fatalf("STT ran %d time(s) on a parked note", rec.count())
			}
		})
	}
}

// A failed reply is never retried.
func TestVoiceSlowNotice_FailedSendNotRetried(t *testing.T) {
	g := newGateChannel(0, &channel.AttachmentTransientError{Err: errors.New("dial tcp: connection refused")})
	g.sendReplyErr = errors.New("telegram: connection setup failed")
	b, scheduler, clock, _ := fetchHarness(t, g)
	(&recordingSTT{}).register(b)
	scheduler.retryBase = voiceSlowNoticeAfter
	route, in, att := schedulerVoice(9501, "voice-slow-fail")
	key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
	if !scheduler.ScheduleAuto(route, "record-slow-fail", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	waitParked(t, scheduler, key, clock.Now().Add(voiceSlowNoticeAfter))
	clock.Advance(voiceSlowNoticeAfter)
	waitForVoiceCondition(t, "the one reply attempt", func() bool { return len(slowNoticeReplies(g)) == 1 })
	waitParked(t, scheduler, key, clock.Now().Add(2*voiceSlowNoticeAfter))
	clock.Advance(2 * voiceSlowNoticeAfter)
	waitParked(t, scheduler, key, clock.Now().Add(scheduler.retryCap))
	time.Sleep(20 * time.Millisecond)
	if got := len(slowNoticeReplies(g)); got != 1 {
		t.Fatalf("reply attempts = %d; want 1 (never retried)", got)
	}
}

// A transcript-only retranscribe has no chat message to answer.
func TestVoiceSlowNotice_NeverForTranscriptOnly(t *testing.T) {
	g := newGateChannel(0, &channel.AttachmentTransientError{Err: errors.New("dial tcp: connection refused")})
	b, scheduler, clock, _ := fetchHarness(t, g)
	(&recordingSTT{}).register(b)
	scheduler.retryBase = voiceSlowNoticeAfter
	route := MakeRouteKey("telegram", 0, nil)
	att := c3types.Attachment{Kind: "voice", FileID: "voice-transcript-only"}
	in := c3types.Inbound{Channel: "telegram", Attachments: []c3types.Attachment{att}}
	key := voiceScheduleKey{route: route, fileID: att.FileID}
	if err := scheduler.ScheduleManual(route, "", in, att, true, make(chan voiceScheduleResult, 1)); err != nil {
		t.Fatal(err)
	}
	waitParked(t, scheduler, key, clock.Now().Add(voiceSlowNoticeAfter))
	clock.Advance(voiceSlowNoticeAfter)
	waitParked(t, scheduler, key, clock.Now().Add(2*voiceSlowNoticeAfter))
	time.Sleep(20 * time.Millisecond)
	if got := g.sendRepliesSnapshot(); len(got) != 0 {
		t.Fatalf("transcript-only note got a chat reply: %+v", got)
	}
}

type blockingVoiceReplyChannel struct {
	*gateChannel
	entered chan struct{}
	release chan struct{}
}

func (c *blockingVoiceReplyChannel) SendReply(args c3types.ReplyArgs) (int64, error) {
	c.entered <- struct{}{}
	<-c.release
	return c.gateChannel.SendReply(args)
}

// A reply stuck in the channel never stalls the scheduler.
func TestVoiceSlowNotice_BlockingChannelDoesNotStallScheduler(t *testing.T) {
	var fetches atomic.Int64
	g := newGateChannel(0, nil)
	g.answer = func(string) (int64, error) {
		if fetches.Add(1) <= 2 {
			return 0, &channel.AttachmentTransientError{Err: errors.New("dial tcp: connection refused")}
		}
		return 100, nil
	}
	b, scheduler, clock, _ := fetchHarness(t, g)
	blocking := &blockingVoiceReplyChannel{gateChannel: g, entered: make(chan struct{}, 1), release: make(chan struct{})}
	t.Cleanup(func() { close(blocking.release) })
	b.chMu.Lock()
	b.channels["telegram"] = &channelRegistration{Channel: blocking}
	b.chMu.Unlock()
	rec := (&recordingSTT{}).register(b)
	jobs := make(chan *ResolveVoiceJob, 1)
	scheduler.submit = schedulerCompletingSubmit(scheduler, jobs)
	scheduler.retryBase = voiceSlowNoticeAfter
	route, in, att := schedulerVoice(9601, "voice-slow-blocking")
	key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
	if !scheduler.ScheduleAuto(route, "record-slow-blocking", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	waitParked(t, scheduler, key, clock.Now().Add(voiceSlowNoticeAfter))
	clock.Advance(voiceSlowNoticeAfter)
	select {
	case <-blocking.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("slow-note reply never reached the channel")
	}
	// While the reply stays blocked, keep time moving without touching s.mu
	// here (so a stalled scheduler fails the test instead of hanging it): the
	// retry must still run and resolve.
	deadline := time.Now().Add(3 * time.Second)
	for job := (*ResolveVoiceJob)(nil); job == nil; {
		select {
		case job = <-jobs:
			if !job.Success {
				t.Fatalf("resolve = %+v", job)
			}
		default:
			if time.Now().After(deadline) {
				t.Fatal("scheduler stalled behind a blocked reply")
			}
			clock.Advance(time.Minute)
			time.Sleep(5 * time.Millisecond)
		}
	}
	if rec.count() != 1 {
		t.Fatalf("STT calls = %d; want 1", rec.count())
	}
}

// A note recovered after a restart keeps its original age: arrival moves back
// to when the message was received, never forward.
func TestVoicePending_RecoveredNoteKeepsItsAge(t *testing.T) {
	_, scheduler, clock := schedulerHarness(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	scheduler.runAttempt = func(context.Context, voiceAttempt) voiceAttemptResult {
		<-release
		return voiceAttemptResult{transient: true}
	}
	route, in, att := schedulerVoice(9101, "voice-recovered")
	if !scheduler.ScheduleAuto(route, "record-recovered", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	received := clock.Now().Add(-3 * time.Hour)
	scheduler.backdateArrival(route, in.MessageID, []c3types.Attachment{att}, received)
	requirePendingSummary(t, scheduler, received, false)

	scheduler.backdateArrival(route, in.MessageID, []c3types.Attachment{att}, clock.Now().Add(time.Hour))
	requirePendingSummary(t, scheduler, received, false)
}
