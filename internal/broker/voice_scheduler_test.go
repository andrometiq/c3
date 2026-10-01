package broker

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/mappings"
)

type fakeVoiceClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*fakeVoiceTimer]struct{}
}

type fakeVoiceTimer struct {
	clock  *fakeVoiceClock
	ch     chan time.Time
	due    time.Time
	active bool
}

func newFakeVoiceClock() *fakeVoiceClock {
	return &fakeVoiceClock{
		now:    time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
		timers: make(map[*fakeVoiceTimer]struct{}),
	}
}

func (c *fakeVoiceClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeVoiceClock) NewTimer(d time.Duration) voiceSchedulerTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &fakeVoiceTimer{clock: c, ch: make(chan time.Time, 1), due: c.now.Add(d), active: true}
	c.timers[timer] = struct{}{}
	return timer
}

func (c *fakeVoiceClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var due []*fakeVoiceTimer
	for timer := range c.timers {
		if timer.active && !now.Before(timer.due) {
			timer.active = false
			due = append(due, timer)
		}
	}
	c.mu.Unlock()
	for _, timer := range due {
		select {
		case timer.ch <- now:
		default:
		}
	}
}

func (t *fakeVoiceTimer) C() <-chan time.Time { return t.ch }

func (t *fakeVoiceTimer) Reset(d time.Duration) {
	t.clock.mu.Lock()
	t.due = t.clock.now.Add(d)
	t.active = true
	t.clock.mu.Unlock()
}

func (t *fakeVoiceTimer) Stop() {
	t.clock.mu.Lock()
	t.active = false
	t.clock.mu.Unlock()
}

func schedulerHarness(t *testing.T) (*Broker, *VoiceScheduler, *fakeVoiceClock) {
	t.Helper()
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := newTestBroker(t, &mappings.MappingsFile{SchemaVersion: 1})
	b.Voice.Stop()
	clock := newFakeVoiceClock()
	b.Voice = newVoiceScheduler(b, clock)
	t.Cleanup(b.Shutdown)
	return b, b.Voice, clock
}

func schedulerVoice(messageID int64, fileID string) (RouteKey, c3types.Inbound, c3types.Attachment) {
	route := MakeRouteKey("telegram", -100, nil)
	att := c3types.Attachment{Kind: "voice", FileID: fileID, MIME: "audio/ogg", Size: 100}
	in := c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: messageID,
		Attachments: []c3types.Attachment{att},
	}
	return route, in, att
}

type webVoiceChannel struct{ *fakeChannel }

func (*webVoiceChannel) Name() string { return "web" }

// A channel that cannot fetch voice gets no broker fetch and no local file; its
// plugin resolves the audio itself (web: LocalAudioProvider).
func TestVoiceSchedulerChannelWithoutFetcherPassesNoLocalFile(t *testing.T) {
	b, scheduler, _ := schedulerHarness(t)
	b.chMu.Lock()
	b.channels["web"] = &channelRegistration{Channel: &webVoiceChannel{fakeChannel: &fakeChannel{}}}
	b.chMu.Unlock()
	var payload c3types.VoicePayload
	b.Plugins.OnVoiceReceived(func(_ context.Context, current c3types.VoicePayload) (string, error) {
		payload = current
		return "local transcript", nil
	})
	att := c3types.Attachment{Kind: "voice", FileID: "local-id", MIME: "audio/ogg", Size: 123}
	inbound := c3types.Inbound{Channel: "web", ChatID: 42, MessageID: 8, Attachments: []c3types.Attachment{att}}
	result := scheduler.transcribe(context.Background(), voiceAttempt{
		key:     voiceScheduleKey{route: MakeRouteKey("web", 42, nil), messageID: 8, fileID: att.FileID},
		inbound: inbound, attachment: att,
	})
	if !result.success || payload.Size != att.Size || payload.Channel != "web" || payload.LocalPath != "" {
		t.Fatalf("result/payload=%+v/%+v", result, payload)
	}
}

func schedulerCompletingSubmit(s *VoiceScheduler, jobs chan<- *ResolveVoiceJob) func(RouteKey, Job) bool {
	return func(_ RouteKey, wrapped Job) bool {
		job := wrapped.ResolveVoice
		if jobs != nil {
			jobs <- job
		}
		for _, target := range job.Targets {
			s.markResolveApplied(job.Key, target.recordID)
		}
		s.completeResolve(job.Key)
		s.resolveJobDone()
		return true
	}
}

func schedulerEntrySnapshot(s *VoiceScheduler, key voiceScheduleKey) (voiceEntryState, time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.entries[key]
	if entry == nil {
		return 0, time.Time{}, false
	}
	return entry.state, entry.nextAttempt, true
}

// Plan test (i): health gates nothing. With the channel DOWN the whole time, a
// transient failure parks with backoff; expiry is anchored only by a real
// failure; RetryNow pulls a 5-minute retry for that channel to now.
func TestVoiceSchedulerBackoffParksAndRetryNowPullsForward(t *testing.T) {
	b, scheduler, clock := schedulerHarness(t)
	b.setLastHealth(c3types.HealthEvent{Channel: "telegram", State: c3types.HealthStateDown, Since: clock.Now()})
	scheduler.jitter = func(d time.Duration) time.Duration { return d }
	scheduler.retryBase = 5 * time.Minute

	release := make(chan struct{})
	var attempts atomic.Int64
	scheduler.runAttempt = func(context.Context, voiceAttempt) voiceAttemptResult {
		if attempts.Add(1) == 1 {
			<-release
			return voiceAttemptResult{transient: true, detail: "network is unreachable"}
		}
		return voiceAttemptResult{success: true, transcript: "done", segmentText: "[Transcribed voice]: done"}
	}
	scheduler.submit = schedulerCompletingSubmit(scheduler, nil)
	route, in, att := schedulerVoice(1, "voice-1")
	key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
	if !scheduler.ScheduleAuto(route, "record-1", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	waitForVoiceCondition(t, "first attempt despite DOWN health", func() bool { return attempts.Load() == 1 })
	firstFailure := func() time.Time {
		scheduler.mu.Lock()
		defer scheduler.mu.Unlock()
		return scheduler.entries[key].firstFailure
	}
	clock.Advance(2 * time.Minute)
	if !firstFailure().IsZero() {
		t.Fatal("expiry was anchored before any real failure")
	}

	close(release)
	waitForVoiceCondition(t, "transient failure to park for 5 minutes", func() bool {
		state, due, ok := schedulerEntrySnapshot(scheduler, key)
		return ok && state == voiceWaiting && due.Equal(clock.Now().Add(5*time.Minute))
	})
	if !firstFailure().Equal(clock.Now()) {
		t.Fatalf("firstFailure=%v, want the failure time %v", firstFailure(), clock.Now())
	}
	clock.Advance(time.Minute)
	scheduler.RetryNow("web")
	time.Sleep(10 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("parked retry ran early (or RetryNow crossed channels): attempts=%d", got)
	}

	scheduler.RetryNow("telegram")
	waitForVoiceCondition(t, "RetryNow to run the parked retry", func() bool { return attempts.Load() == 2 })
	waitForVoiceCondition(t, "resolve completion", func() bool {
		_, _, ok := schedulerEntrySnapshot(scheduler, key)
		return !ok
	})
}

func TestVoiceSchedulerExpiryResolvesExactlyOnce(t *testing.T) {
	_, scheduler, clock := schedulerHarness(t)
	scheduler.setRetryExpiry(90 * time.Second)
	scheduler.jitter = func(d time.Duration) time.Duration { return d }
	var attempts atomic.Int64
	scheduler.runAttempt = func(context.Context, voiceAttempt) voiceAttemptResult {
		attempts.Add(1)
		return voiceAttemptResult{transient: true, detail: "network is unreachable"}
	}
	jobs := make(chan *ResolveVoiceJob, 2)
	scheduler.submit = schedulerCompletingSubmit(scheduler, jobs)
	route, in, att := schedulerVoice(2, "voice-expiry")
	if !scheduler.ScheduleAuto(route, "record-expiry", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	waitForVoiceCondition(t, "first transient attempt", func() bool { return attempts.Load() == 1 })
	clock.Advance(30 * time.Second)
	waitForVoiceCondition(t, "second transient attempt", func() bool { return attempts.Load() == 2 })
	clock.Advance(time.Minute)

	var job *ResolveVoiceJob
	select {
	case job = <-jobs:
	case <-time.After(3 * time.Second):
		t.Fatal("expiry did not submit a terminal resolve")
	}
	if len(job.Targets) != 1 || !strings.Contains(job.Targets[0].group.failNotice(), "retry expired") || job.Success {
		t.Fatalf("expiry resolve=%+v; want one terminal failure and human notice", job)
	}
	if job.SegmentText == "" {
		t.Fatal("expiry resolve has empty durable failure text")
	}
	clock.Advance(24 * time.Hour)
	time.Sleep(10 * time.Millisecond)
	select {
	case extra := <-jobs:
		t.Fatalf("expiry resolved more than once: %+v", extra)
	default:
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("expiry ran another provider attempt; got %d want 2", got)
	}
}

func TestVoiceSchedulerResolveSubmitFailureDoesNotRerunSTT(t *testing.T) {
	_, scheduler, clock := schedulerHarness(t)
	scheduler.resolveDelay = 10 * time.Second
	var attempts atomic.Int64
	scheduler.runAttempt = func(context.Context, voiceAttempt) voiceAttemptResult {
		attempts.Add(1)
		return voiceAttemptResult{success: true, transcript: "once", segmentText: "[Transcribed voice]: once"}
	}
	var submits atomic.Int64
	scheduler.submit = func(_ RouteKey, wrapped Job) bool {
		if submits.Add(1) == 1 {
			return false
		}
		for _, target := range wrapped.ResolveVoice.Targets {
			scheduler.markResolveApplied(wrapped.ResolveVoice.Key, target.recordID)
		}
		scheduler.completeResolve(wrapped.ResolveVoice.Key)
		scheduler.resolveJobDone()
		return true
	}
	route, in, att := schedulerVoice(3, "voice-submit")
	key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
	if !scheduler.ScheduleAuto(route, "record-submit", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	waitForVoiceCondition(t, "failed resolve submission", func() bool { return submits.Load() == 1 })
	clock.Advance(9 * time.Second)
	time.Sleep(10 * time.Millisecond)
	if got := submits.Load(); got != 1 {
		t.Fatalf("resolve resubmitted before due: %d", got)
	}
	clock.Advance(time.Second)
	waitForVoiceCondition(t, "resolve resubmission", func() bool { return submits.Load() == 2 })
	if got := attempts.Load(); got != 1 {
		t.Fatalf("resolve submission failure reran STT %d times; want 1", got)
	}
	waitForVoiceCondition(t, "resolve completion", func() bool {
		_, _, ok := schedulerEntrySnapshot(scheduler, key)
		return !ok
	})
}

func TestVoiceSchedulerManualJoinsInflightAuto(t *testing.T) {
	_, scheduler, _ := schedulerHarness(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var attempts atomic.Int64
	scheduler.runAttempt = func(context.Context, voiceAttempt) voiceAttemptResult {
		attempts.Add(1)
		once.Do(func() { close(started) })
		<-release
		return voiceAttemptResult{success: true, transcript: "shared", segmentText: "[Transcribed voice]: shared"}
	}
	scheduler.submit = schedulerCompletingSubmit(scheduler, nil)
	route, in, att := schedulerVoice(4, "voice-singleflight")
	if !scheduler.ScheduleAuto(route, "record-singleflight", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("auto schedule rejected")
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("auto attempt did not start")
	}
	hook := make(chan voiceScheduleResult, 1)
	if err := scheduler.ScheduleManual(route, "record-singleflight", in, att, false, hook); err != nil {
		t.Fatal("manual schedule did not join the existing lease")
	}
	close(release)
	select {
	case result := <-hook:
		if result.Err != nil || result.Transcript != "shared" {
			t.Fatalf("manual hook result=%+v", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("manual hook did not receive the shared result")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("manual+auto made %d provider calls; want one", got)
	}
}

func TestVoiceSchedulerUsesFixedTwoRunnerBound(t *testing.T) {
	_, scheduler, _ := schedulerHarness(t)
	release := make(chan struct{})
	var calls atomic.Int64
	var active atomic.Int64
	var maxActive atomic.Int64
	scheduler.runAttempt = func(context.Context, voiceAttempt) voiceAttemptResult {
		calls.Add(1)
		current := active.Add(1)
		for {
			old := maxActive.Load()
			if current <= old || maxActive.CompareAndSwap(old, current) {
				break
			}
		}
		<-release
		active.Add(-1)
		return voiceAttemptResult{success: true, transcript: "ok", segmentText: "[Transcribed voice]: ok"}
	}
	scheduler.submit = schedulerCompletingSubmit(scheduler, nil)
	before := runtime.NumGoroutine()
	for i := int64(0); i < 10; i++ {
		route, in, att := schedulerVoice(100+i, "voice-bound-"+time.Unix(i, 0).Format("150405"))
		if !scheduler.ScheduleAuto(route, "record", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
			t.Fatalf("schedule %d rejected", i)
		}
	}
	waitForVoiceCondition(t, "both fixed runners to become active", func() bool { return active.Load() == sttRetryConcurrency })
	if got := maxActive.Load(); got != sttRetryConcurrency {
		t.Fatalf("maximum active attempts=%d; want %d", got, sttRetryConcurrency)
	}
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("scheduling ten parked notes added per-note goroutines: before=%d after=%d", before, after)
	}
	close(release)
	waitForVoiceCondition(t, "all bounded attempts", func() bool { return calls.Load() == 10 })
}

func TestVoiceSchedulerRunnerPanicRetriesEntryAndKeepsBothRunners(t *testing.T) {
	b, scheduler, clock := schedulerHarness(t)
	scheduler.jitter = func(d time.Duration) time.Duration { return d }
	var callbackCalls atomic.Int64
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		if callbackCalls.Add(1) == 1 {
			panic("injected voice callback panic")
		}
		return "survived panic", nil
	})
	scheduler.submit = schedulerCompletingSubmit(scheduler, nil)
	route, in, att := schedulerVoice(401, "voice-panic")
	key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
	if !scheduler.ScheduleAuto(route, "record-panic", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	waitForVoiceCondition(t, "panicked attempt to return to waiting", func() bool {
		state, due, ok := schedulerEntrySnapshot(scheduler, key)
		return ok && state == voiceWaiting && due.Equal(clock.Now().Add(voiceRetryBase))
	})
	clock.Advance(voiceRetryBase)
	waitForVoiceCondition(t, "panicked entry to retry and resolve", func() bool {
		_, _, ok := schedulerEntrySnapshot(scheduler, key)
		return callbackCalls.Load() == 2 && !ok
	})

	release := make(chan struct{})
	var active atomic.Int64
	scheduler.runAttempt = func(context.Context, voiceAttempt) voiceAttemptResult {
		active.Add(1)
		<-release
		active.Add(-1)
		return voiceAttemptResult{success: true, transcript: "ok", segmentText: "[Transcribed voice]: ok"}
	}
	for i := int64(0); i < 2; i++ {
		r, next, voice := schedulerVoice(500+i, fmt.Sprintf("post-panic-%d", i))
		if !scheduler.ScheduleAuto(r, fmt.Sprintf("record-%d", i), next, []c3types.Attachment{voice}, "", voiceEchoReservation{}) {
			t.Fatal("post-panic schedule rejected")
		}
	}
	waitForVoiceCondition(t, "both supervised runners after panic", func() bool { return active.Load() == sttRetryConcurrency })
	close(release)
}

// panicOnceClock panics on its panicAt-th Now call. A fresh scheduler's
// dispatcher makes call 1 when it starts and call 2 inside its first cycle.
type panicOnceClock struct {
	*fakeVoiceClock
	panicAt int64
	calls   atomic.Int64
	fired   atomic.Bool
}

func (c *panicOnceClock) Now() time.Time {
	if c.calls.Add(1) == c.panicAt {
		c.fired.Store(true)
		panic("injected dispatcher clock panic")
	}
	return c.fakeVoiceClock.Now()
}

func TestVoiceSchedulerDispatcherPanicRecoversInLoop(t *testing.T) {
	b, _, _ := schedulerHarness(t)
	b.Voice.Stop()
	clock := &panicOnceClock{fakeVoiceClock: newFakeVoiceClock(), panicAt: 2}
	scheduler := newVoiceScheduler(b, clock)
	b.Voice = scheduler
	waitForVoiceCondition(t, "dispatcher to hit the injected panic", clock.fired.Load)

	var attempts atomic.Int64
	scheduler.runAttempt = func(context.Context, voiceAttempt) voiceAttemptResult {
		attempts.Add(1)
		return voiceAttemptResult{success: true, transcript: "ok", segmentText: "[Transcribed voice]: ok"}
	}
	scheduler.submit = schedulerCompletingSubmit(scheduler, nil)
	route, in, att := schedulerVoice(402, "dispatcher-panic")
	key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
	if !scheduler.ScheduleAuto(route, "record", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	waitForVoiceCondition(t, "dispatcher to continue after panic", func() bool {
		_, _, ok := schedulerEntrySnapshot(scheduler, key)
		return attempts.Load() == 1 && !ok
	})
}

func TestVoiceSchedulerFullWorkQueueUsesFixedBackoff(t *testing.T) {
	_, scheduler, clock := schedulerHarness(t)
	release := make(chan struct{})
	defer close(release)
	scheduler.runAttempt = func(context.Context, voiceAttempt) voiceAttemptResult {
		<-release
		return voiceAttemptResult{success: true, transcript: "ok", segmentText: "[Transcribed voice]: ok"}
	}
	scheduler.submit = schedulerCompletingSubmit(scheduler, nil)
	for i := int64(0); i < voiceWorkQueueSize+sttRetryConcurrency+8; i++ {
		route, in, att := schedulerVoice(600+i, fmt.Sprintf("backlog-%d", i))
		if !scheduler.ScheduleAuto(route, fmt.Sprintf("record-%d", i), in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
			t.Fatal("schedule rejected")
		}
	}
	waitForVoiceCondition(t, "overflow entries to receive dispatcher backoff", func() bool {
		scheduler.mu.Lock()
		defer scheduler.mu.Unlock()
		sawBackoff := false
		for _, entry := range scheduler.entries {
			if entry.state != voiceWaiting {
				continue
			}
			if entry.nextAttempt.Before(clock.Now().Add(voiceDispatchBackoff)) {
				return false
			}
			sawBackoff = true
		}
		return sawBackoff
	})
	if delay := scheduler.nextDelay(clock.Now(), clock.Now().Add(time.Hour)); delay < voiceDispatchBackoff {
		t.Fatalf("full work queue left a dispatcher spin delay of %s; want >= %s", delay, voiceDispatchBackoff)
	}
}

func TestVoiceSchedulerManualHookCapRejectsBeyondBound(t *testing.T) {
	_, scheduler, _ := schedulerHarness(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	scheduler.runAttempt = func(context.Context, voiceAttempt) voiceAttemptResult {
		once.Do(func() { close(started) })
		<-release
		return voiceAttemptResult{success: true, transcript: "ok", segmentText: "[Transcribed voice]: ok"}
	}
	scheduler.submit = schedulerCompletingSubmit(scheduler, nil)
	route, in, att := schedulerVoice(700, "hook-cap")
	if !scheduler.ScheduleAuto(route, "record", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("auto schedule rejected")
	}
	<-started
	for i := 0; i < maxVoiceResultHooks; i++ {
		if err := scheduler.ScheduleManual(route, "record", in, att, false, make(chan voiceScheduleResult, 1)); err != nil {
			t.Fatalf("hook %d rejected before cap: %v", i, err)
		}
	}
	if err := scheduler.ScheduleManual(route, "record", in, att, false, make(chan voiceScheduleResult, 1)); !errors.Is(err, errVoiceResultHookLimit) {
		t.Fatalf("hook beyond cap error = %v, want %v", err, errVoiceResultHookLimit)
	}
	close(release)
}

func TestVoiceSchedulerStopBoundsJoinForIgnoringPlugin(t *testing.T) {
	_, scheduler, _ := schedulerHarness(t)
	started := make(chan struct{})
	release := make(chan struct{})
	scheduler.runAttempt = func(context.Context, voiceAttempt) voiceAttemptResult {
		close(started)
		<-release // deliberately ignores scheduler cancellation
		return voiceAttemptResult{success: true}
	}
	route, in, att := schedulerVoice(800, "shutdown-bound")
	if !scheduler.ScheduleAuto(route, "record", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	<-started
	if scheduler.StopWithin(25 * time.Millisecond) {
		t.Fatal("StopWithin unexpectedly joined a cancellation-ignoring plugin")
	}
	close(release)
	select {
	case <-scheduler.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler did not finish after the ignoring plugin was released")
	}
}

func TestBrokerShutdownJoinsVoiceBeforeStoppingWorkers(t *testing.T) {
	b, scheduler, _ := schedulerHarness(t)
	started := make(chan struct{})
	cancelSeen := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	scheduler.runAttempt = func(ctx context.Context, _ voiceAttempt) voiceAttemptResult {
		once.Do(func() { close(started) })
		<-ctx.Done()
		close(cancelSeen)
		<-release
		return voiceAttemptResult{transient: true}
	}
	route, in, att := schedulerVoice(5, "voice-shutdown")
	if !scheduler.ScheduleAuto(route, "record-shutdown", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("voice attempt did not start")
	}
	done := make(chan struct{})
	go func() {
		b.Shutdown()
		close(done)
	}()
	select {
	case <-cancelSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not cancel voice scheduler")
	}
	b.Workers.mu.Lock()
	workersStopped := b.Workers.stopped
	b.Workers.mu.Unlock()
	if workersStopped {
		t.Fatal("worker pool stopped before the voice attempt joined")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not finish after the voice attempt exited")
	}
	b.Workers.mu.Lock()
	workersStopped = b.Workers.stopped
	b.Workers.mu.Unlock()
	if !workersStopped {
		t.Fatal("worker pool was not stopped after voice scheduler joined")
	}
}
