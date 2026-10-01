package broker

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/plugin/builtins/stt"
)

// Phase 3 (F3): when the channel can fetch voice, the broker is the only
// fetcher. Every attempt that reaches STT carries an attempt-owned local file,
// fetch failures are classified by the channel's typed error, and no attempt
// file outlives its attempt.

// fetchHarness is a scheduler on a fake clock with a gate channel that fetches
// voice into the test's TMPDIR.
func fetchHarness(t *testing.T, g *gateChannel) (*Broker, *VoiceScheduler, *fakeVoiceClock, string) {
	t.Helper()
	voiceDir := t.TempDir()
	t.Setenv("TMPDIR", voiceDir)
	b, scheduler, clock := schedulerHarness(t)
	registerGateChannel(b, g)
	scheduler.jitter = func(d time.Duration) time.Duration { return d }
	return b, scheduler, clock, voiceDir
}

// recordingSTT registers a callback that records each payload and whether its
// LocalPath existed while STT ran.
type recordingSTT struct {
	mu       sync.Mutex
	payloads []c3types.VoicePayload
	existed  []bool
	reply    func(c3types.VoicePayload) string
}

func (r *recordingSTT) register(b *Broker) *recordingSTT {
	b.Plugins.OnVoiceReceived(func(_ context.Context, p c3types.VoicePayload) (string, error) {
		_, err := os.Stat(p.LocalPath)
		r.mu.Lock()
		r.payloads = append(r.payloads, p)
		r.existed = append(r.existed, p.LocalPath != "" && err == nil)
		r.mu.Unlock()
		if r.reply != nil {
			return r.reply(p), nil
		}
		return "transcript of " + p.FileID, nil
	})
	return r
}

func (r *recordingSTT) snapshot() ([]c3types.VoicePayload, []bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]c3types.VoicePayload(nil), r.payloads...), append([]bool(nil), r.existed...)
}

func (r *recordingSTT) count() int {
	payloads, _ := r.snapshot()
	return len(payloads)
}

// requireEveryCallWithLocalFile is the F3 invariant: no STT call without an
// existing attempt-owned file.
func (r *recordingSTT) requireEveryCallWithLocalFile(t *testing.T) {
	t.Helper()
	payloads, existed := r.snapshot()
	for i, p := range payloads {
		if !existed[i] {
			t.Fatalf("STT call %d had no existing local file (LocalPath=%q); the handler would fetch over its own network stack", i, p.LocalPath)
		}
	}
}

func requireNoVoiceFiles(t *testing.T, dir string) {
	t.Helper()
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), channel.TempPrefix) {
			t.Errorf("attempt file outlived its attempt: %s", entry.Name())
		}
	}
}

func waitParked(t *testing.T, s *VoiceScheduler, key voiceScheduleKey, due time.Time) {
	t.Helper()
	waitForVoiceCondition(t, "entry parked until "+due.String(), func() bool {
		state, next, ok := schedulerEntrySnapshot(s, key)
		return ok && state == voiceWaiting && next.Equal(due)
	})
}

func transientAfter(after time.Duration) error {
	return &channel.AttachmentTransientError{After: after, Err: errors.New("telegram: GetFile: Too Many Requests: retry after")}
}

func TestVoiceFetch_SuccessHandsSTTTheAttemptFile(t *testing.T) {
	g := newGateChannel(4242, nil)
	b := gateBroker(t, g)
	defer b.Shutdown()
	rec := (&recordingSTT{}).register(b)

	in := voiceInbound(8001, 100)
	flushVoice(t, b, in)

	payloads, _ := rec.snapshot()
	if len(payloads) != 1 {
		t.Fatalf("STT ran %d time(s), want 1", len(payloads))
	}
	rec.requireEveryCallWithLocalFile(t)
	if payloads[0].Size != 4242 {
		t.Fatalf("payload size = %d, want the fetched size 4242", payloads[0].Size)
	}
	if !strings.Contains(in.Text, "transcript of F-BIG") {
		t.Fatalf("transcript missing: %q", in.Text)
	}
	requireNoVoiceFiles(t, os.Getenv("TMPDIR"))
}

// Manual retranscribe takes the same path: a local file, removed afterwards.
func TestVoiceFetch_RetranscribeHandsSTTTheAttemptFile(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	pc := &probeChannel{fakeChannel: &fakeChannel{}, size: 100}
	b := brokerWithProbe(t, pc)
	defer b.Shutdown()
	rec := (&recordingSTT{}).register(b)

	if resp := retranscribeOn(t, b, "V-MANUAL"); resp.Err != "" {
		t.Fatalf("retranscribe: %q", resp.Err)
	}
	if rec.count() != 1 || pc.calls.Load() != 1 {
		t.Fatalf("STT calls=%d fetches=%d, want 1/1", rec.count(), pc.calls.Load())
	}
	rec.requireEveryCallWithLocalFile(t)
	requireNoVoiceFiles(t, os.Getenv("TMPDIR"))
}

// A transient fetch failure (network, or a stalled getFile that hit its
// deadline) parks the note with backoff and never invokes STT.
func TestVoiceFetch_TransientFailureParksWithoutSTT(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"network", &channel.AttachmentTransientError{Err: errors.New("dial tcp: connection refused")}},
		{"stalled getFile", &channel.AttachmentTransientError{Err: context.DeadlineExceeded}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newGateChannel(0, tc.err)
			b, scheduler, clock, voiceDir := fetchHarness(t, g)
			rec := (&recordingSTT{}).register(b)
			scheduler.submit = schedulerCompletingSubmit(scheduler, nil)
			route, in, att := schedulerVoice(8101, "voice-transient")
			key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
			if !scheduler.ScheduleAuto(route, "record-transient", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
				t.Fatal("schedule rejected")
			}

			waitParked(t, scheduler, key, clock.Now().Add(voiceRetryBase))
			if g.calls.Load() != 1 || rec.count() != 0 {
				t.Fatalf("fetches=%d STT calls=%d; want one fetch and no STT", g.calls.Load(), rec.count())
			}
			requireNoVoiceFiles(t, voiceDir)
		})
	}
}

// R15-3: the typed error is authoritative. A permanent refusal whose text
// happens to contain "timeout" is terminal, never retried by string matching.
func TestVoiceFetch_PermanentErrorMentioningTimeoutIsTerminal(t *testing.T) {
	for _, cause := range []string{
		`telegram: download "voice/timeout.oga": HTTP 404`,
		"telegram: GetFile: unable to getFile: Bad Request: timeout field is invalid",
	} {
		t.Run(cause, func(t *testing.T) {
			g := newGateChannel(0, errors.New(cause))
			b := gateBroker(t, g)
			defer b.Shutdown()
			rec := (&recordingSTT{}).register(b)

			in := voiceInbound(8201, 100)
			flushVoice(t, b, in)

			if !strings.Contains(in.Text, voiceFetchFailedOpening) || !strings.Contains(in.Text, cause) {
				t.Fatalf("want a terminal download failure quoting %q; got %q", cause, in.Text)
			}
			if g.calls.Load() != 1 || rec.count() != 0 {
				t.Fatalf("fetches=%d STT calls=%d; want one fetch and no STT", g.calls.Load(), rec.count())
			}
		})
	}
}

// R9-3: a 429 parks for max(backoff, retry_after) without running STT; the
// retry then succeeds once, with a local file.
func TestVoiceFetch_RateLimitParksForRetryAfterThenSucceeds(t *testing.T) {
	var fetches atomic.Int64
	g := newGateChannel(0, nil)
	g.answer = func(string) (int64, error) {
		if fetches.Add(1) == 1 {
			return 0, transientAfter(120 * time.Second)
		}
		return 100, nil
	}
	b, scheduler, clock, voiceDir := fetchHarness(t, g)
	rec := (&recordingSTT{}).register(b)
	jobs := make(chan *ResolveVoiceJob, 1)
	scheduler.submit = schedulerCompletingSubmit(scheduler, jobs)
	route, in, att := schedulerVoice(8301, "voice-429")
	key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
	if !scheduler.ScheduleAuto(route, "record-429", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}

	waitParked(t, scheduler, key, clock.Now().Add(120*time.Second))
	clock.Advance(119 * time.Second)
	time.Sleep(20 * time.Millisecond)
	if fetches.Load() != 1 || rec.count() != 0 {
		t.Fatalf("before retry_after: fetches=%d STT calls=%d; want 1/0", fetches.Load(), rec.count())
	}
	clock.Advance(time.Second)
	select {
	case job := <-jobs:
		if !job.Success || job.Outcome.Transcript != "transcript of voice-429" {
			t.Fatalf("resolve = %+v; want the transcript", job)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the retry after retry_after never resolved")
	}
	if rec.count() != 1 {
		t.Fatalf("STT ran %d time(s), want exactly 1", rec.count())
	}
	rec.requireEveryCallWithLocalFile(t)
	requireNoVoiceFiles(t, voiceDir)
}

// A retry_after shorter than the backoff never shortens the backoff.
func TestVoiceFetch_RateLimitNeverShortensBackoff(t *testing.T) {
	g := newGateChannel(0, transientAfter(5*time.Second))
	b, scheduler, clock, _ := fetchHarness(t, g)
	(&recordingSTT{}).register(b)
	route, in, att := schedulerVoice(8302, "voice-429-short")
	key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
	if !scheduler.ScheduleAuto(route, "record-429-short", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	waitParked(t, scheduler, key, clock.Now().Add(voiceRetryBase))
}

// R16-2 (accepted trade-off): acceleration may retry before retry_after ends.
// Each early getFile that meets another 429 re-parks with the new wait: no
// terminal outcome and no STT.
func TestVoiceFetch_AcceleratedAttemptsReparkOnRepeated429(t *testing.T) {
	waits := []time.Duration{120 * time.Second, 100 * time.Second, 200 * time.Second}
	var fetches atomic.Int64
	g := newGateChannel(0, nil)
	g.answer = func(string) (int64, error) {
		n := fetches.Add(1)
		return 0, transientAfter(waits[min(int(n), len(waits))-1])
	}
	b, scheduler, clock, _ := fetchHarness(t, g)
	rec := (&recordingSTT{}).register(b)
	jobs := make(chan *ResolveVoiceJob, 1)
	scheduler.submit = schedulerCompletingSubmit(scheduler, jobs)
	route, in, att := schedulerVoice(8401, "voice-429-accelerated")
	key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
	if !scheduler.ScheduleAuto(route, "record-429-accel", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	waitParked(t, scheduler, key, clock.Now().Add(120*time.Second))

	clock.Advance(40 * time.Second)
	scheduler.RetryNow("telegram") // an UP edge
	waitForVoiceCondition(t, "early getFile after RetryNow", func() bool { return fetches.Load() == 2 })
	waitParked(t, scheduler, key, clock.Now().Add(100*time.Second))

	clock.Advance(40 * time.Second)
	hook := make(chan voiceScheduleResult, 1)
	if err := scheduler.ScheduleManual(route, "record-429-accel", in, att, false, hook); err != nil {
		t.Fatalf("manual promotion: %v", err)
	}
	waitForVoiceCondition(t, "early getFile after manual promotion", func() bool { return fetches.Load() == 3 })
	waitParked(t, scheduler, key, clock.Now().Add(200*time.Second))

	select {
	case job := <-jobs:
		t.Fatalf("repeated 429s produced a terminal outcome: %+v", job)
	case result := <-hook:
		t.Fatalf("repeated 429s answered the manual caller: %+v", result)
	default:
	}
	if rec.count() != 0 {
		t.Fatalf("STT ran %d time(s) on a rate-limited fetch", rec.count())
	}
}

// R7-6: STT enabled but its handler unavailable parks the note without a
// fetch; once the handler is back, the next attempt runs with a local file.
func TestVoiceFetch_HandlerUnavailableParksUntilRestored(t *testing.T) {
	g := newGateChannel(100, nil)
	b, scheduler, clock, voiceDir := fetchHarness(t, g)
	var isReady atomic.Bool
	b.Plugins.OnVoiceReady(isReady.Load)
	rec := (&recordingSTT{}).register(b)
	jobs := make(chan *ResolveVoiceJob, 1)
	scheduler.submit = schedulerCompletingSubmit(scheduler, jobs)
	route, in, att := schedulerVoice(8501, "voice-no-handler")
	key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
	if !scheduler.ScheduleAuto(route, "record-no-handler", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}

	waitParked(t, scheduler, key, clock.Now().Add(voiceRetryBase))
	if g.calls.Load() != 0 || rec.count() != 0 {
		t.Fatalf("handler unavailable: fetches=%d STT calls=%d; want 0/0", g.calls.Load(), rec.count())
	}

	isReady.Store(true)
	clock.Advance(voiceRetryBase)
	select {
	case job := <-jobs:
		if !job.Success {
			t.Fatalf("restored handler: resolve = %+v", job)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the retry after the handler returned never resolved")
	}
	rec.requireEveryCallWithLocalFile(t)
	requireNoVoiceFiles(t, voiceDir)
}

// R16-1, through the real STT builtin: with handler_path unconfigured and no
// handler anywhere at startup, attempts park; once a handler is installed in a
// discovery location, the next retry transcribes from the local audio file.
func TestVoiceFetch_HandlerInstalledAfterStartupIsDiscovered(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
	src := t.TempDir()
	t.Setenv("CLAUDE_PLUGIN_ROOT", "")
	t.Setenv("C3_SRC_DIR", src)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("STT_INBOX_DIR", t.TempDir())
	g := newGateChannel(100, nil)
	b, scheduler, clock, voiceDir := fetchHarness(t, g)
	if err := stt.Register(b.Plugins); err != nil {
		t.Fatal(err)
	}
	jobs := make(chan *ResolveVoiceJob, 1)
	scheduler.submit = schedulerCompletingSubmit(scheduler, jobs)
	route, in, att := schedulerVoice(8601, "voice-discovered")
	key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
	if !scheduler.ScheduleAuto(route, "record-discovered", in, []c3types.Attachment{att}, "", voiceEchoReservation{}) {
		t.Fatal("schedule rejected")
	}
	waitParked(t, scheduler, key, clock.Now().Add(voiceRetryBase))
	if g.calls.Load() != 0 {
		t.Fatalf("fetched %d time(s) with no handler installed", g.calls.Load())
	}

	handler := filepath.Join(src, "plugins", "c3", "stt", "stt-handler.py")
	if err := os.MkdirAll(filepath.Dir(handler), 0o755); err != nil {
		t.Fatal(err)
	}
	const script = "import os\nprint('heard ' + open(os.environ['C3_STT_LOCAL_FILE']).read())\n"
	if err := os.WriteFile(handler, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	clock.Advance(voiceRetryBase)
	select {
	case job := <-jobs:
		if !job.Success || job.Outcome.Transcript != "heard fake-audio" {
			t.Fatalf("discovered handler: resolve = %+v; want the local file's transcript", job)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the discovered handler never transcribed")
	}
	requireNoVoiceFiles(t, voiceDir)
}

func TestVoiceFetch_STTDisabledFetchesNothing(t *testing.T) {
	g := newGateChannel(100, nil)
	b := gateBroker(t, g)
	defer b.Shutdown()

	in := voiceInbound(8701, 100)
	flushVoice(t, b, in)

	if g.calls.Load() != 0 {
		t.Fatalf("fetched %d time(s) with STT off; no callback means no fetch", g.calls.Load())
	}
	if !strings.Contains(in.Text, "no_transcript") {
		t.Fatalf("STT off must resolve as today (no_transcript); got %q", in.Text)
	}
}

// R17-1: an STT failure after a successful fetch claims the audio is saved only
// when the handler's retained copy exists at failure time. Either way the
// attempt file is gone.
func TestVoiceFetch_STTFailureTextFollowsTheRetainedCopy(t *testing.T) {
	for _, tc := range []struct {
		name, retained, want, forbid string
	}{
		{"retention copy failed", "", "No local copy", "saved and recoverable"},
		{"retained in the inbox", "/inbox/1-F-BIG.oga", "saved and recoverable", "No local copy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newGateChannel(100, nil)
			g.retained = tc.retained
			b := gateBroker(t, g)
			defer b.Shutdown()
			rec := (&recordingSTT{reply: func(c3types.VoicePayload) string {
				return "[STT FAILED: error — see /tmp/broker.log]"
			}}).register(b)

			in := voiceInbound(8801, 100)
			flushVoice(t, b, in)

			if !strings.Contains(in.Text, tc.want) || strings.Contains(in.Text, tc.forbid) {
				t.Fatalf("failure text must contain %q and not %q; got %q", tc.want, tc.forbid, in.Text)
			}
			if tc.retained != "" && !strings.Contains(in.Text, tc.retained) {
				t.Fatalf("failure text must name the retained copy %s; got %q", tc.retained, in.Text)
			}
			rec.requireEveryCallWithLocalFile(t)
			requireNoVoiceFiles(t, os.Getenv("TMPDIR"))
		})
	}
}

// Whatever the outcome (transcript, STT failure, legacy fetch marker, fetch
// refusal), no attempt file survives: 20 arrivals leave zero files.
func TestVoiceFetch_RepeatedArrivalsLeaveNoFiles(t *testing.T) {
	g := newGateChannel(0, nil)
	g.answer = func(fileID string) (int64, error) {
		if strings.HasSuffix(fileID, "-3") {
			return 0, errors.New("telegram: GetFile: Bad Request: invalid file_id")
		}
		return 100, nil
	}
	b := gateBroker(t, g)
	defer b.Shutdown()
	(&recordingSTT{reply: func(p c3types.VoicePayload) string {
		switch {
		case strings.HasSuffix(p.FileID, "-1"):
			return "[STT FAILED: error — see /tmp/broker.log]"
		case strings.HasSuffix(p.FileID, "-2"):
			return "[STT FETCH FAILED: getFile failed (error_code=400): Bad Request: wrong file]"
		}
		return "words"
	}}).register(b)

	for i := range 20 {
		in := voiceInbound(int64(9000+i), 100)
		in.Attachments[0].FileID = "V" + strings.Repeat("x", i) + "-" + string(rune('0'+i%4))
		flushVoice(t, b, in)
	}
	if g.calls.Load() != 20 {
		t.Fatalf("fetches = %d, want 20", g.calls.Load())
	}
	requireNoVoiceFiles(t, os.Getenv("TMPDIR"))
}
