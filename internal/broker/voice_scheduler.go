package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

const (
	sttRetryConcurrency     = 2
	voiceWorkQueueSize      = 64
	voiceRetryBase          = 30 * time.Second
	voiceRetryCap           = 5 * time.Minute
	voiceRetrySweep         = 2 * time.Minute
	voiceResolveRetryDelay  = time.Second
	defaultVoiceRetryExpiry = 24 * time.Hour
)

type voiceScheduleKey struct {
	route     RouteKey
	messageID int64
	fileID    string
}

type voiceEntryState uint8

const (
	voiceWaiting voiceEntryState = iota
	voiceRunning
	voiceResolveReady
	voiceResolveSubmitted
)

type voiceEntry struct {
	key          voiceScheduleKey
	recordID     string
	inbound      c3types.Inbound
	attachment   c3types.Attachment
	nextAttempt  time.Time
	firstFailure time.Time
	backoff      time.Duration
	manual       bool
	state        voiceEntryState
	group        *voiceGroup
	resolve      voiceResolve
	hooks        []chan<- voiceScheduleResult
}

type voiceGroup struct {
	order       []string
	remaining   int
	transcripts map[string]string
	notices     []string
	echo        voiceEchoReservation
}

// voiceEchoReservation fixes readback order at durable-append time. STT jobs
// may finish out of order, but their optional Telegram readbacks must retain
// the route's original arrival order.
type voiceEchoReservation struct {
	prev <-chan struct{}
	mine chan struct{}
}

type voiceResolve struct {
	segmentText    string
	success        bool
	transcript     string
	detail         string
	echoTranscript string
	failNotice     string
	echo           voiceEchoReservation
}

type voiceScheduleResult struct {
	Transcript string
	Err        error
}

type voiceAttempt struct {
	key        voiceScheduleKey
	recordID   string
	inbound    c3types.Inbound
	attachment c3types.Attachment
}

type voiceAttemptResult struct {
	transient   bool
	success     bool
	segmentText string
	transcript  string
	notice      string
	detail      string
}

type voiceSchedulerClock interface {
	Now() time.Time
	NewTimer(time.Duration) voiceSchedulerTimer
}

type voiceSchedulerTimer interface {
	C() <-chan time.Time
	Reset(time.Duration)
	Stop()
}

type realVoiceClock struct{}

func (realVoiceClock) Now() time.Time { return time.Now() }
func (realVoiceClock) NewTimer(d time.Duration) voiceSchedulerTimer {
	return &realVoiceTimer{timer: time.NewTimer(d)}
}

type realVoiceTimer struct{ timer *time.Timer }

func (t *realVoiceTimer) C() <-chan time.Time { return t.timer.C }
func (t *realVoiceTimer) Stop() {
	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
}
func (t *realVoiceTimer) Reset(d time.Duration) {
	t.Stop()
	t.timer.Reset(d)
}

// VoiceScheduler owns every in-memory voice-enrichment lease. Its registry is a
// cache, never the source of truth: the queue row's VoicePending field survives a
// crash and is sufficient to re-create these entries at startup.
type VoiceScheduler struct {
	broker *Broker
	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	accepting bool
	entries   map[voiceScheduleKey]*voiceEntry
	wake      chan struct{}
	work      chan voiceScheduleKey
	wg        sync.WaitGroup
	stopOnce  sync.Once
	stopped   chan struct{}

	clock        voiceSchedulerClock
	healthDown   func(string) bool
	runAttempt   func(context.Context, voiceAttempt) voiceAttemptResult
	submit       func(RouteKey, Job) bool
	jitter       func(time.Duration) time.Duration
	retryBase    time.Duration
	retryCap     time.Duration
	sweepEvery   time.Duration
	retryExpiry  time.Duration
	resolveDelay time.Duration
}

func newVoiceScheduler(b *Broker, clock voiceSchedulerClock) *VoiceScheduler {
	if clock == nil {
		clock = realVoiceClock{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &VoiceScheduler{
		broker:       b,
		ctx:          ctx,
		cancel:       cancel,
		accepting:    true,
		entries:      make(map[voiceScheduleKey]*voiceEntry),
		wake:         make(chan struct{}, 1),
		work:         make(chan voiceScheduleKey, voiceWorkQueueSize),
		stopped:      make(chan struct{}),
		clock:        clock,
		retryBase:    voiceRetryBase,
		retryCap:     voiceRetryCap,
		sweepEvery:   voiceRetrySweep,
		retryExpiry:  configuredVoiceRetryExpiry(b),
		resolveDelay: voiceResolveRetryDelay,
	}
	s.healthDown = b.channelHealthDown
	s.runAttempt = s.transcribe
	s.submit = b.Workers.Submit
	s.jitter = func(d time.Duration) time.Duration {
		span := d / 5 // ±10%; a zero span is possible only in shortened tests.
		if span <= 0 {
			return d
		}
		return d - span/2 + time.Duration(rand.Int64N(int64(span)+1))
	}
	s.wg.Add(1 + sttRetryConcurrency)
	go s.dispatch()
	for range sttRetryConcurrency {
		go s.run()
	}
	return s
}

func configuredVoiceRetryExpiry(b *Broker) time.Duration {
	if b == nil || b.Mappings() == nil || b.Mappings().Plugins == nil {
		return defaultVoiceRetryExpiry
	}
	raw, ok := b.Mappings().Plugins["stt"]["voice_retry_expiry"]
	if !ok {
		return defaultVoiceRetryExpiry
	}
	var d time.Duration
	switch v := raw.(type) {
	case string:
		parsed, err := time.ParseDuration(v)
		if err == nil {
			d = parsed
		}
	case float64:
		d = time.Duration(v * float64(time.Second))
	case int:
		d = time.Duration(v) * time.Second
	case int64:
		d = time.Duration(v) * time.Second
	}
	if d <= 0 {
		log.Printf("voice scheduler: invalid plugins.stt.voice_retry_expiry=%v; using %s", raw, defaultVoiceRetryExpiry)
		return defaultVoiceRetryExpiry
	}
	return d
}

func (b *Broker) channelHealthDown(channel string) bool {
	b.healthMu.RLock()
	defer b.healthMu.RUnlock()
	ev, ok := b.lastHealth[channel]
	return ok && ev.State == c3types.HealthStateDown
}

func cloneVoiceInbound(in c3types.Inbound) c3types.Inbound {
	in.Attachments = append([]c3types.Attachment(nil), in.Attachments...)
	if in.TopicID != nil {
		t := *in.TopicID
		in.TopicID = &t
	}
	return in
}

func (s *VoiceScheduler) ScheduleAuto(route RouteKey, recordID string, in c3types.Inbound, voices []c3types.Attachment, initialNotice string, echo voiceEchoReservation) bool {
	return s.schedule(route, recordID, in, voices, initialNotice, echo, false, nil)
}

func (s *VoiceScheduler) ScheduleManual(route RouteKey, recordID string, in c3types.Inbound, att c3types.Attachment, hook chan<- voiceScheduleResult) bool {
	return s.schedule(route, recordID, in, []c3types.Attachment{att}, "", voiceEchoReservation{}, true, hook)
}

func (s *VoiceScheduler) schedule(route RouteKey, recordID string, in c3types.Inbound, voices []c3types.Attachment, initialNotice string, echo voiceEchoReservation, manual bool, hook chan<- voiceScheduleResult) bool {
	now := s.clock.Now()
	s.mu.Lock()
	if !s.accepting {
		s.mu.Unlock()
		return false
	}
	group := &voiceGroup{transcripts: make(map[string]string), echo: echo}
	if initialNotice != "" {
		group.notices = append(group.notices, initialNotice)
	}
	added := false
	for _, att := range voices {
		if att.Kind != "voice" || att.FileID == "" {
			continue
		}
		key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
		if existing := s.entries[key]; existing != nil {
			if hook != nil {
				existing.hooks = append(existing.hooks, hook)
			}
			if manual && existing.state == voiceWaiting {
				existing.manual = true
				existing.nextAttempt = now
			}
			added = true
			continue
		}
		entry := &voiceEntry{
			key: key, recordID: recordID, inbound: cloneVoiceInbound(in), attachment: att,
			nextAttempt: now, backoff: s.retryBase, manual: manual, state: voiceWaiting,
		}
		if hook != nil {
			entry.hooks = append(entry.hooks, hook)
		}
		s.entries[key] = entry
		group.order = append(group.order, att.FileID)
		group.remaining++
		added = true
	}
	for _, fileID := range group.order {
		key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: fileID}
		if entry := s.entries[key]; entry != nil && entry.group == nil {
			entry.group = group
		}
	}
	s.mu.Unlock()
	if added {
		s.Wake()
	}
	return added
}

// RecoverPending uses the store-wide startup scan once channels and plugins are
// registered. Missing attachment metadata is retained as a synthetic voice
// attachment so a malformed row fails visibly instead of remaining pending forever.
func (s *VoiceScheduler) RecoverPending() {
	if s == nil || s.broker == nil || s.broker.Queue == nil {
		return
	}
	rowsByRoute := s.broker.Queue.PendingVoiceRowsAll()
	recovered := 0
	for qrk, rows := range rowsByRoute {
		route := MakeRouteKey(qrk.Channel, qrk.ChatID, qrk.TopicID)
		for _, row := range rows {
			pending := make(map[string]bool, len(row.VoicePending))
			for _, fileID := range row.VoicePending {
				pending[fileID] = true
			}
			voices := make([]c3types.Attachment, 0, len(pending))
			for _, att := range row.Inbound.Attachments {
				if att.Kind == "voice" && pending[att.FileID] {
					voices = append(voices, att)
					delete(pending, att.FileID)
				}
			}
			for fileID := range pending {
				log.Printf("voice scheduler: pending row record=%s msg=%d names file_id=%s absent from Attachments; scheduling a visible failure path", row.RecordID, row.Inbound.MessageID, fileID)
				voices = append(voices, c3types.Attachment{Kind: "voice", FileID: fileID})
			}
			if s.ScheduleAuto(route, row.RecordID, row.Inbound, voices, "", voiceEchoReservation{}) {
				recovered++
			}
		}
	}
	if recovered > 0 {
		log.Printf("voice scheduler: recovered %d pending queue row(s) for immediate enrichment", recovered)
	}
}

func (s *VoiceScheduler) Wake() {
	if s == nil {
		return
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *VoiceScheduler) dispatch() {
	defer s.wg.Done()
	defer recoverGoroutine("voiceScheduler.dispatch")
	timer := s.clock.NewTimer(s.sweepEvery)
	defer timer.Stop()
	nextSweep := s.clock.Now().Add(s.sweepEvery)
	for {
		now := s.clock.Now()
		if !now.Before(nextSweep) {
			nextSweep = now.Add(s.sweepEvery)
		}
		s.dispatchDue(now)
		delay := s.nextDelay(now, nextSweep)
		timer.Reset(delay)
		select {
		case <-s.ctx.Done():
			return
		case <-s.wake:
		case <-timer.C():
		}
	}
}

func (s *VoiceScheduler) dispatchDue(now time.Time) {
	var resolves []voiceScheduleKey
	s.mu.Lock()
	if !s.accepting {
		s.mu.Unlock()
		return
	}
	for key, entry := range s.entries {
		if entry.state == voiceWaiting && !entry.firstFailure.IsZero() && !now.Before(entry.firstFailure.Add(s.retryExpiry)) {
			out := voiceAttemptResult{
				segmentText: sttFailureText(entry.attachment, "retry_expired", s.broker.voiceCachedPath(entry.key.route.Channel, entry.key.fileID)),
				notice:      sttFailureNotice, detail: "voice transcription retry expired",
			}
			s.finishTerminalLocked(entry, out)
		}
		if entry.state == voiceResolveReady && !now.Before(entry.nextAttempt) {
			resolves = append(resolves, key)
		}
	}
	sort.Slice(resolves, func(i, j int) bool { return voiceKeyLess(resolves[i], resolves[j]) })

	var due []*voiceEntry
	for _, entry := range s.entries {
		if entry.state != voiceWaiting || now.Before(entry.nextAttempt) || s.healthDown(entry.key.route.Channel) {
			continue
		}
		due = append(due, entry)
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].manual != due[j].manual {
			return due[i].manual
		}
		if !due[i].nextAttempt.Equal(due[j].nextAttempt) {
			return due[i].nextAttempt.Before(due[j].nextAttempt)
		}
		return voiceKeyLess(due[i].key, due[j].key)
	})
	for _, entry := range due {
		select {
		case s.work <- entry.key:
			entry.state = voiceRunning
			entry.manual = false
		default:
			s.mu.Unlock()
			for _, key := range resolves {
				s.submitResolve(key)
			}
			return
		}
	}
	s.mu.Unlock()
	for _, key := range resolves {
		s.submitResolve(key)
	}
}

func voiceKeyLess(a, b voiceScheduleKey) bool {
	if a.route.Channel != b.route.Channel {
		return a.route.Channel < b.route.Channel
	}
	if a.route.ChatID != b.route.ChatID {
		return a.route.ChatID < b.route.ChatID
	}
	if a.route.HasTopic != b.route.HasTopic {
		return !a.route.HasTopic
	}
	if a.route.TopicID != b.route.TopicID {
		return a.route.TopicID < b.route.TopicID
	}
	if a.messageID != b.messageID {
		return a.messageID < b.messageID
	}
	return a.fileID < b.fileID
}

func (s *VoiceScheduler) nextDelay(now, nextSweep time.Time) time.Duration {
	next := nextSweep
	s.mu.Lock()
	for _, entry := range s.entries {
		var candidate time.Time
		switch entry.state {
		case voiceWaiting:
			if !entry.firstFailure.IsZero() {
				candidate = entry.firstFailure.Add(s.retryExpiry)
			}
			if !s.healthDown(entry.key.route.Channel) && (candidate.IsZero() || entry.nextAttempt.Before(candidate)) {
				candidate = entry.nextAttempt
			}
		case voiceResolveReady:
			candidate = entry.nextAttempt
		}
		if !candidate.IsZero() && candidate.Before(next) {
			next = candidate
		}
	}
	s.mu.Unlock()
	d := next.Sub(now)
	if d <= 0 {
		return time.Millisecond
	}
	return d
}

func (s *VoiceScheduler) run() {
	defer s.wg.Done()
	defer recoverGoroutine("voiceScheduler.runner")
	for {
		select {
		case <-s.ctx.Done():
			return
		case key := <-s.work:
			attempt, ok := s.attemptSnapshot(key)
			if !ok {
				continue
			}
			out := s.runAttempt(s.ctx, attempt)
			if s.ctx.Err() != nil {
				return
			}
			s.finishAttempt(key, out)
		}
	}
}

func (s *VoiceScheduler) attemptSnapshot(key voiceScheduleKey) (voiceAttempt, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.entries[key]
	if entry == nil || entry.state != voiceRunning {
		return voiceAttempt{}, false
	}
	// A DOWN edge can land after dispatch queued this key but before a runner
	// snapshots it. Re-check at the actual attempt boundary so known-down health
	// never burns a provider/fetch attempt from the bounded runner queue.
	if s.healthDown(entry.key.route.Channel) {
		entry.state = voiceWaiting
		return voiceAttempt{}, false
	}
	return voiceAttempt{key: key, recordID: entry.recordID, inbound: cloneVoiceInbound(entry.inbound), attachment: entry.attachment}, true
}

func (s *VoiceScheduler) finishAttempt(key voiceScheduleKey, out voiceAttemptResult) {
	now := s.clock.Now()
	s.mu.Lock()
	entry := s.entries[key]
	if entry == nil || entry.state != voiceRunning {
		s.mu.Unlock()
		return
	}
	if out.transient {
		if entry.firstFailure.IsZero() {
			entry.firstFailure = now
		}
		if !now.Before(entry.firstFailure.Add(s.retryExpiry)) {
			out = voiceAttemptResult{
				segmentText: sttFailureText(entry.attachment, "retry_expired", s.broker.voiceCachedPath(entry.key.route.Channel, entry.key.fileID)),
				notice:      sttFailureNotice, detail: "voice transcription retry expired",
			}
			s.finishTerminalLocked(entry, out)
		} else {
			delay := s.jitter(entry.backoff)
			if delay <= 0 {
				delay = time.Millisecond
			}
			entry.nextAttempt = now.Add(delay)
			entry.backoff *= 2
			if entry.backoff > s.retryCap {
				entry.backoff = s.retryCap
			}
			entry.state = voiceWaiting
			log.Printf("voice scheduler: parked chan=%s chat=%d topic=%s msg=%d file_id=%s retry_in=%s", key.route.Channel, key.route.ChatID, TopicKeyStr(key.route), key.messageID, key.fileID, delay.Round(time.Second))
		}
	} else {
		s.finishTerminalLocked(entry, out)
	}
	s.mu.Unlock()
	s.Wake()
}

func (s *VoiceScheduler) finishTerminalLocked(entry *voiceEntry, out voiceAttemptResult) {
	entry.resolve = voiceResolve{
		segmentText: out.segmentText, success: out.success, transcript: out.transcript, detail: out.detail,
	}
	if group := entry.group; group != nil {
		if out.transcript != "" {
			group.transcripts[entry.key.fileID] = out.transcript
		}
		if out.notice != "" && !containsString(group.notices, out.notice) {
			group.notices = append(group.notices, out.notice)
		}
		if group.remaining > 0 {
			group.remaining--
		}
		if group.remaining == 0 {
			var transcript string
			for _, fileID := range group.order {
				if text := group.transcripts[fileID]; text != "" {
					transcript = appendVoiceMarker(transcript, text)
				}
			}
			entry.resolve.echoTranscript = transcript
			entry.resolve.failNotice = strings.Join(group.notices, "\n")
			entry.resolve.echo = group.echo
		}
	}
	entry.state = voiceResolveReady
	entry.nextAttempt = s.clock.Now()
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func (s *VoiceScheduler) submitResolve(key voiceScheduleKey) {
	s.mu.Lock()
	entry := s.entries[key]
	if !s.accepting || entry == nil || entry.state != voiceResolveReady || s.clock.Now().Before(entry.nextAttempt) {
		s.mu.Unlock()
		return
	}
	job := &ResolveVoiceJob{
		Key: key, RecordID: entry.recordID, Inbound: cloneVoiceInbound(entry.inbound),
		FileID: key.fileID, SegmentText: entry.resolve.segmentText, Success: entry.resolve.success,
		EchoTranscript: entry.resolve.echoTranscript, FailNotice: entry.resolve.failNotice,
		Echo: entry.resolve.echo,
	}
	entry.state = voiceResolveSubmitted
	s.wg.Add(1)
	s.mu.Unlock()
	if s.submit(key.route, Job{Kind: JobResolveVoice, ResolveVoice: job}) {
		return
	}
	s.wg.Done()
	s.mu.Lock()
	if current := s.entries[key]; current != nil && current.state == voiceResolveSubmitted {
		current.state = voiceResolveReady
		current.nextAttempt = s.clock.Now().Add(s.resolveDelay)
	}
	s.mu.Unlock()
	s.Wake()
}

func (s *VoiceScheduler) retryResolve(key voiceScheduleKey) {
	s.mu.Lock()
	if current := s.entries[key]; s.accepting && current != nil {
		current.state = voiceResolveReady
		current.nextAttempt = s.clock.Now().Add(s.resolveDelay)
	}
	s.mu.Unlock()
	s.Wake()
}

func (s *VoiceScheduler) completeResolve(key voiceScheduleKey) {
	var hooks []chan<- voiceScheduleResult
	var result voiceScheduleResult
	s.mu.Lock()
	if entry := s.entries[key]; entry != nil {
		hooks = append(hooks, entry.hooks...)
		if entry.resolve.success {
			result.Transcript = entry.resolve.transcript
		} else {
			detail := entry.resolve.detail
			if detail == "" {
				detail = "voice transcription failed"
			}
			result.Err = errors.New(detail)
		}
		delete(s.entries, key)
	}
	s.mu.Unlock()
	for _, hook := range hooks {
		select {
		case hook <- result:
		default:
		}
	}
}

func (s *VoiceScheduler) resolveJobDone() { s.wg.Done() }

func (s *VoiceScheduler) resolveJobDropped(job *ResolveVoiceJob) {
	if job != nil {
		s.retryResolve(job.Key)
	}
	s.resolveJobDone()
}

func (s *VoiceScheduler) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.accepting = false
		s.mu.Unlock()
		s.cancel()
		s.wg.Wait()
		close(s.stopped)
	})
	<-s.stopped
}

func (s *VoiceScheduler) transcribe(ctx context.Context, attempt voiceAttempt) voiceAttemptResult {
	att := attempt.attachment
	cachedPath := s.broker.voiceCachedPath(attempt.key.route.Channel, att.FileID)
	effectiveSize := att.Size
	if cachedPath != "" {
		if info, err := os.Stat(cachedPath); err == nil {
			effectiveSize = info.Size()
		} else {
			log.Printf("voice scheduler: cached audio stat failed chan=%s msg=%d file_id=%s path=%s: %v — using inbound size=%d", attempt.key.route.Channel, attempt.key.messageID, att.FileID, cachedPath, err, effectiveSize)
		}
	} else {
		agent, notice, refuse, retryable, probedSize := s.broker.voiceFetchRefusal(attempt.key.route.Channel, att)
		if refuse {
			if retryable {
				return voiceAttemptResult{transient: true, detail: agent}
			}
			return voiceAttemptResult{segmentText: agent, notice: notice, detail: agent}
		}
		if effectiveSize == 0 {
			effectiveSize = probedSize
		}
	}

	sttCtx, cancel := context.WithTimeout(ctx, sttFlushTimeout)
	raw := s.broker.Plugins.FireOnVoiceReceived(sttCtx, c3types.VoicePayload{
		Channel: attempt.inbound.Channel, ChatID: attempt.inbound.ChatID, TopicID: attempt.inbound.TopicID,
		MessageID: attempt.inbound.MessageID, FileID: att.FileID, MIME: att.MIME, Size: effectiveSize,
	})
	cancel()
	if detail, ok := sttFetchFailure(raw); ok {
		if isNetworkTransient(detail) {
			return voiceAttemptResult{transient: true, detail: detail}
		}
		return voiceAttemptResult{
			segmentText: sttFailureText(att, detail, cachedPath), notice: sttFailureNotice, detail: detail,
		}
	}
	if raw == "" || isSTTFailureMarker(raw) {
		reason := sttFailureReason(raw)
		return voiceAttemptResult{
			segmentText: sttFailureText(att, reason, cachedPath), notice: sttFailureNotice, detail: reason,
		}
	}
	return voiceAttemptResult{
		success: true, transcript: raw, segmentText: s.sttPrefix(attempt.inbound.Channel) + raw,
	}
}

func (s *VoiceScheduler) sttPrefix(channel string) string {
	if s.broker == nil || s.broker.Mappings() == nil {
		return "[Transcribed voice]: "
	}
	cc, ok := s.broker.Mappings().Channels[channel]
	if !ok || cc.STTPrefix == "" {
		return "[Transcribed voice]: "
	}
	return cc.STTPrefix
}

func voicePendingText(fileID string) string {
	return fmt.Sprintf("[voice note %s: transcription in progress — do not act; the transcript will arrive automatically (auto-retried if the network is down); retranscribe only if this message later reports failure]", fileID)
}

// isNetworkTransient reports whether a fetch-error string looks like a TRANSIENT
// network condition worth auto-retrying once connectivity returns. FAIL-CLOSED
// allowlist: only recognized-transient signatures return true, so a permanent
// failure (bad/expired file_id, too-big, a provider error) is never retry-looped.
// Applied ONLY to fetch failures (download), never to provider/transcription
// failures — a download that timed out IS a network condition.
func isNetworkTransient(s string) bool {
	l := strings.ToLower(s)
	for _, sig := range []string{
		"network is unreachable",
		"temporary failure in name resolution",
		"name or service not known",
		"nodename nor servname provided",
		"no such host",
		"no route to host",
		"connection refused",
		"connection reset",
		"timed out",
		"timeout",
		"dial tcp",
		"bad gateway",
		"service unavailable",
		"gateway timeout",
		"temporarily unavailable",
	} {
		if strings.Contains(l, sig) {
			return true
		}
	}
	return false
}
