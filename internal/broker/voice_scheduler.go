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
	"github.com/Andrometiq/c3/internal/intake"
	"github.com/Andrometiq/c3/internal/mappings"
)

const (
	sttRetryConcurrency     = 2
	voiceWorkQueueSize      = 64
	voiceRetryBase          = 30 * time.Second
	voiceRetryCap           = 5 * time.Minute
	voiceRetrySweep         = 2 * time.Minute
	voiceResolveRetryDelay  = time.Second
	voiceDispatchBackoff    = 50 * time.Millisecond
	defaultVoiceRetryExpiry = 24 * time.Hour
	maxVoiceResultHooks     = 32
	voiceSlowNoticeAfter    = 2 * time.Minute
	voiceSlowNoticeText     = "Got this voice note. Transcription is taking longer than usual; C3 is retrying."
)

var (
	errVoiceSchedulerStopping = errors.New("voice scheduler is stopping")
	errVoiceResultHookLimit   = errors.New("too many callers are already waiting for this voice transcription")
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
	inbound      c3types.Inbound
	attachment   c3types.Attachment
	nextAttempt  time.Time
	firstFailure time.Time
	backoff      time.Duration
	manual       bool
	state        voiceEntryState
	groups       []*voiceGroup
	targets      map[string]voiceResolveTarget
	applied      map[string]bool
	resolve      voiceResolve
	hooks        []chan<- voiceScheduleResult
	arrived      time.Time
	slowNoticed  bool // the one "taking longer" reply was attempted
}

type voiceGroup struct {
	persisted   map[string]bool // route-worker owned; separate from audio completion
	outcomes    map[string]intake.STTOutcome
	order       []string
	remaining   int
	finished    map[voiceScheduleKey]bool
	transcripts map[string]string
	notices     []string
	echo        voiceEchoReservation
	echoOnce    sync.Once
}

// voiceEchoReservation fixes readback order at durable-append time. STT jobs
// may finish out of order, but their optional Telegram readbacks must retain
// the route's original arrival order.
type voiceEchoReservation struct {
	prev <-chan struct{}
	mine chan struct{}
}

type voiceResolve struct {
	outcome     intake.STTOutcome
	segmentText string
	success     bool
	transcript  string
	notice      string
	detail      string
}

// voiceResolveTarget is one durable row (or the empty-ID degraded/manual
// revision path) that shares this key's terminal audio outcome. One key may own
// several targets when Telegram delivers an edit while the original row is
// still pending.
type voiceResolveTarget struct {
	inbound          c3types.Inbound
	source           *intake.Source
	attachmentsState intake.AttachmentsState
	recordID         string
	group            *voiceGroup
	stateGroup       *voiceGroup // recovery siblings for a promoted manual target
	findPending      bool
	transcriptOnly   bool
}

type voiceScheduleResult struct {
	Transcript string
	Err        error
}

type voiceAttempt struct {
	key        voiceScheduleKey
	inbound    c3types.Inbound
	attachment c3types.Attachment
}

type voiceAttemptResult struct {
	transient   bool
	retryAfter  time.Duration // a transient's server-requested minimum wait
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
	if b == nil {
		return defaultVoiceRetryExpiry
	}
	return voiceRetryExpiryFromMappings(b.Mappings())
}

func voiceRetryExpiryFromMappings(mf *mappings.MappingsFile) time.Duration {
	if mf == nil || mf.Plugins == nil {
		return defaultVoiceRetryExpiry
	}
	raw, ok := mf.Plugins["stt"]["voice_retry_expiry"]
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

func (s *VoiceScheduler) setRetryExpiry(expiry time.Duration) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.retryExpiry = expiry
	s.mu.Unlock()
	s.Wake()
}

func cloneVoiceInbound(in c3types.Inbound) c3types.Inbound {
	in.Attachments = append([]c3types.Attachment(nil), in.Attachments...)
	if in.TopicID != nil {
		t := *in.TopicID
		in.TopicID = &t
	}
	return in
}

func (s *VoiceScheduler) ScheduleAuto(route RouteKey, recordID string, in c3types.Inbound, voices []c3types.Attachment, initialNotice string, echo voiceEchoReservation, sources ...*intake.Source) bool {
	return s.scheduleAutoIntake(route, recordID, in, voices, initialNotice, echo, intake.Optional(sources), nil)
}

func (s *VoiceScheduler) scheduleAutoIntake(route RouteKey, recordID string, in c3types.Inbound, voices []c3types.Attachment, initialNotice string, echo voiceEchoReservation, source *intake.Source, states intake.AttachmentsState) bool {
	return s.schedule(route, recordID, in, voices, initialNotice, echo, false, false, nil, source, states) == nil
}

func (s *VoiceScheduler) ScheduleManual(route RouteKey, recordID string, in c3types.Inbound, att c3types.Attachment, transcriptOnly bool, hook chan<- voiceScheduleResult) error {
	return s.schedule(route, recordID, in, []c3types.Attachment{att}, "", voiceEchoReservation{}, true, transcriptOnly, hook, nil, nil)
}

func (s *VoiceScheduler) schedule(route RouteKey, recordID string, in c3types.Inbound, voices []c3types.Attachment, initialNotice string, echo voiceEchoReservation, manual, transcriptOnly bool, hook chan<- voiceScheduleResult, source *intake.Source, states intake.AttachmentsState) error {
	source = source.Clone()
	states = states.Clone()
	if states == nil {
		var pending []string
		for _, att := range voices {
			pending = append(pending, att.FileID)
		}
		states = intake.NewAttachmentsState(attachmentStateSource(source, in), pending)
	}
	if err := states.Validate(attachmentStateSource(source, in)); err != nil {
		log.Printf("voice scheduler: invalid attachment state: %v", err)
		return err
	}
	now := s.clock.Now()
	s.mu.Lock()
	if !s.accepting {
		s.mu.Unlock()
		return errVoiceSchedulerStopping
	}
	if hook != nil {
		for _, att := range voices {
			key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
			if existing := s.entries[key]; existing != nil && len(existing.hooks) >= maxVoiceResultHooks {
				s.mu.Unlock()
				return errVoiceResultHookLimit
			}
		}
	}
	group := &voiceGroup{
		transcripts: make(map[string]string), finished: make(map[voiceScheduleKey]bool), echo: echo,
	}
	if initialNotice != "" {
		group.notices = append(group.notices, initialNotice)
	}
	added := false
	seen := make(map[voiceScheduleKey]bool, len(voices))
	for _, att := range voices {
		if att.Kind != "voice" || att.FileID == "" {
			continue
		}
		key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: att.FileID}
		if seen[key] {
			continue
		}
		seen[key] = true
		if existing := s.entries[key]; existing != nil {
			if hook != nil {
				existing.hooks = append(existing.hooks, hook)
			}
			if manual && existing.state == voiceWaiting {
				existing.manual = true
				existing.nextAttempt = now
			}
			if !manual {
				s.addTargetLocked(existing, recordID, group, false, false, in, source, states)
			}
			added = true
			continue
		}
		entry := &voiceEntry{
			key: key, inbound: cloneVoiceInbound(in), attachment: att,
			nextAttempt: now, arrived: now, backoff: s.retryBase, manual: manual, state: voiceWaiting,
			targets: make(map[string]voiceResolveTarget), applied: make(map[string]bool),
		}
		if hook != nil {
			entry.hooks = append(entry.hooks, hook)
		}
		s.entries[key] = entry
		s.addTargetLocked(entry, recordID, group, manual && !transcriptOnly, transcriptOnly, in, source, states)
		added = true
	}
	groupUnused := group.remaining == 0
	s.mu.Unlock()
	if added {
		s.Wake()
	}
	if groupUnused {
		group.closeEcho()
	}
	if !added {
		return errors.New("voice scheduler received no keyed voice attachment")
	}
	return nil
}

func (s *VoiceScheduler) addTargetLocked(entry *voiceEntry, recordID string, group *voiceGroup, findPending, transcriptOnly bool, in c3types.Inbound, source *intake.Source, states intake.AttachmentsState) {
	if entry == nil || group == nil {
		return
	}
	if _, exists := entry.targets[recordID]; exists {
		return
	}
	// A manual call can create the lease before startup recovery has attached
	// the pending row's private identity. When recovery supplies that identity,
	// promote the lookup-on-worker target instead of adding a second target that
	// would resolve the row once and then append a duplicate revision.
	if recordID != "" {
		if pending, exists := entry.targets[""]; exists && pending.findPending {
			delete(entry.targets, "")
			pending.recordID = recordID
			pending.findPending = false
			pending.inbound = cloneVoiceInbound(in)
			pending.source = source.Clone()
			pending.attachmentsState = states.Clone()
			pending.stateGroup = group
			entry.targets[recordID] = pending
			entry.groups = append(entry.groups, group)
			group.order = append(group.order, entry.key.fileID)
			group.remaining++
			if entry.state == voiceResolveReady || entry.state == voiceResolveSubmitted {
				s.finishGroupMemberLocked(entry, group)
			}
			return
		}
	}
	entry.targets[recordID] = voiceResolveTarget{
		recordID: recordID, group: group, findPending: findPending, transcriptOnly: transcriptOnly,
		inbound: cloneVoiceInbound(in), source: source.Clone(), attachmentsState: states.Clone(),
	}
	entry.groups = append(entry.groups, group)
	group.order = append(group.order, entry.key.fileID)
	group.remaining++
	if entry.state == voiceResolveReady || entry.state == voiceResolveSubmitted {
		s.finishGroupMemberLocked(entry, group)
	}
}

func (g *voiceGroup) closeEcho() {
	if g == nil {
		return
	}
	g.echoOnce.Do(func() {
		if g.echo.mine != nil {
			close(g.echo.mine)
		}
	})
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
			if s.scheduleAutoIntake(route, row.RecordID, row.Inbound, voices, "", voiceEchoReservation{}, row.Source, row.AttachmentsState) {
				recovered++
				s.backdateArrival(route, row.Inbound.MessageID, voices, row.Inbound.Timestamp)
			}
		}
	}
	if recovered > 0 {
		log.Printf("voice scheduler: recovered %d pending queue row(s) for immediate enrichment", recovered)
	}
}

// backdateArrival moves a recovered note's arrival back to when the message was
// received, so a note that was already waiting before a restart keeps its age
// for the pending status and the slow-note reply.
func (s *VoiceScheduler) backdateArrival(route RouteKey, messageID int64, voices []c3types.Attachment, receivedAt time.Time) {
	if receivedAt.IsZero() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, att := range voices {
		entry := s.entries[voiceScheduleKey{route: route, messageID: messageID, fileID: att.FileID}]
		if entry != nil && receivedAt.Before(entry.arrived) {
			entry.arrived = receivedAt
		}
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

// RetryNow pulls every waiting entry on channel forward to now. It does not
// reset backoff or the retry expiry; it only stops a recovered channel from
// waiting out a long backoff.
func (s *VoiceScheduler) RetryNow(channel string) {
	if s == nil {
		return
	}
	now := s.clock.Now()
	s.mu.Lock()
	for _, entry := range s.entries {
		if entry.state == voiceWaiting && entry.key.route.Channel == channel && entry.nextAttempt.After(now) {
			entry.nextAttempt = now
		}
	}
	s.mu.Unlock()
	s.Wake()
}

// PendingSummary reports the arrival of the oldest note whose fetch or
// transcription is still waiting or running (resolution retries are not
// counted), and whether that note has already failed once. Zero when idle.
func (s *VoiceScheduler) PendingSummary() (oldest time.Time, retrying bool) {
	if s == nil {
		return time.Time{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.entries {
		if entry.state != voiceWaiting && entry.state != voiceRunning {
			continue
		}
		if oldest.IsZero() || entry.arrived.Before(oldest) {
			oldest, retrying = entry.arrived, !entry.firstFailure.IsZero()
		}
	}
	return oldest, retrying
}

func (s *VoiceScheduler) dispatch() {
	defer s.wg.Done()
	timer := s.clock.NewTimer(s.sweepEvery)
	defer timer.Stop()
	nextSweep := s.clock.Now().Add(s.sweepEvery)
	for {
		delay := s.dispatchCycle(&nextSweep)
		timer.Reset(delay)
		select {
		case <-s.ctx.Done():
			return
		case <-s.wake:
		case <-timer.C():
		}
	}
}

func (s *VoiceScheduler) dispatchCycle(nextSweep *time.Time) (delay time.Duration) {
	delay = voiceDispatchBackoff
	defer recoverGoroutineThen("voiceScheduler.dispatch", func() { s.Wake() })
	now := s.clock.Now()
	if !now.Before(*nextSweep) {
		*nextSweep = now.Add(s.sweepEvery)
	}
	for _, key := range s.dispatchDue(now) {
		s.submitResolve(key)
	}
	return s.nextDelay(now, *nextSweep)
}

func (s *VoiceScheduler) dispatchDue(now time.Time) []voiceScheduleKey {
	var resolves []voiceScheduleKey
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.accepting {
		return nil
	}
	for key, entry := range s.entries {
		if entry.state == voiceWaiting && !entry.firstFailure.IsZero() && !now.Before(entry.firstFailure.Add(s.retryExpiry)) {
			s.finishTerminalLocked(entry, s.retryExpiredOutcome(entry))
		}
		if entry.state == voiceResolveReady && s.groupsCompleteLocked(entry) && !now.Before(entry.nextAttempt) {
			resolves = append(resolves, key)
		}
	}
	sort.Slice(resolves, func(i, j int) bool { return voiceKeyLess(resolves[i], resolves[j]) })

	var due []*voiceEntry
	for _, entry := range s.entries {
		if entry.state != voiceWaiting || now.Before(entry.nextAttempt) {
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
dispatchLoop:
	for i, entry := range due {
		select {
		case s.work <- entry.key:
			entry.state = voiceRunning
			entry.manual = false
		default:
			// A full bounded work queue is expected after outage recovery. Move
			// every still-due entry to one fixed near-term boundary so nextDelay
			// cannot turn the dispatcher into a ~1kHz mutex spin loop.
			for _, waiting := range due[i:] {
				waiting.nextAttempt = now.Add(voiceDispatchBackoff)
			}
			break dispatchLoop
		}
	}
	return resolves
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

func (s *VoiceScheduler) groupsCompleteLocked(entry *voiceEntry) bool {
	if entry == nil || len(entry.groups) == 0 {
		return true
	}
	for _, group := range entry.groups {
		if group.remaining != 0 {
			return false
		}
	}
	return true
}

func (s *VoiceScheduler) retryExpiredOutcome(entry *voiceEntry) voiceAttemptResult {
	detail := "voice transcription retry expired before the audio could be fetched"
	agent, notice := fetchFailureTexts(errors.New(detail), entry.attachment.Size)
	return voiceAttemptResult{segmentText: agent, notice: notice, detail: detail}
}

func (s *VoiceScheduler) nextDelay(now, nextSweep time.Time) time.Duration {
	next := nextSweep
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.entries {
		var candidate time.Time
		switch entry.state {
		case voiceWaiting:
			if !entry.firstFailure.IsZero() {
				candidate = entry.firstFailure.Add(s.retryExpiry)
			}
			if candidate.IsZero() || entry.nextAttempt.Before(candidate) {
				candidate = entry.nextAttempt
			}
		case voiceResolveReady:
			if s.groupsCompleteLocked(entry) {
				candidate = entry.nextAttempt
			}
		}
		if !candidate.IsZero() && candidate.Before(next) {
			next = candidate
		}
	}
	d := next.Sub(now)
	if d <= 0 {
		return time.Millisecond
	}
	return d
}

func (s *VoiceScheduler) run() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case key := <-s.work:
			s.runOne(key)
		}
	}
}

func (s *VoiceScheduler) runOne(key voiceScheduleKey) {
	defer recoverGoroutineThen("voiceScheduler.runner", func() { s.abortAttempt(key) })
	attempt, ok := s.attemptSnapshot(key)
	if !ok {
		return
	}
	out := s.runAttempt(s.ctx, attempt)
	if s.ctx.Err() != nil {
		return
	}
	s.finishAttempt(key, out)
}

func (s *VoiceScheduler) abortAttempt(key voiceScheduleKey) {
	now := s.clock.Now()
	s.mu.Lock()
	entry := s.entries[key]
	if entry != nil && entry.state == voiceRunning {
		if entry.firstFailure.IsZero() {
			entry.firstFailure = now
		}
		delay := s.jitter(entry.backoff)
		if delay <= 0 {
			delay = voiceDispatchBackoff
		}
		entry.nextAttempt = now.Add(delay)
		entry.backoff *= 2
		if entry.backoff > s.retryCap {
			entry.backoff = s.retryCap
		}
		entry.state = voiceWaiting
		log.Printf("voice scheduler: recovered aborted attempt chan=%s chat=%d topic=%s msg=%d file_id=%s; retry_in=%s",
			key.route.Channel, key.route.ChatID, TopicKeyStr(key.route), key.messageID, key.fileID, delay.Round(time.Second))
	}
	s.mu.Unlock()
	s.Wake()
}

func (s *VoiceScheduler) attemptSnapshot(key voiceScheduleKey) (voiceAttempt, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.entries[key]
	if entry == nil || entry.state != voiceRunning {
		return voiceAttempt{}, false
	}
	return voiceAttempt{key: key, inbound: cloneVoiceInbound(entry.inbound), attachment: entry.attachment}, true
}

func (s *VoiceScheduler) finishAttempt(key voiceScheduleKey, out voiceAttemptResult) {
	now := s.clock.Now()
	var slowNotice *c3types.Inbound
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
			out = s.retryExpiredOutcome(entry)
			s.finishTerminalLocked(entry, out)
		} else {
			delay := max(s.jitter(entry.backoff), out.retryAfter)
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
			slowNotice = s.takeSlowNoticeLocked(entry, now)
		}
	} else {
		s.finishTerminalLocked(entry, out)
	}
	s.mu.Unlock()
	if slowNotice != nil {
		go s.sendSlowNotice(*slowNotice)
	}
	s.Wake()
}

// takeSlowNoticeLocked claims the entry's one "taking longer" Telegram reply
// once the note has waited voiceSlowNoticeAfter since arrival. It returns the
// message to reply to, or nil. Transcript-only (manual, no chat message)
// targets never get one.
func (s *VoiceScheduler) takeSlowNoticeLocked(entry *voiceEntry, now time.Time) *c3types.Inbound {
	if entry.slowNoticed || entry.key.route.Channel != "telegram" || now.Sub(entry.arrived) < voiceSlowNoticeAfter {
		return nil
	}
	for _, target := range entry.targets {
		if !target.transcriptOnly {
			entry.slowNoticed = true
			in := cloneVoiceInbound(target.inbound)
			return &in
		}
	}
	return nil
}

// sendSlowNotice runs outside s.mu so a slow channel never stalls the
// scheduler. Best-effort and never retried.
func (s *VoiceScheduler) sendSlowNotice(in c3types.Inbound) {
	defer recoverGoroutine("voiceScheduler.slowNotice")
	ch, err := s.broker.Channel(in.Channel)
	if err != nil {
		return
	}
	if _, err := ch.SendReply(c3types.ReplyArgs{
		Channel: in.Channel, ChatID: in.ChatID, TopicID: in.TopicID, ReplyTo: &in.MessageID,
		Text: c3types.WithTestInjectionMarker(&in, voiceSlowNoticeText),
	}); err != nil {
		log.Printf("voice scheduler: slow-note reply chan=%s chat=%d msg=%d failed (not retried): %v", in.Channel, in.ChatID, in.MessageID, err)
	}
}

func (s *VoiceScheduler) finishTerminalLocked(entry *voiceEntry, out voiceAttemptResult) {
	outcome := intake.STTOutcome{STT: intake.STTFailed, Error: out.detail}
	if out.success {
		outcome = intake.STTOutcome{STT: intake.STTDone, Transcript: out.transcript}
	} else if outcome.Error == "" {
		outcome.Error = "transcription_failed"
	}
	entry.resolve = voiceResolve{
		outcome:     outcome,
		segmentText: out.segmentText, success: out.success, transcript: out.transcript, notice: out.notice, detail: out.detail,
	}
	for _, group := range entry.groups {
		s.finishGroupMemberLocked(entry, group)
	}
	entry.state = voiceResolveReady
	entry.nextAttempt = s.clock.Now()
}

func (s *VoiceScheduler) finishGroupMemberLocked(entry *voiceEntry, group *voiceGroup) {
	if entry == nil || group == nil || group.finished[entry.key] {
		return
	}
	group.finished[entry.key] = true
	if group.outcomes == nil {
		group.outcomes = make(map[string]intake.STTOutcome)
	}
	group.outcomes[entry.key.fileID] = entry.resolve.outcome
	if entry.resolve.transcript != "" {
		group.transcripts[entry.key.fileID] = entry.resolve.transcript
	}
	if notice := entry.resolve.notice; notice != "" && !containsString(group.notices, notice) {
		group.notices = append(group.notices, notice)
	}
	if group.remaining > 0 {
		group.remaining--
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// Resolve submission waits for every group member to finish under scheduler.mu.
// After submission, only this route's worker mutates readback and persistence state.
func (g *voiceGroup) demoteToNotice(fileID, notice string) {
	if g == nil {
		return
	}
	delete(g.transcripts, fileID)
	if !containsString(g.notices, notice) {
		g.notices = append(g.notices, notice)
	}
}

func (g *voiceGroup) persistedOutcome(fileID string, outcome intake.STTOutcome, notice string) {
	if g == nil {
		return
	}
	if outcome.STT == intake.STTFailed && outcome.Error == "transcript_too_large" {
		g.demoteToNotice(fileID, notice)
	}
	if g.persisted == nil {
		g.persisted = make(map[string]bool)
	}
	g.persisted[fileID] = true
}

func (g *voiceGroup) persistenceComplete() bool {
	for _, fileID := range g.order {
		if !g.persisted[fileID] {
			return false
		}
	}
	return true
}

func (g *voiceGroup) echoTranscript() string {
	if g == nil {
		return ""
	}
	var transcript string
	for _, fileID := range g.order {
		if text := g.transcripts[fileID]; text != "" {
			transcript = appendVoiceMarker(transcript, text)
		}
	}
	return transcript
}

func (g *voiceGroup) failNotice() string {
	if g == nil {
		return ""
	}
	return strings.Join(g.notices, "\n")
}

func (s *VoiceScheduler) submitResolve(key voiceScheduleKey) {
	s.mu.Lock()
	entry := s.entries[key]
	if !s.accepting || entry == nil || entry.state != voiceResolveReady || !s.groupsCompleteLocked(entry) || s.clock.Now().Before(entry.nextAttempt) {
		s.mu.Unlock()
		return
	}
	targets := make([]voiceResolveTarget, 0, len(entry.targets))
	for targetID, target := range entry.targets {
		if !entry.applied[targetID] {
			target.inbound = cloneVoiceInbound(target.inbound)
			target.source = target.source.Clone()
			target.attachmentsState = target.attachmentsState.Clone()
			group := target.group
			if target.stateGroup != nil {
				group = target.stateGroup
			}
			if group != nil {
				for fileID, outcome := range group.outcomes {
					target.attachmentsState = target.attachmentsState.WithOutcome(attachmentStateSource(target.source, target.inbound), fileID, outcome)
				}
			}
			targets = append(targets, target)
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].recordID < targets[j].recordID })
	if len(targets) == 0 {
		s.mu.Unlock()
		s.completeResolve(key)
		return
	}
	job := &ResolveVoiceJob{
		Key: key, Targets: targets,
		FileID: key.fileID, SegmentText: entry.resolve.segmentText, Success: entry.resolve.success, Outcome: entry.resolve.outcome,
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
	complete := false
	s.mu.Lock()
	if current := s.entries[key]; s.accepting && current != nil {
		if s.allTargetsAppliedLocked(current) {
			complete = true
		} else {
			current.state = voiceResolveReady
			current.nextAttempt = s.clock.Now().Add(s.resolveDelay)
		}
	}
	s.mu.Unlock()
	if complete {
		s.completeResolve(key)
		return
	}
	s.Wake()
}

func (s *VoiceScheduler) markResolveApplied(key voiceScheduleKey, targetID string) {
	s.mu.Lock()
	if current := s.entries[key]; current != nil {
		current.applied[targetID] = true
	}
	s.mu.Unlock()
}

func (s *VoiceScheduler) allTargetsAppliedLocked(entry *voiceEntry) bool {
	if entry == nil || len(entry.targets) == 0 {
		return true
	}
	for targetID := range entry.targets {
		if !entry.applied[targetID] {
			return false
		}
	}
	return true
}

func (s *VoiceScheduler) completeResolve(key voiceScheduleKey) {
	var hooks []chan<- voiceScheduleResult
	var result voiceScheduleResult
	s.mu.Lock()
	if entry := s.entries[key]; entry != nil {
		if !s.allTargetsAppliedLocked(entry) {
			entry.state = voiceResolveReady
			entry.nextAttempt = s.clock.Now()
			s.mu.Unlock()
			s.Wake()
			return
		}
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
	s.StopWithin(defaultShutdownGrace)
}

func (s *VoiceScheduler) StopWithin(timeout time.Duration) bool {
	if s == nil {
		return true
	}
	s.stopOnce.Do(func() {
		var groups []*voiceGroup
		var hooks []chan<- voiceScheduleResult
		seenGroups := make(map[*voiceGroup]bool)
		s.mu.Lock()
		s.accepting = false
		for _, entry := range s.entries {
			hooks = append(hooks, entry.hooks...)
			entry.hooks = nil
			for _, group := range entry.groups {
				if group != nil && !seenGroups[group] {
					seenGroups[group] = true
					groups = append(groups, group)
				}
			}
		}
		s.mu.Unlock()
		for _, group := range groups {
			group.closeEcho()
		}
		for _, hook := range hooks {
			select {
			case hook <- voiceScheduleResult{Err: errVoiceSchedulerStopping}:
			default:
			}
		}
		s.cancel()
		go func() {
			s.wg.Wait()
			close(s.stopped)
		}()
	})
	if timeout <= 0 {
		timeout = defaultShutdownGrace
	}
	select {
	case <-s.stopped:
		return true
	case <-time.After(timeout):
		log.Printf("voice scheduler: ABANDONING shutdown join after %s; stuck plugin/worker work will exit with the process and durable pending rows recover on restart", timeout)
		return false
	}
}

// transcribe runs one STT attempt. When the channel can fetch voice, every
// attempt that reaches the plugin carries a fresh attempt-owned local file
// (removed when the attempt ends), so the handler never fetches: no audio is
// fetched while STT is off, and a missing handler parks the note instead of
// failing it. Fetch and readiness come before the STT deadline starts.
func (s *VoiceScheduler) transcribe(ctx context.Context, attempt voiceAttempt) voiceAttemptResult {
	att := attempt.attachment
	chanName := attempt.key.route.Channel
	payload := c3types.VoicePayload{
		Channel: attempt.inbound.Channel, ChatID: attempt.inbound.ChatID, TopicID: attempt.inbound.TopicID,
		MessageID: attempt.inbound.MessageID, FileID: att.FileID, MIME: att.MIME, Size: att.Size,
	}
	ch, _ := s.broker.Channel(chanName)
	if fetcher, ok := ch.(voiceFetcher); ok {
		if enabled, ready := s.broker.Plugins.VoiceReady(); enabled {
			if !ready {
				return voiceAttemptResult{transient: true, detail: "stt handler unavailable"}
			}
			path, size, refusal := voiceFetchRefusal(ctx, fetcher, att)
			if refusal != nil {
				return *refusal
			}
			defer os.Remove(path)
			payload.LocalPath = path
			if size > 0 {
				payload.Size = size
			}
		}
	} else if cachedPath := s.broker.voiceCachedPath(chanName, att.FileID); cachedPath != "" {
		if info, err := os.Stat(cachedPath); err == nil {
			payload.Size = info.Size()
		} else {
			log.Printf("voice scheduler: cached audio stat failed chan=%s msg=%d file_id=%s path=%s: %v — using inbound size=%d", chanName, attempt.key.messageID, att.FileID, cachedPath, err, payload.Size)
		}
	}

	sttCtx, cancel := context.WithTimeout(ctx, sttFlushTimeout)
	raw := s.broker.Plugins.FireOnVoiceReceived(sttCtx, payload)
	cancel()
	if detail, ok := sttFetchFailure(raw); ok {
		if isNetworkTransient(detail) {
			return voiceAttemptResult{transient: true, detail: detail}
		}
		agent, notice := fetchFailureTexts(errors.New(detail), att.Size)
		return voiceAttemptResult{
			segmentText: agent, notice: notice, detail: detail,
		}
	}
	if raw == "" || isSTTFailureMarker(raw) {
		reason := sttFailureReason(raw)
		// The handler's retained copy, looked up now: it decides whether the
		// recovery text may say the audio is saved.
		retained := s.broker.voiceCachedPath(chanName, att.FileID)
		return voiceAttemptResult{
			segmentText: sttFailureText(att, reason, retained), notice: sttFailureNotice, detail: reason,
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
// Applied ONLY to the legacy handler's own fetch failures ([STT FETCH FAILED:),
// never to provider/transcription failures; a broker fetch is classified by the
// channel's typed error instead.
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
