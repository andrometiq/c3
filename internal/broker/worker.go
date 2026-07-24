package broker

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/karthikeyan5/c3/internal/c3types"
	"github.com/karthikeyan5/c3/internal/ipc"
	"github.com/karthikeyan5/c3/internal/queue"
)

// JobKind tags route-worker jobs.
type JobKind int

const (
	JobInbound JobKind = iota
	JobOutbound
	JobRelease
	JobFetch
	JobConsume
	JobRefreshText
	JobBacklog
	// The three drain steps (drain.go): peek+freeze on the source, copy on the
	// target, remove on the source — each on the OWNING worker so the store's
	// single-owner discipline holds across a cross-route move.
	JobDrainPeek
	JobDrainAppend
	JobDrainRemove
)

// Job is one unit of work for a route worker. Exactly one of the payload
// fields is set based on Kind.
type Job struct {
	Kind        JobKind
	Inbound     *c3types.Inbound
	Outbound    *OutboundJob
	Fetch       *FetchJob
	Consume     *ConsumeJob
	Refresh     *RefreshTextJob
	Backlog     *BacklogJob
	DrainPeek   *DrainPeekJob
	DrainAppend *DrainAppendJob
	DrainRemove *DrainRemoveJob
}

// BacklogJob asks the worker to read the route's queued total AND a compact
// oldest-first preview ATOMICALLY on the single-owner worker goroutine (I7), so
// an attach-time backlog summary never races the worker's Append/Consume/rewrite
// (a separate Pending-then-Peek off-goroutine could report count>0 with an empty
// or stale preview). PeekN bounds the preview; the result returns via ResultCh.
type BacklogJob struct {
	PeekN    int
	ResultCh chan<- BacklogResult
}

// BacklogResult carries the route's total queued count + oldest-first preview
// (up to PeekN) back to the attach handler.
type BacklogResult struct {
	Total   int
	Preview []c3types.Inbound
	Err     error
}

// FetchJob asks the worker to Peek/Consume the route's durable queue. Limit<0
// (or All) means everything. Ack=true consumes; false peeks. The result returns
// via ResultCh.
type FetchJob struct {
	Limit    int
	All      bool
	Ack      bool
	ResultCh chan<- FetchResult
}

// FetchResult carries the pulled messages + remaining count back to the handler.
type FetchResult struct {
	Messages  []c3types.Inbound
	Remaining int
	Err       error
}

// ConsumeJob consumes the queued lines a Claude live push covered, off the front
// (Claude live-ack path). A single push may MERGE a debounced batch of N stored
// lines into one notification (mergeBatch), so the ack must consume ALL N lines
// the push covered, not just one — otherwise N-1 stored lines are orphaned as
// phantom backlog. Count is the number of stored lines the acked push covered
// (>=1); MessageID is the merged push's id (the last in the batch), logged for
// audit. Consumption is strictly oldest-first (live delivery is in arrival
// order), so consuming Count off the head matches exactly the covered lines.
type ConsumeJob struct {
	MessageID int64
	Count     int
}

// RefreshTextJob asks the worker to refresh, in place, the stored Text of the
// still-queued line whose MessageID matches (retranscribe in-place refresh, spec
// Component 5). Routed through the single-owner worker so the cap-safe rewrite
// never touches the route's files off the worker goroutine. The result (whether a
// line was refreshed) returns via ResultCh.
type RefreshTextJob struct {
	MessageID int64
	NewText   string
	ResultCh  chan<- RefreshResult
}

// RefreshResult carries whether a queued line was refreshed back to the handler.
type RefreshResult struct {
	Refreshed bool
	Err       error
}

// OutboundJob is a queued tool-call dispatched to a channel.
type OutboundJob struct {
	Tool     string
	Args     map[string]any
	ResultCh chan<- OutboundResult
}

type OutboundResult struct {
	Result map[string]any
	Err    error
}

// RouteWorker is the per-route serial executor (spec §4.2.0). One goroutine
// owns inbound pipeline + outbound channel calls + per-route mutable state.
//
// Inbound debouncing: spec §7.3 — buffer up to debounceMax messages or
// debounceWindow time, whichever comes first, then forward as a single
// merged Inbound (concatenated text, latest message_id, sum of attachments).
type RouteWorker struct {
	key     RouteKey
	queue   chan Job
	idle    time.Duration
	broker  *Broker
	cancel  context.CancelFunc
	done    chan struct{}
	mu      sync.Mutex
	stopped bool

	// Typing relay state (P5 / spec R3). The ticker re-pulses SendTyping for
	// the route while the agent works a turn. It is ONLY ever touched from the
	// worker's single run goroutine (armed/re-armed/disarmed in forwardOrFallback
	// + dispatchOutbound, drained by the run loop's <-typingC case), so it needs
	// NO lock — no new goroutine is introduced. typingC mirrors the ticker's
	// channel and is nil while disarmed so the select arm parks. The arm gate
	// (holder HasReplied + Capabilities.Typing) lives in armTyping.
	typingTicker *time.Ticker
	typingC      <-chan time.Time

	// typingIvl is the relay re-pulse cadence. Defaults to typingInterval; tests
	// shorten it to exercise pulse behavior within a short idle window.
	typingIvl time.Duration

	// typingPulses counts CONSECUTIVE typing ticks that fired without a reply
	// ending the turn. It is reset to 0 on arm/re-arm and on a reply (disarm),
	// and incremented on each pulse. Belt-and-suspenders: if it exceeds
	// maxTypingPulses the relay self-disarms even if (for any reason) the idle
	// timeout never trips. See pulseTyping.
	typingPulses int

	// dedup suppresses the at-least-once REPLAY a crash-mid-consume cursor rewind
	// can produce (spec: "dedupe by message_id"). Bounded FIFO; touched only from
	// the worker's single run goroutine (flushInbounds), so it needs no lock.
	dedup *deliveredDedup

	// pendingAck holds human inbounds delivered to the current holder that it has
	// not yet demonstrably processed (no outbound since delivery). The adapter acks
	// a push as delivered the moment it writes to the CLI's stdin — a blind ack that
	// CONSUMES the durable copy — so if the holder then dies/exits without handling
	// them, they vanish silently (the 2026-07-12 dentist incident: a stolen-then-
	// dead session ate two messages with no warning). On confirmed holder death
	// these are re-queued + a notice fires (flushPendingAck); any outbound from the
	// holder clears them (it is alive and handling the turn). Worker-goroutine-only
	// (delivered path + dispatchOutbound + pulseTyping), so it needs no lock.
	pendingAck []*c3types.Inbound

	// prevEchoDone chains the per-topic voice-readback echoes so they post in
	// strict arrival order (spec Phase 3 — the maintainer: "processed one by one"). The
	// echo is dispatched off the critical path (a retrying send can back off for
	// seconds), and a bare `go` would let a later note's fast echo overtake an
	// earlier note whose send is backing off. Each echo now waits on the previous
	// echo's done-channel before sending. This field is the tail of that chain:
	// read+written ONLY on the serial run goroutine (flushInbounds), so the swap is
	// race-free; each spawned goroutine only reads its captured `prev` and closes
	// its own `mine`. Initialized PRE-CLOSED in newRouteWorker so the first echo's
	// `<-prev` proceeds immediately (a nil channel would block forever).
	prevEchoDone chan struct{}
}

// debounceWindow / debounceMax defaults from spec §7.3 + §6.
const (
	defaultDebounceWindow  = 1500 * time.Millisecond
	defaultDebounceMaxMsgs = 50

	// typingInterval is the re-pulse cadence for the deterministic typing relay
	// (P5). Telegram's "typing" chat action expires ~5s after it is sent, so a
	// ~4s re-pulse keeps the indicator continuously visible while the agent
	// works a turn.
	typingInterval = 4 * time.Second

	// maxTypingPulses caps how many CONSECUTIVE typing pulses may fire without a
	// reply ending the turn before the relay self-disarms. The worker's idle
	// timeout is the primary stop (a typing tick no longer extends idle), so
	// this is a belt-and-suspenders bound: at typingInterval=4s, 15 pulses is a
	// full minute of an agent that took an inbound but never replied (e.g. the
	// user switched to CLI mode mid-turn). It must never pulse forever.
	maxTypingPulses = 15

	// maxPendingAck bounds how many delivered-but-unprocessed inbounds a route
	// tracks for the silent-loss safety net (flushPendingAck). Past this the oldest
	// are dropped (logged) — a holder this far behind still gets the newest N
	// re-queued on death, the recoverable-and-visible tail that matters.
	maxPendingAck = 32
)

// newRouteWorker starts a worker that runs until ctx is canceled OR no jobs
// arrive within `idle`. broker may be nil in unit tests.
func newRouteWorker(parent context.Context, key RouteKey, idle time.Duration, broker *Broker) *RouteWorker {
	ctx, cancel := context.WithCancel(parent)
	w := &RouteWorker{
		key:       key,
		queue:     make(chan Job, 64),
		idle:      idle,
		broker:    broker,
		cancel:    cancel,
		done:      make(chan struct{}),
		typingIvl: typingInterval,
		// 2048 is comfortably larger than the 1000-message per-route cap, so a
		// full-queue recovery replay is fully covered by the dedup window.
		dedup: newDeliveredDedup(2048),
	}
	// Pre-close the echo chain's head so the FIRST readback echo's `<-prev`
	// proceeds immediately instead of blocking forever on a nil channel
	// (spec Phase 3 / CRITIQUE FOLD #7).
	w.prevEchoDone = make(chan struct{})
	close(w.prevEchoDone)
	go w.run(ctx)
	return w
}

func (w *RouteWorker) run(ctx context.Context) {
	defer close(w.done)
	// A1: drain BEFORE done closes. Deferred LIFO ⇒ shutdown() runs first, then
	// close(w.done). This ordering is load-bearing for A2: when WorkerPool.Submit /
	// the reaper observe done closed, `stopped` is already true and the queue is
	// already drained, so a stopped worker's Submit reliably returns false (never
	// strands) and Submit's respawn loop terminates. shutdown runs on EVERY exit
	// path (ctx.Done, idleTimer, queue-closed, JobRelease) and even on a recovered
	// panic (recoverGoroutine below runs first, then this).
	defer w.shutdown()
	// Backstop: a panic in the run-loop machinery (outside the per-method guards
	// below) is recovered + logged instead of crashing the whole broker. The
	// worker then exits cleanly (done closes); WorkerPool.Submit respawns a fresh
	// worker on the next job for this route. The per-method guards on the
	// panic-prone work (flushInbounds/flushEvent/dispatchOutbound/pulseTyping)
	// keep THIS worker alive for the common case; this only catches the rest.
	defer recoverGoroutine("worker.run")
	idleTimer := time.NewTimer(w.idle)
	defer idleTimer.Stop()
	// Ensure the typing ticker is stopped on any exit (ctx cancel, idle,
	// release, queue close) so it never leaks past the worker's life.
	defer w.disarmTyping()

	var debBuf []*c3types.Inbound
	var debTimer *time.Timer
	var debC <-chan time.Time

	flushDeb := func() {
		if len(debBuf) > 0 {
			w.flushInbounds(ctx, debBuf)
			debBuf = nil
		}
		if debTimer != nil {
			debTimer.Stop()
			debTimer = nil
		}
		debC = nil
	}

	// resetIdle restarts the worker's idle countdown. It is called ONLY from
	// real-work arms (debounce flush, inbound, outbound) — NOT from the typing
	// tick. A typing pulse must not extend the worker's lifetime: typingInterval
	// (4s) is shorter than any sane idle window, so if the tick reset idle, an
	// armed ticker would re-arm idle forever and a worker that took an inbound
	// but never replied (e.g. user switched to CLI mode) would pulse "typing…"
	// indefinitely and never idle out. Resetting idle only on real work lets the
	// worker idle out normally; its defer disarmTyping() then stops the relay.
	resetIdle := func() {
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(w.idle)
	}

	for {
		select {
		case <-ctx.Done():
			flushDeb()
			return
		case <-idleTimer.C:
			flushDeb()
			return
		case <-debC:
			resetIdle()
			flushDeb()
		case <-w.typingC:
			// Typing relay tick (P5). Runs in the worker's single goroutine —
			// no new concurrency. Pulse the channel's typing action for this
			// route; the ticker keeps firing on its own cadence until disarmed.
			// Deliberately does NOT resetIdle — a typing pulse is not real work
			// and must not keep the worker (and thus the relay) alive forever.
			w.pulseTyping(ctx)
		case job, ok := <-w.queue:
			if !ok {
				flushDeb()
				return
			}
			resetIdle()
			switch job.Kind {
			case JobInbound:
				if job.Inbound == nil {
					continue
				}
				// CB-1: a synthesized channel EVENT (poll_result / reaction /
				// callback) must NEVER share a debounce batch with text. Merging
				// it would (a) drop its Kind/Event through mergeBatch's text-only
				// copy and (b) run hasVoice/STT over a non-voice event. So: flush
				// any buffered text first, then forward the event ALONE, bypassing
				// the debounce buffer and the STT path entirely.
				if job.Inbound.IsEvent() {
					flushDeb()
					w.flushEvent(ctx, job.Inbound)
					continue
				}
				debBuf = append(debBuf, job.Inbound)
				maxMsgs := w.debounceMaxMessages()
				if len(debBuf) >= maxMsgs {
					flushDeb()
					continue
				}
				if debTimer == nil {
					debTimer = time.NewTimer(w.debounceWindow())
					debC = debTimer.C
				}
			case JobOutbound:
				w.dispatchOutbound(ctx, job.Outbound)
			case JobFetch:
				w.handleFetch(ctx, job.Fetch)
			case JobConsume:
				w.handleConsume(ctx, job.Consume)
			case JobRefreshText:
				w.handleRefreshText(ctx, job.Refresh)
			case JobBacklog:
				w.handleBacklog(ctx, job.Backlog)
			case JobDrainPeek:
				w.handleDrainPeek(job.DrainPeek)
			case JobDrainAppend:
				w.handleDrainAppend(job.DrainAppend)
			case JobDrainRemove:
				w.handleDrainRemove(job.DrainRemove)
			case JobRelease:
				flushDeb()
				return
			}
		}
	}
}

// sttFlushTimeout bounds each per-inbound voice STT call made by flushInbounds.
// Without it the call inherits only the run-loop ctx (cancelled solely on broker
// shutdown / w.cancel()) plus the STT builtin's own ~300s subprocess budget — so
// a download that hangs BEFORE that budget applies blocks the worker goroutine,
// and any JobFetch/JobConsume queued behind it, for up to ~5 min. It mirrors
// retranscribeTimeout's value (330s, just above the STT builtin's 300s
// subprocess deadline) so a healthy long voice note still completes, but a hung
// download is cut off in bounded time; a timed-out call returns "" and falls
// through to the self-documenting sttFailureText placeholder. It is a var (not a
// const) only so a test can shorten it; production never reassigns it.
var sttFlushTimeout = 330 * time.Second

// flushInbounds runs the plugin pipeline + forwards a debounce-collapsed
// batch as a single ipc.OpInbound.
//
// Merge rules (spec §7.3):
//   - Text: each inbound's text joined with "\n", in arrival order. Voice
//     STT (OnVoiceReceived) runs per-inbound BEFORE merge so transcripts
//     land in the right order.
//   - MessageID: latest in the batch (canonical id for the merged block).
//   - Timestamp: earliest (when the burst started).
//   - Sender: from the latest message (most recent author wins; bursts
//     from a single user are the common case).
//   - ReplyTo: from the FIRST message that has one (only one quote-reply
//     attribution per merged block — agent sees the original anchor).
//   - Attachments: concatenated in order — agent sees all media.
//   - OnInbound chain runs ONCE on the merged Inbound, not per-message.
func (w *RouteWorker) flushInbounds(ctx context.Context, batch []*c3types.Inbound) {
	defer recoverGoroutine(fmt.Sprintf("worker.flushInbounds chan=%s chat=%d", w.key.Channel, w.key.ChatID))
	if w.broker == nil || len(batch) == 0 {
		return
	}

	// Per-inbound STT substitution.
	if w.broker.Plugins != nil {
		for _, in := range batch {
			// CB-1 defense-in-depth: never run voice/STT over a synthesized
			// event. The run loop already diverts events to flushEvent, so a
			// batch here is all ordinary messages — this guard makes that
			// invariant explicit and survives any future caller change.
			if in.IsEvent() || !hasVoice(in) {
				continue
			}
			payload := c3types.VoicePayload{
				Channel:   in.Channel,
				ChatID:    in.ChatID,
				TopicID:   in.TopicID,
				MessageID: in.MessageID,
				FileID:    in.Attachments[0].FileID,
				MIME:      in.Attachments[0].MIME,
				Size:      in.Attachments[0].Size,
			}
			// Per-call deadline so a hung download can't block the worker
			// goroutine (and the JobFetch/JobConsume jobs queued behind it).
			// cancel() runs immediately (not deferred) because this is a
			// per-inbound loop — a deferred cancel would leak every iteration's
			// timer until flushInbounds returns. A "" result (incl. on timeout)
			// flows to the sttFailureText placeholder path below.
			sttCtx, cancel := context.WithTimeout(ctx, sttFlushTimeout)
			transcript := w.broker.Plugins.FireOnVoiceReceived(sttCtx, payload)
			cancel()
			switch {
			case transcript != "" && !isSTTFailureMarker(transcript):
				in.Text = w.sttPrefix(in.Channel) + transcript
			case in.Text == "":
				// Self-documenting failure: the text the AGENT sees becomes a
				// recovery instruction (it names the file_id + how to fetch /
				// retry), not a dead end. The audio is durably queued and
				// recoverable; the user never re-forwards. This fires on BOTH the
				// empty-transcript path (no STT plugin / timeout) AND a non-empty
				// "[STT FAILED: <reason>]" marker from the builtin — the marker
				// only names the log, so we replace it with the rich text and
				// surface the parsed <reason> via sttFailureReason.
				in.Text = sttFailureText(in, sttFailureReason(transcript))
			}
			// Voice-transcript readback echo (moved out of the Python STT handler).
			// ADDITIVE + NON-FATAL: it is a SEND, so it must NEVER affect the
			// agent-surface in.Text set above, inbound delivery, persistence, or
			// loss-freedom. The echo RETRIES on transient outbound failure
			// (readback.go), which can block for seconds (a 429 burst → up to ~6s per
			// note). Running it inline on this route worker's serial goroutine would
			// stall persistence + delivery of THIS and later notes — breaking the
			// invariant above. So dispatch it off the critical path.
			//
			// Ordering (spec Phase 3 — the maintainer: "processed one by one"): a bare `go`
			// would let a SECOND note's fast echo overtake a FIRST note whose send is
			// backing off, posting the courtesy echoes out of arrival order. Instead
			// we CHAIN them: each echo waits on the previous echo's done-channel
			// before sending, so within one topic they post strictly FIFO. Ordering
			// carries ACROSS batches because prevEchoDone is a worker field. The chain
			// link is read+written ONLY here on the serial run goroutine, so the swap
			// is race-free; each spawned goroutine only reads its captured `prev` and
			// closes its own `mine`. The worker's inbound persistence/delivery path
			// still never blocks on echo retries — run only does a non-blocking
			// pointer swap + `go`, exactly as before.
			//
			// It reads a shallow COPY of the inbound so it can't race the persistence
			// loop's later use of `in` (and so a recycled `in` can't corrupt the
			// in-flight echo). ctx is the run-loop context (worker.go run/flushInbounds
			// params); on shutdown w.cancel() cancels it, each parked echo aborts fast
			// via the select and STILL closes `mine`, so the chain drains without
			// deadlock and no echo goroutine leaks.
			//
			// Cross-worker gap (spec CRITIQUE FOLD #6 — benign, documented not fixed):
			// prevEchoDone orders echoes only within one worker's lifetime. A worker
			// idle-evicts after defaultWorkerIdle (60s, broker.go) when no jobs arrive;
			// a detached echo's max backoff is ~6s (readback.go: 3 attempts, ≤3s cap
			// each), so a pending echo always finishes long before eviction and a
			// respawned worker's fresh chain never races a still-running prior echo —
			// the gap is unreachable in practice.
			prev := w.prevEchoDone
			mine := make(chan struct{})
			w.prevEchoDone = mine
			inCopy := *in
			go func(in *c3types.Inbound, transcript string, prev, mine chan struct{}) {
				// Deferred LIFO: close(mine) is registered FIRST so it runs LAST —
				// the chain always advances even if the readback path panics.
				// recoverGoroutine is registered SECOND so it runs FIRST, recovering
				// a panic (which in any goroutine would otherwise crash the whole
				// broker) before close(mine) unblocks the next echo.
				defer close(mine)
				defer recoverGoroutine("echoReadback")
				select {
				case <-prev: // wait for the previous echo to finish → FIFO
				case <-ctx.Done():
					return // shutdown: drop; close(mine) unblocks the next
				}
				w.echoReadback(in, transcript) // ctx-aware retry aborts fast on shutdown
			}(&inCopy, transcript, prev, mine)
		}
	}

	// Durable storage: persist EACH message (one queue line) AFTER STT
	// substitution so the stored line already carries the transcript. Storage is
	// per-message; the merge below is a delivery-presentation concern only and
	// does not merge stored lines. Append failure = persist failure: do NOT mark
	// the update_id done (markPersisted is only called on success), so the
	// Telegram offset can't pass it and the message is redelivered (loss-free).
	// appended counts the lines this batch ACTUALLY persisted (a successful
	// Append). It is NOT len(batch): dedup-suppressed messages are skipped, and an
	// Append failure does not count. The live push's Covered must equal this count
	// so the Claude delivered-ack Consumes exactly the lines this push added — not
	// len(batch), which would over-consume and eat later backlog (I5).
	appended := 0
	if w.broker != nil && w.broker.Queue != nil {
		qrk := queueRouteKey(w.key)
		for _, in := range batch {
			// Dedup the at-least-once REPLAY a crash-mid-consume can produce
			// (spec: "dedupe by message_id"). alreadySeen is PURE (it does not
			// record), so a failed Append below never poisons the dedup set (C2).
			if w.dedup != nil && w.dedup.alreadySeen(in.MessageID) {
				log.Printf("dedup chan=%s chat=%d topic=%s msg=%d: already delivered, suppressing replay",
					w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID)
				// C3: the FIRST delivery of this message_id already persisted it and
				// recorded its update_id; this redelivery is a no-op for storage but
				// its source update_id is still in-flight in the tracker (dispatchMessage
				// recorded msgToUpdate before Emit). markPersisted resolves that
				// update_id → MarkDone so the contiguous-prefix offset advances over
				// the redelivery instead of wedging ALL inbound permanently.
				w.markPersisted(in)
				continue
			}
			if err := w.broker.Queue.Append(qrk, in); err != nil {
				log.Printf("queue append FAIL chan=%s chat=%d topic=%s msg=%d: %v — offset will NOT advance; Telegram redelivers — %s",
					w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID, err, fallbackSummary(in))
				// C2: do NOT record the id as seen — the Append failed, so a later
				// Telegram redelivery of this same message_id must be allowed to
				// persist (not dedup-suppressed and lost). markPersisted is
				// deliberately NOT called either, so the offset holds and Telegram
				// retains/redelivers (loss-free).
				// Item 1: evict the poll-side dedup entry for this update so the
				// held-offset redelivery genuinely RE-DISPATCHES + retries the
				// Append, instead of spinning as a dedup-skip until the 5-min TTL
				// lapses (or forever, on a permanent failure). markPersistFailed is
				// symmetric with markPersisted: the channel resolves the update_id
				// via its msgToUpdate seam and forgets that update's dedup entry.
				// The offset still holds (markPersisted NOT called) — loss-free.
				w.markPersistFailed(in)
				// Best-effort Telegram notice (spec Error-handling: disk-full =
				// persist failure → log + best-effort Telegram notice). Cooldown'd
				// via the existing fallback tracker so a stuck disk doesn't spam.
				w.notePersistFailure(in)
				continue
			}
			// Record as seen ONLY after a successful Append (C2): this is the point
			// at which a future redelivery is a true duplicate worth suppressing.
			if w.dedup != nil {
				w.dedup.record(in.MessageID)
			}
			appended++
			w.markPersisted(in)
			w.evictIfOverCap(qrk)
		}
	} else if w.broker != nil {
		// Item 3: durable queue disabled (init failed → "durable inbound hold
		// DISABLED for this run", broker.go). We cannot persist, but the
		// persisted-offset tracker is still live, so a ROUTED message must STILL be
		// marked persisted — otherwise its source update_id wedges in-flight
		// forever: the committed offset never advances, so EVERY inbound re-polls
		// forever. Advancing the offset here matches the already-accepted
		// in-memory-only degrade (non-durable hold). No Append happened, so this
		// deliberately does NOT touch w.dedup. The loud DISABLED log fires once at
		// queue init (broker.go); we don't re-log per message here to avoid spam.
		for _, in := range batch {
			w.markPersisted(in)
		}
	}

	// Merge (delivery presentation only).
	merged := mergeBatch(batch)

	// OnInbound chain on the merged inbound.
	if w.broker.Plugins != nil {
		next := w.broker.Plugins.FireOnInbound(ctx, merged)
		if next == nil {
			return // dropped
		}
		merged = next
	}

	// Covered = lines ACTUALLY appended by this push (I5), not len(batch). When no
	// queue is wired (unit tests) appended stays 0 and covEffective normalizes to 1.
	w.forwardOrFallback(ctx, merged, appended)
}

// sttFailureText renders the agent-facing STT-failure recovery message. It is
// self-documenting: the agent learns the audio exists, exactly how to fetch it
// (download_attachment), that it can retry transcription (retranscribe), and
// that the user does NOT need to resend. Includes file_id, mime, and duration
// when known. See broker.log (LogPath) for the provider traceback.
func sttFailureText(in *c3types.Inbound, reason string) string {
	fileID, mime, dur := "", "", ""
	if len(in.Attachments) > 0 {
		fileID = in.Attachments[0].FileID
		mime = in.Attachments[0].MIME
	}
	if mime == "" {
		mime = "audio"
	}
	dur = "duration unknown"
	return fmt.Sprintf("⚠️ [voice transcription failed: %s] The audio is saved and recoverable — the user does not need to resend. Call download_attachment with file_id=%q (%s, %s) to retrieve it, or retranscribe with the same file_id to re-run transcription. Try retranscribe ONCE; if it still fails, ask the sender to resend or type it out — do not retry repeatedly. Provider traceback: %s",
		reason, fileID, mime, dur, LogPath())
}

// sttFailureReason extracts the failure reason to surface in sttFailureText. An
// empty transcript (no STT plugin, or a timeout) means STT never produced a
// marker, so the reason is "no_transcript". Otherwise the transcript is the
// builtin's "[STT FAILED: <reason> — see <path>]" marker (see
// stt.sttFailureMarker) and we parse the <reason> token out of it: the text
// between "[STT FAILED: " and the following " — " (or the closing "]" if the
// hint is absent). If parsing yields nothing usable, fall back to "stt_failed".
// Dependency-free (strings only) to keep worker.go free of the stt import.
func sttFailureReason(transcript string) string {
	if transcript == "" {
		return "no_transcript"
	}
	const prefix = "[STT FAILED: "
	rest, ok := strings.CutPrefix(transcript, prefix)
	if !ok {
		return "stt_failed"
	}
	// Cut at the log-hint separator first, then the closing bracket, so a marker
	// with or without the "(see <path>)" hint both parse to the bare reason.
	if i := strings.Index(rest, " — "); i >= 0 {
		rest = rest[:i]
	} else if i := strings.IndexByte(rest, ']'); i >= 0 {
		rest = rest[:i]
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return "stt_failed"
	}
	return rest
}

// isSTTFailureMarker reports whether a transcript is the STT builtin's failure
// marker ("[STT FAILED: <reason> — see <path>]", see stt.sttFailureMarker)
// rather than a real transcript. A marker (or the empty string) means STT did
// not produce text, so the readback echoes a human notice instead of a
// transcript. Kept as a local predicate so worker.go needs no import of the stt
// builtin (which would be a plugin→broker import cycle).
func isSTTFailureMarker(transcript string) bool {
	return strings.HasPrefix(transcript, "[STT FAILED:")
}

// echoReadback sends the voice-transcript readback back to the SOURCE chat — the
// move of Python's send_transcript_to_telegram / notify_transcription_failed
// into Go, reusing the channel's own reliable senders. It is ADDITIVE and
// strictly NON-FATAL: a SEND can never affect inbound delivery, persistence, or
// loss-freedom, so every error here only logs.
//
//   - On a REAL transcript (non-empty, not a marker): resolve the channel and,
//     IF it implements the OPTIONAL readbacker interface (only Telegram does
//     today), render the frozen readback via SendReadback. A channel without it
//     is skipped silently — other channels need no changes.
//   - On an STT FAILURE (empty transcript or a "[STT FAILED:" marker): send a
//     short human-facing notice via the channel's normal SendReply (this
//     replaces Python's notify_transcription_failed). Once per failed voice
//     inbound, best-effort.
func (w *RouteWorker) echoReadback(in *c3types.Inbound, transcript string) {
	if w.broker == nil {
		return
	}
	ch, err := w.broker.Channel(in.Channel)
	if err != nil {
		// Channel not resolvable (unit tests / non-telegram route): nothing to
		// echo to. Skip silently — the agent surface (in.Text) is already set.
		return
	}
	// Failure: an empty transcript or the STT failure marker → a human notice,
	// not a transcript. The agent-surface marker path (in.Text) is unchanged.
	if transcript == "" || isSTTFailureMarker(transcript) {
		if _, serr := ch.SendReply(c3types.ReplyArgs{
			Channel: in.Channel, ChatID: in.ChatID, TopicID: in.TopicID, ReplyTo: &in.MessageID,
			Text: "⚠️ Couldn't transcribe that voice note — see logs / try again.",
		}); serr != nil {
			log.Printf("readback notice chan=%s chat=%d topic=%s msg=%d: send failed (non-fatal): %v",
				w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID, serr)
		}
		return
	}
	// Success: echo the transcript via the channel's optional readback renderer.
	// Other channels simply don't implement it and are skipped.
	rb, ok := ch.(interface {
		SendReadback(c3types.ReadbackArgs) (int64, error)
	})
	if !ok {
		return
	}
	if _, serr := rb.SendReadback(c3types.ReadbackArgs{
		ChatID: in.ChatID, ReplyTo: &in.MessageID, TopicID: in.TopicID, Transcript: transcript,
	}); serr != nil {
		log.Printf("readback chan=%s chat=%d topic=%s msg=%d: SendReadback failed (non-fatal): %v",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID, serr)
	}
}

// flushEvent forwards a single synthesized channel EVENT (poll_result /
// reaction / callback) straight through delivery. CB-1: events are NEVER
// debounce-merged with text and NEVER run through the voice/STT substitution —
// they are delivered intact and on their own. The OnInbound plugin chain still
// runs (so a plugin can observe/drop an event), but the per-inbound STT loop in
// flushInbounds is skipped entirely. An event whose chain returns nil is
// dropped, matching the message path.
func (w *RouteWorker) flushEvent(ctx context.Context, ev *c3types.Inbound) {
	defer recoverGoroutine(fmt.Sprintf("worker.flushEvent chan=%s chat=%d", w.key.Channel, w.key.ChatID))
	if w.broker == nil || ev == nil {
		return
	}
	// Ask round-trip resolution (Phase 1): an inline-keyboard callback whose data
	// is "ask:<askID>:<idx>" resolves a registered blocking ask — resolveAsk pushes
	// the chosen option to the holder as an OpAskResult and clears the keyboard. On
	// a match we SUPPRESS the generic event entirely (the tap was the answer, not a
	// fresh <channel> event, and plugins should not observe it). A tap for an
	// unknown/already-resolved ask returns false and falls through to the normal
	// event path; the channel already auto-acked it either way.
	if ev.Kind == c3types.InboundCallback && ev.Event != nil && ev.Event.Callback != nil {
		cb := ev.Event.Callback
		if strings.HasPrefix(cb.Data, askCallbackPrefix) {
			if w.broker.resolveAsk(w.key, cb) {
				return
			}
		}
		// Permission relay (Phase 1): a "perm:<verb>:<id>" callback resolves a
		// relayed permission prompt — resolvePerm pushes an OpPermissionVerdict to
		// the holder and clears the keyboard, gated to the operator. A "perm:" tap is
		// C3-internal — it is NEVER a meaningful generic event (unlike "ask:", whose
		// prefix an agent-rendered reply button could legitimately reuse). So always
		// SUPPRESS it, whether or not it resolved: a non-operator / unknown / expired /
		// route-mismatched tap must not be surfaced as a raw callback event into the
		// session (it's already auto-acked by the channel regardless).
		if strings.HasPrefix(cb.Data, permCallbackPrefix) {
			w.broker.resolvePerm(w.key, cb)
			return
		}
	}
	if w.broker.Plugins != nil {
		next := w.broker.Plugins.FireOnInbound(ctx, ev)
		if next == nil {
			return // dropped by a plugin
		}
		ev = next
	}
	w.forwardOrFallback(ctx, ev, 0)
}

// mergeBatch collapses a batch of inbounds into one. See flushInbounds for
// the merge rules. Single-element batches return the element unchanged.
//
// CB-1 invariant: this is only ever called with ORDINARY message inbounds. The
// run loop diverts every Kind != "" event to flushEvent before it can reach a
// debounce batch, so an event never lands here. As defense-in-depth, if an
// event ever does appear in a multi-element batch it is carried through verbatim
// (its Kind/Event preserved) rather than silently text-spliced — see the
// per-element copy below. The single-element fast path already returns the
// element unchanged, preserving Kind/Event for the (event-alone) case.
func mergeBatch(batch []*c3types.Inbound) *c3types.Inbound {
	if len(batch) == 1 {
		return batch[0]
	}
	last := batch[len(batch)-1]
	out := &c3types.Inbound{
		Channel:   last.Channel,
		ChatID:    last.ChatID,
		TopicID:   last.TopicID,
		MessageID: last.MessageID,
		Sender:    last.Sender,
		Timestamp: batch[0].Timestamp,
		// Carry the latest event metadata through so a stray event in a batch is
		// not silently dropped. In normal operation the run loop guarantees no
		// event reaches mergeBatch (events flush alone via flushEvent).
		Kind:  last.Kind,
		Event: last.Event,
	}
	var texts []string
	for _, in := range batch {
		if in.Text != "" {
			texts = append(texts, in.Text)
		}
		out.Attachments = append(out.Attachments, in.Attachments...)
		if out.ReplyTo == nil && in.ReplyTo != nil {
			out.ReplyTo = in.ReplyTo
		}
	}
	out.Text = strings.Join(texts, "\n")
	return out
}

// forwardOrFallback is the post-pipeline delivery: claimed stub or
// cooldown-fallback reply.
//
// Logging policy (DEBUGGING.md):
//   - Successful delivery to an adapter → one terse line, NO content.
//   - Any failure path where the CLI never saw the message → log line
//     INCLUDES content (sender, text [200-char cap], attachment summary).
//     This is the explicit "don't lose undelivered content" rule from
//     2026-05-09: "I don't want you to lose the content without
//     delivering it anywhere".
//
// "Failure paths" here means anything that ends without a `delivered` line:
// holder-conn-bad, write-error, fallback-cooldown-drop, fallback-send-fail,
// AND fallback-sent (the user's message was bounced back to Telegram with a
// boilerplate; no CLI processed it).
func (w *RouteWorker) forwardOrFallback(_ context.Context, in *c3types.Inbound, covered int) {
	holder, claimed := w.broker.Routes.Holder(w.key)

	// Liveness sweep: if the holder's PID is dead (e.g. Claude Code killed
	// the adapter as part of /mcp reconnect, or the user quit the CLI),
	// the claim is stale. Release it and fall through to fallback. Without
	// this, every inbound for a dead-holder claim ends with a "holder.Conn
	// is not *ipc.Conn" log line and silently never reaches anywhere
	// (including no Telegram fallback). This is the 2026-05-14 regression
	// the maintainer hit immediately after /mcp reconnect.
	if claimed && !holder.IsAlive() {
		log.Printf("deliver STALE chan=%s chat=%d topic=%s msg=%d: holder cli=%s pid=%d is dead, releasing claim",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID,
			holder.CLI, holder.PID)
		w.broker.Routes.Release(w.key, holder.ConnID)
		// Silent-loss net: any earlier pushes this now-dead holder never handled
		// were consumed on its blind delivered-ack — re-queue them + notify.
		w.flushPendingAck("The session exited")
		claimed = false
		holder = nil
	}

	// Render-incapable holder (forked-session blackhole fix): the holder is a
	// live, claimed session whose HOST silently drops channel push notifications
	// (a Claude Code session launched without the development-channels flag —
	// typically a --fork-session background job; reported at hello via
	// CannotRenderChannels). Its adapter would ACK the push as delivered, but the
	// frame is discarded before rendering, so a "delivered" inbound VANISHES.
	// Treat it exactly like the alive-but-disconnected BOUNCE below: never push
	// (and thus never mark delivered) — fall through to the durable queue +
	// held-notice, recoverable via fetch_queue (an MCP tool-result, which DOES
	// render) and visible to the human as the held count. Do NOT release the
	// claim: the session legitimately holds the route for OUTBOUND (reply/tools
	// still work). Gated to NON-events: events are never queued and carry no lost
	// content, so leaving them on the current push path is zero behavior change
	// (they'd be dropped by the host anyway, same as today).
	if claimed && !in.IsEvent() && !holder.CanRenderPush() {
		log.Printf("deliver HELD chan=%s chat=%d topic=%s msg=%d: holder cli=%s pid=%d cannot render channel push — queuing for fetch_queue — %s",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID,
			holder.CLI, holder.PID, fallbackSummary(in))
		claimed = false
		holder = nil
	}

	// Holder is alive (kill -0 succeeded) but its conn is nil — the adapter
	// is between reconnects (D2 / adapter-ipc-1). Previously this DROPPED the
	// inbound silently ("alive but disconnected"); a message sent during the
	// adapter's reconnect window was lost forever, with nothing to replay it.
	// Instead, treat an unreachable-but-alive holder like "no live CLI" and
	// fall through to the SAME Telegram fallback the STALE branch uses, so the
	// user is told to resend rather than left wondering where their message
	// went. We do NOT release the claim — the holder is still alive and will
	// rebind its conn on reconnect; only THIS inbound bounces.
	// Snapshot the holder's conn ONCE under its lock (ConnValue is race-safe vs
	// the handler goroutine's MarkDisconnected/Reattach), and use that one value
	// for both the bounce-check and the delivery — a second raw read could race
	// to a different value mid-delivery.
	var holderConn *ipc.Conn
	if claimed {
		holderConn, _ = holder.ConnValue().(*ipc.Conn)
		if holderConn == nil {
			log.Printf("deliver BOUNCE chan=%s chat=%d topic=%s msg=%d: holder cli=%s pid=%d alive but disconnected — falling back so user can resend — %s",
				w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID,
				holder.CLI, holder.PID, fallbackSummary(in))
			claimed = false
			holder = nil
		}
	}

	if claimed {
		conn := holderConn
		// Stamp the covered-line count + remaining backlog onto the live push so
		// (a) the OpInboundDelivered ack can Consume exactly the lines this push
		// covered (a merged batch covers N stored lines, not 1) and (b) the Claude
		// adapter can append the recovery nudge. The covered lines are STILL queued
		// at push time (only Consumed on the ack), so subtract covered from the
		// route's pending count → a fully-caught-up live session shows Pending:0.
		//
		// C1: a synthesized EVENT (poll_result / reaction / callback) is NEVER
		// queued, so it covers ZERO stored lines. Forcing Covered=0 here is
		// defense-in-depth alongside the adapter's IsEvent() ack-guard: even an
		// older adapter that acked an event push would Consume 0 (handleConsume
		// skips Count<=0), so a live event can never over-consume real backlog.
		covered := covEffective(covered) // >=1
		if in.IsEvent() {
			covered = 0
		}
		pending := 0
		if w.broker != nil && w.broker.Queue != nil {
			if n, _ := w.broker.Queue.Pending(queueRouteKey(w.key)); n > covered {
				pending = n - covered // covered lines are still queued until acked
			}
		}
		if err := conn.WriteJSON(ipc.InboundMsg{Op: ipc.OpInbound, Inbound: *in, Covered: covered, Pending: pending}); err != nil {
			log.Printf("deliver FAIL chan=%s chat=%d topic=%s msg=%d to cli=%s pid=%d: %v — %s",
				w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID,
				holder.CLI, holder.PID, err, fallbackSummary(in))
			return
		}
		// Live delivery: the message stays queued until the adapter sends
		// OpInboundDelivered{ok:true}, which Consumes it (queue dispatch task).
		// This keeps an un-acked push recoverable as backlog (recovery nudge).
		log.Printf("delivered chan=%s chat=%d topic=%s msg=%d to cli=%s pid=%d conn=%d",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID,
			holder.CLI, holder.PID, holder.ConnID)
		// Typing relay (P5): an inbound was just delivered to the claimed
		// holder, so the agent is about to work this turn. Arm the typing
		// ticker — but ONLY if this holder has already replied at least once
		// (the deterministic "in Telegram mode" gate; see Stub.hasReplied) and
		// the channel supports typing. armTyping enforces both gates.
		w.armTyping(holder)
		// Silent-loss net: this push was acked-as-delivered and its durable copy
		// gets consumed on the adapter's blind ack, so track it until the holder
		// proves it handled the turn (any outbound clears pendingAck) — else it is
		// re-queued if the holder dies. Events carry no lost content, never queued.
		if !in.IsEvent() {
			w.trackPendingAck(in)
		}
		return
	}
	// A synthesized channel EVENT (poll_result / reaction / callback) is NOT a
	// human message awaiting a reply, so it must never be bounced back as the
	// "no CLI attached" fallback boilerplate. A late timed/auto-close poll can
	// fire after the session detaches; on an unclaimed route we drop it with a
	// metadata-only log instead of spamming the chat. (A CLAIMED route still
	// receives the event unchanged — handled in the claimed branch above.)
	if in.IsEvent() {
		log.Printf("drop chan=%s chat=%d topic=%s msg=%d: no claim, event not bounced as fallback (kind=%s)",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID, in.Kind)
		return
	}
	// Append-if-absent: the normal path already appended this message (and its
	// MessageID is recorded in w.dedup) in flushInbounds. A direct-call/test path
	// (or any future caller that reaches forwardOrFallback without flushInbounds)
	// has NOT — so append it here, gated by the SAME dedup set so the normal path
	// does not double-store (flushInbounds already recorded the id, so alreadySeen
	// returns true here and we skip). We use the alreadySeen/record split (C2) so a
	// failed Append never poisons the dedup set: only record on a successful Append.
	// This keeps the running count below accurate for the message the user just
	// sent. Events are never queued.
	if w.broker.Queue != nil {
		if w.dedup == nil || !w.dedup.alreadySeen(in.MessageID) {
			if err := w.broker.Queue.Append(queueRouteKey(w.key), in); err == nil {
				if w.dedup != nil {
					w.dedup.record(in.MessageID)
				}
			}
		}
	}
	// No live claim: the message is already durably queued (flushInbounds — or
	// the append-if-absent above — stored it). Surface a "held, nothing lost"
	// auto-reply carrying the RUNNING queued count.
	//
	// On a channel that can EDIT messages, send a FRESH held-notice on every hold
	// (never edit in place). Each newly-queued message must re-notify the operator:
	// a silent in-place edit bumps the visible count but fires no notification, so
	// there is no signal that a new message actually landed in the queue. A channel
	// that cannot edit keeps the original cooldown-gated single reply (unchanged;
	// it serves hypothetical bare channels).
	ch, err := w.broker.Channel(in.Channel)
	if err != nil {
		log.Printf("hold FAIL chan=%s chat=%d topic=%s msg=%d: channel lookup: %v — %s",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID, err, fallbackSummary(in))
		return
	}
	count := 1
	if w.broker.Queue != nil {
		if n, _ := w.broker.Queue.Pending(queueRouteKey(w.key)); n > 0 {
			count = n
		}
	}
	args := c3types.ReplyArgs{Channel: in.Channel, ChatID: in.ChatID, TopicID: in.TopicID, Text: heldReplyText(count)}

	if ch.Capabilities().EditMessages {
		// Debounce (msg 6083): coalesce a burst of holds into ONE notice per route
		// per HeldNotices window. The message is already durably queued above; only
		// the re-notification is throttled. Without this a flood — the Windows
		// offset-loop, or many messages arriving fast — fired a fresh held-notice
		// per message. Leading-edge: the first hold in a window alerts immediately
		// (with the current queued count); further holds within the window are
		// silent; a hold after a quiet gap re-alerts — preserving #36's per-message
		// re-alert intent, just rate-limited so it can't flood.
		if w.broker.HeldNotices != nil && !w.broker.HeldNotices.ShouldSend(w.key) {
			log.Printf("hold chan=%s chat=%d topic=%s msg=%d: no claim, queued + held-reply COALESCED (count=%d, within %s debounce) — %s",
				w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID, count, defaultHeldNoticeCooldown, fallbackSummary(in))
			return
		}
		// Send a FRESH held-notice (never edit in place); the leading-edge throttle
		// above prevents a per-message flood. Prior notices are left in place.
		if _, serr := ch.SendReply(args); serr != nil {
			log.Printf("hold FAIL chan=%s chat=%d topic=%s msg=%d: send held-reply: %v — %s",
				w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID, serr, fallbackSummary(in))
			return
		}
		log.Printf("hold chan=%s chat=%d topic=%s msg=%d: no claim, queued + held-reply SENT (count=%d) — %s",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID, count, fallbackSummary(in))
		return
	}

	// Channel cannot edit messages: preserve the original cooldown-gated single
	// reply so a no-edit channel still doesn't spam the topic on every hold.
	if !w.broker.Fallbacks.ShouldSend(w.key) {
		log.Printf("hold chan=%s chat=%d topic=%s msg=%d: no claim, queued; held-reply in cooldown — %s",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID, fallbackSummary(in))
		return
	}
	if _, err := ch.SendReply(args); err != nil {
		log.Printf("hold FAIL chan=%s chat=%d topic=%s msg=%d: send held-reply: %v — %s",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID, err, fallbackSummary(in))
		return
	}
	log.Printf("hold chan=%s chat=%d topic=%s msg=%d: no claim, queued + held-reply (count=%d) — %s",
		w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID, count, fallbackSummary(in))
}

// fallbackSummary returns a one-liner of message content for use in
// failure-path log lines. Content is truncated (text 200 chars) and
// quote-escaped. ONLY call this on paths where the message was NOT
// delivered to a CLI — successful delivery never logs content.
func fallbackSummary(in *c3types.Inbound) string {
	var parts []string
	switch {
	case in.Sender.Username != "" && in.Sender.UserID != 0:
		parts = append(parts, fmt.Sprintf("from=@%s(uid=%d)", in.Sender.Username, in.Sender.UserID))
	case in.Sender.Username != "":
		parts = append(parts, fmt.Sprintf("from=@%s", in.Sender.Username))
	case in.Sender.UserID != 0:
		parts = append(parts, fmt.Sprintf("from=uid=%d", in.Sender.UserID))
	}
	if in.Text != "" {
		text := in.Text
		const maxText = 200
		if len(text) > maxText {
			text = text[:maxText] + "…"
		}
		parts = append(parts, fmt.Sprintf("text=%q", text))
	}
	if in.ReplyTo != nil {
		parts = append(parts, fmt.Sprintf("reply_to=%d", in.ReplyTo.MessageID))
	}
	for _, att := range in.Attachments {
		parts = append(parts, fmt.Sprintf("attach=%s/%d", att.Kind, att.Size))
	}
	if len(parts) == 0 {
		return "(no content)"
	}
	return strings.Join(parts, " ")
}

// covEffective normalizes a covered-line count to >=1 (a live push always covers
// at least the one merged message; 0/negative comes from the event path or a
// direct test call and is treated as a single line).
func covEffective(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

// markPersisted notifies the broker that an inbound's source update_id is now
// durably stored, so the persisted-offset tracker may advance over it. The
// broker holds the channel-side tracker via a registered callback (set when the
// telegram channel starts); a nil callback (unit tests, non-telegram) is a no-op.
func (w *RouteWorker) markPersisted(in *c3types.Inbound) {
	if w.broker == nil {
		return
	}
	w.broker.notifyPersisted(in)
}

// markPersistFailed notifies the broker that an inbound's durable Append FAILED,
// so the telegram channel can evict that update's poll-side dedup entry and let
// the Telegram redelivery genuinely retry (item 1). Symmetric with markPersisted
// but for the failure edge: the offset is NOT advanced (markPersisted is
// deliberately not called), so Telegram retains + redelivers the update
// (loss-free). A nil callback (unit tests / non-telegram) is a no-op.
func (w *RouteWorker) markPersistFailed(in *c3types.Inbound) {
	if w.broker == nil {
		return
	}
	w.broker.notifyPersistFailed(in)
}

// notePersistFailure sends ONE best-effort, cooldown'd Telegram notice when a
// durable Append fails (spec Error-handling: "Disk full on append: treat as
// persist failure → do not advance offset → Telegram retains → log + (best-
// effort) Telegram notice"). The offset non-advance + Telegram retention is the
// real safety net; this notice just tells the human why a message seems stuck.
// Reuses the existing fallback cooldown so a stuck disk does not spam the topic.
func (w *RouteWorker) notePersistFailure(in *c3types.Inbound) {
	if w.broker == nil || w.broker.Fallbacks == nil || !w.broker.Fallbacks.ShouldSend(w.key) {
		return
	}
	ch, err := w.broker.Channel(w.key.Channel)
	if err != nil {
		return
	}
	var topicID *int64
	if w.key.HasTopic {
		t := w.key.TopicID
		topicID = &t
	}
	// in's content is intentionally NOT echoed back to the chat (it failed to
	// persist, but we don't surface user content in an error notice). The caller
	// already logged the metadata via fallbackSummary(in) on the append-fail line.
	_, _ = ch.SendReply(c3types.ReplyArgs{
		Channel: w.key.Channel, ChatID: w.key.ChatID, TopicID: topicID,
		Text: "⚠️ Could not persist a received message (storage error) — it was NOT lost; Telegram will redeliver it. Check the broker host's disk.",
	})
}

// evictIfOverCap enforces the per-route cap. On a drop it logs + sends ONE
// Telegram notice (never silent). Errors are logged, not fatal.
func (w *RouteWorker) evictIfOverCap(qrk queue.RouteKey) {
	dropped, err := w.broker.Queue.EvictOverCap(qrk)
	if err != nil {
		log.Printf("queue evict FAIL chan=%s chat=%d topic=%s: %v", w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), err)
		return
	}
	if dropped == 0 {
		return
	}
	log.Printf("queue CAP chan=%s chat=%d topic=%s: dropped %d oldest held message(s) over cap", w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), dropped)
	if ch, cerr := w.broker.Channel(w.key.Channel); cerr == nil {
		var topicID *int64
		if w.key.HasTopic {
			t := w.key.TopicID
			topicID = &t
		}
		_, _ = ch.SendReply(c3types.ReplyArgs{
			Channel: w.key.Channel, ChatID: w.key.ChatID, TopicID: topicID,
			Text: fmt.Sprintf("⚠️ queue full — dropped %d oldest held message(s); attach a session soon.", dropped),
		})
	}
}

// handleFetch peeks or consumes the route's durable queue and returns the batch.
func (w *RouteWorker) handleFetch(_ context.Context, job *FetchJob) {
	if job == nil || job.ResultCh == nil {
		return
	}
	defer recoverGoroutineThen("worker.handleFetch", func() {
		select {
		case job.ResultCh <- FetchResult{Err: fmt.Errorf("internal panic in fetch_queue")}:
		default:
		}
	})
	if w.broker == nil || w.broker.Queue == nil {
		job.ResultCh <- FetchResult{Err: errOutboundNotImpl}
		return
	}
	qrk := queueRouteKey(w.key)
	n := job.Limit
	if job.All {
		n = -1
	}
	var msgs []c3types.Inbound
	var err error
	if job.Ack {
		msgs, err = w.broker.Queue.Consume(qrk, n)
	} else {
		msgs, err = w.broker.Queue.Peek(qrk, n)
	}
	if err != nil {
		job.ResultCh <- FetchResult{Err: err}
		return
	}
	remaining, _ := w.broker.Queue.Pending(qrk)
	if !job.Ack {
		remaining -= len(msgs) // peek doesn't advance; "remaining after this batch"
		if remaining < 0 {
			remaining = 0
		}
	}
	log.Printf("fetch_queue chan=%s chat=%d topic=%s ack=%v returned=%d remaining=%d",
		w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), job.Ack, len(msgs), remaining)
	job.ResultCh <- FetchResult{Messages: msgs, Remaining: remaining}
}

// handleBacklog reads the route's total queued count + an oldest-first preview
// ATOMICALLY on the single-owner worker goroutine (I7), so an attach-time backlog
// summary is consistent with itself and never races a concurrent Append/Consume/
// rewrite. Peek does not advance the cursor (the agent drains via fetch_queue).
func (w *RouteWorker) handleBacklog(_ context.Context, job *BacklogJob) {
	if job == nil || job.ResultCh == nil {
		return
	}
	defer recoverGoroutineThen("worker.handleBacklog", func() {
		select {
		case job.ResultCh <- BacklogResult{Err: fmt.Errorf("internal panic in backlog")}:
		default:
		}
	})
	if w.broker == nil || w.broker.Queue == nil {
		job.ResultCh <- BacklogResult{}
		return
	}
	qrk := queueRouteKey(w.key)
	total, _ := w.broker.Queue.Pending(qrk)
	if total == 0 {
		job.ResultCh <- BacklogResult{}
		return
	}
	preview, err := w.broker.Queue.Peek(qrk, job.PeekN)
	if err != nil {
		// Surface the total even when the preview read failed (the count came back
		// fine); the handler logs and renders count without items.
		job.ResultCh <- BacklogResult{Total: total, Err: err}
		return
	}
	job.ResultCh <- BacklogResult{Total: total, Preview: preview}
}

// handleConsume drops the oldest Count queued messages (Claude live-ack: a
// pushed notification the adapter accepted, which may have MERGED a debounced
// batch of Count stored lines). Consuming Count off the head matches exactly the
// lines the push covered — otherwise a merged push of N would orphan N-1 stored
// lines as phantom backlog. MessageID is logged for audit; consumption is
// strictly oldest-first (live delivery is in arrival order).
//
// C1: Count<=0 means the push covered ZERO stored lines (an EVENT push, which is
// never queued). We SKIP the consume entirely — bumping 0→1 here would Consume a
// real queued backlog message that the event never delivered, silently dropping
// it. Only a push that actually added lines (Count>=1) consumes.
func (w *RouteWorker) handleConsume(_ context.Context, job *ConsumeJob) {
	if job == nil || w.broker == nil || w.broker.Queue == nil {
		return
	}
	defer recoverGoroutine("worker.handleConsume")
	n := job.Count
	if n < 1 {
		// Event / zero-covered ack: nothing to consume. Skip rather than consume 1.
		return
	}
	qrk := queueRouteKey(w.key)
	if _, err := w.broker.Queue.Consume(qrk, n); err != nil {
		log.Printf("queue consume(live-ack) FAIL chan=%s chat=%d topic=%s msg=%d count=%d: %v",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), job.MessageID, n, err)
	}
}

// handleRefreshText rewrites, in place, the stored Text of the still-queued line
// whose MessageID matches (retranscribe in-place refresh, spec Component 5). Runs
// on the route's single-owner worker so the cap-safe rewrite never races other
// file ops for this route. A miss (message not queued / already consumed) is a
// clean no-op reported as Refreshed=false.
func (w *RouteWorker) handleRefreshText(_ context.Context, job *RefreshTextJob) {
	if job == nil || job.ResultCh == nil {
		return
	}
	defer recoverGoroutineThen("worker.handleRefreshText", func() {
		select {
		case job.ResultCh <- RefreshResult{Err: fmt.Errorf("internal panic in refresh_text")}:
		default:
		}
	})
	if w.broker == nil || w.broker.Queue == nil {
		job.ResultCh <- RefreshResult{Err: errOutboundNotImpl}
		return
	}
	qrk := queueRouteKey(w.key)
	refreshed, err := w.broker.Queue.RefreshText(qrk, job.MessageID, job.NewText)
	if err != nil {
		log.Printf("retranscribe refresh FAIL chan=%s chat=%d topic=%s msg=%d: %v",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), job.MessageID, err)
		job.ResultCh <- RefreshResult{Err: err}
		return
	}
	log.Printf("retranscribe refresh chan=%s chat=%d topic=%s msg=%d refreshed=%v",
		w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), job.MessageID, refreshed)
	job.ResultCh <- RefreshResult{Refreshed: refreshed}
}

// deliveredDedup is a bounded FIFO set of recently-delivered MessageIDs for this
// route. It suppresses the at-least-once REPLAY that a crash-mid-consume cursor
// rewind can produce (spec: "dedupe by message_id"). Bounded so it never grows
// without limit; the window only needs to cover a recovery replay, not history.
type deliveredDedup struct {
	seen  map[int64]struct{}
	order []int64
	cap   int
}

func newDeliveredDedup(capN int) *deliveredDedup {
	return &deliveredDedup{seen: make(map[int64]struct{}, capN), cap: capN}
}

// alreadySeen reports whether id was already delivered. PURE — it never records,
// so a caller may check-then-Append-then-record and skip recording on an Append
// failure (C2: a disk-full Append must not poison the dedup set, or every later
// Telegram redelivery of the same message is suppressed and the message is lost).
// id==0 is unidentifiable and never deduped.
func (d *deliveredDedup) alreadySeen(id int64) bool {
	if id == 0 {
		return false // unidentifiable; never dedup
	}
	_, ok := d.seen[id]
	return ok
}

// record marks id as delivered (dropping the oldest when over cap). Call ONLY
// after the message is durably persisted (a successful Append) so a failed
// Append never records the id (C2). A no-op for id==0 or an already-recorded id.
func (d *deliveredDedup) record(id int64) {
	if id == 0 {
		return // unidentifiable; never dedup
	}
	if _, ok := d.seen[id]; ok {
		return
	}
	d.seen[id] = struct{}{}
	d.order = append(d.order, id)
	if len(d.order) > d.cap {
		old := d.order[0]
		d.order = d.order[1:]
		delete(d.seen, old)
	}
}

// dispatchOutbound translates an OutboundJob into a channel call, returning
// the result via job.ResultCh.
func (w *RouteWorker) dispatchOutbound(_ context.Context, job *OutboundJob) {
	if job == nil || job.ResultCh == nil {
		return
	}
	// A panic in the channel send (dispatchTool) must NOT crash the broker AND
	// must NOT leave the waiting tool-call blocked on ResultCh forever — recover,
	// log, and best-effort deliver a failure result (non-blocking, in case the
	// result was already sent).
	defer recoverGoroutineThen("worker.dispatchOutbound tool="+job.Tool, func() {
		select {
		case job.ResultCh <- OutboundResult{Err: fmt.Errorf("internal panic dispatching tool %q", job.Tool)}:
		default:
		}
	})
	if w.broker == nil {
		job.ResultCh <- OutboundResult{Err: errOutboundNotImpl}
		return
	}
	ch, err := w.broker.Channel(w.key.Channel)
	if err != nil {
		job.ResultCh <- OutboundResult{Err: err}
		return
	}
	result, err := dispatchTool(ch, w.key, job.Tool, job.Args)

	// Typing relay (P5). All of this runs in the worker's single goroutine.
	//   - A successful `reply` ENDS the turn: mark the holder as having replied
	//     (so future turns on this route arm typing) and disarm the ticker.
	//   - A successful non-reply tool-call (edit_message / react /
	//     download_attachment / send_typing) means the agent is still working;
	//     re-arm so typing stays visible across the turn's tool calls. Re-arm is
	//     gated the same way as the initial arm (holder HasReplied + Typing cap)
	//     via armTyping.
	if err == nil {
		// The holder produced outbound this turn — it is alive and processing, so
		// its delivered backlog is being handled: clear the silent-loss tracker.
		w.pendingAck = nil
		if job.Tool == "reply" {
			if holder, ok := w.broker.Routes.Holder(w.key); ok {
				holder.MarkReplied()
			}
			w.disarmTyping()
		} else if holder, ok := w.broker.Routes.Holder(w.key); ok {
			w.armTyping(holder)
		}
	}

	job.ResultCh <- OutboundResult{Result: result, Err: err}
}

// armTyping starts (or keeps running) the per-route typing ticker, IF the
// deterministic gates pass: the holder has replied at least once (Telegram-mode
// proxy) AND the channel advertises Typing. Both gates avoid pulsing "typing…"
// for default CLI-mode sessions. Called only from the worker goroutine
// (forwardOrFallback delivered path + dispatchOutbound non-reply path), so it
// mutates worker-local ticker state without a lock and never holds a lock across
// a network call. An already-armed ticker is left running (re-arm = no-op while
// armed) so the cadence stays steady across a turn's tool calls.
func (w *RouteWorker) armTyping(holder *Stub) {
	if holder == nil || !holder.HasReplied() {
		return
	}
	if w.broker == nil {
		return
	}
	ch, err := w.broker.Channel(w.key.Channel)
	if err != nil || !ch.Capabilities().Typing {
		return
	}
	if w.typingTicker != nil {
		// Already armed. A re-arm marks fresh agent activity (a non-reply tool
		// call this turn), so reset the unanswered-pulse counter but KEEP the
		// existing ticker so the cadence stays steady across a turn's calls.
		w.typingPulses = 0
		return
	}
	ivl := w.typingIvl
	if ivl <= 0 {
		ivl = typingInterval
	}
	w.typingTicker = time.NewTicker(ivl)
	w.typingC = w.typingTicker.C
	w.typingPulses = 0
}

// disarmTyping stops the per-route typing ticker if armed. Idempotent. Called
// from the worker goroutine (dispatchOutbound reply path) and from run's defer
// on worker shutdown — so the ticker never leaks past the worker's life.
func (w *RouteWorker) disarmTyping() {
	if w.typingTicker == nil {
		return
	}
	w.typingTicker.Stop()
	w.typingTicker = nil
	w.typingC = nil
	w.typingPulses = 0
}

// trackPendingAck records a delivered-but-unconfirmed human inbound for the
// silent-loss net, bounded by maxPendingAck (oldest dropped + logged past the
// cap). See the pendingAck field and flushPendingAck.
func (w *RouteWorker) trackPendingAck(in *c3types.Inbound) {
	w.pendingAck = append(w.pendingAck, in)
	if over := len(w.pendingAck) - maxPendingAck; over > 0 {
		log.Printf("pendingAck chan=%s chat=%d topic=%s: over cap, dropping %d oldest tracked delivery(ies)",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), over)
		w.pendingAck = w.pendingAck[over:]
	}
}

// flushPendingAck re-queues every inbound that was delivered to a now-dead holder
// but never handled, then posts one notice to the topic so the operator is told.
// This closes the silent-loss gap: the adapter acked those pushes as delivered
// (consuming the durable copy) on a blind stdin write, but the holder exited
// without processing them. Called ONLY on confirmed holder death, so it never
// fires for a merely-slow live session. No-op when nothing is tracked.
func (w *RouteWorker) flushPendingAck(reason string) {
	if len(w.pendingAck) == 0 {
		return
	}
	lost := w.pendingAck
	w.pendingAck = nil
	requeued := 0
	if w.broker != nil && w.broker.Queue != nil {
		for _, in := range lost {
			if err := w.broker.Queue.Append(queueRouteKey(w.key), in); err != nil {
				log.Printf("pendingAck flush FAIL chan=%s chat=%d topic=%s msg=%d: re-queue: %v",
					w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), in.MessageID, err)
				continue
			}
			requeued++
		}
	}
	log.Printf("pendingAck flush chan=%s chat=%d topic=%s: %s — re-queued %d/%d unprocessed delivery(ies)",
		w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), reason, requeued, len(lost))
	if requeued == 0 || w.broker == nil {
		return
	}
	ch, err := w.broker.Channel(w.key.Channel)
	if err != nil {
		return
	}
	var topicID *int64
	if w.key.HasTopic {
		t := w.key.TopicID
		topicID = &t
	}
	args := c3types.ReplyArgs{
		Channel: w.key.Channel,
		ChatID:  w.key.ChatID,
		TopicID: topicID,
		Text:    unprocessedNoticeText(requeued, reason),
	}
	if _, serr := ch.SendReply(args); serr != nil {
		log.Printf("pendingAck flush chan=%s chat=%d topic=%s: send notice failed: %v",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), serr)
	}
}

// unprocessedNoticeText is the operator notice for flushPendingAck: N inbounds
// were delivered to a holder that died before handling them and have been put
// back on the durable queue, recoverable on the next attach / fetch_queue.
func unprocessedNoticeText(n int, reason string) string {
	noun := "message"
	if n != 1 {
		noun = "messages"
	}
	return fmt.Sprintf("⚠️ %s before answering %d %s it had received. "+
		"They're back in the queue — they'll resurface when a session re-attaches "+
		"(or run fetch_queue). Nothing lost.", reason, n, noun)
}

// pulseTyping fires one SendTyping for the route. Resolves the channel + the
// route's chat/topic, releases (it holds no lock across the call), then sends.
// On error it logs and disarms — a persistently failing channel should not keep
// the ticker spinning. Runs in the worker goroutine (the run loop's <-typingC).
func (w *RouteWorker) pulseTyping(_ context.Context) {
	defer recoverGoroutine(fmt.Sprintf("worker.pulseTyping chan=%s chat=%d", w.key.Channel, w.key.ChatID))
	if w.broker == nil {
		w.disarmTyping()
		return
	}
	// Silent-loss net (proactive): if the holder has died while we still hold
	// pushes it never handled, re-queue them + notify now rather than waiting for
	// the next inbound to detect the dead claim.
	if holder, ok := w.broker.Routes.Holder(w.key); ok && !holder.IsAlive() {
		w.flushPendingAck("The session exited")
		w.disarmTyping()
		return
	}
	// Belt-and-suspenders bound: if the relay has pulsed maxTypingPulses times
	// without a reply ending the turn, disarm even though the idle timeout is the
	// primary stop. Guards against any future change that re-extends idle on a
	// pulse and would otherwise let "typing…" spin forever.
	w.typingPulses++
	if w.typingPulses > maxTypingPulses {
		log.Printf("typing RELAY CAP chan=%s chat=%d topic=%s: %d consecutive pulses with no reply — disarming relay",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), w.typingPulses-1)
		w.disarmTyping()
		return
	}
	ch, err := w.broker.Channel(w.key.Channel)
	if err != nil {
		w.disarmTyping()
		return
	}
	var topicID *int64
	if w.key.HasTopic {
		t := w.key.TopicID
		topicID = &t
	}
	if err := ch.SendTyping(w.key.ChatID, topicID); err != nil {
		log.Printf("typing PULSE FAIL chan=%s chat=%d topic=%s: %v (disarming relay)",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), err)
		w.disarmTyping()
	}
}

// Submit enqueues a job. Returns false if the worker is stopped or the
// queue is full.
func (w *RouteWorker) Submit(job Job) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return false
	}
	select {
	case w.queue <- job:
		return true
	default:
		return false
	}
}

// shutdown makes "worker exiting" and "Submit accepting" mutually exclusive
// (A1). It runs as a defer from run() on EVERY exit path. Under w.mu it (1) sets
// w.stopped so any concurrent/subsequent Submit returns false instead of pushing
// a job into the buffer of a worker whose run goroutine has already returned, and
// (2) drains every job still queued, replying an error to each result-channel-
// bearing job so a caller blocked on <-resultCh is never stranded (the original
// fetch_queue/attach-backlog wedge). Submit also takes w.mu, so the set+drain is
// atomic with respect to enqueue: after shutdown returns, the queue is empty and
// no further job can be enqueued.
//
// Loss-freedom (W1 Watch-out #4): the drain MUST NOT mark any inbound update_id
// done, consume any queue line, or advance any Telegram offset. It only replies
// errors to ResultCh-bearing jobs. JobInbound / JobConsume carry no ResultCh and
// are dropped silently — they re-deliver via the Telegram offset (JobInbound) or
// remain durable backlog (JobConsume); only the real persisted path may ack.
// Never fake-ack.
//
// Idempotent: the stopped flag set is guarded (Stop() may have set it already),
// but the drain ALWAYS runs — a job can slip into the queue between Stop()'s set
// and run()'s exit, and that job must still be drained.
func (w *RouteWorker) shutdown() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.stopped {
		w.stopped = true
	}
	for {
		select {
		case job := <-w.queue:
			// Only ResultCh-bearing jobs (JobFetch / JobOutbound / JobRefreshText /
			// JobBacklog / the three JobDrain* steps) get an errWorkerStopped reply so
			// a blocked caller is never stranded. This is what lets the drain's
			// Steps B/C block INDEFINITELY on their result channels (drain.go, B1):
			// a stopped worker reliably answers instead of wedging the drain
			// goroutine forever. JobInbound / JobConsume / JobRelease carry no
			// ResultCh and have no case below — they drop silently (loss-free; see
			// the doc comment above: they re-deliver via the Telegram offset or
			// remain durable backlog). Never ack.
			switch job.Kind {
			case JobFetch:
				if job.Fetch != nil && job.Fetch.ResultCh != nil {
					select {
					case job.Fetch.ResultCh <- FetchResult{Err: errWorkerStopped}:
					default:
					}
				}
			case JobOutbound:
				if job.Outbound != nil && job.Outbound.ResultCh != nil {
					select {
					case job.Outbound.ResultCh <- OutboundResult{Err: errWorkerStopped}:
					default:
					}
				}
			case JobRefreshText:
				if job.Refresh != nil && job.Refresh.ResultCh != nil {
					select {
					case job.Refresh.ResultCh <- RefreshResult{Err: errWorkerStopped}:
					default:
					}
				}
			case JobBacklog:
				if job.Backlog != nil && job.Backlog.ResultCh != nil {
					select {
					case job.Backlog.ResultCh <- BacklogResult{Err: errWorkerStopped}:
					default:
					}
				}
			case JobDrainPeek:
				if job.DrainPeek != nil && job.DrainPeek.ResultCh != nil {
					select {
					case job.DrainPeek.ResultCh <- DrainPeekResult{Err: errWorkerStopped}:
					default:
					}
				}
			case JobDrainAppend:
				if job.DrainAppend != nil && job.DrainAppend.ResultCh != nil {
					select {
					case job.DrainAppend.ResultCh <- DrainAppendResult{Err: errWorkerStopped}:
					default:
					}
				}
			case JobDrainRemove:
				if job.DrainRemove != nil && job.DrainRemove.ResultCh != nil {
					select {
					case job.DrainRemove.ResultCh <- DrainRemoveResult{Err: errWorkerStopped}:
					default:
					}
				}
			}
		default:
			return
		}
	}
}

// Stop signals the worker to drain and exit. Idempotent.
func (w *RouteWorker) Stop() {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.stopped = true
	w.mu.Unlock()
	w.cancel()
	<-w.done
}

// Done returns a channel that closes when the worker has exited.
func (w *RouteWorker) Done() <-chan struct{} { return w.done }

var errOutboundNotImpl = workerErr("worker has no broker reference")

// errWorkerStopped is the error replied to every result-channel-bearing job that
// shutdown() drains when run() exits with the job still queued (A1). The caller
// (handleFetch/handleToolCall/attach-backlog/retranscribe-refresh) returns this
// as a clean error rather than wedging on a never-answered channel.
var errWorkerStopped = workerErr("worker stopped before job ran")

type workerErr string

func (e workerErr) Error() string { return string(e) }

func hasVoice(in *c3types.Inbound) bool {
	return len(in.Attachments) > 0 && in.Attachments[0].Kind == "voice"
}

func (w *RouteWorker) sttPrefix(chanName string) string {
	if w.broker == nil || w.broker.Mappings() == nil {
		return "[Transcribed voice]: "
	}
	cc, ok := w.broker.Mappings().Channels[chanName]
	if !ok || cc.STTPrefix == "" {
		return "[Transcribed voice]: "
	}
	return cc.STTPrefix
}

func (w *RouteWorker) debounceWindow() time.Duration {
	if w.broker == nil || w.broker.Mappings() == nil {
		return defaultDebounceWindow
	}
	cc, ok := w.broker.Mappings().Channels[w.key.Channel]
	if !ok || cc.DebounceMS <= 0 {
		return defaultDebounceWindow
	}
	return time.Duration(cc.DebounceMS) * time.Millisecond
}

func (w *RouteWorker) debounceMaxMessages() int {
	if w.broker == nil || w.broker.Mappings() == nil {
		return defaultDebounceMaxMsgs
	}
	cc, ok := w.broker.Mappings().Channels[w.key.Channel]
	if !ok || cc.DebounceMaxMessages <= 0 {
		return defaultDebounceMaxMsgs
	}
	return cc.DebounceMaxMessages
}
