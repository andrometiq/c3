package broker

// drain.go is the pooled-queue drain core (design spec
// docs/.loop/pooled-queue-DESIGN-SPEC.md §4 + amendments A1/A2/A4, B1/B6/B8).
// Broker.Drain is the SINGLE implementation every front door calls — the
// /drain Telegram command (phase 3), and any future MCP verb or confirm card
// are thin parsers over the same function (COMMANDS.md "implemented once").
//
// The 4-step flow, each durable step on the route's OWNING worker so the
// store's single-owner discipline holds (internal/queue/store.go):
//
//	Step A  JobDrainPeek on the SOURCE worker — freeze the captured lines plus
//	        their immutable queue-private RecordIDs. Edited messages can reuse
//	        MessageID; durable line identity, not a recomputed ordinal, names
//	        what was selected.
//	Step B  JobDrainAppend on the TARGET worker — presence-check each exact
//	        source RecordID, privately stamp that provenance on the copy, stamp
//	        public DrainedFrom with the canonical numeric source key (B6),
//	        rewrite routing, bake the banner into Text, fsync'd append. This is
//	        the durability commit (INV-1). deliveredDedup is never seeded.
//	Step C  JobDrainRemove on the SOURCE worker — RemoveRecordIDs with the
//	        frozen identities; surviving-intersection semantics (INV-5): a
//	        frozen line consumed meanwhile is reported, never replaced by a
//	        same-MessageID edit that shifted into its old position.
//	Step D  advisory, best-effort — a live alive holder on the target gets a
//	        targeted SystemEvent nudge via a DIRECT conn write (A4: never the
//	        forwardOrFallback/worker push path); otherwise ONE drain-notice is
//	        posted to the target topic. The durable queue is the only truth.

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

// DrainSpec is the fully-RESOLVED input to Broker.Drain. The command layer
// (phase 3) parses and resolves names/serials/`dm` into concrete RouteKeys and
// display names BEFORE calling — Drain itself never touches the topic
// registry, so every front door shares one resolution-free core.
type DrainSpec struct {
	// Source and Target are the resolved route keys. They must differ — the
	// command layer rejects source==target upstream, and Drain asserts it again
	// (fail-closed; ErrDrainSameRoute).
	Source RouteKey
	Target RouteKey
	// SourceName / TargetName are human display names for the reply and the
	// provenance banner (e.g. "project"). Empty falls back to the canonical
	// numeric route key so a nameless route still renders unambiguously.
	SourceName string
	TargetName string
	// Selector picks which pending lines move. The zero value selects ALL.
	Selector DrainSelector
}

// DrainSelector picks which pending lines a drain moves, applied against the
// SOURCE queue's oldest-first pending order at freeze time (Step A). Ordinals
// are 1-based and number LINES (occurrences), not unique MessageIDs (A2).
// The zero value is SelectAll.
type DrainSelector struct {
	Kind SelectorKind
	// N is the first-N count (Kind == SelectFirstN). first-N CLAMPS to the
	// pending count — "first 10" of a 4-deep queue moves 4 and
	// DrainResult.Clamped reports it (§2: "up to N").
	N int
	// Lo / Hi are the inclusive 1-based ordinal bounds (Kind == SelectRange).
	// Unlike first-N, an explicit range past the pending count REJECTS with the
	// actual pending count (RangeBeyondPendingError): the operator named lines
	// that do not exist, so clamping would silently move the wrong window.
	Lo, Hi int
}

// SelectorKind tags DrainSelector. The zero value is SelectAll.
type SelectorKind int

const (
	SelectAll SelectorKind = iota
	SelectFirstN
	SelectRange
)

// DrainResult reports what a drain actually did, in the terms the /drain reply
// echoes (§4: resolved names, ordinal window, counts, first-message preview,
// partial-survival note, live-attached warning).
type DrainResult struct {
	// SourceName / TargetName echo the resolved display names (spec fields,
	// canonical-key fallback) so the reply always names what actually moved.
	SourceName string
	TargetName string
	// SourceKey / TargetKey are the canonical numeric route keys
	// (queue RouteKey.File form). SourceKey is the exact DrainedFrom stamp (B6).
	SourceKey string
	TargetKey string

	// Requested is the frozen selection size — the number of LINES Step A
	// captured (multiset occurrences, not unique ids).
	Requested int
	// WindowLo / WindowHi echo the resolved 1-based oldest-first ordinal window
	// ("moved 6-10"). For first-N after a clamp, WindowHi is the clamped end.
	WindowLo, WindowHi int
	// Clamped is true when a first-N selector asked for more than pending and
	// was clamped ("drained the first 4 — the queue only had 4").
	Clamped bool

	// Appended is the number of lines NEWLY copied into the target by this
	// drain. PresenceSkipped counts frozen lines whose copy was ALREADY pending
	// in the target from a prior partially-landed attempt (INV-3 idempotency) —
	// durably landed either way, so both feed Step C's remove.
	Appended        int
	PresenceSkipped int
	// RemovedFromSource / AlreadyGone are the surviving-intersection report
	// (INV-5): AlreadyGone frozen lines were consumed off the source by a
	// concurrent fetch between freeze and remove ("drained 3 of 5 — 2 already
	// gone"); their copies still landed, a bounded double (house posture).
	RemovedFromSource int
	AlreadyGone       int

	// FirstPreview is a short single-line preview of the OLDEST moved line, for
	// the reply echo (mis-pick detection).
	FirstPreview string
	// TargetPending is the target's total pending count right after Step B —
	// the "M total queued" the advisory quotes.
	TargetPending int

	// Warnings carries non-fatal advisories for the reply (e.g. the ⚠
	// live-attached-source note from the §7 edge table).
	Warnings []string
	// NudgeSent / NoticeSent record which Step-D advisory landed: a targeted
	// SystemEvent to the target's live holder, or the one-per-drain notice
	// posted in the target topic. Both false = advisory failed (queue is still
	// truth; attach's own backlog nudge advertises the lines later).
	NudgeSent  bool
	NoticeSent bool
}

// ErrDrainSameRoute rejects a drain whose source and target resolve to the
// same queue. Asserted in Drain even though the command layer rejects it
// upstream (fail-closed).
var ErrDrainSameRoute = errors.New("drain: source and target are the same queue")

// DrainInProgressError rejects a second concurrent drain on a source that is
// already mid-drain (A1: the per-source in-flight lock kills the
// interleaved-drain double-copy class outright).
type DrainInProgressError struct{ Source string }

func (e *DrainInProgressError) Error() string {
	return fmt.Sprintf("a drain on «%s» is already running — wait for it to finish", e.Source)
}

// EmptySourceError rejects a drain from a queue with nothing pending (§7).
type EmptySourceError struct{ Source string }

func (e *EmptySourceError) Error() string {
	return fmt.Sprintf("queue «%s» is empty — nothing to drain", e.Source)
}

// BadSelectorError rejects a malformed selector (N<1, Lo<1, Lo>Hi). The
// command layer renders Reason with its one-line grammar hint (§7).
type BadSelectorError struct{ Reason string }

func (e *BadSelectorError) Error() string { return "drain: " + e.Reason }

// RangeBeyondPendingError rejects an explicit range that runs past the queue,
// carrying the actual pending count so the reply can offer the correction
// ("queue has 8 — try 6-8 or all", §2). first-N clamps instead — only a
// spelled-out range is held to exactly what the operator named.
type RangeBeyondPendingError struct{ Lo, Hi, Pending int }

func (e *RangeBeyondPendingError) Error() string {
	if e.Lo > e.Pending {
		return fmt.Sprintf("drain: range %d-%d starts beyond the queue — it has only %d pending; try all or a range within 1-%d", e.Lo, e.Hi, e.Pending, e.Pending)
	}
	return fmt.Sprintf("drain: range %d-%d runs past the queue — it has %d pending; try %d-%d or all", e.Lo, e.Hi, e.Pending, e.Lo, e.Pending)
}

// DrainStep names the worker round-trip a broker-busy failure hit, so the
// caller can report honestly WHERE the drain stopped.
type DrainStep string

const (
	DrainStepFreeze DrainStep = "freeze (peek on the source)"
	DrainStepCopy   DrainStep = "copy (append on the target)"
	DrainStepRemove DrainStep = "remove (on the source)"
)

// DrainBusyError is returned when Workers.Submit refuses a step's job (worker
// queue full — cap 64 — or the pool stopped). CopiesLanded distinguishes the
// mid-drain case (Step B done, Step C not submitted): the copies are durably
// in the target and the source still holds the originals — a bounded double,
// never loss — and a re-issue converges (INV-2/INV-3).
type DrainBusyError struct {
	Step         DrainStep
	CopiesLanded bool
}

func (e *DrainBusyError) Error() string {
	if e.CopiesLanded {
		return fmt.Sprintf("drain: broker busy at %s — the copies ARE durably in the target and the source still holds the originals (bounded double, never loss). Re-issue the same drain to converge: the presence check skips the landed copies and the remove completes (INV-2/INV-3).", e.Step)
	}
	return fmt.Sprintf("drain: broker busy at %s (worker queue full or stopped) — nothing was changed; try again shortly", e.Step)
}

// drainLocks is the per-SOURCE in-flight drain guard (A1). Keyed by the
// source's canonical queue file key and deliberately NON-blocking: a second
// drain on a busy source rejects immediately instead of queueing behind the
// first (the operator re-issues consciously). Keyed by source only (B8):
// inverted concurrent drains (X→Y ∥ Y→X) cannot deadlock — tryAcquire never
// holds the mutex across a step, and per-worker serialization orders the file
// ops — a mid-flight line may merely bounce back with a stacked banner
// (edge-table row, recoverable, accepted). The zero value is ready to use.
type drainLocks struct {
	mu       sync.Mutex
	inFlight map[string]struct{}
}

// tryAcquire claims key, returning false when a drain already holds it.
func (d *drainLocks) tryAcquire(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inFlight == nil {
		d.inFlight = map[string]struct{}{}
	}
	if _, busy := d.inFlight[key]; busy {
		return false
	}
	d.inFlight[key] = struct{}{}
	return true
}

// release frees key. Idempotent.
func (d *drainLocks) release(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.inFlight, key)
}

// drainTestHookAfterCopy, when non-nil, runs after Step B's copies have
// durably landed and before Step C removes. Production never sets it; tests
// use it to interleave work in the B→C window deterministically (a concurrent
// source consume for the partial-survival path, a second Drain for the
// in-flight-lock path).
var drainTestHookAfterCopy func()

// Drain moves the selected pending lines from spec.Source's durable queue into
// spec.Target's, loss-free. See the file header for the step map and the
// design spec for the invariants (INV-1..8).
//
// Execution contract (A1 + B1): Drain BLOCKS — run it on its own goroutine,
// never on the telegram poll goroutine or an IPC read loop. Step A is a
// non-destructive peek, so it keeps the workerJobTimeout select (an abandoned
// peek consumes nothing; the late result lands in a readerless cap-1 channel,
// harmless). Steps B and C are durable mutations and block INDEFINITELY on
// their cap-1 result channels — abandoning a mutation that lands late would
// diverge from disk truth, exactly why the fetch(ack) path never times out
// (queue_dispatch.go). A stopped worker replies errWorkerStopped fast
// (shutdown() drains ResultCh-bearing jobs), so "indefinitely" in practice
// means "until the owning worker finishes or exits".
//
// Crash/interruption story: nothing before Step B changes any file. A failure
// after copies land leaves source+target both holding the lines (INV-1:
// double, never zero); re-issuing the same drain converges — the presence
// check skips landed copies (INV-3) and the source, mutated only in Step C
// (INV-2), re-resolves identically.
func (b *Broker) Drain(spec DrainSpec) (DrainResult, error) {
	srcKey := queueRouteKey(spec.Source).File()
	dstKey := queueRouteKey(spec.Target).File()
	res := DrainResult{
		SourceName: drainDisplayName(spec.SourceName, srcKey),
		TargetName: drainDisplayName(spec.TargetName, dstKey),
		SourceKey:  srcKey,
		TargetKey:  dstKey,
	}
	if b.Queue == nil {
		return res, errors.New("drain: durable queue disabled for this run — cannot drain")
	}
	if b.Workers == nil {
		return res, errors.New("drain: worker pool unavailable")
	}
	if srcKey == dstKey {
		return res, ErrDrainSameRoute
	}
	// Per-source in-flight lock (A1): a concurrent /drain on the same source
	// rejects immediately — two interleaved drains would each freeze the same
	// head and double-copy it.
	if !b.drains.tryAcquire(srcKey) {
		return res, &DrainInProgressError{Source: res.SourceName}
	}
	defer b.drains.release(srcKey)

	// §7 edge table: a live-attached source is allowed (the blessed pool is
	// unattached) but warned — a concurrent fetch there can consume frozen
	// lines mid-drain (reported as "already gone", copies bounded-double).
	if holder, held := b.Routes.Holder(spec.Source); held && holder.IsAlive() {
		res.Warnings = append(res.Warnings,
			fmt.Sprintf("⚠ source «%s» is live-attached — a concurrent fetch there may consume frozen lines mid-drain (they were also copied; bounded double)", res.SourceName))
	}

	// ---- Step A: freeze on the SOURCE worker. --------------------------------
	peekCh := make(chan DrainPeekResult, 1)
	if !b.Workers.Submit(spec.Source, Job{Kind: JobDrainPeek, DrainPeek: &DrainPeekJob{ResultCh: peekCh}}) {
		return res, &DrainBusyError{Step: DrainStepFreeze}
	}
	var pendingTracked []queue.TrackedInbound
	select {
	case r := <-peekCh:
		if r.Err != nil {
			return res, fmt.Errorf("drain: freeze peek on «%s»: %w", res.SourceName, r.Err)
		}
		pendingTracked = r.Tracked
	case <-time.After(workerJobTimeout):
		// Peek is non-destructive: abandoning a genuinely stalled worker (e.g. a
		// >30s STT flush ahead of us) changes nothing — the late result is
		// dropped by the cap-1 channel and the operator just retries.
		return res, fmt.Errorf("drain: source worker did not respond within %s — nothing was changed; try again", workerJobTimeout)
	}
	if len(pendingTracked) == 0 {
		return res, &EmptySourceError{Source: res.SourceName}
	}
	pending := make([]c3types.Inbound, len(pendingTracked))
	for i := range pendingTracked {
		pending[i] = pendingTracked[i].Inbound
	}
	captured, lo, hi, clamped, err := applyDrainSelector(pending, spec.Selector)
	if err != nil {
		return res, err
	}
	res.Requested = len(captured)
	res.WindowLo, res.WindowHi = lo, hi
	res.Clamped = clamped
	res.FirstPreview = drainPreview(&captured[0])
	// Freeze immutable source-line IDs for both target presence and source
	// removal. Empty IDs are legacy records: they may be copied, but are never
	// guessed at during removal and never presence-skipped on retry.
	sourceRecordIDs := make([]string, len(captured))
	for i := range sourceRecordIDs {
		sourceRecordIDs[i] = pendingTracked[lo-1+i].RecordID
	}
	log.Printf("drain freeze src=%s dst=%s pending=%d frozen=%d window=%d-%d clamped=%v",
		srcKey, dstKey, len(pending), len(captured), lo, hi, clamped)

	// ---- Step B: copy on the TARGET worker. ----------------------------------
	// The transform is pure (no file access) so it runs here on the drain
	// goroutine over the frozen snapshot; only the presence check + appends
	// need the target worker. Per captured line: banner from the ORIGINAL
	// fields FIRST, then stamp DrainedFrom = canonical numeric source key (B6),
	// then rewrite Channel/ChatID/TopicID to the target route (graft 2 —
	// held-notices and any record-keyed path are built from the RECORD's
	// fields, worker.go forwardOrFallback, so a stale route would mis-post to
	// the source topic). MessageID/Sender/Timestamp/Attachments are preserved
	// verbatim (R14 — a drained voice note stays re-downloadable). Text is
	// frozen as captured at Step A by design (B4): a late source voice resolve
	// between A and C reaches only .trash, the documented cost of the ⚠-warned
	// attached-source path.
	moved := make([]DrainAppendMessage, len(captured))
	for i := range captured {
		m := captured[i]
		banner := drainBanner(res.SourceName, &m)
		m.DrainedFrom = srcKey
		m.Channel = spec.Target.Channel
		m.ChatID = spec.Target.ChatID
		if spec.Target.HasTopic {
			t := spec.Target.TopicID
			m.TopicID = &t
		} else {
			m.TopicID = nil
		}
		m.Text = banner + m.Text
		moved[i] = DrainAppendMessage{Inbound: m, SourceRecordID: sourceRecordIDs[i]}
	}
	appendCh := make(chan DrainAppendResult, 1)
	if !b.Workers.Submit(spec.Target, Job{Kind: JobDrainAppend, DrainAppend: &DrainAppendJob{From: srcKey, Messages: moved, ResultCh: appendCh}}) {
		return res, &DrainBusyError{Step: DrainStepCopy}
	}
	// B1: durable mutation — block indefinitely (see the Drain doc comment).
	ar := <-appendCh
	res.Appended, res.PresenceSkipped, res.TargetPending = ar.Appended, ar.Skipped, ar.Pending
	if ar.Err != nil {
		return res, fmt.Errorf("drain: copy to «%s» failed after landing %d of %d line(s): %w — the source is untouched; re-issue the same drain (landed copies are presence-skipped, INV-2/INV-3)",
			res.TargetName, ar.Appended+ar.Skipped, len(moved), ar.Err)
	}

	if drainTestHookAfterCopy != nil {
		drainTestHookAfterCopy()
	}

	// ---- Step C: remove on the SOURCE worker. --------------------------------
	// Every frozen line durably landed (appended or presence-skipped), so the
	// remove uses the FULL frozen identity selection. RemoveRecordIDs skips
	// identities no longer pending — the surviving intersection (INV-5) — and
	// cannot delete a same-MessageID line that shifted after a concurrent fetch.
	removeCh := make(chan DrainRemoveResult, 1)
	if !b.Workers.Submit(spec.Source, Job{Kind: JobDrainRemove, DrainRemove: &DrainRemoveJob{RecordIDs: sourceRecordIDs, ResultCh: removeCh}}) {
		return res, &DrainBusyError{Step: DrainStepRemove, CopiesLanded: true}
	}
	// B1: durable mutation — block indefinitely.
	rr := <-removeCh
	if rr.Err != nil {
		return res, fmt.Errorf("drain: %d line(s) are safely in «%s» but removing them from «%s» failed: %w — re-issue the same drain to converge (INV-2/INV-3)",
			ar.Appended+ar.Skipped, res.TargetName, res.SourceName, rr.Err)
	}
	res.RemovedFromSource = len(rr.Removed)
	res.AlreadyGone = res.Requested - len(rr.Removed)
	if res.AlreadyGone > 0 {
		res.Warnings = append(res.Warnings,
			fmt.Sprintf("%d of the %d frozen line(s) were already gone from «%s» (consumed mid-drain); their copies still landed in «%s»", res.AlreadyGone, res.Requested, res.SourceName, res.TargetName))
	}

	// ---- Step D: advisory, best-effort (the durable queue is the only truth).
	b.drainAdvisory(spec, &res)

	log.Printf("drain done src=%s dst=%s appended=%d skipped=%d removed=%d gone=%d target_pending=%d nudge=%v notice=%v",
		srcKey, dstKey, res.Appended, res.PresenceSkipped, res.RemovedFromSource, res.AlreadyGone, res.TargetPending, res.NudgeSent, res.NoticeSent)
	return res, nil
}

// drainAdvisory is Step D: tell the target side that lines landed. Best-effort
// on every path — a failed advisory is logged, never an error (the queue is
// truth; a later attach's own backlog nudge advertises the lines anyway).
//
// Live, alive, push-capable holder → ONE targeted SystemEvent written DIRECTLY
// to that holder's conn — the exact shape+write broadcastSystemEvent uses
// (host.go; broker-originated, no user content, so the allowlist-gate bypass
// is sound) but to one stub. NEVER routed through forwardOrFallback or the
// worker push path (A4: an unclaimed target would drop it there, and the
// Covered-zero semantics that make an event unable to consume backlog —
// adapters echo Covered verbatim, handleInboundDelivered drops Count<1 — only
// need the InboundMsg's zero-value Covered, which a direct write preserves).
// A render-incapable holder (forked session) is treated as no-holder, exactly
// like the organic hold path (worker.go CanRenderPush).
//
// No live holder / render-incapable / write failed → ONE drain-notice to the
// target topic via the channel's SendReply (the worker.go held-notice shape).
// One notice per DRAIN, never per message — the organic held path re-notifies
// per hold; a 10-line drain must not spam 10.
func (b *Broker) drainAdvisory(spec DrainSpec, res *DrainResult) {
	landed := res.Appended + res.PresenceSkipped
	if landed == 0 {
		return
	}
	if holder, held := b.Routes.Holder(spec.Target); held && holder.IsAlive() && holder.CanRenderPush() {
		if conn, ok := holder.ConnValue().(*ipc.Conn); ok && conn != nil {
			sysev := &c3types.SystemEvent{
				Source: spec.Target.Channel,
				Level:  "info",
				Title:  "Messages drained in",
				Message: fmt.Sprintf("↩︎ %d message(s) drained in from «%s» — run fetch_queue to pull them (%d total queued)",
					landed, res.SourceName, res.TargetPending),
			}
			in := c3types.Inbound{
				Channel: sysev.Source,
				Kind:    c3types.InboundSystem,
				Event:   &c3types.InboundEvent{System: sysev},
				// No ChatID/Sender/Text — broker-originated, not a routed user
				// message (mirrors broadcastSystemEvent). Covered stays the zero
				// value, so the event can never consume backlog (A4).
			}
			if err := conn.WriteJSON(ipc.InboundMsg{Op: ipc.OpInbound, Inbound: in}); err == nil {
				res.NudgeSent = true
				log.Printf("drain nudge dst=%s delivered to cli=%s pid=%d conn=%d (%d drained in)",
					res.TargetKey, holder.CLI, holder.PID, holder.ConnID, landed)
				return
			} else {
				log.Printf("drain nudge dst=%s write to cli=%s pid=%d conn=%d failed: %v — falling back to topic notice",
					res.TargetKey, holder.CLI, holder.PID, holder.ConnID, err)
			}
		}
	}
	ch, err := b.Channel(spec.Target.Channel)
	if err != nil {
		log.Printf("drain notice dst=%s: channel lookup failed: %v (queue is still truth; attach will advertise the backlog)", res.TargetKey, err)
		return
	}
	var topicID *int64
	if spec.Target.HasTopic {
		t := spec.Target.TopicID
		topicID = &t
	}
	// MarkupNone: SourceName is a topic name any group member with rename
	// rights controls — it must render as inert plain text, never as live
	// Telegram markdown from the trusted bot (an empty Markup would default to
	// markdown, outbound.go).
	if _, serr := ch.SendReply(c3types.ReplyArgs{
		Channel: spec.Target.Channel, ChatID: spec.Target.ChatID, TopicID: topicID,
		Markup: c3types.MarkupNone,
		Text: fmt.Sprintf("↩︎ %d drained in from «%s» — %d total queued. Attach & run fetch_queue to recover.",
			landed, res.SourceName, res.TargetPending),
	}); serr != nil {
		log.Printf("drain notice dst=%s: send failed: %v (queue is still truth; attach will advertise the backlog)", res.TargetKey, serr)
		return
	}
	res.NoticeSent = true
}

// applyDrainSelector resolves a selector against the frozen oldest-first
// pending snapshot, returning the captured lines plus the 1-based ordinal
// window actually selected (echoed in the reply) and whether a first-N
// clamped. The caller has already rejected an empty snapshot, so every
// non-error return captures at least one line. Ordinals number LINES
// (occurrences) in the snapshot, not unique MessageIDs (A2).
func applyDrainSelector(pending []c3types.Inbound, sel DrainSelector) (captured []c3types.Inbound, lo, hi int, clamped bool, err error) {
	switch sel.Kind {
	case SelectAll:
		return pending, 1, len(pending), false, nil
	case SelectFirstN:
		if sel.N < 1 {
			return nil, 0, 0, false, &BadSelectorError{Reason: fmt.Sprintf("first-N needs N ≥ 1 (got %d)", sel.N)}
		}
		n := sel.N
		if n > len(pending) {
			n = len(pending)
			clamped = true
		}
		return pending[:n], 1, n, clamped, nil
	case SelectRange:
		if sel.Lo < 1 {
			return nil, 0, 0, false, &BadSelectorError{Reason: fmt.Sprintf("range ordinals start at 1 (got %d)", sel.Lo)}
		}
		if sel.Lo > sel.Hi {
			return nil, 0, 0, false, &BadSelectorError{Reason: fmt.Sprintf("range %d-%d is inverted — the low ordinal comes first", sel.Lo, sel.Hi)}
		}
		if sel.Hi > len(pending) {
			return nil, 0, 0, false, &RangeBeyondPendingError{Lo: sel.Lo, Hi: sel.Hi, Pending: len(pending)}
		}
		return pending[sel.Lo-1 : sel.Hi], sel.Lo, sel.Hi, false, nil
	default:
		return nil, 0, 0, false, &BadSelectorError{Reason: fmt.Sprintf("unknown selector kind %d", sel.Kind)}
	}
}

// drainBanner renders the human provenance prefix baked into a moved line's
// Text (§4 Step B): "↩︎ from «project» · @user · Jul 8 14:22\n". Computed from
// the ORIGINAL fields BEFORE the routing rewrite. Baking (vs render-time)
// covers every consumer — live push, fetch_queue, all adapters — with zero
// adapter changes, and survives the unattached-transfer-then-later-fetch path;
// DrainedFrom keeps the provenance machine-readable regardless. Banners stack
// on chained re-drains (B6, accepted v1).
func drainBanner(srcName string, in *c3types.Inbound) string {
	sender := "unknown sender"
	switch {
	case in.Sender.Username != "":
		sender = "@" + in.Sender.Username
	case in.Sender.UserID != 0:
		sender = fmt.Sprintf("uid=%d", in.Sender.UserID)
	}
	when := "time unknown"
	if !in.Timestamp.IsZero() {
		when = in.Timestamp.Format("Jan 2 15:04")
	}
	return fmt.Sprintf("↩︎ from «%s» · %s · %s\n", srcName, sender, when)
}

// drainPreviewMax caps the reply's first-message preview (rune-safe).
const drainPreviewMax = 64

// drainPreview renders a short single-line preview of a captured line for the
// reply echo (newlines collapse, rune-safe truncation, media-kind fallback).
// Delegates to previewLine (queue_command.go), which the /queue <q> render
// shares — one preview shape everywhere.
func drainPreview(in *c3types.Inbound) string {
	return previewLine(in, drainPreviewMax)
}

// drainDisplayName prefers the resolved human name, falling back to the
// canonical numeric key so a nameless route still renders unambiguously.
func drainDisplayName(name, key string) string {
	if name != "" {
		return name
	}
	return key
}

// --- worker jobs (run on the owning route worker; single-owner discipline) ---

// DrainPeekJob (Step A) runs on the SOURCE worker: return the full
// oldest-first pending snapshot so Drain can freeze the selection. Peek
// advances nothing, so — unlike Steps B/C (B1) — the broker-side wait may
// safely time out and abandon it.
type DrainPeekJob struct {
	ResultCh chan<- DrainPeekResult
}

// DrainPeekResult carries the pending snapshot back to Drain.
type DrainPeekResult struct {
	Pending []c3types.Inbound
	Tracked []queue.TrackedInbound
	Err     error
}

// DrainAppendJob (Step B) runs on the TARGET worker: land the
// already-transformed (stamped/rewritten/bannered) copies, oldest-first. The
// fsync'd Append is the durability commit (INV-1).
//
// Idempotency (INV-3): ONE PeekTracked at job start indexes pending copies by
// (DrainedFrom==From, private source RecordID). A retry skips only the exact
// source line already landed; a later edit reusing MessageID has a new RecordID
// and lands independently. Legacy copies without provenance never prove
// presence, so uncertainty fails toward a duplicate. Convergence holds only
// while the prior copy is still pending in the target (B6).
type DrainAppendMessage struct {
	Inbound        c3types.Inbound
	SourceRecordID string
}

type DrainAppendJob struct {
	// From is the canonical numeric source route key — the DrainedFrom value
	// stamped on every message and half of the presence discriminator (B6).
	From     string
	Messages []DrainAppendMessage
	ResultCh chan<- DrainAppendResult
}

// DrainAppendResult carries the copy outcome back to Drain. On Err the counts
// report the durably-landed prefix (appended + skipped so far).
type DrainAppendResult struct {
	Appended int
	Skipped  int
	Pending  int // target's total pending after this job
	Err      error
}

// DrainRemoveJob (Step C) runs on the SOURCE worker: RemoveRecordIDs with the
// immutable identities frozen at Step A. Missing/already-consumed/legacy
// untracked records are skipped (surviving intersection, INV-5); a shifted
// same-MessageID edit can never match. Removed lines are snapshotted first.
type DrainRemoveJob struct {
	RecordIDs []string
	ResultCh  chan<- DrainRemoveResult
}

// DrainRemoveResult carries the removed lines (file order) back to Drain.
type DrainRemoveResult struct {
	Removed []c3types.Inbound
	Err     error
}

// handleDrainPeek services Step A on the source's worker goroutine.
func (w *RouteWorker) handleDrainPeek(job *DrainPeekJob) {
	if job == nil || job.ResultCh == nil {
		return
	}
	defer recoverGoroutineThen("worker.handleDrainPeek", func() {
		select {
		case job.ResultCh <- DrainPeekResult{Err: fmt.Errorf("internal panic in drain peek")}:
		default:
		}
	})
	if w.broker == nil || w.broker.Queue == nil {
		job.ResultCh <- DrainPeekResult{Err: errOutboundNotImpl}
		return
	}
	pending, err := w.broker.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if err != nil {
		job.ResultCh <- DrainPeekResult{Err: err}
		return
	}
	log.Printf("drain peek chan=%s chat=%d topic=%s pending=%d",
		w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), len(pending))
	public := make([]c3types.Inbound, len(pending))
	for i := range pending {
		public[i] = pending[i].Inbound
	}
	job.ResultCh <- DrainPeekResult{Pending: public, Tracked: pending}
}

// handleDrainAppend services Step B on the target's worker goroutine. See
// DrainAppendJob for the presence-check contract.
//
// INV-8: the moved MessageIDs are deliberately NOT recorded in w.dedup — the
// per-route dedup exists to suppress the source channel's crash-replay of an
// ORGANIC delivery, and seeding it here would suppress a future organic target
// message that happens to share an id (cross-chat ids collide freely). The
// per-route cap is likewise NOT enforced here (no evictIfOverCap): an eviction
// mid-drain would silently consume the target's own oldest backlog as a side
// effect of an operator move; the next organic append enforces the cap with
// its usual loud notice.
func (w *RouteWorker) handleDrainAppend(job *DrainAppendJob) {
	if job == nil || job.ResultCh == nil {
		return
	}
	defer recoverGoroutineThen("worker.handleDrainAppend", func() {
		select {
		case job.ResultCh <- DrainAppendResult{Err: fmt.Errorf("internal panic in drain append")}:
		default:
		}
	})
	if w.broker == nil || w.broker.Queue == nil {
		job.ResultCh <- DrainAppendResult{Err: errOutboundNotImpl}
		return
	}
	qrk := queueRouteKey(w.key)
	pending, err := w.broker.Queue.PeekTracked(qrk, -1)
	if err != nil {
		job.ResultCh <- DrainAppendResult{Err: fmt.Errorf("presence peek: %w", err)}
		return
	}
	// Presence is exact immutable source-line provenance. Empty provenance on
	// an old target copy cannot prove anything and is deliberately ignored.
	existing := map[string]bool{}
	for i := range pending {
		if pending[i].Inbound.DrainedFrom == job.From && pending[i].SourceRecordID != "" {
			existing[pending[i].SourceRecordID] = true
		}
	}
	appended, skipped := 0, 0
	for i := range job.Messages {
		m := job.Messages[i]
		if m.SourceRecordID != "" && existing[m.SourceRecordID] {
			skipped++
			continue
		}
		if _, aerr := w.broker.Queue.AppendDrainedTracked(qrk, &m.Inbound, m.SourceRecordID); aerr != nil {
			total, _ := w.broker.Queue.Pending(qrk)
			job.ResultCh <- DrainAppendResult{Appended: appended, Skipped: skipped, Pending: total,
				Err: fmt.Errorf("append line %d of %d: %w", i+1, len(job.Messages), aerr)}
			return
		}
		appended++
	}
	total, _ := w.broker.Queue.Pending(qrk)
	log.Printf("drain append chan=%s chat=%d topic=%s from=%s appended=%d skipped=%d pending=%d",
		w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), job.From, appended, skipped, total)
	job.ResultCh <- DrainAppendResult{Appended: appended, Skipped: skipped, Pending: total}
}

// handleDrainRemove services Step C on the source's worker goroutine.
func (w *RouteWorker) handleDrainRemove(job *DrainRemoveJob) {
	if job == nil || job.ResultCh == nil {
		return
	}
	defer recoverGoroutineThen("worker.handleDrainRemove", func() {
		select {
		case job.ResultCh <- DrainRemoveResult{Err: fmt.Errorf("internal panic in drain remove")}:
		default:
		}
	})
	if w.broker == nil || w.broker.Queue == nil {
		job.ResultCh <- DrainRemoveResult{Err: errOutboundNotImpl}
		return
	}
	removed, err := w.broker.Queue.RemoveRecordIDs(queueRouteKey(w.key), job.RecordIDs)
	if err != nil {
		log.Printf("drain remove FAIL chan=%s chat=%d topic=%s: %v",
			w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), err)
		job.ResultCh <- DrainRemoveResult{Err: err}
		return
	}
	log.Printf("drain remove chan=%s chat=%d topic=%s requested=%d removed=%d",
		w.key.Channel, w.key.ChatID, TopicKeyStr(w.key), len(job.RecordIDs), len(removed))
	job.ResultCh <- DrainRemoveResult{Removed: removed}
}
