package telegram

import (
	"sync"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

// reachability combines the inbound (fetchHealth) and outbound (outboundHealth)
// sub-states into the ONE telegram reachability signal the status line shows.
// The broker/status-line contract is one health entry per channel NAME
// (healthFileEntry, health.json), so both directions MUST surface on the single
// "telegram" entry: inbound-down and outbound-down for one root cause (the wire
// is down ⇒ both fail) must NEVER produce two separate notifications.
//
//	combinedDown = inboundDown || outboundDown
//
// A notification fires ONLY on the combined edge: UP→DOWN when the FIRST sub
// goes down, DOWN→UP when the LAST sub clears. Sub-state churn while already
// combined-DOWN (e.g. outbound flips down after inbound already took the state
// down) is silent.
//
// Locking (single-lock, single-source-of-truth — 2026-07-07 review fix): the
// combiner OWNS the outboundHealth machine (r.out) and drives EVERY outbound op
// (outboundFailure/outboundSuccess/probeSuccess) while holding r.mu, so the machine op and the combiner recompute are ATOMIC
// and r.outboundDown is ALWAYS set `= r.out.down` (authoritative — from the
// machine's state, never from a transition edge). This is what makes the two
// impossible to diverge: the previous design kept outbound down-state in TWO
// booleans under TWO locks (outboundHealth.mu + r.mu) updated non-atomically and
// derived r.outboundDown from the transition, which allowed a permanent wedge
// (F1) and a masked outage (F2). r.out has NO lock of its own now.
//
// recordInbound/outboundFailure/outboundSuccess run on MANY goroutines — inbound
// from the poll loop, the silence watchdog, and the heartbeat; outbound from the
// route worker (SendReply) and detached echo goroutines (readback). ONE mutex
// (r.mu) covers ALL of them and ALL of r.out. The inbound sub-machine
// (fetchHealth) keeps its own lock, taken/released by the caller BEFORE r.mu is
// acquired (reportHealth) — the two locks are SEQUENTIAL, never nested, so there
// is no lock-ordering deadlock; r.mu is the only lock for all outbound+combiner
// state.
type reachability struct {
	mu           sync.Mutex
	inboundDown  bool
	outboundDown bool
	combinedDown bool
	since        time.Time // when the combined state was entered
	now          func() time.Time

	// out is the outbound-health machine, OWNED by this combiner: every op on it
	// happens under r.mu (see the locking note above). It shares the combiner's
	// clock so tests drive both from one fakeClock.
	out *outboundHealth
}

// newReachability returns a combiner on the wall clock (both sub-states UP).
func newReachability() *reachability {
	return newReachabilityWithClock(time.Now)
}

// newReachabilityWithClock is the test seam — inject a deterministic clock. The
// owned outboundHealth machine shares that clock.
func newReachabilityWithClock(now func() time.Time) *reachability {
	return &reachability{now: now, since: now(), out: newOutboundHealthWithClock(now)}
}

// recordInbound updates the inbound sub-state and returns the combined edge (if
// any). It never touches outbound: a warm poll says nothing about whether a
// fresh connection or a send works. consec is the driving side's failure count,
// copied into the combined event.
func (r *reachability) recordInbound(down bool, consec int) (bool, c3types.HealthEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inboundDown = down
	return r.recomputeLocked(consec)
}

// outboundFailure drives the owned outbound machine's RecordFailure and returns
// the combined edge (if any), all under r.mu. r.outboundDown is set from the
// machine's AUTHORITATIVE state (r.out.down), never from the returned transition
// — that is the F1 fix (a stale failure can no longer wedge outboundDown=true
// against a machine that a concurrent success already flipped down=false, because
// both ops run under r.mu and the last writer always reconciles outboundDown to
// the machine). reason is the failure cause for the DOWN event; isSetup marks
// setup evidence (see outboundHealth).
func (r *reachability) outboundFailure(reason string, isSetup bool) (bool, c3types.HealthEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.out.RecordFailure(reason, isSetup)
	r.outboundDown = r.out.down // AUTHORITATIVE — from the machine, not the transition
	return r.recomputeLocked(r.out.consecFails)
}

// probeSuccess records a fresh-connection probe success: it clears outbound
// setup evidence (never send evidence) and returns the combined edge (if any).
func (r *reachability) probeSuccess() (bool, c3types.HealthEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.out.ClearSetupEvidence()
	r.outboundDown = r.out.down
	return r.recomputeLocked(r.out.consecFails)
}

// isDown reports the combined state.
func (r *reachability) isDown() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.combinedDown
}

// outboundSuccess drives the owned outbound machine's RecordSuccess and returns
// the combined edge (if any), all under r.mu. Like outboundFailure, r.outboundDown
// is reconciled to the machine's authoritative state so the two can never diverge.
func (r *reachability) outboundSuccess() (bool, c3types.HealthEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.out.RecordSuccess()
	r.outboundDown = r.out.down // AUTHORITATIVE — from the machine, not the transition
	return r.recomputeLocked(r.out.consecFails)
}

// recomputeLocked recomputes combinedDown and returns (fire, event) ONLY on a
// combined edge. On no edge it returns (false, zero-event). Caller holds r.mu.
func (r *reachability) recomputeLocked(consec int) (bool, c3types.HealthEvent) {
	newCombined := r.inboundDown || r.outboundDown
	if newCombined == r.combinedDown {
		return false, c3types.HealthEvent{}
	}
	prevSince := r.since // when the previous combined state was entered
	r.combinedDown = newCombined
	r.since = r.now()
	ev := c3types.HealthEvent{
		Channel: Name,
		Since:   r.since,
		Consec:  consec,
	}
	if newCombined {
		ev.State = c3types.HealthStateDown
		ev.Reason = reachReason(r.inboundDown, r.outboundDown)
	} else {
		ev.State = c3types.HealthStateUp
		ev.Reason = ""                      // recovered; both directions clear
		ev.DownFor = r.since.Sub(prevSince) // how long the combined state was DOWN (for the recovered log line)
	}
	return true, ev
}

// reachReason names which direction(s) are unreachable for the combined DOWN
// event, computed from the current sub-state at fire time.
func reachReason(inboundDown, outboundDown bool) string {
	switch {
	case inboundDown && outboundDown:
		return "unreachable (inbound + outbound)"
	case inboundDown:
		return "inbound unreachable"
	case outboundDown:
		return "outbound send failing"
	default:
		return ""
	}
}
