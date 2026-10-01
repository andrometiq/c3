package telegram

import (
	"time"
)

// outboundHealth is the "can we SEND to Telegram after retries?" state machine —
// the outbound analogue of fetchHealth (inbound). It is deliberately SIMPLER:
// there is NO silence watchdog, because outbound is event-driven — it only has
// an opinion when we actually try to send. A quiet night with no sends says
// nothing about outbound reachability, so silence is never a signal here.
//
// NOT internally synchronized — the reachability combiner owns this instance and
// calls every method while holding reach.mu. This is the single-lock, single-
// source-of-truth fix (2026-07-07): outbound down-state used to live in TWO
// booleans (this machine's `down` under its own mutex + reachability.outboundDown
// under r.mu) updated non-atomically across two locks, which let them diverge
// permanently (a stuck-DOWN wedge, and a masked outage). By removing this mutex
// and having the combiner drive every op under r.mu, the machine op + the
// combiner recompute are ATOMIC and reachability.outboundDown is always set
// `= out.down` (authoritative), so the two can never diverge.
//
// downAfter is measured in distinct FAILURE EVENTS, not raw attempts: a readback
// give-up is ALREADY 3 retried attempts collapsed into one give-up event, while
// a lone un-retried SendReply failure is one event that could be a blip.
// Requiring 2 distinct failure events filters single blips while still firing on
// a genuine sustained outbound outage. (Inbound uses 3 raw getUpdates failures;
// 2 events is the outbound analogue.)
//
// It reuses fetchHealth's healthTransition enum (fetchhealth.go) and the same
// de-spam contract: RecordFailure/RecordSuccess return the edge (if any) so the
// caller fires the combined notification EXACTLY ONCE per edge — never while
// already DOWN, never on a repeated success while already UP.
//
// Evidence has two kinds. Setup evidence (the racing dialer's typed error: no
// HTTP request bytes were sent) is cleared by a fresh-connection probe success
// (ClearSetupEvidence). Send evidence (anything else) is cleared only by a real
// outbound success (RecordSuccess).
type outboundHealth struct {
	down        bool
	consecFails int
	sendFailed  bool      // a non-setup failure is part of the current evidence
	since       time.Time // when the current state was entered
	lastReason  string    // cause of the most recent failure (for the DOWN edge)
	downAfter   int
	now         func() time.Time // injectable clock for tests
}

// defaultOutboundDownAfter is the number of distinct outbound FAILURE EVENTS
// (a readback give-up is already 3 retried attempts; a lone SendReply failure is
// one un-retried attempt and could be a blip) that flips UP → DOWN. See the type
// doc for the rationale behind 2.
const defaultOutboundDownAfter = 2

// newOutboundHealthWithClock constructs an outbound-health machine on the given
// clock, starting UP. The reachability combiner OWNS the instance and shares its
// own clock (see newReachabilityWithClock); tests inject a deterministic clock.
func newOutboundHealthWithClock(now func() time.Time) *outboundHealth {
	return &outboundHealth{
		since:     now(),
		downAfter: defaultOutboundDownAfter,
		now:       now,
	}
}

// RecordFailure records ONE distinct outbound failure EVENT (a give-up or a
// single un-retried send error), already filtered to genuine transient failures
// by the caller. isSetup marks the dialer's typed setup error; anything else is
// send evidence. Returns healthWentDown only on the UP→DOWN edge; further
// failures while DOWN return healthNoChange (de-spam).
func (o *outboundHealth) RecordFailure(reason string, isSetup bool) healthTransition {
	o.consecFails++
	o.lastReason = reason
	if !isSetup {
		o.sendFailed = true
	}
	if o.down {
		return healthNoChange
	}
	if o.consecFails >= o.downAfter {
		o.down = true
		o.since = o.now()
		return healthWentDown
	}
	return healthNoChange
}

// RecordSuccess records a real outbound success, which clears all evidence. If
// currently DOWN it returns healthRecovered (the first success wins recovery —
// no debounce). A success while UP is healthNoChange.
func (o *outboundHealth) RecordSuccess() healthTransition {
	o.sendFailed = false
	return o.ClearSetupEvidence()
}

// ClearSetupEvidence records a fresh-connection probe success. It clears the
// machine only when every recorded failure was a setup failure; any send
// evidence stays until a real outbound success.
func (o *outboundHealth) ClearSetupEvidence() healthTransition {
	if o.sendFailed {
		return healthNoChange
	}
	o.consecFails = 0
	if o.down {
		o.down = false
		o.since = o.now()
		o.lastReason = ""
		return healthRecovered
	}
	return healthNoChange
}

// snapshot returns the current state for building a combined HealthEvent /
// logging. NOT internally synchronized — the caller must hold reach.mu (the
// combiner owns this instance; see the type doc).
func (o *outboundHealth) snapshot() (down bool, consec int, since time.Time, reason string, downFor time.Duration) {
	df := time.Duration(0)
	if o.down {
		df = o.now().Sub(o.since)
	}
	return o.down, o.consecFails, o.since, o.lastReason, df
}
