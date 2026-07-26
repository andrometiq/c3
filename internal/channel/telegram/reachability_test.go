package telegram

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

// newWiredChannel builds a Channel with the inbound machine and the combiner
// (which OWNS the outbound machine) all wired on one deterministic shared clock,
// plus a fakeHost to capture NotifyHealth edges — the production wiring, unlike
// the bare-struct legacy-path tests in health_wiring_test.go. The combiner
// constructs its own outbound-health machine (reach.out) sharing clk.now.
func newWiredChannel() (*Channel, *fakeHost) {
	h := &fakeHost{}
	clk := &fakeClock{t: time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)}
	c := &Channel{
		host:   h,
		cfg:    Config{},
		ctx:    context.Background(),
		health: newFetchHealthWithClock(clk.now),
		reach:  newReachabilityWithClock(clk.now),
	}
	return c, h
}

// --- outboundHealth machine ---------------------------------------------------

// downAfter=2 distinct failure EVENTS: the first is a no-op edge, the second
// flips DOWN, further failures while DOWN are de-spammed, and the first success
// recovers.
func TestOutboundHealth_TwoEventsToDown(t *testing.T) {
	clk := &fakeClock{t: time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)}
	o := newOutboundHealthWithClock(clk.now)
	if tr := o.RecordFailure("a"); tr != healthNoChange {
		t.Fatalf("fail #1 = %v, want healthNoChange (downAfter=2)", tr)
	}
	if tr := o.RecordFailure("a"); tr != healthWentDown {
		t.Fatalf("fail #2 = %v, want healthWentDown", tr)
	}
	for i := 0; i < 3; i++ {
		if tr := o.RecordFailure("a"); tr != healthNoChange {
			t.Fatalf("fail while DOWN #%d = %v, want healthNoChange (de-spam)", i, tr)
		}
	}
	if tr := o.RecordSuccess(); tr != healthRecovered {
		t.Fatalf("first success after DOWN = %v, want healthRecovered", tr)
	}
	if tr := o.RecordSuccess(); tr != healthNoChange {
		t.Fatalf("success while UP = %v, want healthNoChange", tr)
	}
}

// ForceReset clears a DOWN machine to UP WITHOUT an edge, and a fresh run of 2
// failures is needed to re-trip (the counter really reset, not left at 2).
func TestOutboundHealth_ForceResetClears(t *testing.T) {
	clk := &fakeClock{t: time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)}
	o := newOutboundHealthWithClock(clk.now)
	o.RecordFailure("a")
	o.RecordFailure("a") // DOWN
	o.ForceReset()
	if down, consec, _, _, _ := o.snapshot(); down || consec != 0 {
		t.Fatalf("after ForceReset: down=%v consec=%d, want down=false consec=0", down, consec)
	}
	if tr := o.RecordFailure("b"); tr != healthNoChange {
		t.Fatalf("post-reset fail #1 = %v, want healthNoChange", tr)
	}
	if tr := o.RecordFailure("b"); tr != healthWentDown {
		t.Fatalf("post-reset fail #2 = %v, want healthWentDown", tr)
	}
}

// --- reachReason --------------------------------------------------------------

func TestReachReason(t *testing.T) {
	cases := []struct {
		in, out bool
		want    string
	}{
		{true, false, "inbound unreachable"},
		{false, true, "outbound send failing"},
		{true, true, "unreachable (inbound + outbound)"},
		{false, false, ""},
	}
	for _, tc := range cases {
		if got := reachReason(tc.in, tc.out); got != tc.want {
			t.Errorf("reachReason(%v,%v) = %q, want %q", tc.in, tc.out, got, tc.want)
		}
	}
}

// --- combiner edges (direct) --------------------------------------------------

// The combined edge fires on the FIRST sub going down and stays silent while a
// second sub flips down under it; inbound recovery is wire-proof (it clears
// outbound too), so it fires the UP edge even with outbound still nominally down.
func TestReachability_CombinedEdges(t *testing.T) {
	clk := &fakeClock{t: time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)}
	r := newReachabilityWithClock(clk.now)

	fire, ev := r.recordInbound(true, 3)
	if !fire || ev.State != c3types.HealthStateDown || ev.Reason != "inbound unreachable" || ev.Consec != 3 || ev.Channel != Name {
		t.Fatalf("first inbound-down edge = (%v, %+v), want fire DOWN reason='inbound unreachable' consec=3 channel=telegram", fire, ev)
	}
	// Outbound now goes down (2 failure events, downAfter=2) while combined is
	// already DOWN → the machine flips down but no combined edge fires.
	if fire, _ := r.outboundFailure("x"); fire {
		t.Fatal("outbound failure while already combined-DOWN must NOT fire")
	}
	if fire, _ := r.outboundFailure("x"); fire {
		t.Fatal("outbound going down while already combined-DOWN must NOT fire")
	}
	// Inbound recovers → wire-proof clears outbound too → combined UP edge fires.
	fire, ev = r.recordInbound(false, 0)
	if !fire || ev.State != c3types.HealthStateUp {
		t.Fatalf("inbound recovery (wire-proof) edge = (%v, %+v), want fire UP", fire, ev)
	}
}

// Outbound-only down then recover fires exactly one DOWN and one UP on the
// combined state (the "last sub clears" recovery, no wire-proof involved).
func TestReachability_OutboundOnlyDownRecovers(t *testing.T) {
	clk := &fakeClock{t: time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)}
	r := newReachabilityWithClock(clk.now)
	// First failure event: machine still up (downAfter=2), no combined edge.
	if fire, _ := r.outboundFailure("x"); fire {
		t.Fatal("first outbound failure must not fire (downAfter=2)")
	}
	// Second failure event: machine flips down → combined DOWN edge, consec=2.
	fire, ev := r.outboundFailure("x")
	if !fire || ev.State != c3types.HealthStateDown || ev.Reason != "outbound send failing" || ev.Consec != 2 {
		t.Fatalf("outbound-down edge = (%v, %+v), want fire DOWN reason='outbound send failing' consec=2", fire, ev)
	}
	// Success recovers → combined UP edge (last sub clears).
	fire, ev = r.outboundSuccess()
	if !fire || ev.State != c3types.HealthStateUp {
		t.Fatalf("outbound-recovery edge = (%v, %+v), want fire UP", fire, ev)
	}
}

// --- spec §2e behaviors (wired channel) --------------------------------------

// (i) Wire drops → inbound ×3 → combined DOWN fires ONCE; outbound then fails ×2
// while combined already down → NO second NotifyHealth (the core anti-spam case).
func TestReach_WireDrop_InboundDownThenOutbound_SingleNotify(t *testing.T) {
	c, h := newWiredChannel()
	for i := 0; i < 3; i++ {
		c.reportHealth(c.health.RecordFailure("transient (network/timeout/5xx)"))
	}
	evs := h.healthEvents()
	if len(evs) != 1 || evs[0].State != c3types.HealthStateDown {
		t.Fatalf("after inbound ×3, events = %+v, want exactly one DOWN", evs)
	}
	if evs[0].Reason != "inbound unreachable" || evs[0].Channel != Name || evs[0].Consec != 3 {
		t.Fatalf("DOWN event = %+v, want reason='inbound unreachable' channel=telegram consec=3", evs[0])
	}
	// Outbound now fails ×2 while combined already down → no new notification.
	c.feedOutboundFailure(rbTGErr(500), "SendReply transient send error")
	c.feedOutboundFailure(rbTGErr(500), "SendReply transient send error")
	if n := len(h.healthEvents()); n != 1 {
		t.Fatalf("outbound failing while already combined-DOWN must not re-notify; calls=%d, want 1", n)
	}
}

// (ii) Sends failing while polls OK → outbound ×2 → combined DOWN fires once.
func TestReach_OutboundOnly_SingleNotify(t *testing.T) {
	c, h := newWiredChannel()
	c.feedOutboundFailure(rbTGErr(500), "SendReply transient send error")
	if n := len(h.healthEvents()); n != 0 {
		t.Fatalf("first outbound failure must not fire (downAfter=2); calls=%d, want 0", n)
	}
	c.feedOutboundFailure(rbTGErr(500), "SendReply transient send error")
	evs := h.healthEvents()
	if len(evs) != 1 || evs[0].State != c3types.HealthStateDown {
		t.Fatalf("after outbound ×2, events = %+v, want exactly one DOWN", evs)
	}
	if evs[0].Reason != "outbound send failing" || evs[0].Consec != 2 || evs[0].Channel != Name {
		t.Fatalf("DOWN event = %+v, want reason='outbound send failing' consec=2 channel=telegram", evs[0])
	}
}

// (iii) Recovery: inbound success clears BOTH (wire-proof) → combined RECOVERED
// fires once; and the outbound machine is genuinely reset (a lone post-recovery
// failure does not immediately re-trip).
func TestReach_InboundRecoveryClearsBoth(t *testing.T) {
	c, h := newWiredChannel()
	for i := 0; i < 3; i++ {
		c.reportHealth(c.health.RecordFailure("transient (network/timeout/5xx)"))
	}
	c.feedOutboundFailure(rbTGErr(500), "x")
	c.feedOutboundFailure(rbTGErr(500), "x")
	if n := len(h.healthEvents()); n != 1 {
		t.Fatalf("precondition: exactly one combined DOWN, got %d", n)
	}
	// Inbound recovers → wire-proof → combined RECOVERED once.
	c.reportHealth(c.health.RecordSuccess())
	evs := h.healthEvents()
	if len(evs) != 2 || evs[1].State != c3types.HealthStateUp {
		t.Fatalf("after recovery, events = %+v, want [DOWN, UP]", evs)
	}
	// The outbound machine was reset (consec cleared): a single failure now must
	// NOT re-trip (it would if consec had been left at 2).
	c.feedOutboundFailure(rbTGErr(500), "x")
	if n := len(h.healthEvents()); n != 2 {
		t.Fatalf("outbound machine not reset by wire-proof; a lone post-recovery failure re-fired: calls=%d, want 2", n)
	}
}

// (iv) A 429 (rate-limited) send — and a permanent 4xx — never drive outbound
// DOWN (429 = a reachable server pushing back; permanent = the token breaker's
// job).
func TestReach_429AndPermanentDoNotDriveOutboundDown(t *testing.T) {
	c, h := newWiredChannel()
	for i := 0; i < 5; i++ {
		c.feedOutboundFailure(rbTGErr(429), "x") // rate-limited
		c.feedOutboundFailure(rbTGErr(400), "x") // permanent
	}
	if n := len(h.healthEvents()); n != 0 {
		t.Fatalf("429/permanent sends must never drive outbound DOWN; calls=%d, want 0", n)
	}
	if down, consec, _, _, _ := c.reach.out.snapshot(); down || consec != 0 {
		t.Fatalf("429/permanent must not touch the outbound machine; down=%v consec=%d", down, consec)
	}
}

// (v) A ctx-cancel readback give-up does NOT drive outbound DOWN: retryReadbackSend
// returns ctx.Err() at the first backoff (readback.go:419-420), BEFORE the
// give-up feed at :424 is ever reached.
func TestReach_CtxCancelReadbackGiveUp_NoOutboundDown(t *testing.T) {
	c, h := newWiredChannel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.ctx = ctx
	_, err := c.retryReadbackSend(func() (int64, error) { return 0, rbTGErr(500) })
	if err == nil {
		t.Fatal("want ctx error on cancelled readback")
	}
	if n := len(h.healthEvents()); n != 0 {
		t.Fatalf("ctx-cancel give-up must not drive outbound DOWN; calls=%d, want 0", n)
	}
	if down, consec, _, _, _ := c.reach.out.snapshot(); down || consec != 0 {
		t.Fatalf("ctx-cancel must not touch the outbound machine; down=%v consec=%d", down, consec)
	}
}

// (vi) Every combined event carries Channel="telegram" so the broker collapses
// them onto ONE status-line entry, and the state reflects the combined
// reachability across a full down→recover cycle.
func TestReach_SingleTelegramEntryCombinedState(t *testing.T) {
	c, h := newWiredChannel()
	for i := 0; i < 3; i++ {
		c.reportHealth(c.health.RecordFailure("t"))
	}
	c.feedOutboundFailure(rbTGErr(500), "x")
	c.feedOutboundFailure(rbTGErr(500), "x")
	c.reportHealth(c.health.RecordSuccess())
	evs := h.healthEvents()
	if len(evs) != 2 || evs[0].State != c3types.HealthStateDown || evs[1].State != c3types.HealthStateUp {
		t.Fatalf("want exactly [DOWN, UP] on the combined state, got %+v", evs)
	}
	for i, ev := range evs {
		if ev.Channel != Name {
			t.Fatalf("event %d channel=%q, want %q (broker keys ONE entry per channel name)", i, ev.Channel, Name)
		}
	}
}

// --- concurrency (run under -race) -------------------------------------------

// The single combiner mutex must serialize concurrent recordInbound /
// outboundFailure / outboundSuccess with no data race and no deadlock, and leave
// BOTH the combined invariant AND the single-source-of-truth invariant intact:
// combinedDown == inboundDown||outboundDown AND outboundDown == out.down.
func TestReachability_ConcurrentRecords_NoRace(t *testing.T) {
	r := newReachability()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(2)
		go func(n int) { defer wg.Done(); r.recordInbound(n%2 == 0, n) }(i)
		go func(n int) {
			defer wg.Done()
			if n%3 == 0 {
				r.outboundFailure("x")
			} else {
				r.outboundSuccess()
			}
		}(i)
	}
	wg.Wait()
	r.mu.Lock()
	combinedOK := r.combinedDown == (r.inboundDown || r.outboundDown)
	sotOK := r.outboundDown == r.out.down
	r.mu.Unlock()
	if !combinedOK {
		t.Fatal("combinedDown inconsistent with inboundDown||outboundDown after concurrent records")
	}
	if !sotOK {
		t.Fatal("outboundDown diverged from out.down after concurrent records (single-source-of-truth violated)")
	}
}

// The real call shapes — inbound edges (fetchHealth-lock → combiner-lock) and
// outbound feeds (combiner-lock, machine owned) — driven concurrently as
// production does, must be race- and deadlock-free (the inbound sub-machine lock
// is taken/released BEFORE r.mu; the outbound machine has no lock of its own and
// is only ever touched under r.mu; never nested).
func TestReach_ChannelConcurrentFeeds_NoRace(t *testing.T) {
	c := &Channel{
		host:   &fakeHost{},
		ctx:    context.Background(),
		health: newFetchHealth(),
		reach:  newReachability(),
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); c.reportHealth(c.health.RecordFailure("t")) }()
		go func() { defer wg.Done(); c.feedOutboundFailure(rbTGErr(500), "x") }()
		go func() { defer wg.Done(); c.recordOutboundSuccess() }()
	}
	wg.Wait()
}

// REST-STATE-CONSISTENCY (mandatory regression, run under -race -count high):
// hammer, from many goroutines on a WIRED channel, the three real production
// drivers — transient outbound failures (feedOutboundFailure), outbound successes
// (recordOutboundSuccess), and inbound recovery/failure edges (reportHealth) —
// then, after quiescence, assert under r.mu that the two authoritative invariants
// HOLD: combinedDown == inboundDown||outboundDown AND outboundDown == out.down.
//
// This is the direct regression for the two-lock divergence bugs: on the pre-fix
// code (outboundHealth.down under its own mutex + reachability.outboundDown under
// r.mu, combiner derived from the transition) an interleaving leaves the two
// booleans diverged AT REST (F1 wedge) and this assertion FAILS. On the single-
// lock fix every outbound op sets outboundDown = out.down under r.mu, so they can
// never diverge and this PASSES.
func TestReach_RestStateConsistency_NoDivergence(t *testing.T) {
	c, _ := newWiredChannel()
	// Deterministic clock in newWiredChannel keeps timing out of it; the point is
	// the interleaving of state writes, not wall-clock timing.
	var wg sync.WaitGroup
	const workers = 24
	const iters = 400
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				switch (id + i) % 4 {
				case 0:
					c.feedOutboundFailure(rbTGErr(500), "stress transient")
				case 1:
					c.recordOutboundSuccess()
				case 2:
					c.reportHealth(c.health.RecordFailure("stress inbound"))
				case 3:
					c.reportHealth(c.health.RecordSuccess())
				}
			}
		}(w)
	}
	wg.Wait()
	// Assert the invariants at rest, under the single lock that owns ALL of this
	// state (r.mu covers inboundDown, outboundDown, combinedDown, AND r.out).
	c.reach.mu.Lock()
	combinedOK := c.reach.combinedDown == (c.reach.inboundDown || c.reach.outboundDown)
	sotOK := c.reach.outboundDown == c.reach.out.down
	inDown, outDown, comb, machDown := c.reach.inboundDown, c.reach.outboundDown, c.reach.combinedDown, c.reach.out.down
	c.reach.mu.Unlock()
	if !combinedOK {
		t.Fatalf("combined invariant violated at rest: combinedDown=%v want inboundDown(%v)||outboundDown(%v)", comb, inDown, outDown)
	}
	if !sotOK {
		t.Fatalf("single-source-of-truth violated at rest: outboundDown=%v but out.down=%v (the two-lock divergence bug)", outDown, machDown)
	}
}
