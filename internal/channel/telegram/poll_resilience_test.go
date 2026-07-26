package telegram

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

// --- A1: panic recovery + supervisor -----------------------------------------

func TestRunGuarded_RecoversPanicAndDrivesHealth(t *testing.T) {
	h := &fakeHost{}
	c := &Channel{host: h, cfg: Config{}, health: newFetchHealth()}

	panicked := c.runGuarded("tester", func() { panic("boom") })
	if !panicked {
		t.Fatal("runGuarded must report panicked=true when body panics")
	}
	if _, consec, _, _, _, _ := c.health.snapshot(); consec != 1 {
		t.Fatalf("a recovered panic must drive one health failure; consecFails=%d want 1", consec)
	}
	found := false
	for _, l := range h.logs {
		if strings.Contains(l, "PANIC") {
			found = true
		}
	}
	if !found {
		t.Fatal("expected a PANIC log line")
	}
}

func TestRunGuarded_NormalReturnTouchesNothing(t *testing.T) {
	h := &fakeHost{}
	c := &Channel{host: h, cfg: Config{}, health: newFetchHealth()}

	if c.runGuarded("tester", func() {}) {
		t.Fatal("runGuarded must report panicked=false on a normal return")
	}
	if _, consec, _, _, _, _ := c.health.snapshot(); consec != 0 {
		t.Fatalf("a normal return must not record a failure; consecFails=%d want 0", consec)
	}
}

func TestSuperviseLoop_RestartsAfterPanicThenStopsOnNormalReturn(t *testing.T) {
	h := &fakeHost{}
	c := &Channel{host: h, cfg: Config{}, health: newFetchHealth()}
	c.ctx, c.cancel = context.WithCancel(context.Background())

	var calls int32
	done := make(chan struct{})
	go func() {
		c.superviseLoop("tester", time.Millisecond, func() {
			n := atomic.AddInt32(&calls, 1)
			if n < 3 {
				panic("boom") // first two calls panic → supervisor must restart
			}
			c.cancel() // third call returns normally → supervisor must stop
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		c.cancel()
		t.Fatal("superviseLoop did not return; it failed to restart after panic or to stop on normal return")
	}
	if n := atomic.LoadInt32(&calls); n != 3 {
		t.Fatalf("body called %d times; want 3 (2 panics restarted + 1 normal return)", n)
	}
}

// --- A2: conflict-aware heartbeat ---------------------------------------------

func TestRecordHeartbeatSuccess_ConflictActiveDoesNotClearDown(t *testing.T) {
	h := &fakeHost{}
	c := &Channel{host: h, cfg: Config{}, health: newFetchHealth()}

	// Drive DOWN via three getUpdates-style conflict failures.
	for i := 0; i < 3; i++ {
		c.reportHealth(c.health.RecordFailure("409 conflict"))
	}
	if !hasDownEvent(h) {
		t.Fatal("precondition: three failures should have produced a DOWN edge")
	}

	// A getMe heartbeat success while a 409 conflict is active must NOT clear
	// DOWN — getMe never sees a 409, so its success doesn't prove inbound works.
	c.conflictActive.Store(true)
	c.recordHeartbeatSuccess()
	for _, ev := range h.healthEvents() {
		if ev.State == c3types.HealthStateUp {
			t.Fatal("heartbeat getMe success masked an active getUpdates 409 (falsely cleared DOWN)")
		}
	}

	// Once the conflict clears, a heartbeat success recovers normally.
	c.conflictActive.Store(false)
	c.recordHeartbeatSuccess()
	up := false
	for _, ev := range h.healthEvents() {
		if ev.State == c3types.HealthStateUp {
			up = true
		}
	}
	if !up {
		t.Fatal("after the conflict cleared, a heartbeat success should fire RECOVERED")
	}
}

// --- A4: silence-arm DOWN reports a consistent consec + reason ----------------

func TestCheckSilence_ConsistentConsecAndReason(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }
	h := newFetchHealthWithClock(clock)

	now = now.Add(2 * defaultMaxSilence) // exceed the silence threshold
	if tr := h.CheckSilence(); tr != healthWentDown {
		t.Fatalf("CheckSilence past max-silence should go DOWN; got %v", tr)
	}
	down, consec, _, reason, _, _ := h.snapshot()
	if !down {
		t.Fatal("expected DOWN after silence")
	}
	if consec < defaultDownAfterFails {
		t.Fatalf("silence DOWN reported consec=%d; want >= %d so the alert text is consistent with the threshold", consec, defaultDownAfterFails)
	}
	if !strings.Contains(reason, "silence") {
		t.Fatalf("silence DOWN reason = %q; want it to name the silence cause", reason)
	}
}
