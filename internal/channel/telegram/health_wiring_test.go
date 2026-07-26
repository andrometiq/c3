package telegram

import (
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

// TestReportHealth_FiresHostNotifyOnEdge asserts the channel→host wiring: a DOWN
// edge from the health machine produces exactly one host.NotifyHealth call with
// a DOWN HealthEvent, and a subsequent recovery produces exactly one UP call.
// healthNoChange transitions fire nothing (de-spam at the wiring layer).
func TestReportHealth_FiresHostNotifyOnEdge(t *testing.T) {
	h := &fakeHost{}
	c := &Channel{host: h, cfg: Config{}, health: newFetchHealth()}

	// Two failures: no edge yet → no host call.
	c.reportHealth(c.health.RecordFailure("transient"))
	c.reportHealth(c.health.RecordFailure("transient"))
	if n := len(h.healthEvents()); n != 0 {
		t.Fatalf("after 2 failures (no edge), host.NotifyHealth calls = %d, want 0", n)
	}

	// Third failure: DOWN edge → exactly one DOWN call.
	c.reportHealth(c.health.RecordFailure("transient"))
	evs := h.healthEvents()
	if len(evs) != 1 {
		t.Fatalf("after DOWN edge, host.NotifyHealth calls = %d, want 1", len(evs))
	}
	if evs[0].State != c3types.HealthStateDown || evs[0].Channel != Name || evs[0].Consec != 3 {
		t.Fatalf("DOWN event = %+v, want state=down channel=%s consec=3", evs[0], Name)
	}

	// More failures while DOWN: no further host calls (de-spam).
	c.reportHealth(c.health.RecordFailure("transient"))
	c.reportHealth(c.health.RecordFailure("transient"))
	if n := len(h.healthEvents()); n != 1 {
		t.Fatalf("failures while DOWN must not re-notify; calls = %d, want 1", n)
	}

	// Recovery: one UP call.
	c.reportHealth(c.health.RecordSuccess())
	evs = h.healthEvents()
	if len(evs) != 2 {
		t.Fatalf("after recovery, host.NotifyHealth calls = %d, want 2", len(evs))
	}
	if evs[1].State != c3types.HealthStateUp {
		t.Fatalf("recovery event state = %q, want up", evs[1].State)
	}

	// More successes while UP: no further calls.
	c.reportHealth(c.health.RecordSuccess())
	if n := len(h.healthEvents()); n != 2 {
		t.Fatalf("successes while UP must not re-notify; calls = %d, want 2", n)
	}
}
