package broker

import "testing"

func TestStubRegistry_AssignsMonotonicConnID(t *testing.T) {
	r := NewStubRegistry()
	a := r.Register("claude", 1, "/x", nil)
	b := r.Register("codex", 2, "/y", nil)
	c := r.Register("claude", 3, "/x", nil)

	if !(a.ConnID < b.ConnID && b.ConnID < c.ConnID) {
		t.Errorf("ConnIDs not monotonic: %d, %d, %d", a.ConnID, b.ConnID, c.ConnID)
	}
}

func TestStub_StableSessionID(t *testing.T) {
	r := NewStubRegistry()
	s := r.Register("claude", 42, "/x", nil)
	// Register leaves the stable id empty (it arrives later via RecoverSessionReq).
	if s.StableSessionIDValue() != "" {
		t.Fatalf("fresh stub stable id = %q, want empty", s.StableSessionIDValue())
	}
	s.SetStableSessionID("sess-1")
	if s.StableSessionIDValue() != "sess-1" {
		t.Fatalf("StableSessionIDValue = %q, want sess-1", s.StableSessionIDValue())
	}
	// Last write wins (idempotent re-key).
	s.SetStableSessionID("sess-2")
	if s.StableSessionIDValue() != "sess-2" {
		t.Fatalf("StableSessionIDValue = %q, want sess-2 after re-set", s.StableSessionIDValue())
	}
}

// TestStub_ClearRouteIf pins the steal-eviction primitive: it clears
// route+routeConfirmed only when the stub still points at the target key, so a
// mid-switch victim's OTHER route is never zeroed out from under it.
func TestStub_ClearRouteIf(t *testing.T) {
	tidA, tidB := int64(11), int64(22)
	keyA := MakeRouteKey("telegram", -100, &tidA)
	keyB := MakeRouteKey("telegram", -100, &tidB)

	r := NewStubRegistry()
	s := r.Register("claude", 1, "/x", nil)
	s.AddRoute(keyA)
	s.MarkRouteConfirmed(keyA)

	// A no-op: the stub is routed to A, so a steal on B must NOT touch it.
	if removed, _, _ := s.ClearRouteIf(keyB); removed {
		t.Fatal("ClearRouteIf(B) must return false when the stub holds A")
	}
	if s.OutputRoute() == nil || *s.OutputRoute() != keyA {
		t.Fatalf("ClearRouteIf(B) must leave route A intact; got %+v", s.OutputRoute())
	}
	if !s.RouteConfirmed(keyA) {
		t.Fatal("ClearRouteIf(B) must leave routeConfirmed intact")
	}

	// The real steal: clearing the held key zeroes route + confirmation.
	if removed, wasOutput, newOutput := s.ClearRouteIf(keyA); !removed || !wasOutput || newOutput != nil {
		t.Fatal("ClearRouteIf(A) must return true when the stub holds A")
	}
	if s.OutputRoute() != nil {
		t.Fatalf("ClearRouteIf(A) must clear the route; got %+v", s.OutputRoute())
	}
	if s.RouteConfirmed(keyA) {
		t.Fatal("ClearRouteIf(A) must clear routeConfirmed")
	}

	// Clearing an already-empty stub is a harmless no-op.
	if removed, _, _ := s.ClearRouteIf(keyA); removed {
		t.Fatal("ClearRouteIf on an unrouted stub must return false")
	}
}

func TestStubRegistry_GetByConnID(t *testing.T) {
	r := NewStubRegistry()
	s := r.Register("claude", 1, "/x", nil)

	got, ok := r.Get(s.ConnID)
	if !ok {
		t.Fatal("expected to find by ConnID")
	}
	if got.PID != 1 || got.CWD != "/x" {
		t.Errorf("got %+v", got)
	}
}

func TestStubRegistry_Unregister(t *testing.T) {
	r := NewStubRegistry()
	s := r.Register("claude", 1, "/x", nil)
	r.Unregister(s.ConnID)
	if _, ok := r.Get(s.ConnID); ok {
		t.Error("expected stub gone after Unregister")
	}
}
