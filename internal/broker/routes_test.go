package broker

import "testing"

func ptrI64(v int64) *int64 { return &v }

func TestRoutes_ClaimSucceedsOnFreeRoute(t *testing.T) {
	r := NewRoutes()
	stub := &Stub{ConnID: 1, CLI: "claude", PID: 1, CWD: "/x"}
	key := MakeRouteKey("telegram", -100, ptrI64(281))

	holder, ok := r.Claim(key, stub)
	if !ok {
		t.Fatalf("first claim should succeed; got holder %+v", holder)
	}
}

func TestRoutes_SwarmBotsShareOneTopic(t *testing.T) {
	r := NewRoutes()
	tid := ptrI64(10848)
	grok := &Stub{ConnID: 1, CLI: "grok", PID: 1, CWD: "/x"}
	glm := &Stub{ConnID: 2, CLI: "glm", PID: 2, CWD: "/x"}
	if _, ok := r.Claim(MakeRouteKey("telegram:grok", -100, tid), grok); !ok {
		t.Fatal("grok bot should claim the shared topic")
	}
	if _, ok := r.Claim(MakeRouteKey("telegram:glm", -100, tid), glm); !ok {
		t.Fatal("glm bot must claim the SAME chat/topic without colliding — different RouteKey.Channel")
	}
	if _, ok := r.Claim(MakeRouteKey("telegram:grok", -100, tid), glm); ok {
		t.Fatal("same bot on the same topic must still collide")
	}
}

func TestRoutes_ClaimFailsWhenHeld(t *testing.T) {
	r := NewRoutes()
	first := &Stub{ConnID: 1, CLI: "claude", PID: 1, CWD: "/x"}
	second := &Stub{ConnID: 2, CLI: "codex", PID: 2, CWD: "/x"}
	key := MakeRouteKey("telegram", -100, ptrI64(281))

	r.Claim(key, first)
	holder, ok := r.Claim(key, second)
	if ok {
		t.Error("second claim should fail")
	}
	if holder == nil || holder.ConnID != first.ConnID {
		t.Errorf("expected holder = first stub, got %+v", holder)
	}
}

func TestRoutes_ReleaseFreesRoute(t *testing.T) {
	r := NewRoutes()
	first := &Stub{ConnID: 1, CLI: "claude", PID: 1, CWD: "/x"}
	second := &Stub{ConnID: 2, CLI: "codex", PID: 2, CWD: "/x"}
	key := MakeRouteKey("telegram", -100, ptrI64(281))

	r.Claim(key, first)
	r.Release(key, first.ConnID)

	if _, ok := r.Claim(key, second); !ok {
		t.Error("after release, second claim should succeed")
	}
}

func TestRoutes_ReleaseByWrongOwnerIsNoop(t *testing.T) {
	r := NewRoutes()
	first := &Stub{ConnID: 1}
	key := MakeRouteKey("telegram", -100, ptrI64(281))

	r.Claim(key, first)
	r.Release(key, 999)

	holder, _ := r.Holder(key)
	if holder == nil || holder.ConnID != 1 {
		t.Error("release by wrong owner should be no-op")
	}
}

func TestRoutes_ReleaseAllByConnID(t *testing.T) {
	r := NewRoutes()
	stub := &Stub{ConnID: 1}
	k1 := MakeRouteKey("telegram", -100, ptrI64(281))
	k2 := MakeRouteKey("telegram", -100, ptrI64(207))
	r.Claim(k1, stub)
	r.Claim(k2, stub)

	released := r.ReleaseAllByConnID(1)
	if len(released) != 2 {
		t.Errorf("expected 2 routes released, got %d", len(released))
	}
}
