package broker

import (
	"testing"
)

// anonStub builds a stub whose hello named nothing — the shape a third-party
// adapter produces by omitting the identity keys, or sending them under other
// names. The broker parses it fine; the question these tests answer is whether
// it is then treated as an IDENTITY.
func anonStub(connID uint64) *Stub {
	return &Stub{ConnID: connID, CLI: "", PID: 0, CWD: ""}
}

// TestSameLogicalSession_TwoAnonymousConnectionsAreNotTheSameSession is the
// project's core invariant at the point that decides who owns a route.
//
// The defect: sameLogicalSession compared CLI+PID+CWD literally, so two
// connections that each sent an empty CLI, PID 0 and an empty CWD compared
// EQUAL. Routes.Claim then took its TRANSFER branch — handing one connection's
// claims to the other while skipping both the liveness check and the
// force-steal confirmation the user is supposed to see.
func TestSameLogicalSession_TwoAnonymousConnectionsAreNotTheSameSession(t *testing.T) {
	a, b := anonStub(1), anonStub(2)
	if sameLogicalSession(a, b) {
		t.Fatal("two connections that named no identity compared as THE SAME SESSION — the route " +
			"claim of one is now transferable to the other with no liveness check and no force-steal " +
			"confirmation, which is the 'two unknown identities must never compare equal' rule broken " +
			"at the exact point that decides who owns a topic")
	}
}

// TestSameLogicalSession_PartialIdentityIsStillNotAnIdentity covers the halfway
// cases. A CLI with no PID (or the reverse) is a partial description, and a
// partial description is not an identity either — otherwise every adapter that
// forgot one field shares one bucket.
func TestSameLogicalSession_PartialIdentityIsStillNotAnIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b *Stub
	}{
		{"no PID on either side",
			&Stub{ConnID: 1, CLI: "rust", PID: 0, CWD: "/w"},
			&Stub{ConnID: 2, CLI: "rust", PID: 0, CWD: "/w"}},
		{"no CLI on either side",
			&Stub{ConnID: 1, CLI: "", PID: 42, CWD: "/w"},
			&Stub{ConnID: 2, CLI: "", PID: 42, CWD: "/w"}},
		{"negative PID",
			&Stub{ConnID: 1, CLI: "rust", PID: -1, CWD: "/w"},
			&Stub{ConnID: 2, CLI: "rust", PID: -1, CWD: "/w"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if sameLogicalSession(tc.a, tc.b) {
				t.Fatalf("%s: an incomplete identity was matched to another incomplete identity, so "+
					"two unrelated adapters can inherit each other's claims", tc.name)
			}
		})
	}
}

// TestSameLogicalSession_AGenuineReconnectStillMatches is the control that keeps
// the fix from being a blunt refusal. A real adapter reconnecting registers a
// FRESH stub with the same CLI, PID and CWD, and it must still be recognised —
// that transfer is a documented feature (docs/ADAPTERS.md), not an accident.
func TestSameLogicalSession_AGenuineReconnectStillMatches(t *testing.T) {
	a := &Stub{ConnID: 1, CLI: "claude", PID: 4242, CWD: "/home/x/proj"}
	b := &Stub{ConnID: 2, CLI: "claude", PID: 4242, CWD: "/home/x/proj"}
	if !sameLogicalSession(a, b) {
		t.Fatal("a genuine adapter reconnect was NOT recognised as the same session, so it loses its " +
			"claim on every broker reconnect — the fix for anonymous identities must not cost real " +
			"sessions their documented reconnect transfer")
	}
}

// TestSameLogicalSession_AnEmptyCWDStillMatchesWhenCLIAndPIDAgree pins a
// deliberate asymmetry: CWD participates in the comparison but does NOT gate
// validity.
//
// `os.Getwd()` fails when a session's launch directory has been deleted, and the
// bundled adapters do `cwd, _ := os.Getwd()` — so a REAL session can legitimately
// hello with an empty CWD and a real PID. Gating on CWD would strip a bundled
// adapter of its reconnect transfer in a real scenario. This is safe because two
// simultaneously live processes cannot share a PID: with PID > 0 enforced, CWD
// never decides between two unknowns.
func TestSameLogicalSession_AnEmptyCWDStillMatchesWhenCLIAndPIDAgree(t *testing.T) {
	a := &Stub{ConnID: 1, CLI: "claude", PID: 4242, CWD: ""}
	b := &Stub{ConnID: 2, CLI: "claude", PID: 4242, CWD: ""}
	if !sameLogicalSession(a, b) {
		t.Fatal("a session whose launch directory was deleted (os.Getwd failed, so CWD is empty) lost " +
			"its reconnect transfer even though CLI and PID name it exactly — CWD must be compared, " +
			"not required")
	}
}

// TestFindByLogicalSession_RefusesAnonymousBeforeAnyAttach pins the SECOND,
// independent copy of the comparison — and it is the one that matters most.
//
// FindByLogicalSession does not call sameLogicalSession; it carries its own
// hand-rolled triple equality. It runs at HELLO time, before any attach, and the
// caller Unregisters the matched stub and moves its claims to the new
// connection. So an unguarded match here steals a live session's topic from a
// connection that has not asked for anything yet — and fixing only
// sameLogicalSession would leave this path wide open while every test on the
// other path went green. That is exactly the shape of hollow fix this project
// keeps catching, so it gets its own test.
func TestFindByLogicalSession_RefusesAnonymousBeforeAnyAttach(t *testing.T) {
	r := NewRoutes()
	victim := anonStub(1)
	key := MakeRouteKey("telegram", -100, nil)
	if _, ok := r.Claim(key, victim); !ok {
		t.Fatal("setup: the victim could not claim the route")
	}

	if got := r.FindByLogicalSession("", 0, ""); got != nil {
		t.Fatal("a connection that named no identity MATCHED a claim-holding session at hello time, " +
			"before any attach — the caller unregisters that stub and transfers its claims, so this " +
			"is topic theft with no attach call and no user confirmation anywhere in the path")
	}
}

// TestFindByLogicalSession_StillFindsARealReconnect is the matching control: the
// hello-time reconnect transfer is a documented feature and must survive.
func TestFindByLogicalSession_StillFindsARealReconnect(t *testing.T) {
	r := NewRoutes()
	live := &Stub{ConnID: 1, CLI: "claude", PID: 4242, CWD: "/home/x/proj"}
	key := MakeRouteKey("telegram", -100, nil)
	if _, ok := r.Claim(key, live); !ok {
		t.Fatal("setup: the live session could not claim the route")
	}

	if got := r.FindByLogicalSession("claude", 4242, "/home/x/proj"); got != live {
		t.Fatal("a real reconnect was not found at hello time, so it cannot transfer its claims and " +
			"the user's session loses its topic on every broker restart")
	}
}
