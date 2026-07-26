package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

// The invariant under test: identity must be settled before anything that
// depends on it is answered — and its corollary, an explicit user action must
// not be silently undone by a background process that arrives later.
//
// The detach path tombstones the PERSISTED session attachment, but the tombstone
// is keyed on the stable session id, which a resumed session delivers on a
// SessionStart handoff that lands after hello (the adapter watches for it for
// ~30 minutes). Detach in that window and there is no identity to tombstone; the
// late recover op then found a still-Recoverable attachment and re-claimed the
// route the user had just left. Stub.explicitlyDetached is the barrier.

// TestDetachBeforeIdentity_LateRecoverDoesNotReattach is the reviewer's exact
// sequence, end to end over a real connection: a resumed session whose prior
// route is on file, a detach BEFORE any stable identity is known (the adapter
// writes OpRelease unconditionally, even holding no route), then the late
// recover_session that the still-live recovery watch delivers.
func TestDetachBeforeIdentity_LateRecoverDoesNotReattach(t *testing.T) {
	mf := mfWithTelegram()
	mf.AutoAttachOnResume = boolPtr(true)                        // gate ON: recovery would fire
	seedSessionAttachment(mf, "sess-1", time.Now().UTC(), false) // prior route on file → c3 / 281
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/proj")

	// The user detaches. No route is held and no stable id has arrived yet.
	if err := peer.WriteJSON(ipc.ReleaseReq{Op: ipc.OpRelease}); err != nil {
		t.Fatal(err)
	}

	// The SessionStart handoff finally lands and the adapter delivers the id.
	if err := peer.WriteJSON(ipc.RecoverSessionReq{Op: ipc.OpRecoverSession, StableSessionID: "sess-1", CWD: "/proj"}); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.RecoverSessionResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}

	if resp.Recovered {
		t.Fatalf("a session the user explicitly detached was silently re-attached by late recovery: %+v", resp)
	}
	tid := int64(281)
	if _, held := b.Routes.Holder(MakeRouteKey("telegram", -100, &tid)); held {
		t.Fatal("a session the user explicitly detached was silently re-attached by late recovery: the detached route was re-claimed")
	}
	// The "resumed" welcome is posted async from the recovered branch; if that
	// branch ran at all, the send lands well inside this wait.
	waitForReplies(fc, 1)
	for _, call := range fc.sendRepliesSnapshot() {
		t.Fatalf("a session the user explicitly detached was silently re-attached by late recovery: it posted %q into the topic", call.Text)
	}

	// Durability: the identity has arrived, so the detach is now recorded where it
	// survives this connection. Without it a broker bounce (routine — and the Grok
	// adapter re-fires recovery on every reconnect) would hand the route back on
	// the next connection, where the per-connection barrier no longer exists.
	sa, ok := b.Mappings().LookupSessionAttachment("sess-1")
	if !ok {
		t.Fatal("the session attachment must still exist after a refused recovery")
	}
	if !sa.Detached {
		t.Fatal("the detach did not survive the connection: once the identity arrived the attachment must be tombstoned, or the next broker restart re-attaches a session the user detached")
	}
	// The identity itself IS kept — only the route restore is dropped — so a later
	// explicit attach on this connection is still recorded against this session.
	stubs := b.Stubs.Snapshot()
	if len(stubs) != 1 {
		t.Fatalf("expected exactly one live stub, got %d", len(stubs))
	}
	if got := stubs[0].StableSessionIDValue(); got != "sess-1" {
		t.Fatalf("the recover frame's identity half must be kept (so later explicit attaches record against this session); stable id = %q", got)
	}
}

// TestExplicitAttachClearsDetachBarrier pins the other half of the rule: only
// RECOVERY is blocked. A user who detaches and then deliberately attaches again
// is honored — the barrier is retired by the explicit claim (tryClaim), and the
// attach's own upsert clears the tombstone.
func TestExplicitAttachClearsDetachBarrier(t *testing.T) {
	mf := mfWithTelegram()
	seedSessionAttachment(mf, "sess-1", time.Now().UTC(), true) // detached earlier
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	stub := b.Stubs.Register("claude", 1, "/proj", struct{}{})
	stub.SetStableSessionID("sess-1")
	stub.SetExplicitlyDetached(true)

	ack := captureAttached(t, func(c *ipc.Conn) {
		b.attachByName(c, stub, "telegram", "c3", "/proj", "", false, false, false)
	})
	if !ack.OK || ack.Name != "c3" {
		t.Fatalf("an explicit attach after a detach must succeed; got %+v", ack)
	}
	if stub.ExplicitlyDetached() {
		t.Fatal("an explicit attach must retire the detach barrier — otherwise the user's own re-attach leaves the session permanently unrecoverable")
	}
	if sa, ok := b.Mappings().LookupSessionAttachment("sess-1"); !ok || sa.Detached {
		t.Fatalf("an explicit attach must clear the tombstone; got %+v ok=%v", sa, ok)
	}
}

// TestRecoverSession_RefusesExplicitlyDetachedStub pins the barrier at the CLAIM
// site rather than at one caller. The state is constructed directly because in
// the production sequence handleRecoverSession converts the flag into the durable
// tombstone first, so recoverSession would refuse on Recoverable() anyway; this
// test is what stops a future caller of recoverSession — or a refactor that moves
// the tombstone write — from silently re-opening the defect. attachBare's manual
// own-recover (path ii) is exactly such a caller today.
func TestRecoverSession_RefusesExplicitlyDetachedStub(t *testing.T) {
	mf := mfWithTelegram()
	seedSessionAttachment(mf, "sess-1", time.Now().UTC(), false) // fully recoverable
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	stub := b.Stubs.Register("claude", 1, "/proj", nil)
	stub.SetStableSessionID("sess-1")
	stub.SetExplicitlyDetached(true)

	if _, _, _, ok := b.recoverSession(stub); ok {
		t.Fatal("a session the user explicitly detached was silently re-attached by late recovery: recoverSession claimed on a detached connection")
	}
	tid := int64(281)
	if _, held := b.Routes.Holder(MakeRouteKey("telegram", -100, &tid)); held {
		t.Fatal("a session the user explicitly detached was silently re-attached by late recovery: the route was claimed")
	}

	// Same stub, barrier retired by an explicit attach → recovery works again.
	stub.SetExplicitlyDetached(false)
	if _, _, _, ok := b.recoverSession(stub); !ok {
		t.Fatal("the barrier must block ONLY while set; a re-attached session must recover normally")
	}
}

// TestHandleRelease_EmptyStableIDRaisesBarrier is the honest assertion missing
// from TestHandleRelease_EmptyStableIDNoOp (handler_recovery_test.go), which
// checks only that a detach with no known identity does not panic — the "no-op"
// it names is precisely the defect. There is still nothing to tombstone, but the
// release must leave the connection marked so late recovery is refused.
func TestHandleRelease_EmptyStableIDRaisesBarrier(t *testing.T) {
	mf := mfWithTelegram()
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	stub := b.Stubs.Register("claude", 1, "/proj", nil) // no stable id — identity unknown
	b.handleRelease(stub)

	if !stub.ExplicitlyDetached() {
		t.Fatal("a detach with no known identity must still raise the per-connection barrier; otherwise the detach is remembered nowhere and late recovery re-attaches the session")
	}
}
