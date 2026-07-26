package broker

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// SYMPTOM-3 (2026-06-04): multiple `claude` instances launched from the
// SAME parent dir (/home/user/projects) report identical os.Getwd(). A
// bare `/c3:attach` (cwd-default, no name) from a session that MEANT a
// different sub-project silently resolves the saved parent→topic mapping
// and races/steals the topic already held by a live sibling session.
//
// The chosen UX (maintainer): when a BARE cwd-default attach resolves to a
// topic ALREADY HELD by a DIFFERENT LIVE session, do NOT silently claim
// and do NOT show only the raw force_steal y/n prompt. Return a guided
// AttachStatusCwdDefaultCollision message that names the holder and
// suggests attach-by-name (the likely-correct action) or --steal.

// collisionSetup wires a broker whose saved cwd-mapping points the launch
// dir at topic "c3" (281), then has a FIRST live session claim that topic.
// Returns the broker for a SECOND session to race on.
func collisionSetup(t *testing.T, launchCWD string) (*Broker, *fakeChannel) {
	t.Helper()
	mf := mfWithTelegram()
	// Saved mapping: the shared launch dir defaults to topic c3 (281).
	mf.Mappings[launchCWD] = mappings.Mapping{
		Channel: "telegram", ChatID: -100, TopicID: 281,
		Name: "c3", Group: "main",
	}
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	return b, fc
}

// TestAttach_CwdDefault_HeldByDifferentLiveSession_ShowsPicker is the flipped
// SYMPTOM-3 test: post-redesign a bare attach NEVER consults the saved cwd→topic
// mapping (PATH A deleted, spec §2). So even when the cwd-mapped topic is held by
// a different live session, a bare attach returns a pick_topic proposal and makes
// NO claim / no steal — the picker is how the user chooses. The old
// cwd_default_collision status is now dormant (never emitted).
func TestAttach_CwdDefault_HeldByDifferentLiveSession_ShowsPicker(t *testing.T) {
	const launch = "/home/user/projects"
	b, _ := collisionSetup(t, launch)
	defer b.Shutdown()

	// FIRST live session claims topic c3 directly (non-nil conn → IsAlive true;
	// PID 9823 is synthetic and not a live process here).
	holder := b.Stubs.Register("claude", 9823, launch, struct{}{})
	tid := int64(281)
	key := MakeRouteKey("telegram", -100, &tid)
	if !b.tryClaim(nil, holder, key, "c3", false, false) {
		t.Fatal("holder's initial claim should succeed")
	}

	// SECOND session: bare attach from the SAME launch dir. The saved cwd map
	// is no longer consulted → picker, never a claim/steal of the held topic.
	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, launch)
	if err := peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: launch}); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var ack ipc.AttachedMsg
	if err := json.Unmarshal(raw, &ack); err != nil {
		t.Fatal(err)
	}

	if ack.OK {
		t.Fatalf("bare attach must NOT silently claim; got OK with Name=%q", ack.Name)
	}
	if ack.Proposal == nil || ack.Proposal.Action != "pick_topic" {
		t.Fatalf("bare attach must return a pick_topic proposal; got Status=%q Proposal=%+v Err=%q",
			ack.Status, ack.Proposal, ack.Err)
	}
	if ack.Status == ipc.AttachStatusCwdDefaultCollision {
		t.Error("cwd_default_collision status is dormant post-redesign; must not be emitted")
	}
	// The held topic must keep its original holder — no steal, no transfer.
	if h, ok := b.Routes.Holder(key); !ok || h.PID != 9823 {
		t.Errorf("held topic c3 must stay with holder pid 9823; got holder=%+v held=%v", h, ok)
	}
}

// TestAttach_CwdDefault_TopicFree_ShowsPicker is the flipped incident test:
// pre-redesign a bare attach silently claimed the FREE saved-mapping topic (the
// exact silent mis-target class). Post-redesign a bare attach shows the picker
// and claims NOTHING — the cwd map only ever seeds a suggestion (Phase 2).
func TestAttach_CwdDefault_TopicFree_ShowsPicker(t *testing.T) {
	const launch = "/home/user/projects"
	b, _ := collisionSetup(t, launch)
	defer b.Shutdown()

	// No prior holder — topic c3 is free.
	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, launch)
	if err := peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: launch}); err != nil {
		t.Fatal(err)
	}
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if ack.OK {
		t.Fatalf("bare attach to a free cwd-mapped topic must NOT silently claim; got OK Name=%q", ack.Name)
	}
	if ack.Proposal == nil || ack.Proposal.Action != "pick_topic" {
		t.Fatalf("bare attach must return a pick_topic proposal; got Status=%q Proposal=%+v Err=%q",
			ack.Status, ack.Proposal, ack.Err)
	}
	// The topic must remain unclaimed — the picker never binds.
	tid := int64(281)
	if h, ok := b.Routes.Holder(MakeRouteKey("telegram", -100, &tid)); ok {
		t.Errorf("free topic must stay UNCLAIMED after a picker response; got holder=%+v", h)
	}
}

// TestHeldByDifferentLiveSession_Predicate directly pins the helper that
// drives the collision check, exercising each branch of the reused
// holder-detection (held? / same-logical-session? / IsAlive?) in isolation
// — independent of the hello-stage reconnect transfer that an end-to-end
// peer test would trigger first.
func TestHeldByDifferentLiveSession_Predicate(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(281)
	key := MakeRouteKey("telegram", -100, &tid)
	caller := &Stub{CLI: "claude", PID: 1, CWD: "/home/user/projects"}

	// (a) Unheld → no collision.
	if h, c := b.heldByDifferentLiveSession(key, caller); c || h != nil {
		t.Errorf("unheld key must not collide; got holder=%+v collides=%v", h, c)
	}

	// (b) Held by a DIFFERENT live session → collision. Use the running
	// test process's own PID so IsAlive() is unambiguously true.
	live := &Stub{CLI: "claude", PID: os.Getpid(), CWD: "/home/user/projects", Conn: struct{}{}}
	if _, ok := b.Routes.Claim(key, live); !ok {
		t.Fatal("seeding the live holder claim should succeed")
	}
	h, c := b.heldByDifferentLiveSession(key, caller)
	if !c || h == nil || h.PID != live.PID {
		t.Errorf("different live holder must collide; got holder=%+v collides=%v", h, c)
	}

	// (c) Held by the SAME logical session (caller IS the holder) → no
	// collision (it's a re-attach, the claim would succeed). Seed a holder
	// with the caller's exact identity (CLI+PID+CWD) on a separate key.
	selfKey := MakeRouteKey("telegram", -200, &[]int64{412}[0])
	selfHolder := &Stub{CLI: caller.CLI, PID: caller.PID, CWD: caller.CWD, Conn: struct{}{}}
	if _, ok := b.Routes.Claim(selfKey, selfHolder); !ok {
		t.Fatal("seeding the self holder claim should succeed")
	}
	if h, c := b.heldByDifferentLiveSession(selfKey, caller); c || h != nil {
		t.Errorf("same-logical-session holder must NOT collide; got holder=%+v collides=%v", h, c)
	}

	// (d) Held by a DEAD different session → no collision (stale claim
	// would be displaced by Claim anyway). PID 0 is never alive. Inject
	// directly so the seeding itself doesn't go through Claim's liveness
	// path.
	dead := &Stub{CLI: "codex", PID: 0, CWD: "/elsewhere"}
	deadKey := MakeRouteKey("telegram", -100, &[]int64{412}[0])
	b.Routes.mu.Lock()
	b.Routes.m[deadKey] = dead
	b.Routes.mu.Unlock()
	if h, c := b.heldByDifferentLiveSession(deadKey, caller); c || h != nil {
		t.Errorf("dead holder must NOT collide; got holder=%+v collides=%v", h, c)
	}
}

// TestAttach_ExplicitName_HeldTopic_StillForceSteal locks the scope rule:
// an EXPLICIT `/c3:attach c3` to a held topic must STILL behave as today
// (force_steal proposal), NOT the new cwd-default collision status. The
// user explicitly asked for that topic, so the held-topic prompt is
// correct there.
func TestAttach_ExplicitName_HeldTopic_StillForceSteal(t *testing.T) {
	const launch = "/home/user/projects"
	b, _ := collisionSetup(t, launch)
	defer b.Shutdown()

	// Holder owns c3. Register WITH a non-nil conn so IsAlive() is true via
	// IsConnected() (PID 9823 is synthetic and not a live process here).
	holder := b.Stubs.Register("claude", 9823, launch, struct{}{})
	tid := int64(281)
	key := MakeRouteKey("telegram", -100, &tid)
	if !b.tryClaim(nil, holder, key, "c3", false, false) {
		t.Fatal("holder claim should succeed")
	}

	// SECOND session: EXPLICIT name "c3" (not cwd-default).
	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, launch)
	if err := peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: launch, Name: "c3"}); err != nil {
		t.Fatal(err)
	}
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if ack.OK {
		t.Fatal("explicit attach to held topic must not silently succeed")
	}
	if ack.Status == ipc.AttachStatusCwdDefaultCollision {
		t.Error("explicit-name attach must NOT use the cwd-default collision status")
	}
	if !ack.NeedsConfirmation || ack.Proposal == nil || ack.Proposal.Action != "force_steal" {
		t.Errorf("explicit-name held topic should still be force_steal proposal; got Status=%q Proposal=%+v",
			ack.Status, ack.Proposal)
	}
}

// TestAttachBare_AlreadyAttached_IdempotentOK reworks the former
// HeldBySameLogicalSession_NoWarn test for the redesign: a bare attach from a
// session that is ALREADY attached takes attachBare's idempotent guard —
// CurrentRoute != nil → report that route (OK=true) with the resolved Name, no
// re-claim, no picker. The resolved Name is load-bearing: FormatAttached renders
// it and the adapter's replay-remember records it.
func TestAttachBare_AlreadyAttached_IdempotentOK(t *testing.T) {
	const launch = "/home/user/projects"
	b, _ := collisionSetup(t, launch)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, launch)

	// First, an EXPLICIT attach to c3 sets this session's route.
	if err := peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: launch, Name: "c3"}); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var first ipc.AttachedMsg
	if err := json.Unmarshal(raw, &first); err != nil {
		t.Fatal(err)
	}
	if !first.OK || first.Name != "c3" {
		t.Fatalf("explicit attach c3 should succeed; got %+v", first)
	}

	tid := int64(281)
	key := MakeRouteKey("telegram", -100, &tid)
	holder, held := b.Routes.Holder(key)
	if !held {
		t.Fatal("precondition: c3 must be claimed after the explicit attach")
	}

	// Now a BARE attach → idempotent already-attached OK, no re-claim.
	if err := peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: launch}); err != nil {
		t.Fatal(err)
	}
	raw, err = peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var ack ipc.AttachedMsg
	if err := json.Unmarshal(raw, &ack); err != nil {
		t.Fatal(err)
	}

	if !ack.OK {
		t.Fatalf("bare attach while already attached must be idempotent OK; got Err=%q Status=%q Proposal=%+v",
			ack.Err, ack.Status, ack.Proposal)
	}
	if ack.NeedsConfirmation || ack.Proposal != nil {
		t.Errorf("idempotent bare attach must not propose a picker; got Proposal=%+v", ack.Proposal)
	}
	if ack.Name != "c3" || ack.TopicID == nil || *ack.TopicID != 281 {
		t.Errorf("idempotent response must carry the resolved route; got Name=%q TopicID=%v", ack.Name, ack.TopicID)
	}
	// Same holder, same conn — no re-claim, no transfer.
	if h2, ok := b.Routes.Holder(key); !ok || h2.ConnID != holder.ConnID {
		t.Errorf("idempotent bare attach must not re-claim; holder changed from conn=%d to %+v", holder.ConnID, h2)
	}
}
