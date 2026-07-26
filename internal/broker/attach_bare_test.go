package broker

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/queue"
)

// captureAttached runs write (which must produce exactly one AttachedMsg on the
// conn it's given) and returns the decoded message. Lets a test drive attachBare
// / attachByName directly against a pre-configured stub — the only way to
// exercise the recover (ii) path with a pre-set stable session id, since the
// peer/hello flow can't set the sid without also triggering the automatic
// recover op.
func captureAttached(t *testing.T, write func(conn *ipc.Conn)) ipc.AttachedMsg {
	t.Helper()
	cliEnd, brokerEnd := net.Pipe()
	defer cliEnd.Close()
	defer brokerEnd.Close()
	got := make(chan ipc.AttachedMsg, 1)
	go func() {
		raw, err := ipc.NewConn(cliEnd).ReadFrame()
		if err != nil {
			got <- ipc.AttachedMsg{}
			return
		}
		var ack ipc.AttachedMsg
		_ = json.Unmarshal(raw, &ack)
		got <- ack
	}()
	write(ipc.NewConn(brokerEnd))
	select {
	case ack := <-got:
		return ack
	case <-time.After(2 * time.Second):
		t.Fatal("attach produced no response")
		return ipc.AttachedMsg{}
	}
}

// TestAttachBare_MisTargetRegression_FreeTopicWithBacklog_NoClaim is THE test the
// incident needed. A bare attach with a stale/wrong mappings[cwd] entry pointing
// at a FREE topic Y that has queued backlog, and an empty stableSessionID (no
// recover op yet), pre-redesign SILENTLY CLAIMED Y (PATH A) — and a following
// fetch_queue(ack=true) would have drained the wrong topic. Post-redesign: the
// bare attach returns a pick_topic proposal and Y's route holder stays EMPTY —
// no claim means no consume path can target Y at all.
func TestAttachBare_MisTargetRegression_FreeTopicWithBacklog_NoClaim(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	const launch = "/home/user/wrong-project"
	// Stale/wrong cwd mapping → a real free topic Y (c3 / 281).
	mf.Mappings[launch] = mappings.Mapping{
		Channel: "telegram", ChatID: -100, TopicID: 281, Name: "c3", Group: "main",
	}
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	// Y has held backlog.
	tid := int64(281)
	ykey := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	for i := int64(1); i <= 3; i++ {
		if err := b.Queue.Append(qrk, &c3types.Inbound{
			Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: i,
			Sender: c3types.Sender{Username: "k"}, Text: "held", Timestamp: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, launch) // empty stableSessionID

	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: launch})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if ack.OK {
		t.Fatalf("mis-target: bare attach must NOT claim the stale cwd topic; got OK Name=%q", ack.Name)
	}
	if ack.Proposal == nil || ack.Proposal.Action != "pick_topic" {
		t.Fatalf("mis-target: expected a pick_topic proposal; got Status=%q Proposal=%+v Err=%q",
			ack.Status, ack.Proposal, ack.Err)
	}
	// THE assertion: Y stays unclaimed → no route → no consume/drain path exists.
	if h, ok := b.Routes.Holder(ykey); ok {
		t.Fatalf("mis-target: topic Y must stay UNCLAIMED (no drain path); got holder=%+v", h)
	}
}

// TestAttachBare_RecoverableSessionAttachment_SilentClaim covers the one silent
// path (ii): a bare attach whose STABLE session id resolves a recoverable
// session_attachment silently re-claims that OWN route. A different sid cannot
// resolve it (keyed uniqueness) — it gets the picker instead.
func TestAttachBare_RecoverableSessionAttachment_SilentClaim(t *testing.T) {
	mf := mfWithTelegram()
	seedSessionAttachment(mf, "sess-1", time.Now().UTC(), false) // → c3 / 281
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	stub := b.Stubs.Register("claude", 1, "/anywhere", struct{}{})
	stub.SetStableSessionID("sess-1")

	ack := captureAttached(t, func(c *ipc.Conn) {
		b.attachBare(c, stub, "telegram", "/anywhere", "", false, false)
	})
	if !ack.OK {
		t.Fatalf("recoverable session attachment must silent-claim; got Err=%q Proposal=%+v", ack.Err, ack.Proposal)
	}
	if ack.Name != "c3" || ack.TopicID == nil || *ack.TopicID != 281 {
		t.Errorf("silent claim should resolve to c3/281; got Name=%q TopicID=%v", ack.Name, ack.TopicID)
	}
	tid := int64(281)
	if _, ok := b.Routes.Holder(MakeRouteKey("telegram", -100, &tid)); !ok {
		t.Error("the session's own route must be claimed after a silent recover")
	}

	// A DIFFERENT sid must NOT resolve sess-1's attachment.
	other := b.Stubs.Register("claude", 2, "/anywhere", struct{}{})
	other.SetStableSessionID("sess-2")
	ack2 := captureAttached(t, func(c *ipc.Conn) {
		b.attachBare(c, other, "telegram", "/anywhere", "", false, false)
	})
	if ack2.OK {
		t.Fatalf("a different sid must NOT resolve another session's attachment; got OK Name=%q", ack2.Name)
	}
	if ack2.Proposal == nil || ack2.Proposal.Action != "pick_topic" {
		t.Fatalf("different sid → picker; got Proposal=%+v", ack2.Proposal)
	}
}

// TestAttachBare_TombstonedThenExplicitReenablesResume covers the detach
// tombstone: a bare attach after an explicit detach must NOT silent-resume (the
// deliberate detach is honored), and a subsequent explicit attach clears the
// tombstone so a fresh instance of the same session silent-resumes again.
func TestAttachBare_TombstonedThenExplicitReenablesResume(t *testing.T) {
	mf := mfWithTelegram()
	seedSessionAttachment(mf, "sess-1", time.Now().UTC(), true) // tombstoned (detached)
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	tid := int64(281)
	c3key := MakeRouteKey("telegram", -100, &tid)

	// (a) Bare attach with a tombstoned attachment → picker, no silent re-claim.
	stub := b.Stubs.Register("claude", 1, "/proj", struct{}{})
	stub.SetStableSessionID("sess-1")
	ack := captureAttached(t, func(c *ipc.Conn) {
		b.attachBare(c, stub, "telegram", "/proj", "", false, false)
	})
	if ack.OK {
		t.Fatalf("tombstoned attachment must not silent-resume; got OK Name=%q", ack.Name)
	}
	if ack.Proposal == nil || ack.Proposal.Action != "pick_topic" {
		t.Fatalf("tombstone → picker; got Proposal=%+v", ack.Proposal)
	}
	if _, ok := b.Routes.Holder(c3key); ok {
		t.Fatal("tombstoned attachment must not be claimed")
	}

	// (b) An EXPLICIT attach clears the tombstone (persistMapping upserts a fresh
	// session attachment) and claims c3.
	exp := captureAttached(t, func(c *ipc.Conn) {
		b.attachByName(c, stub, "telegram", "c3", "/proj", "", false, false, false)
	})
	if !exp.OK || exp.Name != "c3" {
		t.Fatalf("explicit attach c3 should succeed and clear the tombstone; got %+v", exp)
	}
	if sa, ok := b.Mappings().LookupSessionAttachment("sess-1"); !ok || sa.Detached {
		t.Fatalf("explicit attach must clear the tombstone; got %+v ok=%v", sa, ok)
	}

	// Release the explicit claim so the fresh session's own-route recover isn't
	// skipped as held-by-another-live-session (simulates the first session ending).
	b.Routes.ReleaseAllByConnID(stub.ConnID)

	// (c) A fresh instance of the same session now silent-resumes again.
	fresh := b.Stubs.Register("claude", 3, "/proj", struct{}{})
	fresh.SetStableSessionID("sess-1")
	resume := captureAttached(t, func(c *ipc.Conn) {
		b.attachBare(c, fresh, "telegram", "/proj", "", false, false)
	})
	if !resume.OK || resume.Name != "c3" {
		t.Fatalf("after the tombstone is cleared, a fresh bare attach must silent-resume c3; got %+v", resume)
	}
	if _, ok := b.Routes.Holder(c3key); !ok {
		t.Error("the resumed session must hold c3")
	}
}

// TestAttachBare_OwnRouteHeldByAnotherLiveSession_ShowsPicker: a bare attach
// whose own recorded route is held by ANOTHER live session must skip the recover
// (no steal) and fall to the picker; the original holder keeps its claim.
func TestAttachBare_OwnRouteHeldByAnotherLiveSession_ShowsPicker(t *testing.T) {
	mf := mfWithTelegram()
	seedSessionAttachment(mf, "sess-1", time.Now().UTC(), false) // → c3 / 281
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	tid := int64(281)
	key := MakeRouteKey("telegram", -100, &tid)
	holder := b.Stubs.Register("claude", 9999, "/other", struct{}{})
	if _, ok := b.Routes.Claim(key, holder); !ok {
		t.Fatal("precondition: holder claims c3")
	}

	stub := b.Stubs.Register("claude", 1, "/proj", struct{}{})
	stub.SetStableSessionID("sess-1")
	ack := captureAttached(t, func(c *ipc.Conn) {
		b.attachBare(c, stub, "telegram", "/proj", "", false, false)
	})
	if ack.OK {
		t.Fatalf("recover must skip a route held by another live session; got OK Name=%q", ack.Name)
	}
	if ack.Proposal == nil || ack.Proposal.Action != "pick_topic" {
		t.Fatalf("held own route → picker; got Proposal=%+v", ack.Proposal)
	}
	if h, ok := b.Routes.Holder(key); !ok || h.ConnID != holder.ConnID {
		t.Error("the original live holder must keep the claim")
	}
}

// TestAttach_ExplicitName_IgnoresStaleCwdMapping proves the explicit-name path
// no longer consults the cwd map either: an explicit attach c3 binds c3, not the
// stale cwd target feature-x.
func TestAttach_ExplicitName_IgnoresStaleCwdMapping(t *testing.T) {
	mf := mfWithTelegram()
	const launch = "/home/user/projects"
	mf.Mappings[launch] = mappings.Mapping{
		Channel: "telegram", ChatID: -200, TopicID: 412, Name: "feature-x", Group: "work",
	}
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, launch)
	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: launch, Name: "c3"})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)
	if !ack.OK || ack.Name != "c3" || ack.TopicID == nil || *ack.TopicID != 281 {
		t.Fatalf("explicit attach c3 must bind c3/281, not the cwd-mapped feature-x; got %+v", ack)
	}
	ftid := int64(412)
	if h, ok := b.Routes.Holder(MakeRouteKey("telegram", -200, &ftid)); ok {
		t.Errorf("the stale cwd topic feature-x must stay unclaimed; got holder=%+v", h)
	}
}

// TestAttachBare_EmptySID_NoAttachment_ShowsPicker is the Codex case (and the
// fresh-Claude ~2s pre-hook window): empty stableSessionID + no session
// attachment → always the picker, never a silent claim.
func TestAttachBare_EmptySID_NoAttachment_ShowsPicker(t *testing.T) {
	mf := mfWithTelegram()
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/some/fresh/dir")
	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/some/fresh/dir"})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)
	if ack.OK {
		t.Fatalf("empty sid + no attachment must show the picker, not claim; got OK Name=%q", ack.Name)
	}
	if ack.Proposal == nil || ack.Proposal.Action != "pick_topic" {
		t.Fatalf("expected pick_topic; got Proposal=%+v", ack.Proposal)
	}
}

// TestAttach_BareCreateTrue_Errors: with the cwd-basename backfill deleted, a
// bare create=true (no name) can no longer synthesize a name — it errors.
func TestAttach_BareCreateTrue_Errors(t *testing.T) {
	mf := mfWithTelegram()
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/proj")
	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/proj", Create: true})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)
	if ack.OK {
		t.Fatal("bare create=true must not succeed (no name)")
	}
	if ack.Err == "" {
		t.Errorf("bare create=true must return the name-required error; got %+v", ack)
	}
}

// TestAttach_CreateProposal_RoundTripWithName pins the reworked create flow: an
// unknown explicit name proposes create (carrying the name), and a re-invoke that
// carries the name (attach(name=X, create=true)) creates + claims it.
func TestAttach_CreateProposal_RoundTripWithName(t *testing.T) {
	mf := mfWithTelegram()
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/proj")

	// (a) unknown explicit name → create proposal carrying the name.
	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/proj", Name: "brand-new"})
	raw, _ := peer.ReadFrame()
	var propose ipc.AttachedMsg
	_ = json.Unmarshal(raw, &propose)
	if propose.OK || propose.Proposal == nil || propose.Proposal.Action != "create" || propose.Proposal.Name != "brand-new" {
		t.Fatalf("unknown name should propose create carrying the name; got %+v", propose)
	}

	// (b) re-invoke carrying the name → created + claimed.
	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/proj", Name: "brand-new", Create: true})
	raw, _ = peer.ReadFrame()
	var created ipc.AttachedMsg
	_ = json.Unmarshal(raw, &created)
	if !created.OK || created.Name != "brand-new" {
		t.Fatalf("re-invoke with name+create should create+claim; got %+v", created)
	}
}
