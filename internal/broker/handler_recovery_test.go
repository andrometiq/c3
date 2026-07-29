package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// seedSessionAttachment adds a recoverable c3/topic-281 attachment for id.
func seedSessionAttachment(mf *mappings.MappingsFile, id string, lastAttached time.Time, detached bool) {
	tid := int64(281)
	mf.UpsertSessionAttachment("claude", id, mappings.SessionAttachment{
		Channel: "telegram", ChatID: -100, TopicID: &tid, Name: "c3", Group: "main",
		LastAttachedAt: lastAttached, Detached: detached,
	})
}

func TestAttachDM_RecordsSessionAttachment(t *testing.T) {
	mf := mfWithTelegram()
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: 1, CWD: "/x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil { // hello ack
		t.Fatal(err)
	}
	// Deliver the stable session id via the recover op (no route yet → record-
	// only path is not taken; recoverSession finds nothing, stays unrecovered).
	if err := peer.WriteJSON(ipc.RecoverSessionReq{Op: ipc.OpRecoverSession, StableSessionID: "dm-sess"}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil { // recover resp
		t.Fatal(err)
	}
	if err := peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, Target: "dm"}); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var ack ipc.AttachedMsg
	if err := json.Unmarshal(raw, &ack); err != nil || !ack.OK {
		t.Fatalf("DM attach failed: ack=%+v err=%v", ack, err)
	}
	// recordSessionAttachment runs synchronously before the ack is written, keyed
	// on the stable id set by the recover op.
	sa, ok := b.Mappings().LookupSessionAttachment("claude", "dm-sess")
	if !ok {
		t.Fatal("DM attach must record a session attachment (keyed on the stable id)")
	}
	if sa.Name != "dm" || sa.ChatID != 42 || sa.TopicID != nil {
		t.Fatalf("DM session attachment = %+v (want name=dm chat=42 topic=nil)", sa)
	}
}

func TestHandleRelease_TombstonesSessionAttachment(t *testing.T) {
	mf := mfWithTelegram()
	seedSessionAttachment(mf, "sess-1", time.Now().UTC(), false)
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	stub := b.Stubs.Register("claude", 1, "/x", nil)
	stub.SetStableSessionID("sess-1")
	tid := int64(281)
	b.Routes.Claim(MakeRouteKey("telegram", -100, &tid), stub)
	stub.SetRoute(func() *RouteKey { k := MakeRouteKey("telegram", -100, &tid); return &k }())

	b.handleRelease(stub)

	if stub.CurrentRoute() != nil {
		t.Fatal("release should clear the stub's route")
	}
	sa, ok := b.Mappings().LookupSessionAttachment("claude", "sess-1")
	if !ok || !sa.Detached {
		t.Fatalf("explicit detach must tombstone the session attachment; got %+v ok=%v", sa, ok)
	}
}

func TestHandleRelease_EmptyStableIDNoOp(t *testing.T) {
	mf := mfWithTelegram()
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()
	stub := b.Stubs.Register("claude", 1, "/x", nil) // no stable id set
	b.handleRelease(stub)                            // must not panic; nothing to tombstone
}

func TestConnDrop_DoesNotTombstone(t *testing.T) {
	// The HandleConn conn-drop defer releases claims via ReleaseAllByConnID
	// directly (NOT handleRelease), so a quit-without-detach must leave the
	// session attachment recoverable.
	mf := mfWithTelegram()
	seedSessionAttachment(mf, "sess-1", time.Now().UTC(), false)
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	stub := b.Stubs.Register("claude", 1, "/x", nil)
	stub.SetStableSessionID("sess-1")
	b.Routes.ReleaseAllByConnID(stub.ConnID) // the conn-drop path

	sa, ok := b.Mappings().LookupSessionAttachment("claude", "sess-1")
	if !ok || sa.Detached {
		t.Fatalf("conn-drop must NOT tombstone; got %+v ok=%v", sa, ok)
	}
}
