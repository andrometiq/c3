package broker

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

const (
	switchSessionA = "conversation-a"
	switchSessionB = "conversation-b"
)

func seedIdentitySwitchAttachment(mf *mappings.MappingsFile, id string, topicID int64, name, group string, chatID int64) {
	tid := topicID
	mf.UpsertSessionAttachment(id, mappings.SessionAttachment{
		Channel:        "telegram",
		ChatID:         chatID,
		TopicID:        &tid,
		Name:           name,
		Group:          group,
		LastAttachedAt: time.Now().UTC(),
	})
}

func identitySwitchMappings(withTarget bool) *mappings.MappingsFile {
	mf := mfWithTelegram()
	mf.AutoAttachOnResume = boolPtr(true)
	seedIdentitySwitchAttachment(mf, switchSessionA, 281, "c3", "main", -100)
	if withTarget {
		seedIdentitySwitchAttachment(mf, switchSessionB, 412, "feature-x", "work", -200)
	}
	return mf
}

func recoverIdentityOnPeer(t *testing.T, peer *ipc.Conn, stableID string) ipc.RecoverSessionResp {
	t.Helper()
	if err := peer.WriteJSON(ipc.RecoverSessionReq{
		Op: ipc.OpRecoverSession, StableSessionID: stableID, CWD: "/project",
	}); err != nil {
		t.Fatalf("send recover(%s): %v", stableID, err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("read recover(%s): %v", stableID, err)
	}
	var resp ipc.RecoverSessionResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("decode recover(%s): %v", stableID, err)
	}
	return resp
}

func startIdentitySwitchPeer(t *testing.T, b *Broker) (*ipc.Conn, func()) {
	t.Helper()
	peer, done := peerPair(t, b)
	helloAck(t, peer, "/project")
	return peer, done
}

func switchRoute(topicID int64, chatID int64) RouteKey {
	tid := topicID
	return MakeRouteKey("telegram", chatID, &tid)
}

func requireSwitchAttachment(t *testing.T, b *Broker, id string, topicID int64, detached bool) {
	t.Helper()
	sa, ok := b.Mappings().LookupSessionAttachment(id)
	if !ok || sa.TopicID == nil || *sa.TopicID != topicID || sa.Detached != detached {
		t.Fatalf("conversation %s attachment was not preserved: got %+v ok=%v, want topic=%d detached=%v", id, sa, ok, topicID, detached)
	}
}

// T1: one connection resumes A, then switches in-app to B. The old claim must
// be released and B's own recorded topic reclaimed without merging either
// conversation's durable attachment record.
func TestRecoverSession_InAppSwitch_ReclaimsNewConversationTopic(t *testing.T) {
	b := brokerWithChannel(t, identitySwitchMappings(true), &fakeChannel{})
	defer b.Shutdown()
	peer, done := startIdentitySwitchPeer(t, b)
	defer done()

	if resp := recoverIdentityOnPeer(t, peer, switchSessionA); !resp.Recovered || resp.Name != "c3" {
		t.Fatalf("precondition: conversation A did not recover topicA: %+v", resp)
	}
	resp := recoverIdentityOnPeer(t, peer, switchSessionB)
	if !resp.Recovered || resp.Name != "feature-x" || resp.TopicID == nil || *resp.TopicID != 412 {
		t.Fatalf("identity switch did not reclaim conversation B's saved topic: %+v", resp)
	}

	if _, held := b.Routes.Holder(switchRoute(281, -100)); held {
		t.Fatal("identity switch left conversation A's topic claimed instead of releasing the old route")
	}
	if holder, held := b.Routes.Holder(switchRoute(412, -200)); !held || holder.StableSessionIDValue() != switchSessionB {
		t.Fatalf("identity switch did not leave conversation B holding topicB: held=%v holder=%+v", held, holder)
	}
	requireSwitchAttachment(t, b, switchSessionA, 281, false)
	requireSwitchAttachment(t, b, switchSessionB, 412, false)
}

// T2: B has no attachment. Switching still releases A, does not tombstone A,
// and leaves the live connection unattached.
func TestRecoverSession_InAppSwitch_NoSavedAttachment_ReleasesAndStaysUnattached(t *testing.T) {
	b := brokerWithChannel(t, identitySwitchMappings(false), &fakeChannel{})
	defer b.Shutdown()
	peer, done := startIdentitySwitchPeer(t, b)
	defer done()

	if resp := recoverIdentityOnPeer(t, peer, switchSessionA); !resp.Recovered {
		t.Fatalf("precondition: conversation A did not recover topicA: %+v", resp)
	}
	if resp := recoverIdentityOnPeer(t, peer, switchSessionB); resp.Recovered {
		t.Fatalf("identity switch with no saved target attachment must stay unattached: %+v", resp)
	}
	if _, held := b.Routes.Holder(switchRoute(281, -100)); held {
		t.Fatal("identity switch with no target attachment did not release conversation A's topic")
	}
	stubs := b.Stubs.Snapshot()
	if len(stubs) != 1 || stubs[0].CurrentRoute() != nil {
		t.Fatalf("identity switch with no target attachment retained a live route: stubs=%+v", stubs)
	}
	requireSwitchAttachment(t, b, switchSessionA, 281, false)
}

// T3 pins the record-only trap: B's correct saved attachment must never be
// overwritten with the old route merely because the stub still held A.
func TestRecoverSession_InAppSwitch_DoesNotRecordOldRouteUnderNewID(t *testing.T) {
	b := brokerWithChannel(t, identitySwitchMappings(true), &fakeChannel{})
	defer b.Shutdown()
	peer, done := startIdentitySwitchPeer(t, b)
	defer done()

	if resp := recoverIdentityOnPeer(t, peer, switchSessionA); !resp.Recovered {
		t.Fatalf("precondition: conversation A did not recover topicA: %+v", resp)
	}
	_ = recoverIdentityOnPeer(t, peer, switchSessionB)

	sa, ok := b.Mappings().LookupSessionAttachment(switchSessionB)
	if !ok || sa.TopicID == nil || *sa.TopicID != 412 {
		t.Fatalf("identity switch recorded the old route topicA under new identity B: got %+v ok=%v; switch must release before recovering B", sa, ok)
	}
}

// T4: the gate controls claiming only. Once A is held, disabling the gate
// before the switch must still release A but must not claim B.
func TestRecoverSession_InAppSwitch_GateDisabled_ReleasesButDoesNotClaim(t *testing.T) {
	b := brokerWithChannel(t, identitySwitchMappings(true), &fakeChannel{})
	defer b.Shutdown()
	peer, done := startIdentitySwitchPeer(t, b)
	defer done()

	if resp := recoverIdentityOnPeer(t, peer, switchSessionA); !resp.Recovered {
		t.Fatalf("precondition: conversation A did not recover topicA: %+v", resp)
	}
	disabled := b.Mappings().Clone()
	disabled.AutoAttachOnResume = boolPtr(false)
	b.SetMappings(disabled)

	if resp := recoverIdentityOnPeer(t, peer, switchSessionB); resp.Recovered {
		t.Fatalf("gate-disabled identity switch claimed conversation B's topic: %+v", resp)
	}
	if _, held := b.Routes.Holder(switchRoute(281, -100)); held {
		t.Fatal("gate-disabled identity switch failed to release conversation A's topic")
	}
	if _, held := b.Routes.Holder(switchRoute(412, -200)); held {
		t.Fatal("gate-disabled identity switch claimed conversation B's topic; the gate must govern claiming")
	}
}

// T5: switching away from A releases A, but B's topic remains with a different
// live session and is never stolen.
func TestRecoverSession_InAppSwitch_TargetHeldByLiveSession_RefusesSteal(t *testing.T) {
	b := brokerWithChannel(t, identitySwitchMappings(true), &fakeChannel{})
	defer b.Shutdown()
	target := switchRoute(412, -200)
	other := b.Stubs.Register("claude", 9999, "/other", struct{}{})
	if _, claimed := b.Routes.Claim(target, other); !claimed {
		t.Fatal("precondition: other live session did not claim conversation B's topic")
	}

	peer, done := startIdentitySwitchPeer(t, b)
	defer done()
	if resp := recoverIdentityOnPeer(t, peer, switchSessionA); !resp.Recovered {
		t.Fatalf("precondition: conversation A did not recover topicA: %+v", resp)
	}
	if resp := recoverIdentityOnPeer(t, peer, switchSessionB); resp.Recovered {
		t.Fatalf("identity switch stole a target held by another live session: %+v", resp)
	}
	if _, held := b.Routes.Holder(switchRoute(281, -100)); held {
		t.Fatal("identity switch refused B but failed to release conversation A's old topic")
	}
	if holder, held := b.Routes.Holder(target); !held || holder.ConnID != other.ConnID {
		t.Fatalf("identity switch displaced B's live holder: held=%v holder=%+v", held, holder)
	}
}

// T6: a detach barrier belongs to A. Switching identity clears it for B, while
// a later same-B refire after detaching B remains refused.
func TestRecoverSession_InAppSwitch_ClearsDetachBarrierOfOldConversation(t *testing.T) {
	b := brokerWithChannel(t, identitySwitchMappings(true), &fakeChannel{})
	defer b.Shutdown()
	peer, done := startIdentitySwitchPeer(t, b)
	defer done()

	if resp := recoverIdentityOnPeer(t, peer, switchSessionA); !resp.Recovered {
		t.Fatalf("precondition: conversation A did not recover topicA: %+v", resp)
	}
	if err := peer.WriteJSON(ipc.ReleaseReq{Op: ipc.OpRelease}); err != nil {
		t.Fatalf("detach conversation A: %v", err)
	}
	if resp := recoverIdentityOnPeer(t, peer, switchSessionB); !resp.Recovered || resp.Name != "feature-x" {
		t.Fatalf("identity switch carried conversation A's detach barrier into B and refused B's recovery: %+v", resp)
	}

	if err := peer.WriteJSON(ipc.ReleaseReq{Op: ipc.OpRelease}); err != nil {
		t.Fatalf("detach conversation B: %v", err)
	}
	if resp := recoverIdentityOnPeer(t, peer, switchSessionB); resp.Recovered {
		t.Fatalf("same-id refire cleared B's own detach barrier and reattached it: %+v", resp)
	}
	if _, held := b.Routes.Holder(switchRoute(412, -200)); held {
		t.Fatal("same-id refire after detach reclaimed B's topic; detach must survive idempotent refires")
	}
}

// T7: an idempotent recover registration for the same identity keeps the
// existing record-only behavior and does not disturb the live claim.
func TestRecoverSession_SameIDRefire_RecordOnlyUnchanged(t *testing.T) {
	b := brokerWithChannel(t, identitySwitchMappings(false), &fakeChannel{})
	defer b.Shutdown()
	peer, done := startIdentitySwitchPeer(t, b)
	defer done()

	if resp := recoverIdentityOnPeer(t, peer, switchSessionA); !resp.Recovered {
		t.Fatalf("precondition: conversation A did not recover topicA: %+v", resp)
	}
	if resp := recoverIdentityOnPeer(t, peer, switchSessionA); resp.Recovered {
		t.Fatalf("same-id refire stopped being record-only and reported a new recovery: %+v", resp)
	}
	holder, held := b.Routes.Holder(switchRoute(281, -100))
	if !held || holder.StableSessionIDValue() != switchSessionA {
		t.Fatalf("same-id record-only refire disturbed the existing claim: held=%v holder=%+v", held, holder)
	}
	requireSwitchAttachment(t, b, switchSessionA, 281, false)
}

// T14: the first connection attached A before any stable id registered, then
// dropped while its PID stayed alive. Hello transfers that anonymous claim to
// the reconnect stub. Recovering B must treat the carried identity as a
// mismatch, release A without tombstoning it, and recover B instead of taking
// record-only and overwriting B's durable attachment.
func TestRecoverSession_ReconnectTransferWithoutIdentity_DoesNotRecordOldRouteUnderNewID(t *testing.T) {
	b := brokerWithChannel(t, identitySwitchMappings(true), &fakeChannel{})
	defer b.Shutdown()
	pid := os.Getpid()
	cwd := "/projects/claim-transfer"

	first, closeFirst := peerPair(t, b)
	if err := first.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: pid, CWD: cwd}); err != nil {
		t.Fatalf("first hello: %v", err)
	}
	if _, err := first.ReadFrame(); err != nil {
		t.Fatalf("first hello ack: %v", err)
	}
	topicA := int64(281)
	if err := first.WriteJSON(ipc.AttachReq{
		Op: ipc.OpAttach, TopicID: &topicA, ChatID: -100, Group: "main", CWD: cwd,
	}); err != nil {
		t.Fatalf("attach conversation A before identity: %v", err)
	}
	raw, err := first.ReadFrame()
	if err != nil {
		t.Fatalf("read attach A response: %v", err)
	}
	var attached ipc.AttachedMsg
	if err := json.Unmarshal(raw, &attached); err != nil || !attached.OK {
		t.Fatalf("precondition: anonymous A attach failed: attached=%+v err=%v", attached, err)
	}
	closeFirst()

	keyA := switchRoute(281, -100)
	deadline := time.Now().Add(2 * time.Second)
	for {
		holder, held := b.Routes.Holder(keyA)
		if held && !holder.IsConnected() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("precondition: A's claim did not enter reconnectable disconnected state: held=%v holder=%+v", held, holder)
		}
		time.Sleep(5 * time.Millisecond)
	}

	second, closeSecond := peerPair(t, b)
	defer closeSecond()
	if err := second.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: pid, CWD: cwd}); err != nil {
		t.Fatalf("reconnect hello: %v", err)
	}
	if _, err := second.ReadFrame(); err != nil {
		t.Fatalf("reconnect hello ack: %v", err)
	}
	if holder, held := b.Routes.Holder(keyA); !held || holder.StableSessionIDValue() != "" {
		t.Fatalf("precondition: reconnect did not transfer A's anonymous route: held=%v holder=%+v", held, holder)
	}

	resp := recoverIdentityOnPeer(t, second, switchSessionB)
	saB, ok := b.Mappings().LookupSessionAttachment(switchSessionB)
	if !ok || saB.TopicID == nil || *saB.TopicID != 412 {
		t.Fatalf("claim-transfer corruption: recover(B) recorded conversation A's topic under B: got %+v ok=%v", saB, ok)
	}
	if !resp.Recovered || resp.TopicID == nil || *resp.TopicID != 412 {
		t.Fatalf("claim-transfer switch did not release anonymous A and recover conversation B's topic: %+v", resp)
	}
	requireSwitchAttachment(t, b, switchSessionA, 281, false)
	if _, held := b.Routes.Holder(keyA); held {
		t.Fatal("claim-transfer switch left conversation A's anonymous route claimed under B")
	}
	if holder, held := b.Routes.Holder(switchRoute(412, -200)); !held || holder.StableSessionIDValue() != switchSessionB {
		t.Fatalf("claim-transfer switch did not leave B holding its own route: held=%v holder=%+v", held, holder)
	}
}
