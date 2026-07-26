package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/queue"
)

// recoverViaPeer runs hello then a RecoverSessionReq over a peer pair and
// returns the decoded response. The peer conn is left open for further reads.
func recoverViaPeer(t *testing.T, b *Broker, cwd, stableID string) (*ipc.Conn, func(), ipc.RecoverSessionResp) {
	t.Helper()
	peer, done := peerPair(t, b)
	if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: 1, CWD: cwd}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil { // hello ack
		t.Fatal(err)
	}
	if err := peer.WriteJSON(ipc.RecoverSessionReq{Op: ipc.OpRecoverSession, StableSessionID: stableID, CWD: cwd}); err != nil {
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
	return peer, done, resp
}

// boolPtr returns a *bool for the auto_attach_on_resume gate (nil ⇒ default ON).
func boolPtr(b bool) *bool { return &b }

func TestHandleRecoverSession_NoRouteRecovers(t *testing.T) {
	mf := mfWithTelegram()
	mf.AutoAttachOnResume = boolPtr(true)                        // gate ON: recovery fires as it does in production when opted in
	seedSessionAttachment(mf, "sess-1", time.Now().UTC(), false) // → c3 / 281
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	_, done, resp := recoverViaPeer(t, b, "/anywhere", "sess-1")
	defer done()

	if !resp.Recovered {
		t.Fatalf("expected Recovered=true, got %+v", resp)
	}
	if resp.Name != "c3" || resp.Channel != "telegram" || resp.TopicID == nil || *resp.TopicID != 281 {
		t.Fatalf("recover response fields wrong: %+v", resp)
	}
	// The route must be claimed by the recovering stub.
	tid := int64(281)
	key := MakeRouteKey("telegram", -100, &tid)
	if _, ok := b.Routes.Holder(key); !ok {
		t.Fatal("route should be claimed after a successful recover")
	}
}

// TestHandleRecoverSession_CarriesBacklogPreview covers BUG #2: a recovered
// resume must carry a compact backlog PREVIEW (QueuedSummary), not just a count,
// so the adapter can surface the actual held messages into the resumed session.
func TestHandleRecoverSession_CarriesBacklogPreview(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	mf.AutoAttachOnResume = boolPtr(true)                        // gate ON: recovery fires
	seedSessionAttachment(mf, "sess-1", time.Now().UTC(), false) // → c3 / 281
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	tid := int64(281)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	for i := int64(1); i <= 4; i++ {
		if err := b.Queue.Append(qrk, &c3types.Inbound{
			Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: i,
			Sender: c3types.Sender{Username: "k"}, Text: "held msg", Timestamp: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	_, done, resp := recoverViaPeer(t, b, "/anywhere", "sess-1")
	defer done()

	if !resp.Recovered || resp.QueuedCount != 4 {
		t.Fatalf("expected recovered with 4 queued, got %+v", resp)
	}
	if len(resp.QueuedSummary) == 0 {
		t.Fatal("BUG #2: recover response must carry a backlog preview, not just a count")
	}
	if resp.QueuedSummary[0].MessageID != 1 || resp.QueuedSummary[0].Preview == "" {
		t.Fatalf("first preview item malformed: %+v", resp.QueuedSummary[0])
	}
}

// TestHandleRecoverSession_GateDisabledNoAutoAttach covers the opt-out: with
// auto_attach_on_resume explicitly false, a recoverable attachment is NOT auto-
// re-claimed on resume. The stable id is still recorded (so a later SIGHUP-
// enable works), but the route stays free. (Post-redesign the default is ON, so
// disabling now requires an explicit false — an absent field means enabled.)
func TestHandleRecoverSession_GateDisabledNoAutoAttach(t *testing.T) {
	mf := mfWithTelegram()
	mf.AutoAttachOnResume = boolPtr(false)                       // gate explicitly OFF (default is now ON)
	seedSessionAttachment(mf, "sess-1", time.Now().UTC(), false) // → c3 / 281
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	_, done, resp := recoverViaPeer(t, b, "/anywhere", "sess-1")
	defer done()

	if resp.Recovered {
		t.Fatalf("gate OFF: resume must NOT auto-attach, got %+v", resp)
	}
	tid := int64(281)
	if _, ok := b.Routes.Holder(MakeRouteKey("telegram", -100, &tid)); ok {
		t.Fatal("gate OFF: no route must be claimed on resume")
	}
	// TestHandleRecoverSession_GateReloadFlips proves the stable id is still
	// learned under the OFF gate (a later enable recovers the same session).
}

// TestHandleRecoverSession_GateReloadFlips proves the gate is read LIVE from the
// current mappings snapshot: a SIGHUP-style SetMappings that turns the gate on
// makes the very next resume auto-attach, with no broker restart.
func TestHandleRecoverSession_GateReloadFlips(t *testing.T) {
	mfOff := mfWithTelegram()
	mfOff.AutoAttachOnResume = boolPtr(false)                       // gate explicitly OFF (default is now ON)
	seedSessionAttachment(mfOff, "sess-1", time.Now().UTC(), false) // → c3 / 281
	b := brokerWithChannel(t, mfOff, &fakeChannel{})
	defer b.Shutdown()

	// First resume under the OFF config: no auto-attach.
	_, done1, resp1 := recoverViaPeer(t, b, "/a", "sess-1")
	if resp1.Recovered {
		t.Fatalf("pre-reload (gate OFF): must not recover, got %+v", resp1)
	}
	done1() // drop the first session's conn before the second attaches

	// SIGHUP reload analogue: swap in a config with the gate ON (same route data).
	mfOn := mfWithTelegram()
	mfOn.AutoAttachOnResume = boolPtr(true)
	seedSessionAttachment(mfOn, "sess-1", time.Now().UTC(), false)
	b.SetMappings(mfOn)

	// Second resume of the same session now recovers.
	_, done2, resp2 := recoverViaPeer(t, b, "/b", "sess-1")
	defer done2()
	if !resp2.Recovered || resp2.Name != "c3" {
		t.Fatalf("post-reload (gate ON): must recover to c3, got %+v", resp2)
	}
	tid := int64(281)
	if _, ok := b.Routes.Holder(MakeRouteKey("telegram", -100, &tid)); !ok {
		t.Fatal("post-reload: the route must be claimed after recovery")
	}
}

func TestHandleRecoverSession_TombstonedNoRecovery(t *testing.T) {
	mf := mfWithTelegram()
	seedSessionAttachment(mf, "sess-1", time.Now().UTC(), true) // tombstoned
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	_, done, resp := recoverViaPeer(t, b, "/x", "sess-1")
	defer done()
	if resp.Recovered {
		t.Fatal("tombstoned attachment must not recover")
	}
	tid := int64(281)
	if _, ok := b.Routes.Holder(MakeRouteKey("telegram", -100, &tid)); ok {
		t.Fatal("no route should be claimed for a tombstoned attachment")
	}
}

func TestHandleRecoverSession_ExpiredNoRecovery(t *testing.T) {
	mf := mfWithTelegram()
	seedSessionAttachment(mf, "sess-1", time.Now().UTC().Add(-40*24*time.Hour), false)
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	_, done, resp := recoverViaPeer(t, b, "/x", "sess-1")
	defer done()
	if resp.Recovered {
		t.Fatal("expired attachment must not recover")
	}
}

func TestHandleRecoverSession_MissingNoRecovery(t *testing.T) {
	mf := mfWithTelegram() // no attachment seeded
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	_, done, resp := recoverViaPeer(t, b, "/x", "unknown-sess")
	defer done()
	if resp.Recovered {
		t.Fatal("missing attachment must not recover")
	}
}

func TestHandleRecoverSession_HeldByAnotherLiveSession(t *testing.T) {
	mf := mfWithTelegram()
	seedSessionAttachment(mf, "sess-1", time.Now().UTC(), false) // → c3 / 281
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	// Another LIVE session already holds c3 (topic 281). conn non-nil → alive.
	tid := int64(281)
	key := MakeRouteKey("telegram", -100, &tid)
	holder := b.Stubs.Register("claude", 9999, "/other", struct{}{})
	if _, ok := b.Routes.Claim(key, holder); !ok {
		t.Fatal("precondition: holder should claim the route")
	}

	_, done, resp := recoverViaPeer(t, b, "/anywhere", "sess-1")
	defer done()

	if resp.Recovered {
		t.Fatal("recovery must be skipped when the topic is held by another live session")
	}
	if h, ok := b.Routes.Holder(key); !ok || h.ConnID != holder.ConnID {
		t.Fatal("the original live holder must keep the claim")
	}
}

func TestHandleRecoverSession_DualPathRecordsCurrentRoute(t *testing.T) {
	// Attach-first: the stub already holds a route (claimed by cwd) BEFORE the
	// recover op arrives. The recover op must NOT re-claim, but must RECORD the
	// current route under the stable id (dual-path recording). Uses the DEFAULT
	// config, proving the record-only branch runs regardless of the gate — the
	// cur!=nil path is gate-independent, only the auto-re-claim is gated.
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
	// Attach by topic id 412 (feature-x) first.
	if err := peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, TopicID: func() *int64 { t := int64(412); return &t }(), Group: "work"}); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var ack ipc.AttachedMsg
	if err := json.Unmarshal(raw, &ack); err != nil || !ack.OK {
		t.Fatalf("attach failed: ack=%+v err=%v", ack, err)
	}

	// Now the recover op arrives — already attached.
	if err := peer.WriteJSON(ipc.RecoverSessionReq{Op: ipc.OpRecoverSession, StableSessionID: "late-sess", CWD: "/x"}); err != nil {
		t.Fatal(err)
	}
	raw, err = peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.RecoverSessionResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Recovered {
		t.Fatal("attach-first must NOT report Recovered (no re-claim)")
	}
	// The current route (feature-x / 412) must be recorded under the stable id.
	sa, ok := b.Mappings().LookupSessionAttachment("late-sess")
	if !ok {
		t.Fatal("dual-path: the current route must be recorded under the stable id")
	}
	if sa.Name != "feature-x" || sa.TopicID == nil || *sa.TopicID != 412 || sa.Group != "work" {
		t.Fatalf("recorded attachment = %+v (want feature-x / 412 / work)", sa)
	}
}

func TestHandleRecoverSession_BadRequest(t *testing.T) {
	mf := mfWithTelegram()
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: 1, CWD: "/x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	// Empty stable id → fail-closed error response, no recovery.
	if err := peer.WriteJSON(ipc.RecoverSessionReq{Op: ipc.OpRecoverSession, StableSessionID: ""}); err != nil {
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
	if resp.Recovered || resp.Err == "" {
		t.Fatalf("empty stable id must produce an error response, got %+v", resp)
	}
}

func TestRecoverSession_RefreshesStaleLastAttachedAt(t *testing.T) {
	mf := mfWithTelegram()
	t0 := time.Now().UTC().Add(-2 * time.Hour) // stale: > sessionRefreshInterval
	mf.UpsertSessionAttachment("sess-1", mappings.SessionAttachment{
		Channel: "telegram", ChatID: -100, TopicID: func() *int64 { t := int64(281); return &t }(),
		Name: "c3", LastAttachedAt: t0,
	})
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	stub := b.Stubs.Register("claude", 1, "/x", nil)
	stub.SetStableSessionID("sess-1")
	if _, _, _, ok := b.recoverSession(stub); !ok {
		t.Fatal("expected recoverSession to succeed")
	}
	sa, _ := b.Mappings().LookupSessionAttachment("sess-1")
	if !sa.LastAttachedAt.After(t0) {
		t.Fatalf("stale attachment must be refreshed; %v not after %v", sa.LastAttachedAt, t0)
	}
}

func TestRecoverSession_SkipsRefreshWhenFresh(t *testing.T) {
	mf := mfWithTelegram()
	t0 := time.Now().UTC().Add(-time.Minute) // fresh: < sessionRefreshInterval
	mf.UpsertSessionAttachment("sess-1", mappings.SessionAttachment{
		Channel: "telegram", ChatID: -100, TopicID: func() *int64 { t := int64(281); return &t }(),
		Name: "c3", LastAttachedAt: t0,
	})
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	stub := b.Stubs.Register("claude", 1, "/x", nil)
	stub.SetStableSessionID("sess-1")
	if _, _, _, ok := b.recoverSession(stub); !ok {
		t.Fatal("fresh attachment should still recover")
	}
	sa, _ := b.Mappings().LookupSessionAttachment("sess-1")
	if !sa.LastAttachedAt.Equal(t0) {
		t.Fatalf("fresh attachment must NOT be rewritten (churn); %v != %v", sa.LastAttachedAt, t0)
	}
}

func TestRecoverSession_DMRoute(t *testing.T) {
	mf := mfWithTelegram()
	mf.AutoAttachOnResume = boolPtr(true) // gate ON: recovery fires
	mf.UpsertSessionAttachment("dm-sess", mappings.SessionAttachment{
		Channel: "telegram", ChatID: 42, TopicID: nil, Name: "dm",
		LastAttachedAt: time.Now().UTC(),
	})
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	_, done, resp := recoverViaPeer(t, b, "/anywhere", "dm-sess")
	defer done()
	if !resp.Recovered || resp.Name != "dm" || resp.TopicID != nil {
		t.Fatalf("expected DM recovery (name=dm, no topic), got %+v", resp)
	}
}

func TestRecoverSession_EmptyStableIDNoOp(t *testing.T) {
	mf := mfWithTelegram()
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()
	stub := b.Stubs.Register("claude", 1, "/x", nil) // no stable id
	if _, _, _, ok := b.recoverSession(stub); ok {
		t.Fatal("recoverSession must be a no-op without a stable id")
	}
}

// TestRecoverSession_SinglePeekCountPreviewConsistent pins §3c: recoverSession
// returns the count AND the preview from ONE backlogSummary peek, so they always
// describe the same queue snapshot. With a queue at the preview cap the count
// and the preview length are exactly equal — a regression that peeked twice
// (the old stale-double-peek TOCTOU) could return a count and a preview drawn
// from two different snapshots.
func TestRecoverSession_SinglePeekCountPreviewConsistent(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	seedSessionAttachment(mf, "sess-1", time.Now().UTC(), false) // → c3 / 281
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	tid := int64(281)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	for i := int64(1); i <= backlogSummaryMax; i++ {
		if err := b.Queue.Append(qrk, &c3types.Inbound{
			Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: i,
			Sender: c3types.Sender{Username: "k"}, Text: "held msg", Timestamp: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	stub := b.Stubs.Register("claude", 1, "/x", nil)
	stub.SetStableSessionID("sess-1")

	key, cnt, preview, ok := b.recoverSession(stub)
	if !ok {
		t.Fatal("expected recoverSession to succeed")
	}
	if cnt != backlogSummaryMax {
		t.Fatalf("count = %d, want %d", cnt, backlogSummaryMax)
	}
	if len(preview) != cnt {
		t.Fatalf("preview length %d disagrees with count %d — a single peek must keep them consistent", len(preview), cnt)
	}
	if preview[0].MessageID != 1 || preview[0].Preview == "" {
		t.Fatalf("first preview item malformed: %+v", preview[0])
	}
	if !key.HasTopic || key.TopicID != 281 {
		t.Fatalf("recovered key = %+v, want own topic 281", key)
	}
}
