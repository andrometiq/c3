package broker

import (
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

// TestHandleListClaims_OmitsAndReapsDeadHolder pins the dead-holder reap the
// diff added to handleListClaims (handler.go): a DEAD holder (disconnected +
// PID<=0 => IsAlive()==false) must be OMITTED from the returned ClaimsListMsg
// AND reaped from the routes map, while a LIVE holder (connected + PID alive)
// is retained and present in the list. So `c3-broker status`'s live-claims
// section never renders a ghost claim for a CLI that has exited.
//
// This is a DIFFERENT surface from status_command_test's
// TestStatusForTopic_ReapsDeadHolderAtReadTime, which covers statusForTopic (a
// per-topic lookup with no list). Here we drive handleListClaims itself so the
// omit-from-list `continue` is exercised: we run it against a net.Pipe conn and
// read back the frame it writes (WriteJSON blocks until the peer reads, so the
// handler runs in a goroutine).
func TestHandleListClaims_OmitsAndReapsDeadHolder(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	// Two distinct routes (topics from mfWithTelegram).
	liveTID := int64(281) // -100/281 = "c3"
	deadTID := int64(412) // -200/412 = "feature-x"
	liveKey := MakeRouteKey("telegram", -100, &liveTID)
	deadKey := MakeRouteKey("telegram", -200, &deadTID)

	// LIVE: connected (Conn set) + this-process PID => IsAlive()==true.
	live := &Stub{CLI: "claude", PID: os.Getpid(), ConnID: 1, Conn: struct{}{}}
	// DEAD: disconnected (nil Conn) + PID<=0 => IsAlive()==false.
	dead := &Stub{CLI: "claude", PID: 0, ConnID: 2}
	if _, ok := b.Routes.Claim(liveKey, live); !ok {
		t.Fatal("live claim should succeed")
	}
	if _, ok := b.Routes.Claim(deadKey, dead); !ok {
		t.Fatal("dead claim should succeed")
	}

	agentSide, brokerSide := net.Pipe()
	defer agentSide.Close()
	defer brokerSide.Close()
	go b.handleListClaims(ipc.NewConn(brokerSide))

	peer := ipc.NewConn(agentSide)
	type readResult struct {
		raw []byte
		err error
	}
	got := make(chan readResult, 1)
	go func() {
		raw, err := peer.ReadFrame()
		got <- readResult{raw, err}
	}()

	var raw []byte
	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("ReadFrame: %v", r.err)
		}
		raw = r.raw
	case <-time.After(2 * time.Second):
		t.Fatal("handleListClaims did not write a ClaimsListMsg within 2s")
	}

	var resp ipc.ClaimsListMsg
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("unmarshal ClaimsListMsg: %v", err)
	}
	if resp.Op != ipc.OpClaimsList {
		t.Errorf("op=%q, want %q", resp.Op, ipc.OpClaimsList)
	}
	// The dead holder is OMITTED; only the live one is listed.
	if len(resp.Claims) != 1 {
		t.Fatalf("claims=%d, want 1 (dead holder omitted): %+v", len(resp.Claims), resp.Claims)
	}
	if resp.Claims[0].ConnID != 1 || resp.Claims[0].HolderPID != os.Getpid() {
		t.Errorf("listed claim = %+v, want the LIVE holder (conn=1, pid=%d)",
			resp.Claims[0], os.Getpid())
	}

	// Observable Routes side-effects: the dead holder was Released; the live
	// one retained.
	if _, held := b.Routes.Holder(deadKey); held {
		t.Error("dead holder must be reaped (Released) from the routes map by handleListClaims")
	}
	if _, held := b.Routes.Holder(liveKey); !held {
		t.Error("live holder must be retained in the routes map")
	}
}
