package broker

import (
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

// TestHandleListTopics_ReapsDeadHolderAtReadTime pins the read-time liveness
// check in handleListTopics (handler.go).
//
// WHY IT EXISTS: `c3-broker status` has always reaped a dead holder when it
// listed claims (handleListClaims, pinned by
// TestHandleListClaims_OmitsAndReapsDeadHolder), but `topics` did not — it
// reported ClaimedBy for whatever sat in the routes map, alive or not. So the
// two views of the same fact could give opposite answers: `status` said a topic
// was free while `topics` said a dead CLI still held it. The maintainer hit
// exactly that and asked for the status-style check to run when topics are
// listed, so the list is always accurate about what is attached.
//
// The asymmetry that makes this the right default: reporting a live claim as
// dead would let someone steal a topic out from under a working session;
// reporting a dead claim as live only makes a free topic look busy. This test
// therefore pins BOTH directions — the dead holder is reaped and reported free,
// and the live holder is untouched and still reported as the holder.
func TestHandleListTopics_ReapsDeadHolderAtReadTime(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	liveTID := int64(281) // -100/281 = "c3"
	deadTID := int64(412) // -200/412 = "feature-x"
	liveKey := MakeRouteKey("telegram", -100, &liveTID)
	deadKey := MakeRouteKey("telegram", -200, &deadTID)

	// LIVE: connected (Conn set) + this process's PID => IsAlive()==true.
	live := &Stub{CLI: "claude", PID: os.Getpid(), CWD: "/live", ConnID: 1, Conn: struct{}{}}
	// DEAD: disconnected (nil Conn) + PID<=0 => IsAlive()==false.
	dead := &Stub{CLI: "codex", PID: 0, CWD: "/dead", ConnID: 2}
	if _, ok := b.Routes.Claim(liveKey, live); !ok {
		t.Fatal("live claim should succeed")
	}
	if _, ok := b.Routes.Claim(deadKey, dead); !ok {
		t.Fatal("dead claim should succeed")
	}

	agentSide, brokerSide := net.Pipe()
	defer agentSide.Close()
	defer brokerSide.Close()
	// WriteJSON blocks until the peer reads, so the handler runs in a goroutine.
	go b.handleListTopics(ipc.NewConn(brokerSide))

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
		t.Fatal("handleListTopics did not write a TopicsListMsg within 2s")
	}

	var resp ipc.TopicsListMsg
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("unmarshal TopicsListMsg: %v", err)
	}
	if resp.Op != ipc.OpTopicsList {
		t.Errorf("op=%q, want %q", resp.Op, ipc.OpTopicsList)
	}

	var sawLive, sawDead bool
	for _, e := range resp.Topics {
		switch {
		case e.ChatID == -100 && e.TopicID == liveTID:
			sawLive = true
			if e.ClaimedBy == nil {
				t.Error("live holder dropped from topics: a topic held by a RUNNING session " +
					"must keep reporting its holder, or the next caller steals it out from under a working session")
			} else if e.ClaimedBy.PID != os.Getpid() || e.ClaimedBy.CLI != "claude" {
				t.Errorf("live topic claimed_by = %+v, want the live holder (cli=claude pid=%d)",
					e.ClaimedBy, os.Getpid())
			}
		case e.ChatID == -200 && e.TopicID == deadTID:
			sawDead = true
			if e.ClaimedBy != nil {
				t.Errorf("topics reported a GHOST claim %+v for a holder whose CLI has exited; "+
					"`c3-broker status` reaps this at read time and `topics` must agree", e.ClaimedBy)
			}
		}
	}
	if !sawLive || !sawDead {
		t.Fatalf("expected both configured topics in the list, got %+v", resp.Topics)
	}

	// Observable Routes side-effect: the dead claim is gone, the live one stays.
	if _, held := b.Routes.Holder(deadKey); held {
		t.Error("dead holder must be reaped (Released) from the routes map by handleListTopics, " +
			"not merely hidden from the response — otherwise the stale claim still blocks attach")
	}
	if _, held := b.Routes.Holder(liveKey); !held {
		t.Error("live holder must be retained in the routes map")
	}
}
