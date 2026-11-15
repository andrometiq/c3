package main

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/sessionhandoff"
)

var (
	boardTestTopicID = int64(281)
	boardTestTopic   = ipc.RouteRef{Channel: "telegram", ChatID: -100, TopicID: &boardTestTopicID, Name: "c3"}
	boardTestBoard   = ipc.RouteRef{Channel: channel.BoardChannelName, ChatID: 1, Name: "board:plan"}
)

// boardAttachTurn runs one attach tool call with args and answers it with resp.
func boardAttachTurn(t *testing.T, a *adapter, args map[string]any, resp ipc.AttachedMsg) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_, _ = a.toolAttach(ctx, newAttachReq(t, args))
		close(done)
	}()
	drivePendingAttached(t, a, resp)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("toolAttach did not return")
	}
}

func TestBoardAttachIsNeverRememberedForReplay(t *testing.T) {
	isolateAdapterTest(t)
	title := titleScope(t)
	a := newAdapterWithDummyConn(t)
	boardAttachTurn(t, a, map[string]any{"expr": "c3"}, ipc.AttachedMsg{
		Op: ipc.OpAttached, OK: true, Channel: "telegram", ChatID: -100, TopicID: &boardTestTopicID, Name: "c3",
		Routes: []ipc.RouteRef{boardTestTopic}, Output: &boardTestTopic,
	})
	titled := title.String()
	if titled == "" {
		t.Fatal("a topic attach set no title")
	}
	// An explicit board attach, then a bare attach the broker answered from a board output.
	for _, args := range []map[string]any{{"expr": "+board:plan"}, {}} {
		boardAttachTurn(t, a, args, ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: true, Channel: channel.BoardChannelName, ChatID: 1, Name: "board:plan",
			Routes: []ipc.RouteRef{boardTestTopic, boardTestBoard}, Output: &boardTestBoard,
		})
	}
	a.amu.Lock()
	remembered := a.lastAttach
	a.amu.Unlock()
	if remembered == nil || remembered.Expr != "c3" {
		t.Fatalf("remembered=%+v, want the topic attach", remembered)
	}
	if title.String() != titled {
		t.Fatalf("a board attach retitled the terminal: %q", title.String())
	}
}

func TestRecoveredBoardOutputIsNeverReplayedAsDM(t *testing.T) {
	isolateAdapterTest(t)
	a := newAdapter()
	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() { _ = adapterSide.Close(); _ = brokerSide.Close() })
	a.bmu.Lock()
	a.conn = ipc.NewConn(adapterSide)
	a.bmu.Unlock()
	peer := ipc.NewConn(brokerSide)

	done := make(chan recoverOutcome, 1)
	go func() {
		done <- a.fireRecover(context.Background(), sessionhandoff.Entry{StableSessionID: "stable-board", CWD: "/projects/c3"})
	}()
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatalf("read recover request: %v", err)
	}
	resp, _ := json.Marshal(ipc.RecoverSessionResp{
		Op: ipc.OpRecoverSessionResult, Recovered: true, Channel: channel.BoardChannelName, ChatID: 1, Name: "board:plan",
		Routes: []ipc.RouteRef{boardTestTopic, boardTestBoard}, Output: &boardTestBoard,
	})
	a.dispatchRecoverSessionResult(resp)
	if outcome := <-done; !outcome.recovered {
		t.Fatalf("recover outcome=%+v, want recovered", outcome)
	}
	a.amu.Lock()
	remembered, routes := a.lastAttach, len(a.routes)
	a.amu.Unlock()
	if remembered != nil {
		t.Fatalf("a recovered board output was remembered as %+v (a fresh broker would get a DM attach)", remembered)
	}
	if routes != 2 {
		t.Fatalf("routes after recovery=%d, want the topic and the board", routes)
	}
}
