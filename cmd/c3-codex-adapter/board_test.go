package main

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
)

var (
	boardTestTopicID = int64(281)
	boardTestTopic   = ipc.RouteRef{Channel: "telegram", ChatID: -100, TopicID: &boardTestTopicID, Name: "c3", Group: "main"}
	boardTestBoard   = ipc.RouteRef{Channel: channel.BoardChannelName, ChatID: 1, Name: "board:plan"}
)

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

// collectedConn wires a to a pipe whose frames the returned func drains.
func collectedConn(t *testing.T, a *adapter) func() []ipc.AttachReq {
	t.Helper()
	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() { _ = adapterSide.Close(); _ = brokerSide.Close() })
	a.bmu.Lock()
	a.conn = ipc.NewConn(adapterSide)
	a.bmu.Unlock()
	frames := make(chan ipc.AttachReq, 8)
	go func() {
		peer := ipc.NewConn(brokerSide)
		for {
			raw, err := peer.ReadFrame()
			if err != nil {
				close(frames)
				return
			}
			var req ipc.AttachReq
			_ = json.Unmarshal(raw, &req)
			frames <- req
		}
	}()
	return func() []ipc.AttachReq {
		var got []ipc.AttachReq
		for {
			select {
			case req := <-frames:
				got = append(got, req)
			case <-time.After(100 * time.Millisecond):
				return got
			}
		}
	}
}

func TestBoardAttachIsNeverRememberedForReplay(t *testing.T) {
	title := titleScope(t)
	a := newAdapterWithDummyConn(t)
	boardAttachTurn(t, a, map[string]any{"expr": "c3"}, ipc.AttachedMsg{
		Op: ipc.OpAttached, OK: true, Channel: "telegram", ChatID: -100, TopicID: &boardTestTopicID, Name: "c3",
		Routes: []ipc.RouteRef{boardTestTopic}, Output: &boardTestTopic,
	})
	titled := title.String()
	for _, args := range []map[string]any{{"expr": "+board:plan"}, {}} {
		boardAttachTurn(t, a, args, ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: true, Channel: channel.BoardChannelName, ChatID: 1, Name: "board:plan",
			Routes: []ipc.RouteRef{boardTestTopic, boardTestBoard}, Output: &boardTestBoard,
		})
	}
	a.amu.Lock()
	remembered := a.lastAttach
	a.amu.Unlock()
	if remembered == nil || remembered.Expr != "c3" || titled == "" || title.String() != titled {
		t.Fatalf("remembered=%+v title=%q (was %q), want the topic attach and its title", remembered, title.String(), titled)
	}

	// The replay re-adds every held route but boards, whichever is output.
	frames := collectedConn(t, a)
	for _, output := range []ipc.RouteRef{boardTestTopic, boardTestBoard} {
		a.setRouteState([]ipc.RouteRef{boardTestTopic, boardTestBoard}, &output)
		a.replayLastAttach()
		replays := frames()
		if len(replays) != 1 || replays[0].TopicID == nil || *replays[0].TopicID != boardTestTopicID || !replays[0].Replay {
			t.Fatalf("output %s: replay frames=%+v, want only the topic", output.Name, replays)
		}
	}

	// A release that leaves a board as output remembers nothing and keeps the title.
	ch := make(chan ipc.ToolResultMsg, 1)
	a.pmu.Lock()
	a.pending["release_result"] = ch
	a.pmu.Unlock()
	raw, _ := json.Marshal(ipc.ReleaseResp{Op: ipc.OpReleaseResult, OK: true, Routes: []ipc.RouteRef{boardTestBoard}, Output: &boardTestBoard})
	a.dispatchReleaseResult(raw)
	<-ch
	a.amu.Lock()
	remembered = a.lastAttach
	a.amu.Unlock()
	if remembered != nil || title.String() != titled {
		t.Fatalf("after a release to a board output: remembered=%+v title=%q", remembered, title.String())
	}
}

func TestRecoveredBoardOutputIsNeverReplayedAsDM(t *testing.T) {
	a := newAdapter()
	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() { _ = adapterSide.Close(); _ = brokerSide.Close() })
	conn := ipc.NewConn(adapterSide)
	a.bmu.Lock()
	a.conn = conn
	a.bmu.Unlock()
	peer := ipc.NewConn(brokerSide)

	done := make(chan struct{})
	go func() {
		a.fireRecover(context.Background(), conn, "thread-board", "/projects/c3")
		close(done)
	}()
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatalf("read recover request: %v", err)
	}
	resp, _ := json.Marshal(ipc.RecoverSessionResp{
		Op: ipc.OpRecoverSessionResult, Recovered: true, Channel: channel.BoardChannelName, ChatID: 1, Name: "board:plan",
		Routes: []ipc.RouteRef{boardTestTopic, boardTestBoard}, Output: &boardTestBoard,
	})
	a.dispatchRecoverSessionResult(conn, resp)
	<-done
	a.amu.Lock()
	remembered, routes := a.lastAttach, len(a.routes)
	a.amu.Unlock()
	if remembered != nil || routes != 2 {
		t.Fatalf("remembered=%+v routes=%d, want nothing remembered and both routes held", remembered, routes)
	}
}
