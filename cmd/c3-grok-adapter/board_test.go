package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/termtitle"
)

var (
	boardTestTopicID = int64(281)
	boardTestTopic   = ipc.RouteRef{Channel: "telegram", ChatID: -100, TopicID: &boardTestTopicID, Name: "c3", Group: "main"}
	boardTestBoard   = ipc.RouteRef{Channel: channel.BoardChannelName, ChatID: 1, Name: "board:plan"}
)

// boardTestBroker wires a to a pipe whose broker end answers every recover
// request with resp and drains everything else.
func boardTestBroker(t *testing.T, a *adapter, resp ipc.RecoverSessionResp) {
	t.Helper()
	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() { _ = adapterSide.Close(); _ = brokerSide.Close() })
	a.bmu.Lock()
	a.conn = ipc.NewConn(adapterSide)
	a.bmu.Unlock()
	resp.Op = ipc.OpRecoverSessionResult
	answer, _ := json.Marshal(resp)
	go func() {
		peer := ipc.NewConn(brokerSide)
		for {
			raw, err := peer.ReadFrame()
			if err != nil {
				return
			}
			if op, _ := ipc.PeekOp(raw); op == ipc.OpRecoverSession {
				a.dispatchRecoverSessionResult(answer)
			}
		}
	}()
}

func boardTitleCapture(t *testing.T) *bytes.Buffer {
	t.Helper()
	t.Setenv("C3_NO_TERMINAL_TITLE", "")
	var title bytes.Buffer
	previousWriter := termtitle.SetWriter(&title)
	previousTTY := termtitle.SetIsTTY(func() bool { return true })
	t.Cleanup(func() {
		termtitle.SetWriter(previousWriter)
		termtitle.SetIsTTY(previousTTY)
	})
	return &title
}

// boardAttachTurn runs one attach tool call with args and answers it with resp.
func boardAttachTurn(t *testing.T, a *adapter, args map[string]any, resp ipc.AttachedMsg) {
	t.Helper()
	raw, _ := json.Marshal(args)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_, _ = a.toolAttach(ctx, &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "attach", Arguments: raw}})
		close(done)
	}()
	for {
		a.pmu.Lock()
		_, pending := a.pending["attached"]
		a.pmu.Unlock()
		if pending {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("the attach never reached the broker")
		case <-time.After(2 * time.Millisecond):
		}
	}
	msg, _ := json.Marshal(resp)
	a.dispatchAttached(msg)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("toolAttach did not return")
	}
}

func TestBoardAttachIsNeverRememberedForReplay(t *testing.T) {
	t.Setenv("GROK_HOME", t.TempDir())
	t.Setenv("GROK_SESSION_ID", "")
	t.Setenv("C3_GROK_SESSION_ID", "")
	t.Setenv("C3_GROK_CWD", t.TempDir())
	title := boardTitleCapture(t)
	a := newAdapter()
	boardTestBroker(t, a, ipc.RecoverSessionResp{})
	boardAttachTurn(t, a, map[string]any{"expr": "c3"}, ipc.AttachedMsg{
		Op: ipc.OpAttached, OK: true, Channel: "telegram", ChatID: -100, TopicID: &boardTestTopicID, Name: "c3",
		Routes: []ipc.RouteRef{boardTestTopic}, Output: &boardTestTopic,
	})
	titled := title.String()
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
	if remembered == nil || remembered.Expr != "c3" || titled == "" || title.String() != titled {
		t.Fatalf("remembered=%+v title=%q (was %q), want the topic attach and its title", remembered, title.String(), titled)
	}
}

func TestRecoveredBoardOutputIsNeverReplayedAsDM(t *testing.T) {
	t.Setenv("GROK_HOME", t.TempDir())
	t.Setenv("GROK_SESSION_ID", "")
	t.Setenv("C3_GROK_SESSION_ID", "")
	t.Setenv("C3_GROK_CWD", t.TempDir())
	for _, test := range []struct {
		name         string
		output       ipc.RouteRef
		wantReplayed bool
	}{
		{name: "a topic output is remembered", output: boardTestTopic, wantReplayed: true},
		{name: "a board output is not", output: boardTestBoard},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := newAdapter()
			boardTestBroker(t, a, ipc.RecoverSessionResp{
				Recovered: true, Channel: test.output.Channel, ChatID: test.output.ChatID, TopicID: test.output.TopicID,
				Name: test.output.Name, Group: test.output.Group,
				Routes: []ipc.RouteRef{boardTestTopic, boardTestBoard}, Output: &test.output,
			})
			a.fireRecover(context.Background(), "session-board", "/projects/c3")
			a.amu.Lock()
			remembered, routes := a.lastAttach, len(a.routes)
			a.amu.Unlock()
			if isReplayed := remembered != nil; isReplayed != test.wantReplayed || routes != 2 {
				t.Fatalf("remembered=%+v routes=%d, want replayed=%v with both routes held", remembered, routes, test.wantReplayed)
			}
			if remembered != nil && (remembered.TopicID == nil || *remembered.TopicID != boardTestTopicID) {
				t.Fatalf("remembered=%+v, want the topic by id", remembered)
			}
		})
	}
}
