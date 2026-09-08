package main

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/gorilla/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func detachPeer(t *testing.T, a *adapter) *brokerPeer {
	t.Helper()
	left, right := net.Pipe()
	t.Cleanup(func() { left.Close(); right.Close() })
	a.bmu.Lock()
	a.conn = ipc.NewConn(left)
	a.bmu.Unlock()
	a.brokerVersion.Store(int64(ipc.ProtocolVersion))
	return newBrokerPeer(right)
}

func detachRoutes(t *testing.T, a *adapter, peer *brokerPeer, target string, resp ipc.ReleaseResp) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan *mcp.CallToolResult, 1)
	go func() {
		result, err := a.toolDetach(ctx, multiRouteToolRequest(t, "detach", map[string]any{"target": target}))
		if err != nil {
			t.Error(err)
		}
		done <- result
	}()
	raw, ok := peer.next(t, time.Second)
	if !ok || frameOp(t, raw) != ipc.OpRelease {
		t.Fatalf("missing release: %s", raw)
	}
	if target != "" {
		data, _ := json.Marshal(resp)
		a.dispatchReleaseResult(data)
	}
	select {
	case result := <-done:
		if result == nil || result.IsError {
			t.Fatalf("detach failed: %+v", result)
		}
	case <-ctx.Done():
		t.Fatal("detach stuck")
	}
}

func TestDetachReconnectForgetsReleasedRoutes(t *testing.T) {
	tid := int64(281)
	topic := ipc.RouteRef{Channel: "telegram", ChatID: -100, TopicID: &tid, Name: "released-topic", Group: "project"}
	web := ipc.RouteRef{Channel: "web", ChatID: 42, Name: "web"}
	for _, tc := range []struct {
		name, target string
		remaining    []ipc.RouteRef
		output       *ipc.RouteRef
	}{
		{name: "full"},
		{name: "last-target", target: "telegram"},
		{name: "keep-web", target: "telegram", remaining: []ipc.RouteRef{web}, output: &web},
		{name: "keep-topic", target: "web", remaining: []ipc.RouteRef{topic}, output: &topic},
	} {
		t.Run(tc.name, func(t *testing.T) {
			title := titleScope(t)
			t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
			t.Setenv("C3_CODEX_APP_SERVER_WS", "ws://127.0.0.1:1")
			a := newAdapter()
			a.threadID = "conversation"
			peer := detachPeer(t, a)
			expr := "+web"
			if tc.target == "telegram" {
				expr = "+released-topic"
			}
			a.rememberAttach(ipc.AttachReq{Op: ipc.OpAttach, Expr: expr, CWD: "/project"})
			routes := []ipc.RouteRef{topic, web}
			if tc.name == "last-target" {
				routes = []ipc.RouteRef{topic}
			}
			a.setRouteState(routes, &routes[len(routes)-1])
			detachRoutes(t, a, peer, tc.target, ipc.ReleaseResp{OK: true, Routes: tc.remaining, Output: tc.output})
			wantTitle := "\x1b]0;\x07"
			if tc.output != nil {
				wantTitle = "\x1b]0;c3: " + tc.output.Name
				if tc.output.TopicID != nil && tc.output.Group != "" {
					wantTitle += " · " + tc.output.Group
				}
				wantTitle += "\x07"
				if a.lastAttach == nil || a.lastAttach.Expr != "" {
					t.Errorf("released expression still cached: %+v", a.lastAttach)
				}
			} else if a.lastAttach != nil {
				t.Errorf("released attach still cached: %+v", a.lastAttach)
			}
			if got := title.String(); got != wantTitle {
				t.Errorf("title=%q want %q", got, wantTitle)
			}
			// Replace only the transport: never dial or spawn a real broker.
			peer = detachPeer(t, a)
			a.replayLastAttach()
			raw, sent := peer.next(t, 30*time.Millisecond)
			if tc.output == nil {
				if sent {
					t.Errorf("released route replayed: %s", raw)
				}
			} else {
				var replay ipc.AttachReq
				if !sent || json.Unmarshal(raw, &replay) != nil {
					t.Fatalf("missing surviving route replay: %s", raw)
				}
				if !replay.Replay || replay.Steal || replay.Expr != "" {
					t.Errorf("unsafe replay: %+v", replay)
				}
				if tc.output.Channel == "web" {
					if replay.Target != "web" {
						t.Errorf("wrong surviving route: %+v", replay)
					}
				} else if replay.TopicID == nil || *replay.TopicID != tid || replay.ChatID != topic.ChatID || replay.Group != topic.Group {
					t.Errorf("lost surviving topic identity: %+v", replay)
				}
			}
			want := ""
			if tc.output != nil {
				want = ipc.FormatRouteRef(*tc.output)
			}
			if got := a.currentTopicName(); got != want {
				t.Errorf("topic=%q want %q", got, want)
			}
			instructions := a.buildInstructions()
			status, _ := a.toolCodexForward(context.Background(), multiRouteToolRequest(t, "codex_forward", nil))
			if tc.output == nil || tc.output.Channel == "web" {
				if strings.Contains(instructions, topic.Name) || strings.Contains(multiRouteResultText(t, status), topic.Name) {
					t.Error("released topic advertised")
				}
			}
			if len(a.routes) != len(tc.remaining) || len(a.routeNames) != len(tc.remaining) {
				t.Error("released route still cached")
			}
			if a.codexForwardConfig().ThreadID != "conversation" {
				t.Error("detach lost conversation pin")
			}
		})
	}
}

func TestDetachRejectsLateRecoveryState(t *testing.T) {
	a := newAdapter()
	peer := detachPeer(t, a)
	done := make(chan struct{})
	go func() { a.fireRecover(context.Background(), a.currentConn(), "conversation", "/project"); close(done) }()
	if raw, ok := peer.next(t, time.Second); !ok || frameOp(t, raw) != ipc.OpRecoverSession {
		t.Fatal("missing recovery")
	}
	detachRoutes(t, a, peer, "", ipc.ReleaseResp{})
	tid := int64(281)
	route := ipc.RouteRef{Channel: "telegram", ChatID: -100, TopicID: &tid, Name: "released-topic"}
	raw, _ := json.Marshal(ipc.RecoverSessionResp{Recovered: true, Name: route.Name, ChatID: route.ChatID, TopicID: &tid, Routes: []ipc.RouteRef{route}, Output: &route})
	a.dispatchRecoverSessionResult(a.currentConn(), raw)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recovery stuck")
	}
	if a.lastAttach != nil || a.currentTopicName() != "" {
		t.Fatal("late recovery restored released state")
	}
	a.replayLastAttach()
	if raw, ok := peer.next(t, 30*time.Millisecond); ok {
		t.Fatalf("late recovery re-claimed: %s", raw)
	}
}

func TestDetachInvalidatesOnlyReleasedDeliveries(t *testing.T) {
	for _, target := range []string{"", "telegram"} {
		t.Run("target="+target, func(t *testing.T) {
			t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
			a := newAdapter()
			peer := detachPeer(t, a)
			topic := ipc.RouteRef{Channel: "telegram", ChatID: 1, Name: "old"}
			web := ipc.RouteRef{Channel: "web", ChatID: 1, Name: "web"}
			a.setRouteState([]ipc.RouteRef{topic, web}, &web)
			// Hold the worker before dequeue completion; detach must invalidate its state.
			a.deliveryMu.Lock()
			defer a.deliveryMu.Unlock()
			enqueueRecord(a, "telegram", 1, "old", "old-record")
			enqueueRecord(a, "web", 2, "keep", "keep-record")
			a.recoveryNoticeSent.Store(true)
			a.queueUnsupported = true
			a.deliveryUncertain = true
			resp := ipc.ReleaseResp{OK: true}
			if target != "" {
				resp.Routes, resp.Output = []ipc.RouteRef{web}, &web
			}
			detachRoutes(t, a, peer, target, resp)
			a.deliveryStateMu.Lock()
			defer a.deliveryStateMu.Unlock()
			for state := range a.forwards {
				want := target == "" || state.ids[0] == "old-record"
				if state.obsolete != want {
					t.Errorf("record %s obsolete=%t want %t", state.ids[0], state.obsolete, want)
				}
			}
			if target == "" && a.recoveryNoticeSent.Load() {
				t.Error("full detach kept old failure notice latch")
			}
			if !a.queueUnsupported || !a.deliveryUncertain {
				t.Error("detach reset session delivery safety state")
			}
		})
	}
}

func TestDetachThenReattachDoesNotDeliverReleasedInput(t *testing.T) {
	submitted := make(chan string, 8)
	ws := controlledQueue(t, func(c *websocket.Conn, req map[string]any, text string) {
		submitted <- text
		acceptQueued(c, req)
	})
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
	t.Setenv("C3_CODEX_APP_SERVER_WS", ws)
	t.Setenv("C3_CODEX_THREAD_ID", "conversation")
	a := newAdapter()
	peer := detachPeer(t, a)
	topic := ipc.RouteRef{Channel: "telegram", ChatID: 1, Name: "released-topic"}
	web := ipc.RouteRef{Channel: "web", ChatID: 1, Name: "web"}
	a.setRouteState([]ipc.RouteRef{topic}, &topic)
	a.deliveryMu.Lock()
	enqueueRecord(a, "telegram", 1, "old buffered input", "old")
	detachRoutes(t, a, peer, "", ipc.ReleaseResp{})
	a.pmu.Lock()
	a.pending["attached"] = make(chan ipc.ToolResultMsg, 1)
	a.pmu.Unlock()
	raw, _ := json.Marshal(ipc.AttachedMsg{OK: true, Routes: []ipc.RouteRef{web}, Output: &web})
	a.dispatchAttached(raw)
	enqueueRecord(a, "telegram", 2, "late old input", "late")
	enqueueRecord(a, "web", 3, "new input", "new")
	a.deliveryMu.Unlock()
	raw, ok := peer.next(t, time.Second)
	var ack ipc.InboundDeliveredMsg
	if !ok || json.Unmarshal(raw, &ack) != nil || ack.Op != ipc.OpInboundDelivered || ack.DeliveryToken != "new input" {
		t.Fatalf("wrong delivery after detach: %s", raw)
	}
	select {
	case text := <-submitted:
		if !strings.Contains(text, "new input") {
			t.Fatalf("released input submitted: %s", text)
		}
	case <-time.After(time.Second):
		t.Fatal("new route did not deliver")
	}
	if raw, ok := peer.next(t, 30*time.Millisecond); ok {
		t.Fatalf("unexpected extra delivery: %s", raw)
	}
}

func TestReconnectReplaysConfirmedRoutesWithOutputLast(t *testing.T) {
	a := newAdapter()
	peer := detachPeer(t, a)
	tid := int64(281)
	topic := ipc.RouteRef{Channel: "telegram", ChatID: -100, TopicID: &tid, Name: "project", Group: "group"}
	web := ipc.RouteRef{Channel: "web", ChatID: 42, Name: "web"}
	a.rememberAttach(ipc.AttachReq{Op: ipc.OpAttach, Expr: "+web", Steal: true})
	a.setRouteState([]ipc.RouteRef{web, topic}, &web)
	a.replayLastAttach()
	for _, target := range []string{"", "web"} {
		raw, ok := peer.next(t, time.Second)
		var req ipc.AttachReq
		if !ok || json.Unmarshal(raw, &req) != nil {
			t.Fatalf("missing replay: %s", raw)
		}
		if !req.Add || !req.Replay || req.Steal || req.Target != target {
			t.Fatalf("wrong replay: %+v", req)
		}
		if target == "" && (req.TopicID == nil || *req.TopicID != tid || req.ChatID != topic.ChatID) {
			t.Fatalf("topic identity lost: %+v", req)
		}
	}
}

func TestDetachObsoletesInflightCompletion(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "accepted", true: "failed"}[fail], func(t *testing.T) {
			entered, resume := make(chan struct{}), make(chan struct{})
			ws := controlledQueue(t, func(c *websocket.Conn, req map[string]any, text string) {
				if strings.Contains(text, "old input") {
					close(entered)
					<-resume
					if fail {
						_ = c.Close()
						return
					}
				}
				acceptQueued(c, req)
			})
			t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
			t.Setenv("C3_CODEX_APP_SERVER_WS", ws)
			t.Setenv("C3_CODEX_THREAD_ID", "conversation")
			a := newAdapter()
			peer := detachPeer(t, a)
			topic := ipc.RouteRef{Channel: "telegram", ChatID: 1, Name: "old"}
			a.setRouteState([]ipc.RouteRef{topic}, &topic)
			enqueueRecord(a, "telegram", 1, "old input", "old")
			select {
			case <-entered:
			case <-time.After(time.Second):
				close(resume)
				t.Fatal("submission never started")
			}
			// Always unblock the mock before server cleanup, including on assertion failure.
			func() {
				defer close(resume)
				detachRoutes(t, a, peer, "", ipc.ReleaseResp{})
				web := ipc.RouteRef{Channel: "web", ChatID: 1, Name: "web"}
				a.setRouteState([]ipc.RouteRef{web}, &web)
				enqueueRecord(a, "web", 2, "new input", "new")
			}()
			raw, ok := peer.next(t, time.Second)
			var ack ipc.InboundDeliveredMsg
			if !ok || json.Unmarshal(raw, &ack) != nil || ack.Op != ipc.OpInboundDelivered || ack.DeliveryToken != "new input" {
				t.Fatalf("obsolete completion acknowledged: %s", raw)
			}
			if a.recoveryNoticeSent.Load() {
				t.Error("obsolete completion relatched failure notice")
			}
		})
	}
}

func TestDetachRejectsLateReplayResponse(t *testing.T) {
	for _, target := range []string{"", "telegram"} {
		t.Run("target="+target, func(t *testing.T) {
			a := newAdapter()
			peer := detachPeer(t, a)
			topic := ipc.RouteRef{Channel: "telegram", ChatID: 1, Name: "released-topic"}
			web := ipc.RouteRef{Channel: "web", ChatID: 1, Name: "web"}
			a.setRouteState([]ipc.RouteRef{topic, web}, &topic)
			resp := ipc.ReleaseResp{OK: true}
			if target != "" {
				resp.Routes, resp.Output = []ipc.RouteRef{web}, &web
			}
			detachRoutes(t, a, peer, target, resp)
			// A replay written before release can still have its response in flight.
			raw, _ := json.Marshal(ipc.AttachedMsg{OK: true, Routes: []ipc.RouteRef{topic, web}, Output: &topic})
			a.dispatchAttached(raw)
			if a.currentTopicName() == topic.Name || len(a.routes) != len(resp.Routes) {
				t.Fatal("late replay response restored released route")
			}
		})
	}
}
