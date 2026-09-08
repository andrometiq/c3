package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/gorilla/websocket"
)

func renderFrames(peer *ipc.Conn) <-chan []byte {
	frames := make(chan []byte, 32)
	go func() {
		defer close(frames)
		for {
			raw, err := peer.ReadFrame()
			if err != nil {
				return
			}
			frames <- raw
		}
	}()
	return frames
}

func nextRenderFrame(t *testing.T, frames <-chan []byte) ipc.RenderRoute {
	t.Helper()
	select {
	case raw := <-frames:
		var msg ipc.RenderStateMsg
		if err := json.Unmarshal(raw, &msg); err != nil || msg.Op != ipc.OpRenderState {
			t.Fatalf("expected route update, got %s (%v)", raw, err)
		}
		return msg.RenderRoute
	case <-time.After(2 * time.Second):
		t.Fatal("broker received no render route update")
	}
	return ipc.RenderRoute{}
}

func TestCodexPullOnlyTransitionsReachBroker(t *testing.T) {
	for _, reason := range []string{"queue method unsupported", "fetch outcome unknown; restart required", "conversation identity unresolved"} {
		t.Run(reason, func(t *testing.T) {
			t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
			t.Setenv("C3_CODEX_QUEUE_BIN", "")
			t.Setenv("C3_CODEX_THREAD_ID", "pinned")
			a, peer := adapterWithBrokerConn(t)
			frames := renderFrames(peer)
			switch reason {
			case "queue method unsupported":
				ws := controlledQueue(t, func(c *websocket.Conn, req map[string]any, _ string) {
					_ = c.WriteJSON(map[string]any{"id": req["id"], "error": map[string]any{"code": -32601, "message": "unknown method"}})
				})
				t.Setenv("C3_CODEX_APP_SERVER_WS", ws)
				enqueueRecord(a, "telegram", 1, "held", "row")
			case "fetch outcome unknown; restart required":
				a.stopUncertainFetch(true)
			case "conversation identity unresolved":
				t.Setenv("C3_CODEX_THREAD_ID", "")
				enqueueRecord(a, "telegram", 1, "held", "row")
			}
			if got := nextRenderFrame(t, frames); got != (ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: reason}) {
				t.Fatalf("route: %+v", got)
			}
			// A broker reconnect must carry the latch in hello, even if the one-shot
			// recovery notice was already emitted on the previous connection.
			done := make(chan error, 1)
			go func() { done <- a.hello() }()
			select {
			case raw := <-frames:
				var hello ipc.HelloMsg
				if err := json.Unmarshal(raw, &hello); err != nil || hello.Op != ipc.OpHello || hello.RenderState != ipc.RenderQueueOnly || hello.RenderReason != reason {
					t.Fatalf("hello: %s", raw)
				}
			case <-time.After(time.Second):
				t.Fatal("no hello")
			}
			if err := peer.WriteJSON(ipc.HelloAckMsg{Op: ipc.OpHelloAck}); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCodexStartupRouteAndPinRecovery(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		t.Run(map[bool]string{false: "unpinned", true: "pinned"}[pinned], func(t *testing.T) {
			t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
			t.Setenv("C3_CODEX_QUEUE_BIN", "")
			t.Setenv("C3_CODEX_THREAD_ID", "")
			if pinned {
				t.Setenv("C3_CODEX_THREAD_ID", "pinned")
			}
			a, peer := adapterWithBrokerConn(t)
			frames := renderFrames(peer)
			done := make(chan error, 1)
			go func() { done <- a.hello() }()
			var hello ipc.HelloMsg
			select {
			case raw := <-frames:
				if err := json.Unmarshal(raw, &hello); err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("no startup hello")
			}
			want := ipc.RenderQueueOnly
			if pinned {
				want = ipc.RenderCapable
			}
			if hello.RenderState != want || (!pinned && hello.RenderReason != "conversation identity unresolved") {
				t.Fatalf("startup: %+v", hello)
			}
			if err := peer.WriteJSON(ipc.HelloAckMsg{Op: ipc.OpHelloAck}); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			// A fresh process has no uncertainty/unsupported latch. Pinning the
			// conversation restores capability without requiring an inbound probe.
			if !pinned {
				if _, err := a.stableSessionID(context.Background(), codexForwardConfig{ThreadID: "pinned"}); err != nil {
					t.Fatal(err)
				}
				if got := nextRenderFrame(t, frames); got != (ipc.RenderRoute{State: ipc.RenderCapable}) {
					t.Fatalf("recovery: %+v", got)
				}
			}
			t.Setenv("C3_CODEX_APP_SERVER_WS", startFakeCodexAppServer(t))
			enqueueRecord(a, "telegram", 2, "accepted", "row")
			select {
			case raw := <-frames:
				var ack ipc.InboundDeliveredMsg
				if err := json.Unmarshal(raw, &ack); err != nil || ack.Op != ipc.OpInboundDelivered || !ack.OK || ack.DeliveryToken != "accepted" {
					t.Fatalf("acceptance: %s", raw)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("recovered adapter did not accept queued input")
			}
		})
	}
}

func TestLatchedArrivalNudgesAndStatusNoticeIsBestEffort(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(map[bool]string{false: "unsupported", true: "uncertain"}[uncertain], func(t *testing.T) {
			t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
			a, buf := adapterWithCaptureTransport(t)
			// Exercise the serial delivery branch directly, without a consumer race.
			a.deliveryStateMu.Lock()
			a.deliveryUncertain, a.queueUnsupported = uncertain, !uncertain
			a.deliveryStateMu.Unlock()
			a.attachedTopic = "project"
			for i := 0; i < 2; i++ {
				a.deliverCodex(codexForwardReq{inbound: c3types.Inbound{Text: "held"}, covered: 3, pending: 2})
			}
			if got := buf.String(); strings.Count(got, "notifications/message") != 2 || !strings.Contains(got, "5 pending") || !strings.Contains(got, "project") {
				t.Fatalf("missing per-arrival nudge: %s", got)
			}
			// Operational events are skipped while latched, with no recursive nudges.
			a.deliverCodex(codexForwardReq{inbound: c3types.Inbound{Kind: c3types.InboundSystem, Event: &c3types.InboundEvent{System: &c3types.SystemEvent{Message: "notice"}}}})
			if strings.Count(buf.String(), "notifications/message") != 2 {
				t.Fatal("status notice recursively nudged")
			}
		})
	}
}

func TestPullOnlyDocumentationContract(t *testing.T) {
	doc, err := os.ReadFile("../../docs/ADAPTERS.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"one-time MCP log notification announces the switch to pull-only", "then holds new messages and notifies per message", "with `fetch_queue`"} {
		if !strings.Contains(string(doc), part) {
			t.Errorf("adapter contract missing %q", part)
		}
	}
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "The delivery loop skips it while\n// pull-only is latched") {
		t.Error("status notice comment must describe the pull-only limitation")
	}
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
	if got := newAdapter().buildInstructions(); !strings.Contains(got, "or when delivery has been reported pull-only, fetch_queue periodically") {
		t.Fatalf("instructions omit periodic pull: %s", got)
	}
}

func TestCodexPullOnlyTransitionDuringHelloIsNotLost(t *testing.T) {
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
	t.Setenv("C3_CODEX_QUEUE_BIN", "")
	t.Setenv("C3_CODEX_THREAD_ID", "pinned")
	a, peer := adapterWithBrokerConn(t)
	frames := renderFrames(peer)
	a.connHelloPending = true
	done := make(chan error, 1)
	go func() { done <- a.hello() }()
	select {
	case raw := <-frames:
		var hello ipc.HelloMsg
		if err := json.Unmarshal(raw, &hello); err != nil || hello.Op != ipc.OpHello || hello.RenderState != ipc.RenderCapable {
			t.Fatalf("hello: %s", raw)
		}
	case <-time.After(time.Second):
		t.Fatal("hello missing")
	}
	a.stopUncertainFetch(true)
	select {
	case raw := <-frames:
		t.Fatalf("route update overtook hello ack: %s", raw)
	case <-time.After(20 * time.Millisecond):
	}
	if err := peer.WriteJSON(ipc.HelloAckMsg{Op: ipc.OpHelloAck}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := nextRenderFrame(t, frames); got.State != ipc.RenderQueueOnly || got.Reason != "fetch outcome unknown; restart required" {
		t.Fatalf("lost transition: %+v", got)
	}
}
