package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/gorilla/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The fixture records the real submitted text before deciding whether to accept.
func controlledQueue(t *testing.T, submit func(*websocket.Conn, map[string]any, string)) string {
	t.Helper()
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			var req map[string]any
			if c.ReadJSON(&req) != nil {
				return
			}
			id, ok := req["id"]
			if !ok {
				continue
			}
			switch req["method"] {
			case "thread/queue/add":
				params := req["params"].(map[string]any)
				text := params["input"].([]any)[0].(map[string]any)["text"].(string)
				submit(c, req, text)
			case "initialize":
				_ = c.WriteJSON(map[string]any{"id": id, "result": map[string]any{}})
			default:
				t.Errorf("unexpected request: %v", req["method"])
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func acceptQueued(c *websocket.Conn, req map[string]any) {
	_ = c.WriteJSON(map[string]any{"id": req["id"], "result": map[string]any{"queuedSubmission": map[string]any{"id": "accepted", "clientUserMessageId": req["params"].(map[string]any)["clientUserMessageId"]}}})
}

func ackFrames(t *testing.T, peer *ipc.Conn) <-chan ipc.InboundDeliveredMsg {
	t.Helper()
	out := make(chan ipc.InboundDeliveredMsg, 32)
	go func() {
		defer close(out)
		for {
			raw, err := peer.ReadFrame()
			if err != nil {
				return
			}
			var ack ipc.InboundDeliveredMsg
			_ = json.Unmarshal(raw, &ack)
			if ack.Op == ipc.OpInboundDelivered {
				out <- ack
			}
		}
	}()
	return out
}

func expectNoAck(t *testing.T, acks <-chan ipc.InboundDeliveredMsg) {
	t.Helper()
	select {
	case ack := <-acks:
		t.Fatalf("unexpected ack: %+v", ack)
	case <-time.After(50 * time.Millisecond):
	}
}

func expectAck(t *testing.T, acks <-chan ipc.InboundDeliveredMsg, id int64, token string) {
	t.Helper()
	select {
	case ack := <-acks:
		if !ack.OK || ack.UpdateID != id || ack.DeliveryToken != token {
			t.Fatalf("wrong ack: %+v", ack)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("missing confirmed-delivery ack")
	}
}

func enqueueRecord(a *adapter, channel string, id int64, text string, records ...string) {
	raw, _ := json.Marshal(ipc.InboundMsg{Op: ipc.OpInbound, Inbound: c3types.Inbound{Channel: channel, ChatID: 1, MessageID: id, Text: text}, Covered: len(records), RecordIDs: records, DeliveryToken: text})
	a.handleInbound(raw)
}

func dispatchConsumed(a *adapter, ids []string, remaining int) {
	ch := make(chan ipc.FetchQueueResp, 1)
	a.fqmu.Lock()
	a.fqPending["fetch"] = ch
	a.fqAck["fetch"] = true
	a.fqmu.Unlock()
	resp := ipc.FetchQueueResp{ID: "fetch", Remaining: remaining}
	for _, id := range ids {
		resp.Messages = append(resp.Messages, c3types.Inbound{ConsumedRecordID: id})
	}
	raw, _ := json.Marshal(resp)
	a.dispatchFetchQueueResult(raw)
}

func TestQueueAcceptanceControlsBrokerAck(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "delayed-acceptance", true: "lost-response"}[lost], func(t *testing.T) {
			submitted := make(chan string, 8)
			release := make(chan struct{})
			ws := controlledQueue(t, func(c *websocket.Conn, req map[string]any, text string) {
				submitted <- text
				if strings.Contains(text, "payload") {
					<-release
					if lost {
						_ = c.Close()
						return
					}
				}
				acceptQueued(c, req)
			})
			t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
			t.Setenv("C3_CODEX_THREAD_ID", "pinned")
			t.Setenv("C3_CODEX_APP_SERVER_WS", ws)
			a, peer := adapterWithBrokerConn(t)
			acks := ackFrames(t, peer)
			enqueueRecord(a, "telegram", 1, "payload", "record-1")
			select {
			case text := <-submitted:
				if !strings.Contains(text, "payload") {
					t.Fatal(text)
				}
			case <-time.After(time.Second):
				t.Fatal("no submission")
			}
			expectNoAck(t, acks)
			close(release)
			if lost {
				expectNoAck(t, acks)
			} else {
				expectAck(t, acks, 1, "payload")
			}
			// A later successful token never consumes the failed token's record.
			enqueueRecord(a, "web", 2, "later", "record-2")
			expectAck(t, acks, 2, "later")
		})
	}
}

func TestQueueUnsupportedIdlessAndUncertainErrors(t *testing.T) {
	cases := []struct {
		name, frame string
		unsupported bool
	}{
		{"real-idless-variant", `{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid request: unknown variant ` + "`thread/queue/add`" + `, expected one of ` + "`initialize`, `turn/start`" + `"}}`, true},
		{"generic-invalid", `{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid request"}}`, false},
		{"different-variant", `{"jsonrpc":"2.0","error":{"code":-32600,"message":"unknown variant ` + "`other`, expected `thread/queue/add`" + `"}}`, false},
		{"malformed", `{"jsonrpc":"2.0","error":"broken"}`, false},
		{"timeout", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ws := controlledQueue(t, func(c *websocket.Conn, req map[string]any, _ string) {
				if tc.frame == "" {
					time.Sleep(80 * time.Millisecond)
					return
				}
				_ = c.WriteMessage(websocket.TextMessage, []byte(tc.frame))
			})
			err := forwardInboundToCodexAppServer(context.Background(), &c3types.Inbound{Text: "held"}, codexForwardConfig{WSURL: ws, ThreadID: "pinned", Timeout: 40 * time.Millisecond})
			if err == nil || errors.Is(err, errCodexQueueUnsupported) != tc.unsupported {
				t.Fatalf("error=%v, unsupported=%v", err, tc.unsupported)
			}
		})
	}
}

func TestCodexFullDrainRestoresAckAndDiscardsStaleForward(t *testing.T) {
	submitted := make(chan string, 16)
	ws := controlledQueue(t, func(c *websocket.Conn, req map[string]any, text string) { submitted <- text; acceptQueued(c, req) })
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
	t.Setenv("C3_CODEX_THREAD_ID", "pinned")
	t.Setenv("C3_CODEX_APP_SERVER_WS", ws)
	a, peer := adapterWithBrokerConn(t)
	acks := ackFrames(t, peer)
	a.deliveryMu.Lock()
	enqueueRecord(a, "telegram", 1, "already-drained", "old")
	// This push arrives before the response, but its record was not consumed.
	enqueueRecord(a, "telegram", 2, "fresh-before-response", "fresh")
	enqueueRecord(a, "web", 3, "sibling-route", "web")
	dispatchConsumed(a, []string{"old"}, 0)
	a.deliveryMu.Unlock()
	expectAck(t, acks, 2, "fresh-before-response")
	expectAck(t, acks, 3, "sibling-route")
	for i := 0; i < 2; i++ {
		text := <-submitted
		if strings.Contains(text, "already-drained") {
			t.Fatalf("stale submission reached transport: %s", text)
		}
	}
	select {
	case text := <-submitted:
		t.Fatalf("extra submission: %s", text)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestPartialMergedFetchExcludesStalePayload(t *testing.T) {
	submitted := make(chan string, 16)
	ws := controlledQueue(t, func(c *websocket.Conn, req map[string]any, text string) { submitted <- text; acceptQueued(c, req) })
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
	t.Setenv("C3_CODEX_THREAD_ID", "pinned")
	t.Setenv("C3_CODEX_APP_SERVER_WS", ws)
	a, peer := adapterWithBrokerConn(t)
	acks := ackFrames(t, peer)
	a.deliveryMu.Lock()
	enqueueRecord(a, "telegram", 2, "merged-stale-and-survivor", "a", "b")
	enqueueRecord(a, "web", 3, "unaffected", "c")
	dispatchConsumed(a, []string{"a"}, 1)
	a.deliveryMu.Unlock()
	expectAck(t, acks, 3, "unaffected")
	// Flush the notice behind the unaffected delivery before inspecting content.
	deadline := time.After(time.Second)
	for {
		select {
		case text := <-submitted:
			if strings.Contains(text, "merged-stale-and-survivor") {
				t.Fatal("partly consumed merged payload submitted")
			}
			if strings.Contains(text, "remaining sources") {
				expectNoAck(t, acks)
				return
			}
		case <-deadline:
			t.Fatal("remaining merged sources were not surfaced for pull")
		}
	}
}

func TestObsoleteInFlightCompletionCannotAckOrRelatch(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[failure], func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			ws := controlledQueue(t, func(c *websocket.Conn, req map[string]any, text string) {
				if strings.Contains(text, "obsolete") {
					close(entered)
					<-release
					if failure {
						_ = c.Close()
						return
					}
				}
				acceptQueued(c, req)
			})
			t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
			t.Setenv("C3_CODEX_THREAD_ID", "pinned")
			t.Setenv("C3_CODEX_APP_SERVER_WS", ws)
			a, peer := adapterWithBrokerConn(t)
			acks := ackFrames(t, peer)
			enqueueRecord(a, "telegram", 1, "obsolete", "old")
			<-entered
			dispatchConsumed(a, []string{"old"}, 0)
			close(release)
			enqueueRecord(a, "telegram", 2, "new", "fresh")
			expectAck(t, acks, 2, "new")
			if a.recoveryNoticeSent.Load() {
				t.Fatal("obsolete failure relatched recovered delivery")
			}
		})
	}
}

func TestDestructiveFetchWaitsForInflightDelivery(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	ws := controlledQueue(t, func(c *websocket.Conn, req map[string]any, text string) {
		if strings.Contains(text, "inflight") {
			close(entered)
			<-release
			_ = c.Close()
			return
		}
		acceptQueued(c, req)
	})
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
	t.Setenv("C3_CODEX_THREAD_ID", "pinned")
	t.Setenv("C3_CODEX_APP_SERVER_WS", ws)
	a, peer := adapterWithBrokerConn(t)
	enqueueRecord(a, "telegram", 1, "inflight", "row")
	<-entered
	frames := make(chan []byte, 8)
	go func() {
		for {
			raw, err := peer.ReadFrame()
			if err != nil {
				return
			}
			frames <- raw
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan *mcp.CallToolResult, 1)
	go func() {
		r, _ := a.toolFetchQueue(ctx, newAttachReq(t, map[string]any{"channel": "telegram", "limit": "all"}))
		done <- r
	}()
	select {
	case raw := <-frames:
		t.Fatalf("destructive fetch raced submission: %s", raw)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	var fq ipc.FetchQueueReq
	select {
	case raw := <-frames:
		if err := json.Unmarshal(raw, &fq); err != nil || fq.Op != ipc.OpFetchQueue || fq.Channel != "telegram" {
			t.Fatalf("fetch frame %s", raw)
		}
	case <-ctx.Done():
		t.Fatal("fetch never started")
	}
	raw, _ := json.Marshal(ipc.FetchQueueResp{ID: fq.ID, Messages: []c3types.Inbound{{ConsumedRecordID: "row", Text: "inflight"}}})
	a.dispatchFetchQueueResult(raw)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("fetch did not complete")
	}
	if a.recoveryNoticeSent.Load() {
		t.Fatal("completed recovery was undone by in-flight failure")
	}
}

func TestFailedResolutionAttachRemainsPullOnly(t *testing.T) {
	var submissions atomic.Int32
	ws := controlledQueue(t, func(c *websocket.Conn, req map[string]any, _ string) { submissions.Add(1); acceptQueued(c, req) })
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
	t.Setenv("C3_CODEX_THREAD_ID", "")
	t.Setenv("C3_CODEX_APP_SERVER_WS", ws)
	a, peer := adapterWithBrokerConn(t)
	acks := ackFrames(t, peer)
	// Simulate the failed startup resolver, then a successful explicit attach.
	if _, err := a.stableSessionID(context.Background(), codexForwardConfig{}); err == nil {
		t.Fatal("resolution should fail")
	}
	raw, _ := json.Marshal(ipc.AttachedMsg{OK: true, Channel: "telegram", ChatID: 1, QueuedCount: 1})
	a.dispatchAttached(raw)
	enqueueRecord(a, "telegram", 1, "must-not-discover", "row")
	expectNoAck(t, acks)
	if submissions.Load() != 0 {
		t.Fatal("unpinned attached adapter submitted content")
	}
}

func TestBenignAttachDoesNotConsumeFailureNudge(t *testing.T) {
	a, buf := adapterWithCaptureTransport(t)
	a.pmu.Lock()
	a.pending["attached"] = make(chan ipc.ToolResultMsg, 1)
	a.pmu.Unlock()
	raw, _ := json.Marshal(ipc.AttachedMsg{OK: true, QueuedCount: 3})
	a.dispatchAttached(raw)
	a.latchForwardBlocked("first real failure")
	if got := buf.String(); !strings.Contains(got, "first real failure") || strings.Count(got, "notifications/message") != 1 {
		t.Fatalf("missing one-shot recovery notice: %s", got)
	}
}

func TestSystemEventPreservesEveryField(t *testing.T) {
	in := c3types.Inbound{Kind: c3types.InboundSystem, Event: &c3types.InboundEvent{System: &c3types.SystemEvent{Source: "scheduler", Level: "warning", Title: "Task held", Message: "details"}}}
	got := captureCodexForwardedText(t, codexForwardReq{inbound: in})
	for _, value := range []string{"scheduler", "warning", "Task held", "details"} {
		if !strings.Contains(got, value) {
			t.Fatalf("lost %q: %s", value, got)
		}
	}
}

func TestUnsupportedQueueHoldsMessagesAndReportsPullOnly(t *testing.T) {
	var calls atomic.Int32
	ws := controlledQueue(t, func(c *websocket.Conn, req map[string]any, _ string) {
		calls.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid request: unknown variant `+"`thread/queue/add`"+`"}}`))
	})
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
	t.Setenv("C3_CODEX_THREAD_ID", "pinned")
	t.Setenv("C3_CODEX_APP_SERVER_WS", ws)
	a, peer := adapterWithBrokerConn(t)
	acks := ackFrames(t, peer)
	enqueueRecord(a, "telegram", 1, "first", "a")
	expectNoAck(t, acks)
	enqueueRecord(a, "telegram", 2, "second", "b")
	expectNoAck(t, acks)
	result, err := a.toolCodexForward(context.Background(), newAttachReq(t, map[string]any{}))
	if err != nil || !strings.Contains(attachResultText(t, result), "pull-only (thread/queue/add unsupported)") {
		t.Fatalf("missing pull-only diagnostics: %+v %v", result, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("unsupported queue should be probed once, got %d", calls.Load())
	}
}

func TestFailedResolutionAttachExplainsPullOnly(t *testing.T) {
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
	t.Setenv("C3_CODEX_THREAD_ID", "")
	a := newAdapterWithDummyConn(t)
	if _, err := a.stableSessionID(context.Background(), codexForwardConfig{}); err == nil {
		t.Fatal("expected failed resolution")
	}
	result := callToolAttachSync(t, a, ipc.AttachedMsg{OK: true, Channel: "telegram", Name: "project"})
	if got := attachResultText(t, result); !strings.Contains(got, "identity unresolved; pull-only") {
		t.Fatalf("attach omitted identity limitation: %s", got)
	}
}

func TestCanceledDestructiveFetchPausesLiveDelivery(t *testing.T) {
	var calls atomic.Int32
	ws := controlledQueue(t, func(c *websocket.Conn, req map[string]any, _ string) { calls.Add(1); acceptQueued(c, req) })
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
	t.Setenv("C3_CODEX_THREAD_ID", "pinned")
	t.Setenv("C3_CODEX_APP_SERVER_WS", ws)
	a, peer := adapterWithBrokerConn(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan *mcp.CallToolResult, 1)
	go func() { r, _ := a.toolFetchQueue(ctx, newAttachReq(t, map[string]any{})); done <- r }()
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case r := <-done:
		if !strings.Contains(attachResultText(t, r), "outcome is unknown") {
			t.Fatal("missing unknown-outcome notice")
		}
	case <-time.After(time.Second):
		t.Fatal("fetch cancellation stuck")
	}
	acks := ackFrames(t, peer)
	enqueueRecord(a, "telegram", 1, "must-stay-held", "row")
	expectNoAck(t, acks)
	if calls.Load() != 0 {
		t.Fatal("submitted while destructive fetch outcome unknown")
	}
}

func TestEventPayloadsSubmitWithoutBrokerAcknowledgement(t *testing.T) {
	submitted := make(chan string, 8)
	ws := controlledQueue(t, func(c *websocket.Conn, req map[string]any, text string) { submitted <- text; acceptQueued(c, req) })
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
	t.Setenv("C3_CODEX_THREAD_ID", "pinned")
	t.Setenv("C3_CODEX_APP_SERVER_WS", ws)
	a, peer := adapterWithBrokerConn(t)
	acks := ackFrames(t, peer)
	for _, kind := range []c3types.InboundKind{c3types.InboundCallback, c3types.InboundReaction, c3types.InboundPollResult, c3types.InboundSystem} {
		event := &c3types.InboundEvent{}
		switch kind {
		case c3types.InboundCallback:
			event.Callback = &c3types.CallbackEvent{Data: "button-data"}
		case c3types.InboundReaction:
			event.Reaction = &c3types.ReactionEvent{MessageID: 42}
		case c3types.InboundPollResult:
			event.PollResult = &c3types.PollResult{PollID: "poll-id", IsClosed: true}
		case c3types.InboundSystem:
			event.System = &c3types.SystemEvent{Source: "source", Level: "warn", Title: "title", Message: "message"}
		}
		payload, _ := json.Marshal(event)
		raw, _ := json.Marshal(ipc.InboundMsg{Inbound: c3types.Inbound{Kind: kind, Event: event}, Covered: 1, DeliveryToken: "must-not-ack"})
		a.handleInbound(raw)
		select {
		case text := <-submitted:
			if !strings.Contains(text, string(payload)) {
				t.Fatalf("%s lost payload: %s", kind, text)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s never reached transport", kind)
		}
	}
	// A following ordinary message proves that every event completed first.
	enqueueRecord(a, "telegram", 99, "after-events", "record")
	expectAck(t, acks, 99, "after-events")
	expectNoAck(t, acks)
}

func TestBenignRecoveryDoesNotConsumeFailureNudge(t *testing.T) {
	a, buf := adapterWithCaptureTransport(t)
	a.rsmu.Lock()
	a.rsPending[nil] = make(chan ipc.RecoverSessionResp, 1)
	a.rsmu.Unlock()
	raw, _ := json.Marshal(ipc.RecoverSessionResp{Recovered: true, QueuedCount: 3})
	a.dispatchRecoverSessionResult(nil, raw)
	a.latchForwardBlocked("first real failure")
	if got := buf.String(); !strings.Contains(got, "first real failure") {
		t.Fatalf("recovery backlog consumed failure notice: %s", got)
	}
}
