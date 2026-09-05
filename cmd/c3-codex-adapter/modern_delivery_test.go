package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/gorilla/websocket"
)

func TestModernCodexQueuesWithoutStartingCompetingTurn(t *testing.T) {
	for _, mode := range []string{"accepted", "malformed", "rejected", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			var starts atomic.Int32
			var queueCalls atomic.Int32
			upgrader := websocket.Upgrader{}
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer c.Close()
				for {
					var request map[string]any
					if c.ReadJSON(&request) != nil {
						return
					}
					id, ok := request["id"]
					if !ok {
						continue
					}
					response := map[string]any{"id": id, "result": map[string]any{}}
					switch request["method"] {
					case "thread/queue/add":
						queueCalls.Add(1)
						params := request["params"].(map[string]any)
						if params["threadId"] != "pinned-thread" || !codexThreadUUID.MatchString(params["clientUserMessageId"].(string)) {
							t.Error("queue recipient or idempotency id missing")
						}
						switch mode {
						case "accepted":
							response["result"] = map[string]any{"queuedSubmission": map[string]any{"id": "queued-1"}}
						case "rejected":
							response["error"] = map[string]any{"code": -32000, "message": "unavailable"}
						case "legacy":
							response["error"] = map[string]any{"code": -32601, "message": "unknown method"}
						}
					case "turn/start":
						starts.Add(1)
					case "thread/loaded/list":
						t.Error("pinned delivery must not discover another thread")
					}
					if c.WriteJSON(response) != nil {
						return
					}
				}
			}))
			defer s.Close()
			err := forwardInboundToCodexAppServer(context.Background(), &c3types.Inbound{MessageID: 8, Text: "hello"}, codexForwardConfig{WSURL: "ws" + strings.TrimPrefix(s.URL, "http"), ThreadID: "pinned-thread", Timeout: time.Second})
			wantSuccess := mode == "accepted" || mode == "legacy"
			if (err == nil) != wantSuccess {
				t.Fatalf("result %v for %s", err, mode)
			}
			if queueCalls.Load() != 1 {
				t.Fatal("expected one durable queue attempt")
			}
			wantStarts := int32(0)
			if mode == "legacy" {
				wantStarts = 1
			}
			if starts.Load() != wantStarts {
				t.Fatalf("turn/start count %d, want %d", starts.Load(), wantStarts)
			}
		})
	}
}

func TestCodexDeliveryIdentityAndRetryKeyRemainStable(t *testing.T) {
	a := newAdapter()
	a.threadID = "original-thread"
	t.Setenv("C3_CODEX_THREAD_ID", "other-thread")
	if a.codexForwardConfig().ThreadID != "original-thread" {
		t.Fatal("delivery drifted away from recovered identity")
	}
	in := &c3types.Inbound{Channel: "telegram", ChatID: -1, MessageID: 2, Text: "hello"}
	one := codexInboundMessageID("original-thread", in)
	if one != codexInboundMessageID("original-thread", in) {
		t.Fatal("retry key changed")
	}
	in.Text = "edited"
	if one == codexInboundMessageID("original-thread", in) || one == codexInboundMessageID("other-thread", in) {
		t.Fatal("different content/recipient reused retry key")
	}
}

func TestCodexEventPayloadSurvivesDelivery(t *testing.T) {
	in := &c3types.Inbound{Kind: c3types.InboundCallback, Event: &c3types.InboundEvent{Callback: &c3types.CallbackEvent{MessageID: 9, Data: "continue"}}}
	got := formatInboundTurnText(in)
	if !strings.Contains(got, "continue") || !strings.Contains(got, "callback") {
		t.Fatalf("lost callback: %s", got)
	}
}

func TestCodexFullDrainRestoresAckAndDiscardsStaleForward(t *testing.T) {
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "1")
	t.Setenv("C3_CODEX_APP_SERVER_WS", startFakeCodexAppServer(t))
	t.Setenv("C3_CODEX_THREAD_ID", "thread-1")
	a, peer := adapterWithBrokerConn(t)
	a.forwardBlocked.Store(true)
	stale := codexForwardReq{inbound: c3types.Inbound{MessageID: 1, Text: "already drained"}, covered: 1, conn: a.currentConn(), epoch: a.forwardEpoch.Load()}
	a.clearForwardBlocked()
	a.forwardCh <- stale
	fresh := ipc.InboundMsg{Op: ipc.OpInbound, Covered: 1, Inbound: c3types.Inbound{MessageID: 2, Text: "new message"}}
	raw, _ := json.Marshal(fresh)
	a.handleInbound(raw)
	ack, ok := readDeliveredAck(t, peer, 3*time.Second)
	if !ok || ack.UpdateID != 2 {
		t.Fatalf("stale message was acked or recovery remained blocked: %+v, %t", ack, ok)
	}
}
