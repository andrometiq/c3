package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/gorilla/websocket"
)

func TestActiveTurnDelivery(t *testing.T) {
	for _, mode := range []string{"active", "idle", "race", "unsupported", "malformed", "disconnect"} {
		t.Run(mode, func(t *testing.T) {
			var steers, queues atomic.Int32
			upgrader := websocket.Upgrader{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
					reply := map[string]any{"id": id, "result": map[string]any{}}
					switch request["method"] {
					case "thread/turns/list":
						p := request["params"].(map[string]any)
						if p["threadId"] != "pinned-thread" || p["limit"] != float64(1) || p["itemsView"] != "notLoaded" {
							t.Error("turn lookup must be bounded and pinned")
						}
						status := "inProgress"
						if mode == "idle" {
							status = "completed"
						}
						reply["result"] = map[string]any{"data": []any{map[string]any{"id": "active-turn", "status": status}}}
						if mode == "unsupported" {
							reply["error"] = map[string]any{"code": -32601, "message": "unknown method"}
						}
					case "turn/steer":
						steers.Add(1)
						p := request["params"].(map[string]any)
						if p["threadId"] != "pinned-thread" || p["expectedTurnId"] != "active-turn" || !codexThreadUUID.MatchString(p["clientUserMessageId"].(string)) {
							t.Error("steer identity missing")
						}
						input := p["input"].([]any)[0].(map[string]any)
						if !strings.Contains(input["text"].(string), "follow-up") {
							t.Error("inbound text lost")
						}
						switch mode {
						case "race":
							reply["error"] = map[string]any{"code": -32600, "message": "turn is no longer active"}
						case "disconnect":
							return
						case "malformed":
							reply["result"] = map[string]any{"turnId": "other-turn"}
						default:
							reply["result"] = map[string]any{"turnId": "active-turn"}
						}
					case "thread/queue/add":
						queues.Add(1)
						reply["result"] = map[string]any{"queuedSubmission": map[string]any{"id": "queued-message"}}
					case "turn/start", "thread/resume", "thread/loaded/list":
						t.Errorf("unexpected competing/discovery call: %v", request["method"])
					}
					if c.WriteJSON(reply) != nil {
						return
					}
				}
			}))
			defer server.Close()
			cfg := codexForwardConfig{WSURL: "ws" + strings.TrimPrefix(server.URL, "http"), ThreadID: "pinned-thread", Timeout: time.Second}
			count := 1
			if mode == "active" {
				count = 3
			}
			for i := 0; i < count; i++ {
				err := forwardInboundToCodexAppServer(context.Background(), &c3types.Inbound{MessageID: int64(i + 1), Text: "follow-up"}, cfg)
				wantErr := mode == "malformed" || mode == "disconnect"
				if (err != nil) != wantErr {
					t.Fatalf("error=%v, wantErr=%v", err, wantErr)
				}
			}
			wantQueues := int32(0)
			if mode == "idle" || mode == "race" || mode == "unsupported" {
				wantQueues = 1
			}
			if queues.Load() != wantQueues {
				t.Fatalf("queue count=%d want=%d", queues.Load(), wantQueues)
			}
			if mode == "active" && steers.Load() != 3 {
				t.Fatal("successive messages did not all steer")
			}
		})
	}
}
