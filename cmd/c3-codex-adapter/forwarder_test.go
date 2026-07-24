package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/karthikeyan5/c3/internal/c3types"
)

func TestForwardInboundToCodexAppServerStartsTurn(t *testing.T) {
	var got []map[string]any
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer c.Close()
		for {
			var msg map[string]any
			if err := c.ReadJSON(&msg); err != nil {
				return
			}
			got = append(got, msg)
			id, hasID := msg["id"]
			if !hasID {
				continue
			}
			method, _ := msg["method"].(string)
			result := map[string]any{}
			switch method {
			case "thread/loaded/list":
				result["data"] = []string{"thread-1"}
			case "turn/start":
				result["turn"] = map[string]any{"id": "turn-1"}
			default:
				result["ok"] = true
			}
			if err := c.WriteJSON(map[string]any{"id": id, "result": result}); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + server.URL[len("http"):]
	msg := c3types.Inbound{
		Channel:   "telegram",
		ChatID:    12345678,
		MessageID: 1491,
		Sender:    c3types.Sender{UserID: 12345678, Username: "alice"},
		Text:      "[Transcribed voice]: Hello my testing 1 2 3",
	}

	err := forwardInboundToCodexAppServer(context.Background(), &msg, codexForwardConfig{
		WSURL:   wsURL,
		CWD:     "/home/user/projects",
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatalf("forward failed: %v", err)
	}

	methods := make([]string, 0, len(got))
	for _, msg := range got {
		if method, ok := msg["method"].(string); ok {
			methods = append(methods, method)
		}
	}
	wantMethods := []string{"initialize", "initialized", "thread/loaded/list", "thread/resume", "turn/start"}
	if len(methods) != len(wantMethods) {
		t.Fatalf("methods = %#v, want %#v", methods, wantMethods)
	}
	for i := range wantMethods {
		if methods[i] != wantMethods[i] {
			t.Fatalf("methods = %#v, want %#v", methods, wantMethods)
		}
	}

	turnStart := got[len(got)-1]
	params := turnStart["params"].(map[string]any)
	if params["threadId"] != "thread-1" {
		t.Fatalf("threadId = %v, want thread-1", params["threadId"])
	}
	input := params["input"].([]any)
	item := input[0].(map[string]any)
	text := item["text"].(string)
	// #55 (2026-07-24): the live-forward turn now renders in the SAME trimmed form
	// as the fetch_queue readback — bare message text first, then a compact
	// metadata line — instead of the old "Telegram message from … (chat=… thread=…)"
	// header. formatInboundTurnText delegates to c3types.RenderQueuedInbound.
	if text != "[Transcribed voice]: Hello my testing 1 2 3\nfrom=@alice message_id=1491" {
		t.Fatalf("turn text = %q", text)
	}
}

func TestForwardInboundToCodexAppServerPicksLoadedThreadForCWD(t *testing.T) {
	var threadListParams map[string]any
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer c.Close()
		for {
			var msg map[string]any
			if err := c.ReadJSON(&msg); err != nil {
				return
			}
			id, hasID := msg["id"]
			if !hasID {
				continue
			}
			method, _ := msg["method"].(string)
			result := map[string]any{"ok": true}
			switch method {
			case "thread/loaded/list":
				result = map[string]any{"data": []string{"thread-old", "thread-new"}}
			case "thread/list":
				threadListParams = msg["params"].(map[string]any)
				result = map[string]any{"data": []map[string]any{{"id": "thread-new"}}}
			}
			if err := c.WriteJSON(map[string]any{"id": id, "result": result}); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + server.URL[len("http"):]
	err := forwardInboundToCodexAppServer(context.Background(), &c3types.Inbound{
		Channel: "telegram",
		ChatID:  12345678,
		Sender:  c3types.Sender{Username: "alice"},
		Text:    "hi",
	}, codexForwardConfig{WSURL: wsURL, CWD: "/home/user/projects/c3", Timeout: time.Second})
	if err != nil {
		t.Fatalf("forward failed: %v", err)
	}
	if threadListParams["cwd"] != "/home/user/projects/c3" {
		encoded, _ := json.Marshal(threadListParams)
		t.Fatalf("thread/list params = %s", encoded)
	}
}

// D-RC1: the live-forward turn text must carry the same information density as the
// queued (fetch_queue) renderer — message_id and the full reply context — not just
// sender/chat/thread/text. Without this, a reply forwarded live to Codex loses the
// quoted-message metadata the agent needs to thread its response.
func TestFormatInboundTurnText_IncludesReplyAndMessageID(t *testing.T) {
	in := &c3types.Inbound{
		Channel:   "telegram",
		ChatID:    -100,
		MessageID: 102,
		Sender:    c3types.Sender{Username: "alice"},
		ReplyTo: &c3types.ReplyContext{
			MessageID: 101,
			User:      c3types.Sender{Username: "alice"},
			Text:      ".",
		},
		Text: "Reply to this",
	}
	got := formatInboundTurnText(in)
	for _, want := range []string{"message_id=102", "reply_to=101", "reply_to_user=@alice"} {
		if !strings.Contains(got, want) {
			t.Errorf("formatInboundTurnText missing %q; got %q", want, got)
		}
	}
}
