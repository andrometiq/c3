package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/gorilla/websocket"
)

func TestForwardInboundToCodexAppServerQueuesInput(t *testing.T) {
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
			case "thread/queue/add":
				result["queuedSubmission"] = map[string]any{"id": "q-1", "clientUserMessageId": msg["params"].(map[string]any)["clientUserMessageId"]}
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
	wantMethods := []string{"initialize", "initialized", "thread/loaded/list", "thread/queue/add"}
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

func captureCodexForwardedText(t *testing.T, req codexForwardReq) string {
	t.Helper()
	textCh := make(chan string, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			var msg map[string]any
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}
			id, hasID := msg["id"]
			if !hasID {
				continue
			}
			if msg["method"] == "thread/queue/add" {
				params, _ := msg["params"].(map[string]any)
				input, _ := params["input"].([]any)
				if len(input) > 0 {
					item, _ := input[0].(map[string]any)
					text, _ := item["text"].(string)
					textCh <- text
				}
			}
			if err := conn.WriteJSON(map[string]any{"id": id, "result": map[string]any{"queuedSubmission": map[string]any{"id": "q-1", "clientUserMessageId": msg["params"].(map[string]any)["clientUserMessageId"]}}}); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	if err := forwardInboundToCodexAppServerWithPrefix(context.Background(), &req.inbound, req.originTag, codexForwardConfig{
		WSURL:    "ws" + server.URL[len("http"):],
		ThreadID: "thread-test",
		Timeout:  time.Second,
	}); err != nil {
		t.Fatalf("forward live inbound: %v", err)
	}
	select {
	case text := <-textCh:
		return text
	case <-time.After(time.Second):
		t.Fatal("turn/start text was not captured")
		return ""
	}
}

func TestHandleInboundForwarderOriginRouteTags(t *testing.T) {
	t.Setenv("C3_CODEX_ALLOW_MANUAL_FORWARD", "1")
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "")
	topicID := int64(281)
	telegram := ipc.RouteRef{Channel: "telegram", ChatID: -100, TopicID: &topicID, Name: "c3"}
	web := ipc.RouteRef{Channel: "web", ChatID: 42, Name: "web"}

	for _, tc := range []struct {
		name   string
		routes []ipc.RouteRef
		output ipc.RouteRef
		in     c3types.Inbound
		prefix string
	}{
		{name: "single-route", routes: []ipc.RouteRef{telegram}, output: telegram, in: c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &topicID, MessageID: 1, Text: "hello"}},
		{name: "multi-telegram", routes: []ipc.RouteRef{telegram, web}, output: web, in: c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &topicID, MessageID: 2, Text: "hello"}, prefix: "[telegram · c3] "},
		{name: "multi-web", routes: []ipc.RouteRef{telegram, web}, output: web, in: c3types.Inbound{Channel: "web", ChatID: 42, MessageID: 3, Text: "hello"}, prefix: "[web] "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &adapter{forwardCh: make(chan codexForwardReq, 1)}
			a.setRouteState(tc.routes, &tc.output)
			raw, err := json.Marshal(ipc.InboundMsg{Op: ipc.OpInbound, Inbound: tc.in})
			if err != nil {
				t.Fatal(err)
			}
			a.handleInbound(raw)
			var req codexForwardReq
			select {
			case req = <-a.forwardCh:
			case <-time.After(time.Second):
				t.Fatal("handleInbound did not enqueue the live forward")
			}
			if got, want := captureCodexForwardedText(t, req), tc.prefix+formatInboundTurnText(&tc.in); got != want {
				t.Fatalf("forwarded turn text=%q, want %q", got, want)
			}
		})
	}
}

func TestForwardInboundToCodexAppServerRefusesAmbiguousCWD(t *testing.T) {
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
			result := map[string]any{"queuedSubmission": map[string]any{"id": "q-1", "clientUserMessageId": msg["params"].(map[string]any)["clientUserMessageId"]}}
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
	if !errors.Is(err, errCodexThreadAmbiguous) {
		t.Fatalf("ambiguous loaded threads must fail closed: %v", err)
	}
	if threadListParams != nil {
		t.Fatal("cwd must not be used to guess a thread")
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
