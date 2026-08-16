package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

// fakeLeader is a minimal leader that accepts one client, expects register,
// then serves ACP session/load + session/prompt.
func startFakeLeader(t *testing.T) (sockPath string, prompts *[]string, cleanup func()) {
	t.Helper()
	dir := t.TempDir()
	sockPath = filepath.Join(dir, "leader.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var mu sync.Mutex
	got := []string{}
	prompts = &got
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := []byte{}
		readMsg := func() (map[string]any, error) {
			for {
				if len(buf) >= 4 {
					n := int(buf[0])<<24 | int(buf[1])<<16 | int(buf[2])<<8 | int(buf[3])
					if len(buf) >= 4+n {
						body := buf[4 : 4+n]
						buf = buf[4+n:]
						var m map[string]any
						if err := json.Unmarshal(body, &m); err != nil {
							return nil, err
						}
						return m, nil
					}
				}
				tmp := make([]byte, 4096)
				_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				k, err := conn.Read(tmp)
				if err != nil {
					return nil, err
				}
				buf = append(buf, tmp[:k]...)
			}
		}
		writeMsg := func(v any) {
			raw, _ := json.Marshal(v)
			hdr := []byte{byte(len(raw) >> 24), byte(len(raw) >> 16), byte(len(raw) >> 8), byte(len(raw))}
			_, _ = conn.Write(hdr)
			_, _ = conn.Write(raw)
		}
		// register
		m, err := readMsg()
		if err != nil || m["type"] != "register" {
			return
		}
		writeMsg(map[string]any{"type": "registered", "client_id": 1, "ready": true,
			"leader_protocol_version": 1, "leader_binary_version": "test",
			"leader_capabilities": map[string]any{}})
		writeMsg(map[string]any{"type": "leader_ready"})

		// ACP loop
		for {
			m, err := readMsg()
			if err != nil {
				return
			}
			if m["type"] != "acp" {
				continue
			}
			payload, _ := m["payload"].(string)
			var acp map[string]any
			if err := json.Unmarshal([]byte(payload), &acp); err != nil {
				continue
			}
			id := acp["id"]
			method, _ := acp["method"].(string)
			switch method {
			case "initialize":
				writeMsg(map[string]any{"type": "acp", "payload": mustJSON(map[string]any{
					"jsonrpc": "2.0", "id": id, "result": map[string]any{"protocolVersion": 1},
				})})
			case "notifications/initialized":
				// no response
			case "session/load":
				writeMsg(map[string]any{"type": "acp", "payload": mustJSON(map[string]any{
					"jsonrpc": "2.0", "id": id, "result": map[string]any{"sessionId": "sess-test"},
				})})
			case "session/prompt":
				text := ""
				params, _ := acp["params"].(map[string]any)
				if prompt, ok := params["prompt"].([]any); ok && len(prompt) > 0 {
					if block, ok := prompt[0].(map[string]any); ok {
						text, _ = block["text"].(string)
					}
				}
				if text != "" {
					mu.Lock()
					got = append(got, text)
					mu.Unlock()
				}
				// Land user message first (adapter acks C3 here), then finish
				// turn. The chunk echoes OUR text on OUR session, like the real
				// leader (GROK-INJECT.md probe) — the landing confirm requires
				// both to match (session-bound + text-bound).
				writeMsg(map[string]any{"type": "acp", "payload": mustJSON(map[string]any{
					"jsonrpc": "2.0", "method": "session/update",
					"params": map[string]any{
						"sessionId": "sess-test",
						"update": map[string]any{
							"sessionUpdate": "user_message_chunk",
							"content":       map[string]any{"type": "text", "text": text},
						},
					},
				})})
				// Small delay then final result (exercises pendingDrain path).
				time.Sleep(20 * time.Millisecond)
				writeMsg(map[string]any{"type": "acp", "payload": mustJSON(map[string]any{
					"jsonrpc": "2.0", "id": id, "result": map[string]any{"stopReason": "end_turn"},
				})})
			default:
				writeMsg(map[string]any{"type": "acp", "payload": mustJSON(map[string]any{
					"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32601, "message": "nope"},
				})})
			}
		}
	}()
	cleanup = func() {
		_ = ln.Close()
		<-done
	}
	return sockPath, prompts, cleanup
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func TestLeaderInject_SessionPrompt(t *testing.T) {
	sock, prompts, cleanup := startFakeLeader(t)
	defer cleanup()

	c := &leaderClient{
		sessionID: "sess-test",
		cwd:       t.TempDir(),
		sockPath:  sock,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Inject(ctx, "hello from telegram"); err != nil {
		t.Fatalf("Inject: %v", err)
	}
	// second inject reuses session
	if err := c.Inject(ctx, "second message"); err != nil {
		t.Fatalf("Inject 2: %v", err)
	}
	if len(*prompts) != 2 {
		t.Fatalf("prompts = %#v; want 2", *prompts)
	}
	if (*prompts)[0] != "hello from telegram" || (*prompts)[1] != "second message" {
		t.Fatalf("prompts = %#v", *prompts)
	}
}

func TestLeaderInject_NoSocket(t *testing.T) {
	c := &leaderClient{
		sessionID: "sess",
		cwd:       t.TempDir(),
		sockPath:  filepath.Join(t.TempDir(), "missing.sock"),
	}
	err := c.Inject(context.Background(), "x")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestFormatInboundTurnText(t *testing.T) {
	tid := int64(914)
	in := c3types.Inbound{
		ChatID:    -100,
		TopicID:   &tid,
		MessageID: 42,
		Text:      "hi from phone",
	}
	in.Sender.Username = "maintainer"
	in.Sender.UserID = 99
	text := formatInboundTurnText(&in)
	// Body first so TUI preview shows real content.
	if !strings.HasPrefix(text, "hi from phone") {
		t.Fatalf("body should lead, got:\n%q", text)
	}
	// Meta rides a host-owned sentinel line derived from broker fields only.
	if !strings.Contains(text, "\n\n"+c3TrailerSentinel+" — @maintainer (99) · -100/914") {
		t.Fatalf("missing sentinel meta line, got:\n%q", text)
	}
	if strings.Contains(text, "<channel") || strings.HasPrefix(text, "message:") {
		t.Fatalf("bad shape: %q", text)
	}
}

func TestFormatInboundTurnText_DelegatesMergedRender(t *testing.T) {
	in := c3types.Inbound{
		ChatID: -100, MessageID: 52, Sender: c3types.Sender{Username: "bob"},
		Merged: []c3types.MergedSource{
			{MessageID: 51, Sender: c3types.Sender{Username: "alice"}, Text: "first"},
			{MessageID: 52, Sender: c3types.Sender{Username: "bob"}, Text: "second"},
		},
		Attachments: []c3types.Attachment{{Kind: "photo", FileID: "P51", SourceMessageID: 51}},
	}
	wantShared := c3types.RenderQueuedInbound(&in)
	got := formatInboundTurnText(&in)
	if !strings.HasPrefix(got, wantShared+"\n\n"+c3TrailerSentinel) {
		t.Fatalf("Grok live render did not delegate to shared renderer:\n got: %q\nwant prefix: %q", got, wantShared)
	}
}

func TestAncestorPIDs_IncludesSelf(t *testing.T) {
	self := os.Getpid()
	m := ancestorPIDs(self)
	if !m[self] {
		t.Fatalf("expected pid %d in ancestors: %v", self, m)
	}
}
