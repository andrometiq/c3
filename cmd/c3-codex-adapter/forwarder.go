package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/gorilla/websocket"
)

// errCodexForwardNoWS signals that forwarding is enabled but no app-server WS URL
// is configured, so the forward delivered NOTHING. It is returned (not nil) so the
// caller does NOT ack: acking a no-op forward would let the broker consume the
// queued copy of a message the agent never received live — a silent drop. It is a
// distinct sentinel (not a generic error) so the caller can stay quiet about this
// benign (mis)configuration instead of logging a "failure" per inbound.
var errCodexForwardNoWS = errors.New("codex forward: no app-server WS URL configured (C3_CODEX_APP_SERVER_WS unset)")

type codexForwardConfig struct {
	WSURL    string
	ThreadID string
	CWD      string
	Timeout  time.Duration
}

type codexWSClient struct {
	conn    *websocket.Conn
	nextID  int
	timeout time.Duration
}

func forwardInboundToCodexAppServer(ctx context.Context, in *c3types.Inbound, cfg codexForwardConfig) error {
	if cfg.WSURL == "" {
		return errCodexForwardNoWS
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}
	dialer := websocket.Dialer{HandshakeTimeout: cfg.Timeout}
	conn, _, err := dialer.DialContext(ctx, cfg.WSURL, nil)
	if err != nil {
		return fmt.Errorf("dial codex app-server: %w", err)
	}
	defer conn.Close()

	client := &codexWSClient{conn: conn, timeout: cfg.Timeout}
	if _, err := client.request(ctx, "initialize", codexInitializeParams()); err != nil {
		return err
	}
	if err := client.notify("initialized", nil); err != nil {
		return err
	}

	threadID := cfg.ThreadID
	if threadID == "" {
		threadID, err = client.discoverThread(ctx, cfg.CWD)
		if err != nil {
			return err
		}
	}
	if threadID == "" {
		return fmt.Errorf("no loaded Codex thread found")
	}
	if _, err := client.request(ctx, "thread/resume", map[string]any{
		"threadId":     threadID,
		"excludeTurns": true,
	}); err != nil {
		return err
	}
	_, err = client.request(ctx, "turn/start", map[string]any{
		"threadId": threadID,
		"input": []map[string]any{{
			"type":          "text",
			"text":          formatInboundTurnText(in),
			"text_elements": []any{},
		}},
	})
	return err
}

// codexInitializeParams is the app-server `initialize` payload every C3 → Codex
// WebSocket call sends. Shared by the inbound forwarder and the session-identity
// probe (recover.go) so both introduce themselves to Codex identically.
func codexInitializeParams() map[string]any {
	return map[string]any{
		"clientInfo": map[string]any{
			"name":    "c3-codex-bridge",
			"title":   "C3 Codex bridge",
			"version": adapterVersion,
		},
		"capabilities": map[string]any{
			"experimentalApi": true,
			"optOutNotificationMethods": []string{
				"item/agentMessage/delta",
				"item/reasoning/textDelta",
				"item/reasoning/summaryTextDelta",
			},
		},
	}
}

func (c *codexWSClient) discoverThread(ctx context.Context, cwd string) (string, error) {
	loadedResp, err := c.request(ctx, "thread/loaded/list", map[string]any{"limit": 20})
	if err != nil {
		return "", err
	}
	loaded := stringSlice(loadedResp["data"])
	if len(loaded) == 0 {
		return "", nil
	}
	if len(loaded) == 1 {
		return loaded[0], nil
	}

	listResp, err := c.request(ctx, "thread/list", map[string]any{
		"limit":          50,
		"sortKey":        "updated_at",
		"sortDirection":  "desc",
		"cwd":            cwd,
		"useStateDbOnly": true,
	})
	if err != nil {
		return "", err
	}
	loadedSet := map[string]bool{}
	for _, id := range loaded {
		loadedSet[id] = true
	}
	if threads, ok := listResp["data"].([]any); ok {
		for _, raw := range threads {
			thread, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			id := fmt.Sprint(thread["id"])
			if loadedSet[id] {
				return id, nil
			}
		}
	}
	return loaded[0], nil
}

func (c *codexWSClient) request(ctx context.Context, method string, params map[string]any) (map[string]any, error) {
	c.nextID++
	id := c.nextID
	msg := map[string]any{"id": id, "method": method}
	if params != nil {
		msg["params"] = params
	}
	if err := c.conn.WriteJSON(msg); err != nil {
		return nil, fmt.Errorf("%s: write: %w", method, err)
	}
	deadline := time.Now().Add(c.timeout)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		_ = c.conn.SetReadDeadline(deadline)
		var resp map[string]any
		if err := c.conn.ReadJSON(&resp); err != nil {
			return nil, fmt.Errorf("%s: read: %w", method, err)
		}
		if gotID, ok := numericID(resp["id"]); ok && gotID == id {
			if rawErr, ok := resp["error"]; ok && rawErr != nil {
				encoded, _ := json.Marshal(rawErr)
				return nil, fmt.Errorf("%s: %s", method, encoded)
			}
			if result, ok := resp["result"].(map[string]any); ok {
				return result, nil
			}
			return map[string]any{}, nil
		}
		if _, hasID := resp["id"]; hasID {
			if _, hasMethod := resp["method"]; hasMethod {
				_ = c.conn.WriteJSON(map[string]any{
					"id": resp["id"],
					"error": map[string]any{
						"code":    -32601,
						"message": "c3 codex bridge does not handle app-server requests",
					},
				})
			}
		}
	}
}

func (c *codexWSClient) notify(method string, params map[string]any) error {
	msg := map[string]any{"method": method}
	if params != nil {
		msg["params"] = params
	}
	if err := c.conn.WriteJSON(msg); err != nil {
		return fmt.Errorf("%s: write notify: %w", method, err)
	}
	return nil
}

// formatInboundTurnText renders one inbound as a Codex turn. It delegates to the
// shared c3types.RenderQueuedInbound so the LIVE-forward turn is byte-identical to
// what the SAME message would render as via fetch_queue (D-RC1 + task #55,
// 2026-07-24): live push and the queued readback must produce the same trimmed
// format. The trimmed form keeps message_id + full reply context (the metadata the
// agent needs to thread a reply) and a compact kind+file_id attachment reference,
// while dropping the verbose per-message attachment block the maintainer flagged.
func formatInboundTurnText(in *c3types.Inbound) string {
	return c3types.RenderQueuedInbound(in)
}

func stringSlice(v any) []string {
	items, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, fmt.Sprint(item))
	}
	return out
}

func numericID(v any) (int, bool) {
	switch x := v.(type) {
	case float64:
		return int(x), true
	case int:
		return x, true
	case json.Number:
		n, err := x.Int64()
		return int(n), err == nil
	default:
		return 0, false
	}
}

func codexForwardConfigFromEnv() codexForwardConfig {
	cwd := os.Getenv("C3_CODEX_CWD")
	if cwd == "" {
		cwd, _ = os.Getwd()
	}
	return codexForwardConfig{
		WSURL:    os.Getenv("C3_CODEX_APP_SERVER_WS"),
		ThreadID: os.Getenv("C3_CODEX_THREAD_ID"),
		CWD:      cwd,
		Timeout:  15 * time.Second,
	}
}
