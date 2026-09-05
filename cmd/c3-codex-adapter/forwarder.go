package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
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
	QueueBin string
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
	if cfg.QueueBin != "" {
		return forwardInboundToCodexQueue(ctx, in, cfg)
	}
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
	// Modern Codex accepts durable input while the visible TUI is busy. Only
	// an explicit method-not-found permits the legacy turn/start fallback:
	// a timeout may mean the queue accepted the message but its reply was lost.
	queued, err := client.request(ctx, "thread/queue/add", map[string]any{
		"threadId":            threadID,
		"clientUserMessageId": codexInboundMessageID(threadID, in),
		"input":               []map[string]any{{"type": "text", "text": formatInboundTurnText(in), "text_elements": []any{}}},
	})
	if err == nil {
		if submission, ok := queued["queuedSubmission"].(map[string]any); ok {
			if id, ok := submission["id"].(string); ok && id != "" {
				return nil
			}
		}
		return errors.New("Codex queue did not confirm a queued submission")
	}
	var rpcErr *codexRPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != -32601 {
		return err
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

func codexInboundMessageID(threadID string, in *c3types.Inbound) string {
	// Stable across retries but different for edits and different recipients.
	h := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%d\x00%s", threadID, in.Channel, in.ChatID, in.MessageID, formatInboundTurnText(in))))
	h[6] = h[6]&0x0f | 0x50
	h[8] = h[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", h[:4], h[4:6], h[6:8], h[8:10], h[10:16])
}

type codexRPCError struct {
	Method  string
	Code    int
	Message string
}

func (e *codexRPCError) Error() string {
	return fmt.Sprintf("%s: RPC error %d: %s", e.Method, e.Code, e.Message)
}

var codexThreadUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Native queue delivery reaches the existing local TUI without creating or
// resuming a second app-server thread. Explicit executable and UUID pins are
// required: never infer a recipient from cwd, a session name, or loaded[0].
// A successful queue acknowledgement transfers durability to Codex; while the
// TUI is busy, it handles queued input after the current turn.
func forwardInboundToCodexQueue(ctx context.Context, in *c3types.Inbound, cfg codexForwardConfig) error {
	if !filepath.IsAbs(cfg.QueueBin) || !codexThreadUUID.MatchString(cfg.ThreadID) {
		return errors.New("codex queue requires an absolute C3_CODEX_QUEUE_BIN and explicit C3_CODEX_THREAD_ID UUID")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, cfg.QueueBin, "queue", "--thread", cfg.ThreadID, "--message", formatInboundTurnText(in))
	cmd.Dir = cfg.CWD
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("codex queue did not accept inbound: %w", err)
	}
	ack := strings.TrimSpace(string(output))
	if !strings.HasPrefix(ack, "Queued message ") || !strings.HasSuffix(ack, " for thread "+cfg.ThreadID+".") {
		return errors.New("codex queue returned no acknowledgement for the pinned thread")
	}
	return nil
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
	loaded, err := loadedThreadIDs(loadedResp["data"])
	if err != nil {
		return "", err
	}
	if len(loaded) == 0 {
		return "", nil
	}
	if len(loaded) == 1 {
		return loaded[0], nil
	}

	return "", fmt.Errorf("%w: pin C3_CODEX_THREAD_ID; cwd %q does not identify a conversation", errCodexThreadAmbiguous, cwd)
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
				if rpc, ok := rawErr.(map[string]any); ok {
					if code, ok := numericID(rpc["code"]); ok {
						message, _ := rpc["message"].(string)
						return nil, &codexRPCError{Method: method, Code: code, Message: message}
					}
				}
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
	if in.IsEvent() {
		if in.Event != nil && in.Event.System != nil {
			return "C3 system notice: " + in.Event.System.Message
		}
		payload, _ := json.Marshal(in.Event)
		return fmt.Sprintf("C3 %s event: %s", in.Kind, payload)
	}
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
		QueueBin: os.Getenv("C3_CODEX_QUEUE_BIN"),
		WSURL:    os.Getenv("C3_CODEX_APP_SERVER_WS"),
		ThreadID: os.Getenv("C3_CODEX_THREAD_ID"),
		CWD:      cwd,
		Timeout:  15 * time.Second,
	}
}
