package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/ipc"
)

// scriptedConn is a fake mcp.Connection that returns a fixed sequence of frames
// from Read, then io.EOF. Write/Close/SessionID are no-ops. Used to exercise the
// permission interceptor's Read wrapper in isolation.
type scriptedConn struct {
	frames []jsonrpc.Message
	idx    int
}

func (c *scriptedConn) Read(context.Context) (jsonrpc.Message, error) {
	if c.idx >= len(c.frames) {
		return nil, io.EOF
	}
	m := c.frames[c.idx]
	c.idx++
	return m, nil
}
func (c *scriptedConn) Write(context.Context, jsonrpc.Message) error { return nil }
func (c *scriptedConn) Close() error                                 { return nil }
func (c *scriptedConn) SessionID() string                            { return "" }

// scriptedTransport hands out a scriptedConn from Connect, so a notifyTransport
// can wrap it through the real Connect path.
type scriptedTransport struct{ conn mcp.Connection }

func (t *scriptedTransport) Connect(context.Context) (mcp.Connection, error) {
	return t.conn, nil
}

func mustID(t *testing.T, v any) jsonrpc.ID {
	t.Helper()
	id, err := jsonrpc.MakeID(v)
	if err != nil {
		t.Fatalf("MakeID(%v): %v", v, err)
	}
	return id
}

// TestInterceptConn_PassThrough is THE critical regression guard: every frame
// that is NOT a permission_request notification must pass through the wrapped
// Read byte-for-byte (and as the identical message object). The whole session's
// inbound path runs through this wrapper, so a bug here breaks everything.
func TestInterceptConn_PassThrough(t *testing.T) {
	ctx := context.Background()

	// A representative spread of frames the SDK actually sends/receives.
	callWithID := &jsonrpc.Request{ID: mustID(t, float64(1)), Method: "tools/call", Params: json.RawMessage(`{"name":"reply"}`)}
	initialized := &jsonrpc.Request{Method: "notifications/initialized"}
	channelNote := &jsonrpc.Request{Method: "notifications/claude/channel", Params: json.RawMessage(`{"content":"hi"}`)}
	// A call whose method merely shares the permission prefix but is NOT the exact
	// permission_request method must NOT be diverted.
	prefixLookalike := &jsonrpc.Request{Method: "notifications/claude/channel/permission", Params: json.RawMessage(`{"x":1}`)}
	// A permission_request that carries an ID (a CALL, not a notification) is NOT
	// the notification we divert — pass it through untouched.
	permAsCall := &jsonrpc.Request{ID: mustID(t, float64(7)), Method: permissionRequestMethod, Params: json.RawMessage(`{"request_id":"abcde"}`)}
	resp := &jsonrpc.Response{ID: mustID(t, float64(2)), Result: json.RawMessage(`{"ok":true}`)}

	want := []jsonrpc.Message{callWithID, initialized, channelNote, prefixLookalike, permAsCall, resp}

	tx := newNotifyTransport(&scriptedTransport{conn: &scriptedConn{frames: want}})
	// Set a handler that MUST NOT fire for any of these frames.
	var fired int
	tx.SetPermissionHandler(func(string, string, string) { fired++ })
	conn, err := tx.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	for i, w := range want {
		got, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("frame %d: Read err: %v", i, err)
		}
		// Pointer identity — the strongest possible "untouched" guarantee.
		if got != w {
			t.Fatalf("frame %d: wrapped Read returned a different object than the inner conn produced", i)
		}
		// And byte-for-byte equality as a belt-and-suspenders check.
		gb, _ := jsonrpc.EncodeMessage(got)
		wb, _ := jsonrpc.EncodeMessage(w)
		if !bytes.Equal(gb, wb) {
			t.Fatalf("frame %d: wire bytes changed\n got=%s\nwant=%s", i, gb, wb)
		}
	}
	if fired != 0 {
		t.Fatalf("permission handler fired %d times for non-permission frames; want 0", fired)
	}
}

// TestInterceptConn_DivertsPermissionRequest: a permission_request NOTIFICATION
// (no id) is diverted — it is NOT returned to the SDK, the handler is invoked
// with the parsed fields, and Read returns the NEXT frame instead.
func TestInterceptConn_DivertsPermissionRequest(t *testing.T) {
	ctx := context.Background()

	permReq := &jsonrpc.Request{
		Method: permissionRequestMethod,
		Params: json.RawMessage(`{"request_id":"abcde","tool_name":"Bash","description":"run a shell command","input_preview":"rm -rf /tmp/x"}`),
	}
	next := &jsonrpc.Request{ID: mustID(t, float64(1)), Method: "tools/call", Params: json.RawMessage(`{"name":"reply"}`)}

	tx := newNotifyTransport(&scriptedTransport{conn: &scriptedConn{frames: []jsonrpc.Message{permReq, next}}})
	type capture struct{ id, tool, preview string }
	got := make(chan capture, 1)
	tx.SetPermissionHandler(func(id, tool, preview string) {
		got <- capture{id, tool, preview}
	})
	conn, err := tx.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// The first Read must SKIP the diverted permission_request and return `next`.
	msg, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if msg != next {
		t.Fatalf("Read returned the wrong frame; want the tools/call after the diverted permission_request")
	}
	select {
	case c := <-got:
		if c.id != "abcde" || c.tool != "Bash" {
			t.Fatalf("handler got %+v; want id=abcde tool=Bash", c)
		}
		if c.preview != "rm -rf /tmp/x" {
			t.Fatalf("handler preview = %q; want the input_preview", c.preview)
		}
	case <-time.After(time.Second):
		t.Fatal("permission handler was not invoked for the diverted frame")
	}
}

// TestDispatchPermissionVerdict_EmitShape: an OpPermissionVerdict from the broker
// is emitted to Claude Code as a `notifications/claude/channel/permission` frame
// carrying {request_id, behavior}.
func TestDispatchPermissionVerdict_EmitShape(t *testing.T) {
	var buf safeBuffer
	tx := newNotifyTransport(&mcp.IOTransport{
		Reader: nopCloseReader{strings.NewReader("")},
		Writer: nopCloseWriter{&buf},
	})
	if _, err := tx.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	a := newAdapter()
	a.notifyTx = tx

	a.dispatchPermissionVerdict(mustMarshal(t, ipc.PermissionVerdictMsg{
		Op: ipc.OpPermissionVerdict, RequestID: "abcde", Behavior: "allow",
	}))

	raw := buf.Bytes()
	if !bytes.HasSuffix(raw, []byte{'\n'}) {
		t.Fatalf("frame must be newline-terminated; got %q", raw)
	}
	var msg struct {
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}
	if err := json.Unmarshal(bytes.TrimRight(raw, "\n"), &msg); err != nil {
		t.Fatalf("unmarshal: %v\nwire: %s", err, raw)
	}
	if msg.Method != "notifications/claude/channel/permission" {
		t.Fatalf("method = %q; want notifications/claude/channel/permission", msg.Method)
	}
	if msg.Params["request_id"] != "abcde" {
		t.Fatalf("params.request_id = %v; want abcde", msg.Params["request_id"])
	}
	if msg.Params["behavior"] != "allow" {
		t.Fatalf("params.behavior = %v; want allow", msg.Params["behavior"])
	}
}

// TestHandlePermissionRequest_SendsToBroker: the interceptor's handler forwards a
// diverted permission_request to the broker as an OpPermissionRequest frame
// carrying the parsed fields.
func TestHandlePermissionRequest_SendsToBroker(t *testing.T) {
	a, peer := adapterWithConn(t)

	// net.Pipe is synchronous: the broker write blocks until the test reads, so
	// drive the handler from a goroutine.
	go a.handlePermissionRequest("abcde", "Bash", "rm -rf /tmp/x")

	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("read permission_request frame: %v", err)
	}
	var req ipc.PermissionReq
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if req.Op != ipc.OpPermissionRequest {
		t.Fatalf("op = %q; want %q", req.Op, ipc.OpPermissionRequest)
	}
	if req.RequestID != "abcde" || req.ToolName != "Bash" || req.Preview != "rm -rf /tmp/x" {
		t.Fatalf("unexpected payload: %+v", req)
	}
}

// TestExperimentalDeclaresPermission guards the initialize-response capability
// map: it must advertise claude/channel/permission so Claude Code routes
// permission verdicts back to this server.
func TestExperimentalDeclaresPermission(t *testing.T) {
	a := newAdapter()
	srv := a.buildMCPServer()
	if srv == nil {
		t.Fatal("buildMCPServer returned nil")
	}

	clientT, serverT := mcp.NewInMemoryTransports()
	a.notifyTx = newNotifyTransport(serverT)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = srv.Run(ctx, a.notifyTx) }()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.0.0"}, nil)
	sess, err := client.Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	defer sess.Close()
	res := sess.InitializeResult()
	if res == nil || res.Capabilities == nil || res.Capabilities.Experimental == nil {
		t.Fatal("missing experimental capabilities")
	}
	if _, ok := res.Capabilities.Experimental["claude/channel/permission"]; !ok {
		t.Fatalf("experimental missing claude/channel/permission; got %v", res.Capabilities.Experimental)
	}
}
