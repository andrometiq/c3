// notifyTransport is a thin wrapper around an mcp.Transport that captures
// the live mcp.Connection so the adapter can emit MCP notifications under
// arbitrary method strings (specifically Claude Code's custom
// `notifications/claude/channel` method that the official SDK doesn't
// register in its sending-method table).
//
// SDK background: the modelcontextprotocol/go-sdk's ServerSession only
// exposes typed Notify* methods for spec-defined notifications
// (NotifyProgress, Log, …). Custom notification methods are not in
// `clientMethodInfos`, so `defaultSendingMethodHandler` returns
// `jsonrpc2.ErrNotHandled`. The supported escape hatch is the Transport
// layer: a Transport hands out a `mcp.Connection`, and `Connection.Write`
// accepts any `jsonrpc.Message`. We hold the live Connection in this
// wrapper and synthesize `*jsonrpc.Request` notifications (no ID → it
// serializes as a notification) with our custom method on demand.
//
// Wire shape: `jsonrpc.EncodeMessage(msg) + '\n'` (see SDK's
// transport.go:617-665 ioConn.Write). This is byte-identical to what
// the hand-rolled MCP server emitted before migration:
//
//	{"jsonrpc":"2.0","method":"notifications/claude/channel","params":{…}}\n
package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// permissionRequestMethod is the exact custom MCP notification method Claude
// Code's harness emits to relay a tool-use permission prompt. The Go MCP SDK
// will NOT deliver it to a handler (checkRequest rejects unknown methods before
// middleware runs and there is no setNotificationHandler equivalent), so the
// receive interceptor below diverts it off the SDK's read path. Every OTHER frame
// passes through byte-for-byte.
const permissionRequestMethod = "notifications/claude/channel/permission_request"

// permRequestHandler is dispatched asynchronously for each diverted
// permission_request frame, carrying the parsed
// {request_id, tool_name, preview}. preview is input_preview (falling back to
// description) plus transcript offsets captured before asynchronous dispatch.
// Set via SetPermissionHandler BEFORE Connect; nil means a diverted frame is
// simply dropped (logging-only fallback).
type permRequestHandler func(requestID, toolName, preview string, snapshot permissionSnapshot)

// notifyTransport wraps an inner Transport and exposes the live Connection
// for custom notifications. It ALSO wraps the inbound side: the Connection it
// hands back from Connect intercepts permission_request frames (see interceptConn)
// so the adapter can relay them, while passing every other frame through untouched.
type notifyTransport struct {
	inner     mcp.Transport
	writeSlot chan struct{}

	mu       sync.Mutex
	conn     mcp.Connection
	permFunc permRequestHandler
	permSnap func() permissionSnapshot
}

func newNotifyTransport(inner mcp.Transport) *notifyTransport {
	return &notifyTransport{inner: inner, writeSlot: make(chan struct{}, 1)}
}

// SetPermissionHandler installs the callback invoked for diverted
// permission_request frames. Call it BEFORE Connect (the live session sets it
// once at startup). Safe to call concurrently; the wrapped Read reads the handler
// under the same lock so a late set is still observed.
func (t *notifyTransport) SetPermissionHandler(h permRequestHandler) {
	t.mu.Lock()
	t.permFunc = h
	t.mu.Unlock()
}

func (t *notifyTransport) SetPermissionSnapshotter(snapshotter func() permissionSnapshot) {
	t.mu.Lock()
	t.permSnap = snapshotter
	t.mu.Unlock()
}

func (t *notifyTransport) permissionDispatch() (permRequestHandler, func() permissionSnapshot) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.permFunc, t.permSnap
}

// Connect implements mcp.Transport. It delegates to the wrapped transport,
// captures the resulting Connection for the send side (Notify), and returns the
// Connection WRAPPED so the SDK's read loop runs through the permission
// interceptor.
func (t *notifyTransport) Connect(ctx context.Context) (mcp.Connection, error) {
	c, err := t.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.conn = c
	t.mu.Unlock()
	return &interceptConn{Connection: c, owner: t}, nil
}

// interceptConn wraps an mcp.Connection and diverts permission_request
// NOTIFICATIONS off the SDK's read path. It mirrors the SDK's loggingConn shape:
// embed the delegate (so Write/Close/SessionID pass straight through) and override
// only Read. CRITICAL: every frame that is not the exact permission_request
// notification is returned UNCHANGED — this is on the inbound path the whole
// session depends on.
//
// Accepted SDK deviation: embedding the Connection interface means the wrapper
// fails the SDK's unexported `serverConnection` type assertion, so the server
// skips one call to the inner conn's `sessionUpdated` (which only records the
// negotiated protocol version, used solely to reject incoming JSON-RPC batches on
// protocol >= 2025-06-18). Harmless on the CC stdio path (CC sends no batches;
// the worst case is leniency, not a crash or a security hole), and the SDK's own
// shipped loggingConn wrapper skips it identically. `hasSessionID` still passes
// (SessionID is exported and promoted).
type interceptConn struct {
	mcp.Connection
	owner *notifyTransport
}

// Read delegates to the inner Connection. If the next frame is a
// permission_request notification (a *jsonrpc.Request with NO id whose Method is
// permissionRequestMethod), it is diverted to the permission handler and the loop
// reads the NEXT frame, so the SDK never sees it. Any other frame — request,
// notification, response, error — is returned exactly as the inner conn produced
// it.
func (c *interceptConn) Read(ctx context.Context) (jsonrpc.Message, error) {
	for {
		msg, err := c.Connection.Read(ctx)
		if err != nil {
			return msg, err
		}
		req, ok := msg.(*jsonrpc.Request)
		// Pass through anything that is not exactly a permission_request
		// NOTIFICATION (a request carrying an id is a CALL, not the notification we
		// divert; a Response is not a *jsonrpc.Request at all).
		if !ok || req.ID.IsValid() || req.Method != permissionRequestMethod {
			return msg, nil
		}
		if h, snapshotter := c.owner.permissionDispatch(); h != nil {
			if id, tool, preview := parsePermissionRequest(req.Params); id != "" {
				var snapshot permissionSnapshot
				if snapshotter != nil {
					snapshot = snapshotter()
				}
				// Dispatch ASYNC: the handler writes to the broker (which then does a
				// blocking Telegram send), and this runs on the SDK's single inbound
				// read goroutine — calling it synchronously would let a broker/Telegram
				// stall freeze the whole session's inbound path. Per-request ordering is
				// irrelevant (each relay is keyed by request_id), so a goroutine is safe
				// and matches the fire-and-forget contract.
				go h(id, tool, preview, snapshot)
			}
		}
		// Diverted — loop to read the next frame so the SDK never sees this one.
	}
}

// parsePermissionRequest extracts {request_id, tool_name, preview} from a
// permission_request notification's params. The harness payload is
// {request_id, tool_name, description, input_preview}; preview prefers
// input_preview (the truncated input snippet) and falls back to description. A
// missing/garbled payload yields "" so the caller drops it.
func parsePermissionRequest(params json.RawMessage) (requestID, toolName, preview string) {
	if len(params) == 0 {
		return "", "", ""
	}
	var p struct {
		RequestID    string `json:"request_id"`
		ToolName     string `json:"tool_name"`
		Description  string `json:"description"`
		InputPreview string `json:"input_preview"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return "", "", ""
	}
	preview = p.InputPreview
	if preview == "" {
		preview = p.Description
	}
	return p.RequestID, p.ToolName, preview
}

// Disconnect clears the stored Connection so a subsequent Notify will
// return the "connection not yet established" sentinel error rather
// than writing to a possibly-dead Connection.
//
// The adapter does NOT call this today: the SDK's IOTransport /
// StdioTransport do not transparently reconnect — they fail-and-let-
// caller-restart, and the adapter restarts the whole process on a
// dropped transport. The method exists as preventative scaffolding for
// a future SDK version that adds transparent reconnect. If/when the
// SDK gains that capability, the adapter should call Disconnect
// before the reconnect path so the wrapper's captured Connection
// reference doesn't outlive the underlying transport.
//
// Closes report MINOR m2 (2026-05-19).
func (t *notifyTransport) Disconnect() {
	t.mu.Lock()
	t.conn = nil
	t.mu.Unlock()
}

// Notify writes a JSON-RPC notification with the given method and params
// onto the captured connection. params is JSON-marshaled with the same
// settings the SDK uses internally (no HTML escaping is handled by the
// jsonrpc encoder downstream from us).
func (t *notifyTransport) Notify(ctx context.Context, method string, params any) error {
	ctx, cancel := context.WithTimeout(ctx, liveReadbackWindow)
	defer cancel()
	t.mu.Lock()
	c := t.conn
	t.mu.Unlock()
	if c == nil {
		return errors.New("notifyTransport: connection not yet established")
	}
	var raw json.RawMessage
	if params != nil {
		data, err := json.Marshal(params)
		if err != nil {
			return err
		}
		raw = data
	}
	// A jsonrpc.Request with no ID serializes as a notification
	// (id field omitted via omitempty on the wire struct).
	msg := &jsonrpc.Request{Method: method, Params: raw}
	// The SDK checks context only before Write, so a stalled stdout cannot be
	// interrupted there. Bound admission to one active write and let cancellation
	// release the caller; no later notifications accumulate behind a stuck writer.
	select {
	case t.writeSlot <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-t.writeSlot
		return err
	}
	done := make(chan error, 1)
	go func() {
		defer func() { <-t.writeSlot }()
		done <- c.Write(ctx, msg)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
