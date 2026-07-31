// logNotifyTransport wraps an mcp.Transport and captures the live
// mcp.Connection so the adapter can emit notifications under arbitrary
// method names. Used for `notifications/message` log frames in the Cursor
// adapter (the SDK's typed Log API enforces level filtering and would
// rather not be drilled through for the unfiltered shape the broker
// expects; the cleanest path is to write the raw notification ourselves
// via the underlying Connection).
//
// Mirrors cmd/c3-claude-adapter/notify_transport.go — same rationale,
// same wire-shape guarantees. See that file for the full SDK background.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type logNotifyTransport struct {
	inner mcp.Transport

	mu   sync.Mutex
	conn mcp.Connection
}

func newLogNotifyTransport(inner mcp.Transport) *logNotifyTransport {
	return &logNotifyTransport{inner: inner}
}

// Connect implements mcp.Transport.
func (t *logNotifyTransport) Connect(ctx context.Context) (mcp.Connection, error) {
	c, err := t.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.conn = c
	t.mu.Unlock()
	return c, nil
}

// Disconnect clears the stored Connection so a subsequent Notify will
// return the "connection not yet established" sentinel error rather
// than writing to a possibly-dead Connection.
func (t *logNotifyTransport) Disconnect() {
	t.mu.Lock()
	t.conn = nil
	t.mu.Unlock()
}

// Notify writes a JSON-RPC notification with the given method and params
// onto the captured connection.
func (t *logNotifyTransport) Notify(ctx context.Context, method string, params any) error {
	t.mu.Lock()
	c := t.conn
	t.mu.Unlock()
	if c == nil {
		return errors.New("logNotifyTransport: connection not yet established")
	}
	var raw json.RawMessage
	if params != nil {
		data, err := json.Marshal(params)
		if err != nil {
			return err
		}
		raw = data
	}
	msg := &jsonrpc.Request{Method: method, Params: raw}
	return c.Write(ctx, msg)
}
