package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The SDK's buffered stdio reader may prefetch the next request. This transport
// instead owns every consumed byte under the same lock as the exec decision.
// An incomplete frame, a dispatched call, or a writing response blocks exec.
// Kernel-buffered input stays on the inherited descriptor across exec.
type upgradeTransport struct {
	mu            sync.Mutex
	outputMu      sync.Mutex
	readAvailable func([]byte) (int, error)
	output        io.Writer
	buffer        []byte
	calls         map[jsonrpc.ID]bool
	notifications int
	writes        int
	state         mcp.ServerSessionState
	closed        bool
	resumeReads   chan struct{} // nonnil while an upgrade retry reconnects
}

func (t *upgradeTransport) Connect(context.Context) (mcp.Connection, error) { return t, nil }
func (t *upgradeTransport) SessionID() string                               { return "" }
func (t *upgradeTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	if t.resumeReads != nil {
		close(t.resumeReads)
		t.resumeReads = nil
	}
	return nil
}
func (t *upgradeTransport) Read(ctx context.Context) (jsonrpc.Message, error) {
	for {
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			return nil, io.EOF
		}
		if resume := t.resumeReads; resume != nil {
			t.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-resume:
				continue
			}
		}
		if i := bytes.IndexByte(t.buffer, '\n'); i >= 0 {
			frame := t.buffer[:i]
			msg, err := jsonrpc.DecodeMessage(frame)
			t.buffer = append([]byte(nil), t.buffer[i+1:]...)
			if err == nil {
				if req, ok := msg.(*jsonrpc.Request); ok {
					if req.ID.IsValid() {
						if t.calls == nil {
							t.calls = map[jsonrpc.ID]bool{}
						}
						t.calls[req.ID] = true
					} else if req.Method == "notifications/initialized" || req.Method == permissionRequestMethod {
						t.notifications++
					}
					if req.Method == "initialize" {
						_ = json.Unmarshal(req.Params, &t.state.InitializeParams)
					}
					if req.Method == "notifications/initialized" {
						t.state.InitializedParams = &mcp.InitializedParams{}
					}
					if req.Method == "logging/setLevel" {
						var p mcp.SetLoggingLevelParams
						if json.Unmarshal(req.Params, &p) == nil {
							t.state.LogLevel = p.Level
						}
					}
				}
			}
			t.mu.Unlock()
			return msg, err
		}
		var buf [8 * 1024]byte
		n, err := t.readAvailable(buf[:])
		t.buffer = append(t.buffer, buf[:n]...)
		t.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if n > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func (t *upgradeTransport) Write(ctx context.Context, msg jsonrpc.Message) error {
	t.mu.Lock()
	t.writes++
	t.mu.Unlock()
	defer func() { t.mu.Lock(); t.writes--; t.mu.Unlock() }()
	data, err := jsonrpc.EncodeMessage(msg)
	if err != nil {
		return err
	}
	t.outputMu.Lock()
	_, err = t.output.Write(append(data, '\n')) // unbuffered: return means stdout flushed
	t.outputMu.Unlock()
	if err == nil {
		if response, ok := msg.(*jsonrpc.Response); ok {
			t.mu.Lock()
			delete(t.calls, response.ID)
			t.mu.Unlock()
		}
	}
	return err
}
func (t *upgradeTransport) notificationDone() { t.mu.Lock(); t.notifications--; t.mu.Unlock() }
func (t *upgradeTransport) busy() bool {
	return len(t.buffer) > 0 || len(t.calls) > 0 || t.notifications > 0 || t.writes > 0
}
