package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// safeBuffer is a mutex-guarded bytes.Buffer so the transport's writer goroutine
// and the test's reader don't race.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type nopCloseReader struct{ io.Reader }

func (nopCloseReader) Close() error { return nil }

type nopCloseWriter struct{ io.Writer }

func (nopCloseWriter) Close() error { return nil }

// adapterWithCaptureTransport wires a Codex adapter whose transport writes
// newline-delimited JSON-RPC to buf, so handleInbound's nudge frame can be
// inspected.
func adapterWithCaptureTransport(t *testing.T) (*adapter, *safeBuffer) {
	t.Helper()
	a := newAdapter()
	buf := &safeBuffer{}
	tx := newLogNotifyTransport(&mcp.IOTransport{
		Reader: nopCloseReader{strings.NewReader("")},
		Writer: nopCloseWriter{buf},
	})
	if _, err := tx.Connect(context.Background()); err != nil {
		t.Fatalf("transport Connect: %v", err)
	}
	a.transport = tx
	return a, buf
}

// notifyData extracts the params.data string from the first
// notifications/message frame written to buf.
func notifyData(t *testing.T, buf *safeBuffer) string {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var req struct {
			Method string `json:"method"`
			Params struct {
				Data string `json:"data"`
			} `json:"params"`
		}
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			continue
		}
		if req.Method == "notifications/message" {
			return req.Params.Data
		}
	}
	t.Fatalf("no notifications/message frame found in transport output: %q", buf.String())
	return ""
}

// I6: the Codex pending nudge must count Pending + Covered (the true number still
// queued — Codex never sends OpInboundDelivered, so the just-pushed Covered lines
// are also still queued). A push with Pending=2, Covered=1 must nudge "3 pending".
func TestHandleInbound_Codex_NudgeCountsPendingPlusCovered(t *testing.T) {
	a, buf := adapterWithCaptureTransport(t)

	msg := ipc.InboundMsg{
		Op:      ipc.OpInbound,
		Pending: 2,
		Covered: 1,
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 9, Text: "hi"},
	}
	raw, _ := json.Marshal(msg)
	a.handleInbound(raw)

	data := notifyData(t, buf)
	if !strings.Contains(data, "3 pending") {
		t.Fatalf("nudge data = %q, want '3 pending' (Pending 2 + Covered 1)", data)
	}
}

// I6 floor: a push reporting zero Pending/Covered (e.g. an older broker that does
// not stamp them) must still nudge at least "1 pending" — this push delivered at
// least itself.
func TestHandleInbound_Codex_NudgeFloorsAtOne(t *testing.T) {
	a, buf := adapterWithCaptureTransport(t)

	msg := ipc.InboundMsg{
		Op:      ipc.OpInbound,
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 10, Text: "hi"},
	}
	raw, _ := json.Marshal(msg)
	a.handleInbound(raw)

	data := notifyData(t, buf)
	if !strings.Contains(data, "1 pending") {
		t.Fatalf("nudge data = %q, want a '1 pending' floor when Pending+Covered is 0", data)
	}
}
