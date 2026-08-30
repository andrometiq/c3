package main

import (
	"bytes"
	"io"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/ipc"
)

// safeBuffer wraps bytes.Buffer with a mutex so concurrent writes from the
// SDK's transport (which holds an internal write mutex of its own but
// shares the buffer through our test wrappers) don't race the test's
// reads.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]byte, b.buf.Len())
	copy(out, b.buf.Bytes())
	return out
}

// nopCloseReader / nopCloseWriter wrap an io.Reader / io.Writer with a
// no-op Close so they satisfy mcp.IOTransport's io.ReadCloser /
// io.WriteCloser requirements.
type nopCloseReader struct{ io.Reader }

func (nopCloseReader) Close() error { return nil }

type nopCloseWriter struct{ io.Writer }

func (nopCloseWriter) Close() error { return nil }

// newInMemoryTransports is a thin alias for mcp.NewInMemoryTransports
// kept as a helper so existing test bodies read cleanly.
func newInMemoryTransports() (*mcp.InMemoryTransport, *mcp.InMemoryTransport) {
	return mcp.NewInMemoryTransports()
}

// newTestClient constructs a minimal mcp.Client used in adapter tests.
func newTestClient() *mcp.Client {
	return mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.0.0"}, nil)
}

// setTestOutputRoute seeds the same coherent route snapshot production receives
// from attach/recover results. Tests that only need a topic label must not write
// attachedTopic independently of routes/outputRoute.
func setTestOutputRoute(a *adapter, name string) {
	route := ipc.RouteRef{Channel: "telegram", Name: name}
	a.setRouteState([]ipc.RouteRef{route}, &route)
}
