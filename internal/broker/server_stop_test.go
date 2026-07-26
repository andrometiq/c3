package broker

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// TestServer_StopUnblocksParkedConnection is the direct regression guard for the
// SIGTERM drain-wedge. An adapter keeps a persistent connection whose HandleConn
// goroutine sits in a blocking ReadFrame. The OLD Stop() closed only the listener
// and then wg.Wait()'d on that goroutine forever — the broker logged "shutting
// down" and hung until SIGKILL. The fix closes the accepted connection too, so
// the read unblocks and Stop() returns promptly.
//
// Crucially, the test NEVER closes the client side before Stop — that is the
// whole point: a live adapter stays connected, and shutdown must not depend on it
// hanging up.
func TestServer_StopUnblocksParkedConnection(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "test.sock")

	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	srv, err := Listen(sockPath, b)
	if err != nil {
		t.Fatal(err)
	}

	c, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	conn := ipc.NewConn(c)

	// Complete the hello handshake so the server-side HandleConn is now parked in
	// its Stage-2 dispatch loop, blocked on ReadFrame — the exact wedge state.
	if err := conn.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: 1, CWD: "/x"}); err != nil {
		t.Fatal(err)
	}
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.ReadFrame(); err != nil {
		t.Fatalf("hello ack: %v", err)
	}
	c.SetReadDeadline(time.Time{}) // clear — the client now just sits connected

	// Stop must return promptly even though the client conn is still open. The
	// guard is deliberately well UNDER serverStopGrace (the fallback bound): a pass
	// here means closeConns actively unblocked the parked ReadFrame, not that the
	// bounded wait merely timed out. If closeConns regressed, Stop would take the
	// full serverStopGrace and blow this guard.
	stopped := make(chan struct{})
	go func() {
		srv.Stop()
		close(stopped)
	}()
	const guard = 2 * time.Second // < serverStopGrace, so only an active unblock passes
	select {
	case <-stopped:
	case <-time.After(guard):
		t.Fatalf("Server.Stop() did not return within %v while an adapter stayed connected — the drain-wedge is back (closeConns not unblocking the parked read)", guard)
	}
}
