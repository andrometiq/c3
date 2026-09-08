package ipc

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestWriteJSONContextCancelsBlockedWrite(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	conn := NewConn(client)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- conn.WriteJSONContext(ctx, map[string]string{"frame": "blocked"}) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("blocked write succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked socket ignored deadline")
	}
	// Partial writes cannot leave a reusable, desynchronized connection.
	if err := conn.WriteJSON(map[string]string{"frame": "later"}); err == nil {
		t.Fatal("timed-out connection remained writable")
	}
}

func TestWriteJSONContextCancelsWriterAdmission(t *testing.T) {
	conn, peer := newPipePair(t)
	defer conn.Close()
	defer peer.Close()
	conn.wmu <- struct{}{} // another writer owns the socket
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- conn.WriteJSONContext(ctx, map[string]string{"frame": "waiting"}) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("admission: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("writer admission ignored deadline")
	}
	<-conn.wmu
	// A cancellation before admission must not close someone else's connection.
	go func() { done <- conn.WriteJSON(map[string]string{"frame": "healthy"}) }()
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
