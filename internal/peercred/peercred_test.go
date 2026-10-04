//go:build linux || darwin

package peercred

import (
	"net"
	"path/filepath"
	"testing"
)

func TestVerifySameUserAcceptsOwnSocket(t *testing.T) {
	listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "s.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		if conn, err := listener.Accept(); err == nil {
			defer conn.Close()
			_, _ = conn.Read(make([]byte, 1))
		}
	}()
	conn, err := net.Dial("unix", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := VerifySameUser(conn); err != nil {
		t.Fatalf("own socket refused: %v", err)
	}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	if VerifySameUser(client) == nil {
		t.Fatal("a non-unix connection was accepted")
	}
}
