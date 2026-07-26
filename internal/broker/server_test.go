package broker

import (
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

func TestServer_AcceptsAndHandlesHello(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "test.sock")

	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	srv, err := Listen(sockPath, b)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	c, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	conn := ipc.NewConn(c)

	if err := conn.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: 1, CWD: "/x"}); err != nil {
		t.Fatal(err)
	}
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	raw, err := conn.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var ack ipc.HelloAckMsg
	if err := json.Unmarshal(raw, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Op != ipc.OpHelloAck {
		t.Errorf("op=%q, want hello_ack", ack.Op)
	}
}
