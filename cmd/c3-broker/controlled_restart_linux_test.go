package main

import (
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/ipc"
)

// A broker that exited but was never reaped by its parent is a zombie. kill(pid,
// 0) still succeeds on it, so a restart that took that as "still running" waited
// out its 91s budget and refused to start the successor, leaving no broker.
func TestControlledRestartTreatsZombieBrokerAsExited(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	old := exec.Command("true")
	if err := old.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = old.Wait() })
	var info unix.Siginfo
	// WNOWAIT leaves the exited child unreaped: a real zombie.
	if err := unix.Waitid(unix.P_PID, old.Process.Pid, &info, unix.WEXITED|unix.WNOWAIT, nil); err != nil {
		t.Fatal(err)
	}
	pidPath, err := broker.PidFilePath()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(pidPath, []byte(strconv.Itoa(old.Process.Pid)), 0o600); err != nil {
		t.Fatal(err)
	}
	socketPath, err := broker.SocketPath()
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		c, err := listener.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		conn := ipc.NewConn(c)
		_, _ = conn.ReadFrame()
		_ = conn.WriteJSON(ipc.HelloAckMsg{Op: ipc.OpHelloAck, ProtocolVersion: ipc.ProtocolVersion})
		_, _ = conn.ReadFrame()
		_ = conn.WriteJSON(ipc.BrokerRestartReply{Op: ipc.OpBrokerRestartReply, OK: true})
	}()
	type outcome struct {
		stopped bool
		note    string
	}
	done := make(chan outcome, 1)
	go func() {
		stopped, note := stopBrokerForControlledRestart()
		done <- outcome{stopped, note}
	}()
	select {
	case got := <-done:
		if !got.stopped || got.note != "" {
			t.Fatalf("restart did not treat the exited broker as stopped: stopped=%v note=%q", got.stopped, got.note)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("defect: restart still waiting on zombie broker pid %d; it would refuse to start the successor", old.Process.Pid)
	}
}
