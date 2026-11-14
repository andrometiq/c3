package main

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// A recorded app-server that exited but was never reaped is not running.
func TestAppServerPortOwner_ZombieIsNoLongerRunning(t *testing.T) {
	child := exec.Command("true")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Wait() })
	var info unix.Siginfo
	// WNOWAIT leaves the exited child unreaped: a real zombie.
	if err := unix.Waitid(unix.P_PID, child.Process.Pid, &info, unix.WEXITED|unix.WNOWAIT, nil); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{"pid": child.Process.Pid})
	if err != nil {
		t.Fatal(err)
	}
	got := appServerPortOwnerWithReader("ws://127.0.0.1:32123", func(string) ([]byte, error) { return data, nil })
	if !strings.Contains(got, "no longer running") {
		t.Fatalf("zombie app-server described as live: %q", got)
	}
}
