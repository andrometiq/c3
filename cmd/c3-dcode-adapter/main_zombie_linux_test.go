package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// An event socket left by a TUI that exited but was never reaped must not make
// the one live socket ambiguous.
func TestUniqueLiveEventSocket_IgnoresZombieOwner(t *testing.T) {
	zombie := exec.Command("true")
	if err := zombie.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = zombie.Wait() })
	var info unix.Siginfo
	// WNOWAIT leaves the exited child unreaped: a real zombie.
	if err := unix.Waitid(unix.P_PID, zombie.Process.Pid, &info, unix.WEXITED|unix.WNOWAIT, nil); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	live := filepath.Join(dir, fmt.Sprintf("events-%d.sock", os.Getpid()))
	for _, path := range []string{live, filepath.Join(dir, fmt.Sprintf("events-%d.sock", zombie.Process.Pid))} {
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if got := uniqueLiveEventSocket(dir); got != live {
		t.Fatalf("defect: zombie owner pid %d counted as live; got %q, want %q", zombie.Process.Pid, got, live)
	}
}
