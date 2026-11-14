package main

import (
	"os/exec"
	"testing"

	"golang.org/x/sys/unix"
)

// A TUI that exited but was never reaped must not count as a second live
// session and turn the single live one into an ambiguity.
func TestSessionIDFromActiveSessions_ZombieIsNotAlive(t *testing.T) {
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
	home := t.TempDir()
	t.Setenv("GROK_HOME", home)
	writeActiveSessionsFile(t, home, []activeSession{
		{SessionID: "sess-zombie", PID: zombie.Process.Pid, CWD: "/x"},
		{SessionID: "sess-live", PID: 1, CWD: "/y"},
	})
	if got := sessionIDFromActiveSessions(1 << 30); got != "sess-live" {
		t.Fatalf("defect: zombie TUI pid %d counted as live; resolved %q, want sess-live", zombie.Process.Pid, got)
	}
}
