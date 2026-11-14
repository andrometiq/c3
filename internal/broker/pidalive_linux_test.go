package broker

import (
	"os"
	"os/exec"
	"testing"

	"golang.org/x/sys/unix"
)

// A zombie adapter has exited: its claims must not be preserved as if it lived.
func TestIsPIDAliveTreatsZombieAsExited(t *testing.T) {
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
	if isPIDAlive(child.Process.Pid) {
		t.Fatalf("defect: zombie pid %d reported alive", child.Process.Pid)
	}
	if !isPIDAlive(os.Getpid()) {
		t.Fatal("running process reported dead")
	}
}
