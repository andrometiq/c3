//go:build !windows

package spawn

import (
	"errors"
	"os"
	"os/exec"
	"slices"
	"syscall"
	"testing"
	"time"
)

// TestDetached_ReapsExitedChild guards the zombie-broker fix shared by both
// adapters. spawnBroker is racy by design: several adapters can spawn a broker
// at once, but the singleton flock lets only ONE win — the losers exit within
// milliseconds. setsid puts the child in a new session but does NOT reparent it
// to init, so without async reaping each loser lingers as a <defunct> zombie
// child of the caller until the whole process ends (the accumulation observed
// 2026-06-22).
//
// We spawn a fast-exiting child through Detached and assert the kernel reports
// it gone (ESRCH) — i.e. reaped. A zombie still answers kill(pid,0) with nil
// (the pid slot is held until reaped), so an unreaped child returns nil for the
// whole window and the test fails. (PID reuse hitting this exact freed pid
// within the window is negligible.)
func TestDetached_ReapsExitedChild(t *testing.T) {
	cmd := exec.Command("true") // resolved from PATH; exits 0 immediately, no shell
	if err := Detached(cmd); err != nil {
		t.Fatalf("Detached: %v", err)
	}
	pid := cmd.Process.Pid

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return // reaped — success
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("child pid=%d was not reaped within 3s (zombie leak)", pid)
}

// TestDetached_SetsSysProcAttr asserts Detached sets Setsid on the command so
// the spawned child detaches into its own session (and thus survives the
// caller's process-group teardown). We inspect the configured SysProcAttr
// rather than racing a live child's session id — the Start() behavior is the
// kernel's, the contract we own is "we asked for a new session".
func TestDetached_SetsSysProcAttr(t *testing.T) {
	cmd := exec.Command("true")
	if err := Detached(cmd); err != nil {
		t.Fatalf("Detached: %v", err)
	}
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setsid {
		t.Fatalf("Detached must set SysProcAttr.Setsid=true; got %+v", cmd.SysProcAttr)
	}
	if cmd.Stdin != nil || cmd.Stdout != nil || cmd.Stderr != nil {
		t.Fatalf("Detached must null all stdio; got stdin=%v stdout=%v stderr=%v",
			cmd.Stdin, cmd.Stdout, cmd.Stderr)
	}
}

// An adopted child stays in the HoldForExec list (so a second exec hands it on)
// until it exits and is reaped; a pid that is not our child is dropped at once.
func TestAdopt_ReapsInheritedChildAndKeepsItForTheNextExec(t *testing.T) {
	child := exec.Command("sleep", "30")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	pid := child.Process.Pid
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
	Adopt([]int{pid, os.Getppid()})
	if !slices.Contains(heldPIDs(), pid) {
		t.Fatalf("adopted child %d missing from the handoff %v; a second exec would orphan its waiter", pid, heldPIDs())
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if left := heldPIDs(); errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) && !slices.Contains(left, pid) && !slices.Contains(left, os.Getppid()) {
			return
		}
	}
	t.Fatalf("adopted child %d not reaped, or the handoff kept %v", pid, heldPIDs())
}

func heldPIDs() []int {
	pids, release := HoldForExec()
	release()
	return pids
}
