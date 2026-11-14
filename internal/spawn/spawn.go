// Package spawn centralises the detached-child launch helper shared by the
// C3 adapters (cmd/c3-claude-adapter, cmd/c3-codex-adapter). Both adapters
// fork a `c3-broker` when the socket is unreachable; the launch semantics
// (new session via setsid, no inherited stdio, async reap) must be identical
// across adapters, so the implementation lives here once instead of being
// duplicated verbatim in each adapter (D7 / recovery-hardening 2026-06-22).
package spawn

import (
	"os"
	"os/exec"
	"sync"
)

// unreaped holds the pids of children Detached or Adopt is waiting on. exec(2)
// keeps those children (same pid, same parent) but drops the goroutines waiting
// on them, so a process about to exec itself hands HoldForExec's list to its
// next image, which passes it to Adopt. Without that handoff a broker spawned
// before an adapter's self-upgrade becomes a zombie of the adapter when it
// exits. The lock also spans Detached's Start and registration, so no child can
// exist unlisted while HoldForExec holds it.
var unreaped = struct {
	sync.Mutex
	pids map[int]int // pid -> waiters, so a reused pid survives its old waiter's exit
}{pids: map[int]int{}}

// Detached starts cmd in a new session (detached from the caller's process
// group via setsid) with no inherited stdio, then reaps it asynchronously so
// a child that exits never lingers as a <defunct> zombie.
//
// Why reap: spawnBroker is racy by design — several adapters can call it at
// once, but the broker's singleton flock lets only ONE win; the losers exit
// within milliseconds. setsid creates a new SESSION but does NOT reparent the
// child to init, so until the calling process itself exits an unreaped loser
// stays a zombie child of the caller (the <defunct> accumulation observed
// 2026-06-22). The Wait() goroutine reaps it the moment it dies.
//
// Stderr is explicitly NOT inherited from the caller. The adapter's stderr is
// piped to the CLI host's plugin host; piping broker log lines through that
// channel made the plugin host appear distressed (lots of unexplained stderr
// noise during normal broker bounces), which we suspect contributes to the
// host closing the adapter's stdin during a broker restart. The broker has its
// own structured log at $XDG_STATE_HOME/c3/broker.log via SetupLogging; the
// host has no reason to see broker stderr.
func Detached(cmd *exec.Cmd) error {
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = sysSetsid()
	unreaped.Lock()
	defer unreaped.Unlock()
	if err := cmd.Start(); err != nil {
		return err
	}
	// Reap on exit so a broker that loses the flock race (or dies later) never
	// lingers as a <defunct> zombie. One goroutine per spawn; for the winning
	// broker it simply blocks for the daemon's lifetime, and if the caller
	// exits first the broker is reparented to init, which reaps it instead.
	reapInBackground(cmd.Process.Pid, cmd.Wait)
	return nil
}

// HoldForExec returns the pids this process is still waiting on and blocks
// Detached and the reapers until release runs, so the list stays exact through
// the exec(2) that follows. A successful exec never returns, so release only
// runs when the exec fails (or the caller gives up first).
func HoldForExec() (pids []int, release func()) {
	unreaped.Lock()
	pids = make([]int, 0, len(unreaped.pids))
	for pid := range unreaped.pids {
		pids = append(pids, pid)
	}
	return pids, unreaped.Unlock
}

// Adopt waits on children an earlier image of this process started before it
// exec'd (its HoldForExec list). A pid that is not this process's child fails
// its wait at once and is dropped. pid <= 0 is skipped: waiting on it would
// reap an arbitrary child (wait4(-1)) or a process group.
func Adopt(pids []int) {
	unreaped.Lock()
	defer unreaped.Unlock()
	for _, pid := range pids {
		if pid <= 0 {
			continue
		}
		process, err := os.FindProcess(pid)
		if err != nil {
			continue
		}
		reapInBackground(pid, func() error { _, err := process.Wait(); return err })
	}
}

// reapInBackground records pid and waits on it in a goroutine. The caller holds
// unreaped's lock.
func reapInBackground(pid int, wait func() error) {
	unreaped.pids[pid]++
	go func() {
		_ = wait()
		unreaped.Lock()
		if unreaped.pids[pid]--; unreaped.pids[pid] <= 0 {
			delete(unreaped.pids, pid)
		}
		unreaped.Unlock()
	}()
}
