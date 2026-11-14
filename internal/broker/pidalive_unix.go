//go:build !windows

package broker

import (
	"syscall"

	"github.com/Andrometiq/c3/internal/osutil"
)

// isPIDAlive returns true if a process with the given PID is running.
// Sends signal 0 (a no-op) and checks for ESRCH.
func isPIDAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	// ESRCH = no such process. Anything else (e.g. EPERM) means we can't
	// signal it but it's still there. A zombie is there but has exited.
	if err := syscall.Kill(pid, 0); err == syscall.ESRCH {
		return false
	}
	return !osutil.IsZombie(pid)
}
