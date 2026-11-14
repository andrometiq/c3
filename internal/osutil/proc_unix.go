//go:build !windows

// Package osutil holds small OS-portability helpers (signal semantics, process
// liveness/termination) so the rest of C3 can stay free of build tags. The unix
// implementation keeps the exact pre-port behaviour; a Windows sibling lives in
// proc_windows.go.
package osutil

import (
	"os"
	"syscall"
)

// ReloadSignals returns the signals that mean "reload config" (SIGHUP on unix).
func ReloadSignals() []os.Signal { return []os.Signal{syscall.SIGHUP} }

// IsReloadSignal reports whether s is a config-reload signal (SIGHUP on unix).
func IsReloadSignal(s os.Signal) bool { return s == syscall.SIGHUP }

// SignalReload asks a process to reload its config (SIGHUP on unix).
func SignalReload(pid int) error { return syscall.Kill(pid, syscall.SIGHUP) }

// TerminateProcess asks a process to shut down gracefully (SIGTERM on unix).
func TerminateProcess(pid int) error { return syscall.Kill(pid, syscall.SIGTERM) }

// ProcessSignalable reports whether the current process can signal pid — i.e.
// the process is running AND we have permission to signal it (unix:
// kill(pid,0)==nil, so ESRCH/EPERM both report false). This preserves the
// semantics of the broker's "is a live broker running (and ours)?" liveness
// probes, which treated any kill(pid,0) error as "not actionable". A zombie
// (exited, not yet reaped) still accepts kill(pid,0) but is not running, so it
// reports false: a restart waiting on it would otherwise never see it exit.
func ProcessSignalable(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(pid, 0) == nil && !IsZombie(pid)
}
