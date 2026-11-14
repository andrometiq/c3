package osutil

import "golang.org/x/sys/unix"

// processStateZombie is SZOMB from <sys/proc.h>: exited, awaiting collection by
// its parent.
const processStateZombie = 5

// IsZombie reports whether pid has exited but its parent has not reaped it yet;
// see the Linux sibling. An unreadable process record reports false.
func IsZombie(pid int) bool {
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	return err == nil && info.Proc.P_stat == processStateZombie
}
