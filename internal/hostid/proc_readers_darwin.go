package hostid

import "golang.org/x/sys/unix"

func PlatformProcReaders() ProcReaders {
	return DarwinProcReaders(func(pid int) ([]byte, error) {
		return unix.SysctlRaw("kern.procargs2", pid)
	}, func(pid int) (int, error) {
		info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
		if err != nil {
			return 0, err
		}
		return int(info.Eproc.Ppid), nil
	})
}
