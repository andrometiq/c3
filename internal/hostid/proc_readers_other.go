//go:build !darwin

package hostid

func PlatformProcReaders() ProcReaders {
	return ProcReaders{Cmdline: readProcCmdline, PPID: readProcPPID}
}
