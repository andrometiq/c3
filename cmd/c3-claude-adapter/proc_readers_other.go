//go:build !darwin

package main

func platformProcReaders() procReaders {
	return procReaders{cmdline: readProcCmdline, ppid: readProcPPID}
}
