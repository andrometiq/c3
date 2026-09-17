package hostid

import (
	"bytes"
	"encoding/binary"
)

// kern.procargs2 is argc (native int32), executable path + NUL padding,
// followed by argc NUL-terminated argv strings. Never inspect the environment.
// Both supported Darwin architectures are little endian. Layout reference:
// https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/kern_sysctl.c
func DarwinProcReaders(args func(int) ([]byte, error), parent func(int) (int, error)) ProcReaders {
	return ProcReaders{
		Cmdline: func(pid int) ([]string, bool) {
			data, err := args(pid)
			if err != nil || len(data) < 4 {
				return nil, false
			}
			argc := int(binary.LittleEndian.Uint32(data[:4]))
			if argc < 1 || argc > len(data)-4 {
				return nil, false
			}
			data = data[4:]
			end := bytes.IndexByte(data, 0)
			if end < 0 {
				return nil, false
			}
			data = bytes.TrimLeft(data[end+1:], "\x00")
			argv := make([]string, 0, argc)
			for i := 0; i < argc; i++ {
				end = bytes.IndexByte(data, 0)
				if end < 0 {
					return nil, false
				}
				argv = append(argv, string(data[:end]))
				data = data[end+1:]
			}
			return argv, true
		},
		PPID: func(pid int) (int, bool) { p, err := parent(pid); return p, err == nil },
	}
}
