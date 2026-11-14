package osutil

import (
	"bytes"
	"os"
	"strconv"
)

// IsZombie reports whether pid has exited but its parent has not reaped it yet.
// kill(pid, 0) still succeeds on such a process, so a liveness probe must ask
// separately. Unreadable process state reports false (the kill(pid, 0) answer
// stands).
func IsZombie(pid int) bool {
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	// "pid (comm) state ...": comm may itself contain ") ", so cut at the last one.
	end := bytes.LastIndexByte(stat, ')')
	if end < 0 || end+2 >= len(stat) {
		return false
	}
	state := stat[end+2]
	return state == 'Z' || state == 'X'
}
