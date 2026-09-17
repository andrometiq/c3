// Package hostid identifies the owning Claude process from argv, never environment.
package hostid

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// ProcReaders supplies process ancestry and optional owner-identity file reads.
type ProcReaders struct {
	Cmdline func(int) ([]string, bool)
	PPID    func(int) (int, bool)
	// ReadFile defaults to os.ReadFile; tests supply synthetic stat and boot data.
	ReadFile func(string) ([]byte, error)
}

// OwningClaudePID preserves the immediate-parent fallback for socket-validated inboxes.
func OwningClaudePID(parent int, r ProcReaders) int {
	pid, reachedInit := walkClaudeHost(parent, r)
	if reachedInit {
		return parent
	}
	return pid
}

// IdentifiedClaudeHost returns only a positively identified host, never a fallback.
func IdentifiedClaudeHost(startParent int, r ProcReaders) (int, bool) {
	pid, _ := walkClaudeHost(startParent, r)
	return pid, pid > 1
}

// The second result distinguishes a fully exhausted walk from unreadable or
// truncated ancestry, preserving the inbox caller's existing fallback boundary.
func walkClaudeHost(parent int, r ProcReaders) (int, bool) {
	if parent <= 1 || r.Cmdline == nil || r.PPID == nil {
		return 0, false
	}
	pid := parent
	seen := map[int]bool{}
	for depth := 0; depth < 40 && pid > 1; depth++ {
		if seen[pid] {
			return 0, false
		}
		seen[pid] = true
		args, ok := r.Cmdline(pid)
		if !ok {
			return 0, false
		}
		if IsNode(args) {
			args, ok = NodeScript(args)
			if !ok {
				return 0, false
			}
		}
		if IsClaudeHost(args) {
			return pid, false
		}
		pid, ok = r.PPID(pid)
		if !ok {
			return 0, false
		}
	}
	return 0, pid <= 1
}

// IsClaudeHost reports whether argv looks like the Claude Code CLI host process:
// an arg0 basename of "claude", a native binary directly under claude/versions/,
// or the CLI package script (npm/node install: node .../@anthropic-ai/claude-code/cli.js).
// Native installs may execute the resolved versioned path instead of the symlink.
// A bare version-number basename alone is insufficient. The adapter's
// own "c3-claude-adapter" arg0 does NOT match, so self is never taken for a host.
func IsClaudeHost(args []string) bool {
	if len(args) == 0 {
		return false
	}
	if filepath.Base(args[0]) == "claude" {
		return true
	}
	dir := filepath.Dir(args[0])
	if filepath.Base(dir) == "versions" && filepath.Base(filepath.Dir(dir)) == "claude" {
		return true
	}
	if IsNode(args) {
		script, certain := NodeScript(args)
		return certain && len(script) > 0 && strings.HasSuffix(script[0], "/@anthropic-ai/claude-code/cli.js")
	}
	if strings.HasSuffix(args[0], "/@anthropic-ai/claude-code/cli.js") {
		return true
	}
	return false
}

func IsNode(args []string) bool {
	return len(args) > 0 && (filepath.Base(args[0]) == "node" || filepath.Base(args[0]) == "nodejs")
}

// Stop at the script operand: flags in script arguments belong to the host.
// Unknown bare options may take a value, so cannot justify walking past Node.
func NodeScript(args []string) ([]string, bool) {
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			return args[i+1:], i+1 < len(args)
		}
		if !strings.HasPrefix(arg, "-") {
			return args[i:], true
		}
		option, _, _ := strings.Cut(arg, "=")
		switch option {
		case "--eval", "--print", "--check", "--run", "-e", "-p", "-c":
			return nil, false // these modes do not execute a script operand
		}
		if strings.HasPrefix(arg, "--") && strings.Contains(arg, "=") {
			continue
		}
		switch arg {
		case "--require", "-r", "--import", "--loader", "--experimental-loader", "--max-old-space-size":
			i++
			if i >= len(args) || strings.HasPrefix(args[i], "-") {
				return nil, false
			}
		case "--no-warnings", "--trace-warnings", "--enable-source-maps", "--experimental-strip-types":
		default:
			return nil, false
		}
	}
	return nil, false
}

// readProcCmdline reads /proc/<pid>/cmdline (NUL-separated argv). Returns ok=false
// when the file is absent (non-Linux) or unreadable. A process with an empty
// cmdline (kernel threads, zombies) yields ok=false so the walk treats it as
// unknown rather than a host.
func readProcCmdline(pid int) ([]string, bool) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
	if err != nil || len(data) == 0 {
		return nil, false
	}
	parts := strings.Split(strings.TrimRight(string(data), "\x00"), "\x00")
	out := parts[:0]
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// readProcPPID parses the parent pid from /proc/<pid>/stat. The stat format is
// `pid (comm) state ppid ...`; comm can contain spaces and parentheses, so we
// split after the LAST ')' — ppid is the second whitespace field beyond it.
// Returns ok=false on any read/parse failure (non-Linux, gone process).
func readProcPPID(pid int) (int, bool) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, false
	}
	s := string(data)
	rp := strings.LastIndexByte(s, ')')
	if rp < 0 || rp+1 >= len(s) {
		return 0, false
	}
	fields := strings.Fields(s[rp+1:])
	// fields[0] = state, fields[1] = ppid.
	if len(fields) < 2 {
		return 0, false
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, false
	}
	return ppid, true
}

// ProcStartTime reads Linux stat field 22, beyond the final closing comm paren.
func ProcStartTime(pid int) (int64, bool) {
	return procStartTime(pid, os.ReadFile)
}

func procStartTime(pid int, readFile func(string) ([]byte, error)) (int64, bool) {
	data, err := readFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, false
	}
	rp := strings.LastIndexByte(string(data), ')')
	if rp < 0 {
		return 0, false
	}
	fields := strings.Fields(string(data[rp+1:]))
	if len(fields) < 20 {
		return 0, false
	}
	start, err := strconv.ParseInt(fields[19], 10, 64)
	return start, err == nil && start >= 0
}

// BootID8 returns the first eight hex digits of a well-formed Linux boot UUID.
func BootID8() (string, bool) { return bootID8(os.ReadFile) }

func bootID8(readFile func(string) ([]byte, error)) (string, bool) {
	data, err := readFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", false
	}
	id := strings.TrimSpace(string(data))
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return "", false
	}
	raw := strings.ReplaceAll(id, "-", "")
	if len(raw) != 32 {
		return "", false
	}
	if _, err := hex.DecodeString(raw); err != nil {
		return "", false
	}
	return strings.ToLower(id[:8]), true
}

// OwnerKey returns host_<boot8>_<pid>_<starttime> on Linux only.
// Boot and process birth prevent stale aliases from surviving PID reuse.
func OwnerKey(startParent int, r ProcReaders) (string, bool) {
	if runtime.GOOS != "linux" {
		return "", false
	}
	pid, ok := IdentifiedClaudeHost(startParent, r)
	if !ok {
		return "", false
	}
	readFile := r.ReadFile
	if readFile == nil {
		readFile = os.ReadFile
	}
	start, ok := procStartTime(pid, readFile)
	if !ok {
		return "", false
	}
	boot, ok := bootID8(readFile)
	if !ok {
		return "", false
	}
	return fmt.Sprintf("host_%s_%d_%d", boot, pid, start), true
}
