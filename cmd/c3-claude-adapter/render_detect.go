package main

// Render-capability detection for the forked-session inbound blackhole.
//
// A Claude Code session launched WITHOUT
// `--dangerously-load-development-channels=plugin:c3@c3` (typically a
// --fork-session background job) still spawns this adapter and receives every
// inbound, but Claude Code SILENTLY DROPS the notifications/claude/channel frame
// before rendering. The adapter's write to stdin succeeds, so it would ack the
// push as delivered and the broker would drop the durable copy — the message
// vanishes. This detects that case so the adapter can report it at hello and the
// broker can hold such inbound in the queue (recoverable via fetch_queue) instead.
//
// Signal: the dev-channels flag naming the c3 plugin appears verbatim in the
// launching `claude` process's command line (empirically confirmed on the target
// host; a shim-injected flag is also visible in cmdline). The adapter is an MCP
// stdio child of `claude`, but a fork tree can insert pty-host/session nodes
// between them, so we WALK the /proc ancestor chain rather than reading only the
// direct parent.

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/Andrometiq/c3/internal/ipc"
)

// devChannelsFlag is the Claude Code launch flag that loads development channel
// plugins. Its absence for the c3 plugin is what makes the host silently drop
// channel push notifications.
const devChannelsFlag = "--dangerously-load-development-channels"

// procReaders abstracts the /proc reads so detectRenderCapable is unit-testable
// against a synthetic process tree. On a real host these are backed by
// /proc/<pid>/cmdline and /proc/<pid>/stat.
type procReaders struct {
	// cmdline returns the argv of pid and whether it could be read.
	cmdline func(pid int) ([]string, bool)
	// ppid returns the parent pid of pid and whether it could be read.
	ppid func(pid int) (int, bool)
}

// Detection fails closed: a false queue-only costs a fetch; a false capable
// can lose a message. Only the NEAREST positively identified host counts.
func hostRenderRoute() ipc.RenderRoute {
	return detectRenderRoute(runtime.GOOS, os.Getpid(), platformProcReaders())
}

func detectRenderCapable(startPID int, r procReaders) bool {
	return detectRenderRoute("linux", startPID, r).State == ipc.RenderCapable
}

func detectRenderRoute(goos string, startPID int, r procReaders) ipc.RenderRoute {
	queue := func(reason string) ipc.RenderRoute {
		return ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: reason}
	}
	if goos == "windows" {
		return queue("windows")
	}
	if r.cmdline == nil || r.ppid == nil {
		return queue("process tree unreadable")
	}
	pid, ok := r.ppid(startPID)
	if !ok {
		return queue("process tree unreadable")
	}
	for depth := 0; depth < 40 && pid > 1; depth++ {
		args, readable := r.cmdline(pid)
		if !readable {
			return queue("process tree unreadable")
		}
		if isNode(args) {
			script, certain := nodeScript(args)
			if !certain {
				return queue("node script uncertain")
			}
			args = script
		}
		if isClaudeHost(args) {
			if cmdlineHasDevChannelForC3(args) {
				return ipc.RenderRoute{State: ipc.RenderCapable}
			}
			if cmdlineHasChannelForC3(args, "--channels") {
				return ipc.RenderRoute{State: ipc.RenderProbing, Reason: "channels flag present, awaiting confirmation"}
			}
			return queue("no dev-channels flag on host")
		}
		parent, readable := r.ppid(pid)
		if !readable {
			return queue("process tree unreadable")
		}
		if parent == pid {
			return queue("process tree truncated")
		}
		pid = parent
	}
	if pid > 1 {
		return queue("process tree truncated")
	}
	return queue("no Claude Code host identified in the process tree")
}

// cmdlineHasDevChannelForC3 reports whether argv carries the dev-channels flag
// with a value that names the c3 plugin. Handles both `--flag value` (value in
// following token(s) until the next flag) and `--flag=value`, plus comma-joined
// multi-plugin lists. Requires the c3 token specifically so that enabling a
// DIFFERENT dev plugin does not read as capable for c3.
func cmdlineHasDevChannelForC3(args []string) bool {
	return cmdlineHasChannelForC3(args, devChannelsFlag)
}

func cmdlineHasChannelForC3(args []string, flag string) bool {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			break
		}
		if v, ok := strings.CutPrefix(a, flag+"="); ok {
			if pluginTokenMatchesC3(v) {
				return true
			}
			continue
		}
		if a == flag {
			for j := i + 1; j < len(args); j++ {
				if strings.HasPrefix(args[j], "-") {
					break // reached the next flag; the value list ended.
				}
				if pluginTokenMatchesC3(args[j]) {
					return true
				}
			}
		}
	}
	return false
}

// pluginTokenMatchesC3 reports whether a dev-channels value token references the
// c3 plugin. Tokens look like "plugin:c3@c3" (plugin:<name>@<marketplace>) and
// may be comma-joined. Matches the c3 plugin name specifically — not any
// substring "c3" — so an unrelated path/id containing "c3" never false-matches.
func pluginTokenMatchesC3(tok string) bool {
	for _, t := range strings.Split(tok, ",") {
		t = strings.TrimSpace(t)
		if t == "plugin:c3@c3" || strings.HasPrefix(t, "plugin:c3@") || t == "c3" {
			return true
		}
	}
	return false
}

// isClaudeHost reports whether argv looks like the Claude Code CLI host process:
// an arg0 basename of "claude", a native binary directly under claude/versions/,
// or the CLI package script (npm/node install: node .../@anthropic-ai/claude-code/cli.js).
// Native installs may execute the resolved versioned path instead of the symlink.
// A bare version-number basename alone is insufficient. The adapter's
// own "c3-claude-adapter" arg0 does NOT match, so self is never taken for a host.
func isClaudeHost(args []string) bool {
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
	if isNode(args) {
		script, certain := nodeScript(args)
		return certain && len(script) > 0 && strings.HasSuffix(script[0], "/@anthropic-ai/claude-code/cli.js")
	}
	if strings.HasSuffix(args[0], "/@anthropic-ai/claude-code/cli.js") {
		return true
	}
	return false
}

func isNode(args []string) bool {
	return len(args) > 0 && (filepath.Base(args[0]) == "node" || filepath.Base(args[0]) == "nodejs")
}

// Stop at the script operand: flags in script arguments belong to the host.
// Unknown bare options may take a value, so cannot justify walking past Node.
func nodeScript(args []string) ([]string, bool) {
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

// isCursorHost reports whether argv looks like Cursor Agent CLI. Cursor loads
// Claude Code plugins from ~/.claude/plugins, so this adapter can be spawned
// under `agent` / `cursor-agent` alongside c3-cursor-adapter — a dual-MCP
// footgun that makes Telegram welcome say "claude" and can black-hole inbound.
func isCursorHost(args []string) bool {
	if len(args) == 0 {
		return false
	}
	base := filepath.Base(args[0])
	switch base {
	case "agent", "cursor-agent", "cursor-agent-local", "agent-local":
		return true
	}
	for _, a := range args {
		if strings.Contains(a, "cursor-agent/versions/") || strings.Contains(a, "cursor-agent-local/") {
			return true
		}
	}
	return false
}

// hostIsCursorAgent walks the /proc ancestor chain looking for a Cursor Agent
// CLI host. Used to refuse startup under Cursor so install-cursor's
// c3-cursor-adapter is the only C3 MCP that can claim routes.
func hostIsCursorAgent() bool {
	if runtime.GOOS == "windows" {
		return false // no /proc walk; Cursor-on-Windows dual-load is rarer today
	}
	return detectCursorHost(os.Getpid(), platformProcReaders())
}

func detectCursorHost(startPID int, r procReaders) bool {
	const maxDepth = 40
	pid := startPID
	for depth := 0; depth < maxDepth; depth++ {
		args, ok := r.cmdline(pid)
		if !ok {
			break
		}
		if isCursorHost(args) {
			return true
		}
		parent, ok := r.ppid(pid)
		if !ok || parent <= 1 || parent == pid {
			break
		}
		pid = parent
	}
	return false
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
