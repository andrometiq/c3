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
	"strings"

	"github.com/Andrometiq/c3/internal/hostid"
	"github.com/Andrometiq/c3/internal/ipc"
)

// devChannelsFlag is the Claude Code launch flag that loads development channel
// plugins. Its absence for the c3 plugin is what makes the host silently drop
// channel push notifications.
const devChannelsFlag = "--dangerously-load-development-channels"

// Detection fails closed: a false queue-only costs a fetch; a false capable
// can lose a message. Only the NEAREST positively identified host counts.
func hostRenderRoute() ipc.RenderRoute {
	return detectRenderRoute(runtime.GOOS, os.Getpid(), hostid.PlatformProcReaders())
}

func detectRenderRoute(goos string, startPID int, r hostid.ProcReaders) ipc.RenderRoute {
	queue := func(reason string) ipc.RenderRoute {
		return ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: reason}
	}
	if goos == "windows" {
		return queue("windows")
	}
	if r.Cmdline == nil || r.PPID == nil {
		return queue("process tree unreadable")
	}
	pid, ok := r.PPID(startPID)
	if !ok {
		return queue("process tree unreadable")
	}
	for depth := 0; depth < 40 && pid > 1; depth++ {
		args, readable := r.Cmdline(pid)
		if !readable {
			return queue("process tree unreadable")
		}
		if hostid.IsNode(args) {
			script, certain := hostid.NodeScript(args)
			if !certain {
				return queue("node script uncertain")
			}
			args = script
		}
		if hostid.IsClaudeHost(args) {
			if cmdlineHasDevChannelForC3(args) {
				return ipc.RenderRoute{State: ipc.RenderCapable}
			}
			if cmdlineHasChannelForC3(args, "--channels") {
				return ipc.RenderRoute{State: ipc.RenderProbing, Reason: "channels flag present, awaiting confirmation"}
			}
			return queue("no dev-channels flag on host")
		}
		parent, readable := r.PPID(pid)
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
	return detectCursorHost(os.Getpid(), hostid.PlatformProcReaders())
}

func detectCursorHost(startPID int, r hostid.ProcReaders) bool {
	const maxDepth = 40
	pid := startPID
	for depth := 0; depth < maxDepth; depth++ {
		args, ok := r.Cmdline(pid)
		if !ok {
			break
		}
		if isCursorHost(args) {
			return true
		}
		parent, ok := r.PPID(pid)
		if !ok || parent <= 1 || parent == pid {
			break
		}
		pid = parent
	}
	return false
}
