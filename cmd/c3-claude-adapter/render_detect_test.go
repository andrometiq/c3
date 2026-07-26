package main

import (
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

// The fake-tree tests exercise detectRenderCapable's logic; this grounds the REAL
// /proc parsers (readProcCmdline / readProcPPID) against this live test process so
// the production accessors are proven, not just the injected ones. Skipped where
// /proc is absent (non-Linux).
func TestRealProcReaders_Smoke(t *testing.T) {
	if _, err := os.Stat("/proc/self/stat"); err != nil {
		t.Skip("no /proc on this host")
	}
	self := os.Getpid()
	args, ok := readProcCmdline(self)
	if !ok || len(args) == 0 {
		t.Fatalf("readProcCmdline(self) = %v, %v; want a non-empty argv", args, ok)
	}
	ppid, ok := readProcPPID(self)
	if !ok {
		t.Fatal("readProcPPID(self) not ok")
	}
	if ppid != os.Getppid() {
		t.Errorf("readProcPPID(self) = %d, want os.Getppid() = %d", ppid, os.Getppid())
	}
	// End-to-end through the real readers: the test process's ancestry is `go
	// test`, not a Claude host, so detection must default to capable (true) — never
	// a false blackhole in a non-Claude environment.
	if !hostCanRenderChannels() {
		t.Error("in a non-Claude test process, detection must default to renderable")
	}
}

// fakeTree builds procReaders over a synthetic process tree: cmdlines maps a pid
// to its argv, parents maps a pid to its ppid. A missing pid in cmdlines reads as
// unreadable (ok=false); a missing pid in parents ends the walk.
func fakeTree(cmdlines map[int][]string, parents map[int]int) procReaders {
	return procReaders{
		cmdline: func(pid int) ([]string, bool) {
			args, ok := cmdlines[pid]
			return args, ok
		},
		ppid: func(pid int) (int, bool) {
			p, ok := parents[pid]
			return p, ok
		},
	}
}

func TestDetectRenderCapable(t *testing.T) {
	claudeFlag := []string{"claude", devChannelsFlag, "plugin:c3@c3"}
	claudeFlagResume := []string{"claude", devChannelsFlag, "plugin:c3@c3", "--resume"}
	claudeNoFlag := []string{"claude"}
	adapter := []string{"c3-claude-adapter"}

	tests := []struct {
		name     string
		cmdlines map[int][]string
		parents  map[int]int
		start    int
		want     bool
	}{
		{
			// Normal launch: adapter is a direct child of a flagged claude.
			name:     "flag on direct parent → capable",
			cmdlines: map[int][]string{10: adapter, 11: claudeFlag},
			parents:  map[int]int{10: 11, 11: 1},
			start:    10,
			want:     true,
		},
		{
			// Fork tree: pty-host / session nodes sit between the adapter and the
			// flagged claude — the walk must reach the grandparent+.
			name: "flag on a grandparent (fork tree) → capable",
			cmdlines: map[int][]string{
				10: adapter,
				11: {"cli-session-host"},
				12: {"pty-host"},
				13: claudeFlagResume,
			},
			parents: map[int]int{10: 11, 11: 12, 12: 13, 13: 1},
			start:   10,
			want:    true,
		},
		{
			// The blackhole: a Claude host launched WITHOUT the flag.
			name:     "claude host, no flag → NOT capable",
			cmdlines: map[int][]string{10: adapter, 11: claudeNoFlag},
			parents:  map[int]int{10: 11, 11: 1},
			start:    10,
			want:     false,
		},
		{
			// Some other dev plugin is enabled but not c3 → c3 still blackholed.
			name: "different-plugin flag only → NOT capable",
			cmdlines: map[int][]string{
				10: adapter,
				11: {"claude", devChannelsFlag, "plugin:other@mkt"},
			},
			parents: map[int]int{10: 11, 11: 1},
			start:   10,
			want:    false,
		},
		{
			// No Claude host identifiable in the chain → uncertain → capable.
			name:     "no claude host in chain → capable (uncertain)",
			cmdlines: map[int][]string{10: adapter, 11: {"zsh"}, 12: {"systemd"}},
			parents:  map[int]int{10: 11, 11: 12, 12: 1},
			start:    10,
			want:     true,
		},
		{
			// /proc unreadable from the start → uncertain → capable (non-Linux).
			name:     "unreadable start pid → capable (uncertain)",
			cmdlines: map[int][]string{},
			parents:  map[int]int{},
			start:    10,
			want:     true,
		},
		{
			// Walk truncates (parent cmdline unreadable) BEFORE reaching a host and
			// with no flag seen → must default capable, never a false blackhole.
			name:     "truncated walk before host → capable (uncertain)",
			cmdlines: map[int][]string{10: adapter}, // parent 11 has no cmdline entry
			parents:  map[int]int{10: 11},
			start:    10,
			want:     true,
		},
		{
			// --flag=value form with a comma-joined multi-plugin list including c3.
			name: "equals form + comma list including c3 → capable",
			cmdlines: map[int][]string{
				10: adapter,
				11: {"claude", devChannelsFlag + "=plugin:a@x,plugin:c3@c3"},
			},
			parents: map[int]int{10: 11, 11: 1},
			start:   10,
			want:    true,
		},
		{
			// npm/node install: node .../@anthropic-ai/claude-code/cli.js, no flag →
			// identified as a host → NOT capable.
			name: "node-install claude host, no flag → NOT capable",
			cmdlines: map[int][]string{
				10: adapter,
				11: {"node", "/home/u/.nvm/node/bin/@anthropic-ai/claude-code/cli.js"},
			},
			parents: map[int]int{10: 11, 11: 1},
			start:   10,
			want:    false,
		},
		{
			// Multi-plugin value spread across separate tokens (--flag a b), c3 is
			// the second value token — must still match.
			name: "space-separated multi value with c3 second → capable",
			cmdlines: map[int][]string{
				10: adapter,
				11: {"claude", devChannelsFlag, "plugin:a@x", "plugin:c3@c3"},
			},
			parents: map[int]int{10: 11, 11: 1},
			start:   10,
			want:    true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := detectRenderCapable(tc.start, fakeTree(tc.cmdlines, tc.parents))
			if got != tc.want {
				t.Errorf("detectRenderCapable = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCmdlineHasDevChannelForC3(t *testing.T) {
	yes := [][]string{
		{"claude", devChannelsFlag, "plugin:c3@c3"},
		{"claude", devChannelsFlag, "plugin:c3@other"},
		{"claude", devChannelsFlag + "=plugin:c3@c3"},
		{"claude", devChannelsFlag, "c3"},
		{"claude", devChannelsFlag, "plugin:x@y", "plugin:c3@c3"},
	}
	for _, a := range yes {
		if !cmdlineHasDevChannelForC3(a) {
			t.Errorf("expected match for %v", a)
		}
	}
	no := [][]string{
		{"claude"},
		{"claude", "--resume"},
		{"claude", devChannelsFlag, "plugin:other@x"},
		{"claude", devChannelsFlag, "--resume"}, // value list ends at next flag
		{"c3-claude-adapter"},
		// A stray "c3" that is NOT a dev-channels value must not match.
		{"claude", "--project", "/home/u/projects/app"},
	}
	for _, a := range no {
		if cmdlineHasDevChannelForC3(a) {
			t.Errorf("expected NO match for %v", a)
		}
	}
}

func TestIsClaudeHost(t *testing.T) {
	hosts := [][]string{
		{"claude"},
		{"/usr/bin/claude", "--resume"},
		{"node", "/x/@anthropic-ai/claude-code/cli.js"},
	}
	for _, a := range hosts {
		if !isClaudeHost(a) {
			t.Errorf("expected host for %v", a)
		}
	}
	notHosts := [][]string{
		{"c3-claude-adapter"}, // the adapter itself must never be taken for a host
		{"zsh"},
		{"/usr/lib/systemd/systemd", "--user"},
		{},
	}
	for _, a := range notHosts {
		if isClaudeHost(a) {
			t.Errorf("expected NOT host for %v", a)
		}
	}
}

// buildInstructions must carry the degraded-delivery warning only when the host
// cannot render, and never on the capable fast path.
func TestBuildInstructions_DegradedWarningGate(t *testing.T) {
	a := newAdapter() // hostRenderCapable defaults true
	if got := a.buildInstructions(); containsSub(got, "fetch_queue` tool to retrieve") {
		t.Error("capable session must NOT carry the degraded-delivery warning")
	}
	a.hostRenderCapable = false
	got := a.buildInstructions()
	if !containsSub(got, "DEGRADED") || !containsSub(got, "fetch_queue") {
		t.Errorf("render-incapable session must carry the degraded warning; got:\n%s", got)
	}
}

func containsSub(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// hello() must report CannotRenderChannels as the inverse of hostRenderCapable so
// the broker learns to hold this session's inbound in the queue.
func TestHello_ReportsCannotRenderChannels(t *testing.T) {
	for _, tc := range []struct {
		name         string
		canRender    bool
		wantCannotRC bool
	}{
		{"capable session omits/false", true, false},
		{"incapable session sets true", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newAdapter()
			a.hostRenderCapable = tc.canRender

			pipeA, pipeB := net.Pipe()
			defer pipeA.Close()
			defer pipeB.Close()
			a.bmu.Lock()
			a.conn = ipc.NewConn(pipeA)
			a.bmu.Unlock()

			peer := ipc.NewConn(pipeB)
			type res struct {
				raw []byte
				err error
			}
			ch := make(chan res, 1)
			go func() {
				raw, err := peer.ReadFrame()
				if err == nil {
					// hello() blocks reading the ack; feed it one so it returns.
					_ = peer.WriteJSON(ipc.HelloAckMsg{Op: ipc.OpHelloAck, ConnID: 1})
				}
				ch <- res{raw, err}
			}()

			if err := a.hello(); err != nil {
				t.Fatalf("hello: %v", err)
			}
			var r res
			select {
			case r = <-ch:
			case <-time.After(2 * time.Second):
				t.Fatal("no hello frame within 2s")
			}
			if r.err != nil {
				t.Fatalf("read hello: %v", r.err)
			}
			var hello ipc.HelloMsg
			if err := json.Unmarshal(r.raw, &hello); err != nil {
				t.Fatalf("unmarshal hello: %v", err)
			}
			if hello.CannotRenderChannels != tc.wantCannotRC {
				t.Errorf("CannotRenderChannels = %v, want %v", hello.CannotRenderChannels, tc.wantCannotRC)
			}
		})
	}
}
