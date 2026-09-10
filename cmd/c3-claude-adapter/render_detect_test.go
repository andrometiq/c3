package main

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

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
	isolateAdapterTest(t)
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
			// No identified host: fail closed to queue-only.
			name:     "no claude host in chain → queue-only (uncertain)",
			cmdlines: map[int][]string{10: adapter, 11: {"zsh"}, 12: {"systemd"}},
			parents:  map[int]int{10: 11, 11: 12, 12: 1},
			start:    10,
			want:     false,
		},
		{
			// Unreadable process tree: fail closed to queue-only.
			name:     "unreadable start pid → queue-only (uncertain)",
			cmdlines: map[int][]string{},
			parents:  map[int]int{},
			start:    10,
			want:     false,
		},
		{
			// Walk truncates (parent cmdline unreadable) BEFORE reaching a host and
			// with no host seen: retain inbound for fetch_queue.
			name:     "truncated walk before host → queue-only (uncertain)",
			cmdlines: map[int][]string{10: adapter}, // parent 11 has no cmdline entry
			parents:  map[int]int{10: 11},
			start:    10,
			want:     false,
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
			got := detectRenderRoute("linux", tc.start, fakeTree(tc.cmdlines, tc.parents)).State == ipc.RenderCapable
			if got != tc.want {
				t.Errorf("channel eligible = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCmdlineHasDevChannelForC3(t *testing.T) {
	isolateAdapterTest(t)
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
	isolateAdapterTest(t)
	hosts := [][]string{
		{"claude"},
		{"/usr/bin/claude", "--resume"},
		{"/opt/claude/versions/2.1.263", "--session-id", "test-session"},
		{"claude/versions/2.1.263"},
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
		{"2.1.263", devChannelsFlag, "plugin:c3@c3"},
		{"/opt/versions/2.1.263"},
		{"/opt/not-claude/versions/2.1.263"},
		{"/opt/claude/not-versions/2.1.263"},
		{"/opt/claude/versions/2.1.263/other"},
		{"sh", "/opt/claude/versions/2.1.263"},
		{"/usr/lib/systemd/systemd", "--user"},
		{},
	}
	for _, a := range notHosts {
		if isClaudeHost(a) {
			t.Errorf("expected NOT host for %v", a)
		}
	}
}

func TestIsCursorHost(t *testing.T) {
	isolateAdapterTest(t)
	yes := [][]string{
		{"/home/u/.local/bin/agent", "--use-system-ca", "/home/u/.local/share/cursor-agent/versions/2026.07.23/index.js"},
		{"cursor-agent"},
		{"/usr/bin/node", "/home/u/.local/share/cursor-agent/versions/x/index.js"},
	}
	for _, a := range yes {
		if !isCursorHost(a) {
			t.Errorf("expected Cursor host for %v", a)
		}
	}
	no := [][]string{
		{"claude"},
		{"c3-claude-adapter"},
		{"/usr/bin/node", "some-other-app/index.js"},
	}
	for _, a := range no {
		if isCursorHost(a) {
			t.Errorf("expected NOT Cursor host for %v", a)
		}
	}
}

func TestDetectCursorHost_AncestorWalk(t *testing.T) {
	isolateAdapterTest(t)
	tree := map[int]struct {
		args []string
		ppid int
	}{
		10: {args: []string{"c3-claude-adapter"}, ppid: 20},
		20: {args: []string{"agent", "--use-system-ca", "/x/cursor-agent/versions/1/index.js"}, ppid: 1},
	}
	r := procReaders{
		cmdline: func(pid int) ([]string, bool) {
			n, ok := tree[pid]
			return n.args, ok
		},
		ppid: func(pid int) (int, bool) {
			n, ok := tree[pid]
			return n.ppid, ok
		},
	}
	if !detectCursorHost(10, r) {
		t.Fatal("adapter under agent/cursor-agent must be detected as Cursor host")
	}
	tree[20] = struct {
		args []string
		ppid int
	}{args: []string{"claude"}, ppid: 1}
	if detectCursorHost(10, r) {
		t.Fatal("adapter under claude must NOT be detected as Cursor host")
	}
}

// buildInstructions must carry the degraded-delivery warning only when the host
// cannot render, and never on the capable fast path.
func TestBuildInstructions_DegradedWarningGate(t *testing.T) {
	isolateAdapterTest(t)
	a := newAdapter()
	a.renderRoute = ipc.RenderRoute{State: ipc.RenderCapable}
	if got := a.buildInstructions(); containsSub(got, "fetch_queue` tool to retrieve") {
		t.Error("capable session must NOT carry the degraded-delivery warning")
	}
	a.renderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "no dev-channels flag on host"}
	got := a.buildInstructions()
	if !containsSub(got, "Live route: queue-only") || !containsSub(got, "fetch_queue") {
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

// Hello keeps the legacy gate conservative and reports all three route states.
func TestHello_ReportsCannotRenderChannels(t *testing.T) {
	isolateAdapterTest(t)
	for _, tc := range []struct {
		name         string
		state        string
		wantCannotRC bool
	}{
		{"capable session omits/false", ipc.RenderCapable, false},
		{"incapable session sets true", ipc.RenderQueueOnly, true},
		{"probing conservatively sets true", ipc.RenderProbing, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newAdapter()
			a.deliveryHostInitialized.Store(true)
			a.notifyTx = newNotifyTransport(&scriptedTransport{conn: &scriptedConn{}})
			a.renderRoute = ipc.RenderRoute{State: tc.state, Reason: "test reason"}

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
			if hello.RenderState != tc.state || hello.RenderReason != "test reason" {
				t.Fatalf("hello state = %q", hello.RenderState)
			}
			if hello.CannotRenderChannels != tc.wantCannotRC {
				t.Errorf("CannotRenderChannels = %v, want %v", hello.CannotRenderChannels, tc.wantCannotRC)
			}
		})
	}
}

func TestNodeOptionsCannotHideNearestClaude(t *testing.T) {
	isolateAdapterTest(t)
	for _, options := range [][]string{
		{"--max-old-space-size=4096"}, {"--no-warnings", "--max-old-space-size=4096"},
		{"--require", "/work/preload.js"}, {"--unknown-value-taking", "value"}, {"--eval", "something"}, {"--eval=something"}, {"--print=1"},
	} {
		args := append([]string{"node"}, options...)
		args = append(args, "/work/node_modules/@anthropic-ai/claude-code/cli.js")
		readers := fakeTree(map[int][]string{
			3: args,
			2: {"claude", devChannelsFlag + "=plugin:c3@c3"},
		}, map[int]int{4: 3, 3: 2, 2: 1})
		if route := detectRenderRoute("linux", 4, readers); route.State != ipc.RenderQueueOnly {
			t.Fatalf("%v: %v", options, route)
		}
	}
}

func TestNodeOptionsIdentifyFlaggedScript(t *testing.T) {
	isolateAdapterTest(t)
	args := []string{"node", "--no-warnings", "--max-old-space-size=4096", "/work/node_modules/@anthropic-ai/claude-code/cli.js", devChannelsFlag + "=plugin:c3@c3"}
	readers := fakeTree(map[int][]string{3: args, 2: {"claude"}}, map[int]int{4: 3, 3: 2, 2: 1})
	if route := detectRenderRoute("linux", 4, readers); route.State != ipc.RenderCapable {
		t.Fatalf("flagged Node script was not identified: %+v", route)
	}
}
