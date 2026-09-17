package main

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/hostid"
	"github.com/Andrometiq/c3/internal/ipc"
)

func TestRenderRouteNearestHostAcrossPlatforms(t *testing.T) {
	isolateAdapterTest(t)
	for _, platform := range []string{"linux", "darwin", "windows"} {
		for _, tc := range []struct {
			name   string
			host   []string
			want   string
			reason string
		}{
			{"flagless nested", []string{"claude"}, ipc.RenderQueueOnly, "no dev-channels flag on host"},
			{"dev separated", []string{"claude", devChannelsFlag, "plugin:c3@c3"}, ipc.RenderCapable, ""},
			{"dev equals", []string{"claude", devChannelsFlag + "=plugin:c3@c3"}, ipc.RenderCapable, ""},
			{"eligible", []string{"claude", "--channels", "plugin:c3@c3"}, ipc.RenderProbing, "channels flag present, awaiting confirmation"},
			{"eligible equals", []string{"claude", "--channels=plugin:c3@c3"}, ipc.RenderProbing, "channels flag present, awaiting confirmation"},
			{"native versioned dev separated", []string{"/opt/claude/versions/2.1.263", devChannelsFlag, "plugin:c3@c3"}, ipc.RenderCapable, ""},
			{"native versioned dev equals", []string{"/opt/claude/versions/2.1.263", devChannelsFlag + "=plugin:c3@c3"}, ipc.RenderCapable, ""},
			{"native versioned flagless nested", []string{"/opt/claude/versions/2.1.263"}, ipc.RenderQueueOnly, "no dev-channels flag on host"},
			{"native versioned eligible", []string{"/opt/claude/versions/2.1.263", "--channels", "plugin:c3@c3"}, ipc.RenderProbing, "channels flag present, awaiting confirmation"},
			{"missing proc", nil, ipc.RenderQueueOnly, "process tree unreadable"},
		} {
			t.Run(platform+"/"+tc.name, func(t *testing.T) {
				argv := map[int][]string{10: {"c3-claude-adapter"}, 20: tc.host, 30: {"claude", devChannelsFlag, "plugin:c3@c3"}}
				if tc.host == nil {
					delete(argv, 20)
				}
				parents := map[int]int{10: 20, 20: 30, 30: 1}
				readers := fakeTree(argv, parents)
				if platform == "darwin" {
					readers = hostid.DarwinProcReaders(func(pid int) ([]byte, error) {
						args, ok := argv[pid]
						if !ok {
							return nil, errors.New("unreadable")
						}
						data := make([]byte, 4)
						binary.LittleEndian.PutUint32(data, uint32(len(args)))
						data = append(data, []byte("/executable\x00\x00")...)
						data = append(data, []byte(strings.Join(args, "\x00")+"\x00")...)
						// Never mistake environment text for host argv flags.
						return append(data, []byte(devChannelsFlag+"=plugin:c3@c3\x00")...), nil
					}, func(pid int) (int, error) {
						p, ok := parents[pid]
						if !ok {
							return 0, errors.New("unreadable")
						}
						return p, nil
					})
				}
				want := tc.want
				reason := tc.reason
				if platform == "windows" {
					want = ipc.RenderQueueOnly
					reason = "windows"
				}
				got := detectRenderRoute(platform, 10, readers)
				if got.State != want || got.Reason != reason {
					t.Fatalf("route=%+v want=%s reason=%q", got, want, reason)
				}
			})
		}
	}
}

func TestRenderRouteProcessTreeReasons(t *testing.T) {
	isolateAdapterTest(t)
	for _, tc := range []struct {
		name    string
		parents map[int]int
		want    string
	}{
		{"no host", map[int]int{10: 20, 20: 1}, "no Claude Code host identified in the process tree"},
		{"unreadable parent", map[int]int{10: 20}, "process tree unreadable"},
		{"unreadable argv", map[int]int{10: 30}, "process tree unreadable"},
		{"self parent", map[int]int{10: 20, 20: 20}, "process tree truncated"},
		{"depth limit", map[int]int{10: 20, 20: 21, 21: 20}, "process tree truncated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			readers := fakeTree(map[int][]string{20: {"sh"}, 21: {"sh"}}, tc.parents)
			got := detectRenderRoute("linux", 10, readers)
			if got.State != ipc.RenderQueueOnly || got.Reason != tc.want {
				t.Fatalf("route=%+v want queue-only reason=%q", got, tc.want)
			}
		})
	}
}

func TestRenderDetectionRejectsHostNamesInPrompt(t *testing.T) {
	isolateAdapterTest(t)
	if cmdlineHasDevChannelForC3([]string{"claude", "--", devChannelsFlag, "plugin:c3@c3"}) {
		t.Fatal("prompt after option terminator qualified host")
	}
	if hostid.IsClaudeHost([]string{"sh", "-c", "echo @anthropic-ai/claude-code"}) {
		t.Fatal("prompt text mistaken for host")
	}
	for _, parents := range []map[int]int{{10: 20, 20: 20}, {10: 20}, {10: 1}} {
		got := detectRenderRoute("linux", 10, fakeTree(map[int][]string{20: {"sh"}}, parents))
		if got.State != ipc.RenderQueueOnly {
			t.Fatalf("uncertain walk: %+v", got)
		}
	}
}
