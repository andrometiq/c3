package main

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/ipc"
)

func TestRenderRouteNearestHostAcrossPlatforms(t *testing.T) {
	for _, platform := range []string{"linux", "darwin", "windows"} {
		for _, tc := range []struct {
			name string
			host []string
			want string
		}{
			{"flagless nested", []string{"claude"}, ipc.RenderQueueOnly},
			{"dev separated", []string{"claude", devChannelsFlag, "plugin:c3@c3"}, ipc.RenderCapable},
			{"dev equals", []string{"claude", devChannelsFlag + "=plugin:c3@c3"}, ipc.RenderCapable},
			{"eligible", []string{"claude", "--channels", "plugin:c3@c3"}, ipc.RenderProbing},
			{"eligible equals", []string{"claude", "--channels=plugin:c3@c3"}, ipc.RenderProbing},
			{"missing proc", nil, ipc.RenderQueueOnly},
		} {
			t.Run(platform+"/"+tc.name, func(t *testing.T) {
				argv := map[int][]string{10: {"c3-claude-adapter"}, 20: tc.host, 30: {"claude", devChannelsFlag, "plugin:c3@c3"}}
				if tc.host == nil {
					delete(argv, 20)
				}
				parents := map[int]int{10: 20, 20: 30, 30: 1}
				readers := fakeTree(argv, parents)
				if platform == "darwin" {
					readers = darwinProcReaders(func(pid int) ([]byte, error) {
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
				if platform == "windows" {
					want = ipc.RenderQueueOnly
				}
				got := detectRenderRoute(platform, 10, readers)
				if got.State != want {
					t.Fatalf("route=%+v want=%s", got, want)
				}
			})
		}
	}
}

func TestRenderDetectionRejectsHostNamesInPrompt(t *testing.T) {
	if cmdlineHasDevChannelForC3([]string{"claude", "--", devChannelsFlag, "plugin:c3@c3"}) {
		t.Fatal("prompt after option terminator qualified host")
	}
	if isClaudeHost([]string{"sh", "-c", "echo @anthropic-ai/claude-code"}) {
		t.Fatal("prompt text mistaken for host")
	}
	for _, parents := range []map[int]int{{10: 20, 20: 20}, {10: 20}, {10: 1}} {
		got := detectRenderRoute("linux", 10, fakeTree(map[int][]string{20: {"sh"}}, parents))
		if got.State != ipc.RenderQueueOnly {
			t.Fatalf("uncertain walk: %+v", got)
		}
	}
}
