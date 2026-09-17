package hostid

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
)

type syntheticProc struct {
	args   []string
	parent int
}

func syntheticReaders(tree map[int]syntheticProc) ProcReaders {
	return ProcReaders{
		Cmdline: func(pid int) ([]string, bool) { p, ok := tree[pid]; return p.args, ok },
		PPID:    func(pid int) (int, bool) { p, ok := tree[pid]; return p.parent, ok },
	}
}

func TestIdentifiedClaudeHost(t *testing.T) {
	for _, tc := range []struct {
		name string
		tree map[int]syntheticProc
		want int
	}{
		{"claude ancestor", map[int]syntheticProc{10: {[]string{"sh"}, 20}, 20: {[]string{"/bin/claude"}, 1}}, 20},
		{"nearest host", map[int]syntheticProc{10: {[]string{"claude"}, 20}, 20: {[]string{"claude"}, 1}}, 10},
		{"shells and adapter only", map[int]syntheticProc{10: {[]string{"c3-claude-adapter"}, 20}, 20: {[]string{"sh"}, 1}}, 0},
		{"node install", map[int]syntheticProc{10: {[]string{"sh"}, 20}, 20: {[]string{"node", "/opt/node_modules/@anthropic-ai/claude-code/cli.js"}, 1}}, 20},
		{"unreadable cmdline", map[int]syntheticProc{10: {[]string{"sh"}, 20}}, 0},
		{"uncertain node", map[int]syntheticProc{10: {[]string{"node", "--unknown", "claude"}, 20}, 20: {[]string{"claude"}, 1}}, 0},
		{"cycle", map[int]syntheticProc{10: {[]string{"sh"}, 20}, 20: {[]string{"sh"}, 10}}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pid, ok := IdentifiedClaudeHost(10, syntheticReaders(tc.tree))
			if pid != tc.want || ok != (tc.want != 0) {
				t.Fatalf("host=(%d,%v), want %d", pid, ok, tc.want)
			}
		})
	}
	if pid, ok := IdentifiedClaudeHost(1, ProcReaders{}); pid != 0 || ok {
		t.Fatal("identified init")
	}
	if pid, ok := IdentifiedClaudeHost(10, ProcReaders{}); pid != 0 || ok {
		t.Fatal("identified without readers")
	}
	r := syntheticReaders(map[int]syntheticProc{10: {[]string{"sh"}, 1}})
	if got := OwningClaudePID(10, r); got != 10 {
		t.Fatalf("socket caller lost parent fallback: %d", got)
	}
	r.PPID = func(int) (int, bool) { return 0, false }
	if pid, ok := IdentifiedClaudeHost(10, r); pid != 0 || ok {
		t.Fatal("identified through unreadable parent")
	}
	r = ProcReaders{Cmdline: func(int) ([]string, bool) { return []string{"sh"}, true }, PPID: func(pid int) (int, bool) { return pid + 1, true }}
	if pid, ok := IdentifiedClaudeHost(10, r); pid != 0 || ok {
		t.Fatal("unbounded walk identified host")
	}
}

func syntheticStat(start string) string {
	fields := []string{"S", "1"}
	for field := 5; field <= 21; field++ {
		fields = append(fields, fmt.Sprint(field))
	}
	fields = append(fields, start, "23")
	return "1234 (weird (comm) x) " + strings.Join(fields, " ")
}

func TestProcStartTime(t *testing.T) {
	for _, tc := range []struct {
		name, stat       string
		readable, wantOK bool
	}{
		{"spaces and parentheses", syntheticStat("424242"), true, true},
		{"missing", "", false, false},
		{"no comm terminator", "1234 malformed", true, false},
		{"short", "1234 (comm) S 1", true, false},
		{"not numeric", syntheticStat("bad"), true, false},
		{"negative", syntheticStat("-1"), true, false},
		{"overflow", syntheticStat("99999999999999999999999"), true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := procStartTime(1234, func(path string) ([]byte, error) {
				if path != "/proc/1234/stat" {
					t.Fatalf("unexpected read: %s", path)
				}
				if !tc.readable {
					return nil, os.ErrNotExist
				}
				return []byte(tc.stat), nil
			})
			if ok != tc.wantOK || (ok && got != 424242) {
				t.Fatalf("start=(%d,%v)", got, ok)
			}
		})
	}
}

func TestBootID8(t *testing.T) {
	for _, tc := range []struct {
		id   string
		want bool
	}{
		{"abcdef12-3456-7890-abcd-ef1234567890\n", true},
		{"ABCDEF12-3456-7890-abcd-ef1234567890", true},
		{"abcdef12", false},
		{"abcdef12_3456-7890-abcd-ef1234567890", false},
		{"abcdef12-3456-7890-abcd-ef123456789z", false},
	} {
		got, ok := bootID8(func(string) ([]byte, error) { return []byte(tc.id), nil })
		if ok != tc.want || (ok && got != "abcdef12") {
			t.Fatalf("boot %q=(%q,%v)", tc.id, got, ok)
		}
	}
	if got, ok := bootID8(func(string) ([]byte, error) { return nil, os.ErrPermission }); got != "" || ok {
		t.Fatal("accepted unreadable boot id")
	}
}

func TestOwnerKey(t *testing.T) {
	for _, failure := range []string{"", "host", "stat", "boot", "malformed stat", "malformed boot"} {
		t.Run("failure="+failure, func(t *testing.T) {
			args := []string{"claude"}
			if failure == "host" {
				args = []string{"sh"}
			}
			r := syntheticReaders(map[int]syntheticProc{10: {[]string{"sh"}, 1234}, 1234: {args, 1}})
			r.ReadFile = func(path string) ([]byte, error) {
				switch path {
				case "/proc/1234/stat":
					if failure == "stat" {
						return nil, os.ErrNotExist
					}
					if failure == "malformed stat" {
						return []byte("bad"), nil
					}
					return []byte(syntheticStat("424242")), nil
				case "/proc/sys/kernel/random/boot_id":
					if failure == "boot" {
						return nil, os.ErrPermission
					}
					if failure == "malformed boot" {
						return []byte("bad"), nil
					}
					return []byte("abcdef12-3456-7890-abcd-ef1234567890"), nil
				default:
					t.Fatalf("unexpected read: %s", path)
					return nil, os.ErrNotExist
				}
			}
			got, ok := OwnerKey(10, r)
			if failure == "" && runtime.GOOS == "linux" {
				if !ok || got != "host_abcdef12_1234_424242" {
					t.Fatalf("key=(%q,%v)", got, ok)
				}
			} else if ok || got != "" {
				t.Fatalf("failure did not close: (%q,%v)", got, ok)
			}
		})
	}
}
