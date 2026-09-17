package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/hostid"
	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/queue"
	"github.com/Andrometiq/c3/internal/sessionhandoff"
)

// captureStdout swaps os.Stdout for a pipe for the duration of fn and returns
// everything the hook printed. runSessionHook writes its resume-backlog hint via
// fmt.Printf (os.Stdout), so this captures it without a subprocess.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stdout
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var sb strings.Builder
		_, _ = io.Copy(&sb, r)
		done <- sb.String()
	}()
	fn()
	os.Stdout = orig
	_ = w.Close()
	out := <-done
	_ = r.Close()
	return out
}

// resumeBacklogTopicID is the topic the resume-backlog-hint fixtures attach to.
var resumeBacklogTopicID = int64(555)

// setupResumeBacklogEnv wires a temp XDG_CONFIG_HOME (mappings.json), a temp
// C3_QUEUE_DIR, and CLAUDE_ENV_FILE (so the handoff write succeeds and the hint
// runs downstream of it). When writeAttach is true it records a session
// attachment for stableID on a telegram topic; queueN appends that many held
// messages to the matching queue route.
func setupResumeBacklogEnv(t *testing.T, stableID string, writeAttach bool, queueN int) {
	t.Helper()
	setupTestEnv(t) // XDG_STATE_HOME + clears grok/antigravity env

	cfg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", cfg)
	qdir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", qdir)
	envFile := filepath.Join(t.TempDir(), "inst-resume", "sessionstart-hook-1.sh")
	t.Setenv("CLAUDE_ENV_FILE", envFile)

	if writeAttach {
		mf := &mappings.MappingsFile{SchemaVersion: 1}
		mf.UpsertSessionAttachment("claude", stableID, mappings.SessionAttachment{
			Channel: "telegram", ChatID: -100, TopicID: &resumeBacklogTopicID, Name: "myproject",
			LastAttachedAt: time.Now().UTC(),
		})
		data, err := json.MarshalIndent(mf, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(cfg, "c3", "mappings.json")
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if queueN > 0 {
		store, err := queue.NewStore(qdir)
		if err != nil {
			t.Fatal(err)
		}
		rk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &resumeBacklogTopicID}
		for i := 0; i < queueN; i++ {
			if err := store.Append(rk, &c3types.Inbound{
				Channel: "telegram", ChatID: -100, MessageID: int64(i + 1), Text: "held", Timestamp: time.Now(),
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// TestRunSessionHook_ResumeBacklogHint covers task #47 Part 1: on a RESUMED
// session whose last-attached topic still holds messages, the hook prints ONE
// SessionStart-context line naming the topic + count + fetch_queue; every other
// case (non-resume source, no attachment, empty queue) stays silent.
func TestRunSessionHook_ResumeBacklogHint(t *testing.T) {
	const stableID = "70341717-stable"
	cases := []struct {
		name        string
		source      string
		writeAttach bool
		queueN      int
		wantHint    bool
	}{
		{"resume with held backlog surfaces hint", "resume", true, 2, true},
		{"startup stays silent", "startup", true, 2, false},
		{"clear stays silent", "clear", true, 2, false},
		{"compact stays silent", "compact", true, 2, false},
		{"resume with no attachment (missing mappings) stays silent", "resume", false, 0, false},
		{"resume with empty queue stays silent", "resume", true, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setupResumeBacklogEnv(t, stableID, tc.writeAttach, tc.queueN)
			input := fmt.Sprintf(`{"session_id":%q,"cwd":"/home/k/proj","source":%q,"hook_event_name":"SessionStart"}`, stableID, tc.source)
			var out string
			withStdin(t, input, func() {
				out = captureStdout(t, func() {
					if err := runSessionHook(); err != nil {
						t.Fatalf("runSessionHook must return nil: %v", err)
					}
				})
			})
			if tc.wantHint {
				for _, want := range []string{"myproject", "2", "fetch_queue"} {
					if !strings.Contains(out, want) {
						t.Fatalf("resume hint missing %q; got %q", want, out)
					}
				}
			} else if strings.TrimSpace(out) != "" {
				t.Fatalf("expected no stdout hint, got %q", out)
			}
		})
	}
}

// TestRunSessionHook_ResumeUnreadableMappingsSilent: a mappings.json that exists
// but is unparseable must make the hint no-op silently (mappings.Read errors),
// while the handoff itself is still written — the hint is best-effort and never
// breaks the hook (exit 0, nil error).
func TestRunSessionHook_ResumeUnreadableMappingsSilent(t *testing.T) {
	setupTestEnv(t)
	cfg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", cfg)
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	envFile := filepath.Join(t.TempDir(), "inst-bad", "sessionstart-hook-1.sh")
	t.Setenv("CLAUDE_ENV_FILE", envFile)

	path := filepath.Join(cfg, "c3", "mappings.json")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{not valid json"), 0600); err != nil {
		t.Fatal(err)
	}

	var out string
	withStdin(t, `{"session_id":"70341717-stable","cwd":"/x","source":"resume","hook_event_name":"SessionStart"}`, func() {
		out = captureStdout(t, func() {
			if err := runSessionHook(); err != nil {
				t.Fatalf("runSessionHook must return nil on unreadable mappings: %v", err)
			}
		})
	})
	if strings.TrimSpace(out) != "" {
		t.Fatalf("unreadable mappings must be silent, got %q", out)
	}
	if _, ok := sessionhandoff.Read("inst-bad"); !ok {
		t.Fatal("handoff should still be written even when the backlog hint no-ops")
	}
}

// withStdin replaces os.Stdin with a pipe carrying data for the duration of fn.
func withStdin(t *testing.T, data string, fn func()) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = orig; _ = r.Close() }()
	go func() {
		_, _ = w.WriteString(data)
		_ = w.Close()
	}()
	fn()
}

func setupTestEnv(t *testing.T) string {
	t.Helper()
	originalOwnerKey := sessionHookOwnerKey
	sessionHookOwnerKey = func() (string, bool) { return "", false }
	t.Cleanup(func() { sessionHookOwnerKey = originalOwnerKey })
	t.Setenv("ANTIGRAVITY_CONVERSATION_ID", "")
	t.Setenv("GROK_SESSION_ID", "")
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	return state
}

func TestRunSessionHook_WritesHandoff(t *testing.T) {
	_ = setupTestEnv(t)
	// CLAUDE_ENV_FILE's parent dir basename is the ephemeral instance id.
	envFile := filepath.Join(t.TempDir(), "b60e8044-instance", "sessionstart-hook-1.sh")
	t.Setenv("CLAUDE_ENV_FILE", envFile)

	input := `{"session_id":"70341717-stable","cwd":"/workspace/project","source":"resume","transcript_path":"/state/sessions/70341717-stable.jsonl","hook_event_name":"SessionStart"}`
	withStdin(t, input, func() {
		if err := runSessionHook(); err != nil {
			t.Fatalf("runSessionHook returned error (must be nil): %v", err)
		}
	})

	e, ok := sessionhandoff.Read("b60e8044-instance")
	if !ok {
		t.Fatal("expected a handoff entry for the instance id")
	}
	if e.StableSessionID != "70341717-stable" {
		t.Fatalf("StableSessionID = %q, want 70341717-stable", e.StableSessionID)
	}
	if e.CWD != "/workspace/project" || e.Source != "resume" || e.TranscriptPath != "/state/sessions/70341717-stable.jsonl" {
		t.Fatalf("handoff entry = %+v", e)
	}
}

func TestRunSessionHook_LegacyAndEmptyTranscriptPathStillWrite(t *testing.T) {
	for _, input := range []string{
		`{"session_id":"stable-legacy","cwd":"/workspace/project","source":"startup"}`,
		`{"session_id":"stable-legacy","cwd":"/workspace/project","source":"startup","transcript_path":""}`,
	} {
		t.Run(input, func(t *testing.T) {
			setupTestEnv(t)
			t.Setenv("CLAUDE_ENV_FILE", filepath.Join(t.TempDir(), "legacy-instance", "hook.sh"))
			withStdin(t, input, func() {
				if err := runSessionHook(); err != nil {
					t.Fatalf("runSessionHook must exit 0: %v", err)
				}
			})
			entry, ok := sessionhandoff.Read("legacy-instance")
			if !ok || entry.TranscriptPath != "" {
				t.Fatalf("legacy/empty transcript handoff missing: ok=%v entry=%+v", ok, entry)
			}
		})
	}
}

// TestRunSessionHook_WritesHandoffUnderBothInstanceAndStableKeys covers the
// 2026-08-25 auto-reattach regression: Claude Code ≥2.1.245 exports the STABLE
// (resumed) session id — not the ephemeral instance id — to MCP servers as
// CLAUDE_CODE_SESSION_ID, so the adapter polls for <stable>.json while the hook,
// keyed on CLAUDE_ENV_FILE, only ever wrote <instance>.json. The hook must now
// write BOTH keys so recovery fires whichever id the host exports.
func TestRunSessionHook_WritesHandoffUnderBothInstanceAndStableKeys(t *testing.T) {
	_ = setupTestEnv(t)
	envFile := filepath.Join(t.TempDir(), "ephem-instance", "sessionstart-hook-1.sh")
	t.Setenv("CLAUDE_ENV_FILE", envFile)

	input := `{"session_id":"stable-sess","cwd":"/home/k/proj","source":"resume","hook_event_name":"SessionStart"}`
	withStdin(t, input, func() {
		if err := runSessionHook(); err != nil {
			t.Fatalf("runSessionHook returned error (must be nil): %v", err)
		}
	})

	// Old convention (env == ephemeral id): <instance>.json resolves.
	byInstance, ok := sessionhandoff.Read("ephem-instance")
	if !ok || byInstance.StableSessionID != "stable-sess" {
		t.Fatalf("instance-keyed handoff missing/wrong: ok=%v entry=%+v", ok, byInstance)
	}
	// New convention (env == stable id): <stable>.json must also resolve, to the
	// same stable id (a self-referential, terminal handoff).
	byStable, ok := sessionhandoff.Read("stable-sess")
	if !ok || byStable.StableSessionID != "stable-sess" {
		t.Fatalf("stable-keyed handoff missing/wrong: ok=%v entry=%+v", ok, byStable)
	}
	if byInstance.CWD != "/home/k/proj" || byStable.CWD != "/home/k/proj" {
		t.Fatalf("handoff CWDs = %q / %q, want /home/k/proj", byInstance.CWD, byStable.CWD)
	}
	// Both aliases share one timestamp so the terminal-handoff walk terminates on
	// the not-newer guard rather than chasing between them.
	if byInstance.UnixNano != byStable.UnixNano {
		t.Fatalf("aliases must share one UnixNano: instance=%d stable=%d", byInstance.UnixNano, byStable.UnixNano)
	}
}

func TestRunSessionHook_EmptyEnvFileNoOp(t *testing.T) {
	state := setupTestEnv(t)
	t.Setenv("CLAUDE_ENV_FILE", "") // no instance id derivable

	withStdin(t, `{"session_id":"70341717-stable","source":"resume"}`, func() {
		if err := runSessionHook(); err != nil {
			t.Fatalf("runSessionHook must exit 0 with empty CLAUDE_ENV_FILE: %v", err)
		}
	})
	// Nothing should have been written anywhere under session-instances.
	dir := filepath.Join(state, "c3", "session-instances")
	if ents, err := os.ReadDir(dir); err == nil && len(ents) > 0 {
		t.Fatalf("no handoff should be written without an instance id; found %d entries", len(ents))
	}
}

func TestRunSessionHook_EmptySessionIDNoOp(t *testing.T) {
	_ = setupTestEnv(t)
	envFile := filepath.Join(t.TempDir(), "inst-xyz", "sessionstart-hook-1.sh")
	t.Setenv("CLAUDE_ENV_FILE", envFile)

	withStdin(t, `{"cwd":"/x","source":"startup"}`, func() { // no session_id
		if err := runSessionHook(); err != nil {
			t.Fatalf("runSessionHook must exit 0 with empty session_id: %v", err)
		}
	})
	if _, ok := sessionhandoff.Read("inst-xyz"); ok {
		t.Fatal("no handoff should be written without a session id")
	}
}

func TestRunSessionHook_GrokWritesHandoffByStableID(t *testing.T) {
	_ = setupTestEnv(t)
	t.Setenv("CLAUDE_ENV_FILE", "")              // no Claude instance id derivable
	t.Setenv("GROK_SESSION_ID", "grok-uuid-123") // Grok env present → Grok branch

	withStdin(t, `{"session_id":"grok-uuid-123","cwd":"/w","source":"startup"}`, func() {
		if err := runSessionHook(); err != nil {
			t.Fatalf("runSessionHook returned error (must be nil): %v", err)
		}
	})

	e, ok := sessionhandoff.Read("grok-uuid-123")
	if !ok {
		t.Fatal("expected a handoff entry keyed by the Grok stable session id")
	}
	if e.StableSessionID != "grok-uuid-123" || e.CWD != "/w" {
		t.Fatalf("handoff entry = %+v", e)
	}
}

func TestRunSessionHook_GrokRejectsUnsafeSessionID(t *testing.T) {
	state := setupTestEnv(t)
	t.Setenv("CLAUDE_ENV_FILE", "")
	t.Setenv("GROK_SESSION_ID", "present") // trigger the Grok branch

	// A traversal-shaped session id must be refused as a handoff key (exit 0,
	// nothing written) — the guard mirrors sessionhandoff.Path's invariant.
	withStdin(t, `{"session_id":"../evil","cwd":"/w","source":"startup"}`, func() {
		if err := runSessionHook(); err != nil {
			t.Fatalf("runSessionHook must exit 0 on an unsafe session id: %v", err)
		}
	})
	dir := filepath.Join(state, "c3", "session-instances")
	if ents, err := os.ReadDir(dir); err == nil && len(ents) > 0 {
		t.Fatalf("no handoff should be written for an unsafe session id; found %d entries", len(ents))
	}
}

func TestRunSessionHook_GarbageStdinNoOp(t *testing.T) {
	_ = setupTestEnv(t)
	envFile := filepath.Join(t.TempDir(), "inst-garbage", "sessionstart-hook-1.sh")
	t.Setenv("CLAUDE_ENV_FILE", envFile)

	withStdin(t, `{not valid json`, func() {
		if err := runSessionHook(); err != nil {
			t.Fatalf("runSessionHook must exit 0 on garbage stdin: %v", err)
		}
	})
	if _, ok := sessionhandoff.Read("inst-garbage"); ok {
		t.Fatal("no handoff should be written on garbage stdin")
	}
}

func TestRunSessionHook_OwnerAlias(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux owner keys")
	}
	for _, tc := range []struct {
		name                  string
		noHost, noStart, grok bool
		wantFiles             int
	}{
		{name: "three identical aliases", wantFiles: 3},
		{name: "no Claude host", noHost: true, wantFiles: 2},
		{name: "start time unreadable", noStart: true, wantFiles: 2},
		{name: "nested Grok", grok: true, wantFiles: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupTestEnv(t)
			t.Setenv("GROK_HOOK_EVENT", "")
			t.Setenv("CLAUDE_ENV_FILE", filepath.Join(t.TempDir(), "A", "hook.sh"))
			if tc.grok {
				t.Setenv("CLAUDE_ENV_FILE", "")
				t.Setenv("GROK_SESSION_ID", "B")
			}
			readers := hostid.ProcReaders{
				Cmdline: func(pid int) ([]string, bool) {
					if pid == 10 {
						return []string{"sh"}, true
					}
					if pid == 20 && !tc.noHost {
						return []string{"claude"}, true
					}
					return []string{"sh"}, true
				},
				PPID: func(pid int) (int, bool) {
					if pid == 10 {
						return 20, true
					}
					return 1, true
				},
				ReadFile: func(path string) ([]byte, error) {
					switch path {
					case "/proc/20/stat":
						if tc.noStart {
							return nil, os.ErrNotExist
						}
						return []byte("20 (claude) S 1 " + strings.Repeat("0 ", 17) + "12345 0"), nil
					case "/proc/sys/kernel/random/boot_id":
						return []byte("abcdef12-3456-7890-abcd-ef1234567890"), nil
					default:
						t.Fatalf("unexpected read: %s", path)
						return nil, os.ErrNotExist
					}
				},
			}
			sessionHookOwnerKey = func() (string, bool) {
				if tc.grok {
					t.Fatal("Grok consulted Claude owner resolver")
				}
				return hostid.OwnerKey(10, readers)
			}
			withStdin(t, `{"session_id":"B","cwd":"/workspace/project","source":"startup","transcript_path":"/state/B.jsonl"}`, func() {
				if err := runSessionHook(); err != nil {
					t.Fatalf("hook must exit zero: %v", err)
				}
			})
			dir, err := sessionhandoff.Dir()
			if err != nil {
				t.Fatal(err)
			}
			files, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != tc.wantFiles {
				t.Fatalf("files=%v, want %d", files, tc.wantFiles)
			}
			stable, err := os.ReadFile(filepath.Join(dir, "B.json"))
			if err != nil {
				t.Fatal(err)
			}
			wantNames := map[string]bool{"B.json": true}
			if !tc.grok {
				wantNames["A.json"] = true
			}
			if tc.wantFiles == 3 {
				wantNames["host_abcdef12_20_12345.json"] = true
			}
			for _, file := range files {
				if !wantNames[file.Name()] {
					t.Fatalf("unexpected alias: %s", file.Name())
				}
				data, err := os.ReadFile(filepath.Join(dir, file.Name()))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(data, stable) {
					t.Fatalf("alias %s differs from stable entry", file.Name())
				}
			}
			entry, ok := sessionhandoff.Read("B")
			if !ok || entry.UnixNano == 0 || entry.TranscriptPath != "/state/B.jsonl" {
				t.Fatalf("entry=%+v ok=%v", entry, ok)
			}
		})
	}
}
