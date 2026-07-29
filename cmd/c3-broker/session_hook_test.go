package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
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

	input := `{"session_id":"70341717-stable","cwd":"/home/k/proj","source":"resume","hook_event_name":"SessionStart"}`
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
	if e.CWD != "/home/k/proj" || e.Source != "resume" {
		t.Fatalf("handoff entry = %+v", e)
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
