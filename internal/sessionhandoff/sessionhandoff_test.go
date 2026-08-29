package sessionhandoff

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// useScratchDir points Dir() at a per-test temp directory via XDG_STATE_HOME.
func useScratchDir(t *testing.T) string {
	t.Helper()
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	return filepath.Join(state, "c3", "session-instances")
}

func TestWriteReadRoundTrip(t *testing.T) {
	dir := useScratchDir(t)
	want := Entry{
		StableSessionID: "70341717-stable", CWD: "/workspace/project", Source: "resume",
		TranscriptPath: "/state/sessions/70341717-stable.jsonl", UnixNano: time.Now().UnixNano(),
	}
	if err := Write("b60e8044-instance", want); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// File lives where Path says, mode 0600.
	p := filepath.Join(dir, "b60e8044-instance.json")
	info, err := os.Stat(p)
	if err != nil {
		t.Fatalf("stat handoff file: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("handoff file mode = %v, want 0600", info.Mode().Perm())
	}
	got, ok := Read("b60e8044-instance")
	if !ok {
		t.Fatal("Read returned ok=false for a written entry")
	}
	if got.StableSessionID != want.StableSessionID || got.CWD != want.CWD || got.Source != want.Source || got.TranscriptPath != want.TranscriptPath {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got, want)
	}
}

func TestReadLegacyEntryWithoutTranscriptPath(t *testing.T) {
	dir := useScratchDir(t)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "legacy.json"), []byte(`{"stable_session_id":"stable","unix_nano":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	entry, ok := Read("legacy")
	if !ok || entry.StableSessionID != "stable" || entry.TranscriptPath != "" {
		t.Fatalf("legacy handoff did not remain readable: ok=%v entry=%+v", ok, entry)
	}
}

func TestReadMissingIsNotFound(t *testing.T) {
	useScratchDir(t)
	if _, ok := Read("never-written"); ok {
		t.Fatal("Read of an absent entry must return ok=false")
	}
}

func TestReadCorruptIsNotFound(t *testing.T) {
	dir := useScratchDir(t)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "inst.json"), []byte("{not json"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, ok := Read("inst"); ok {
		t.Fatal("corrupt entry must read as ok=false")
	}
}

func TestReadEmptyStableIDIsNotFound(t *testing.T) {
	dir := useScratchDir(t)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "inst.json"), []byte(`{"cwd":"/x","unix_nano":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, ok := Read("inst"); ok {
		t.Fatal("entry with empty stable id must read as ok=false")
	}
}

func TestPathSanitization(t *testing.T) {
	useScratchDir(t)
	for _, bad := range []string{"", ".", "..", "../x", "a/b", "/abs", string(filepath.Separator)} {
		if _, err := Path(bad); err == nil {
			t.Fatalf("Path(%q) must error (unsafe instance id)", bad)
		}
	}
	if _, err := Path("clean-uuid-1234"); err != nil {
		t.Fatalf("Path(clean) unexpected error: %v", err)
	}
}

func TestWriteRejectsUnsafeInstanceID(t *testing.T) {
	useScratchDir(t)
	if err := Write("../escape", Entry{StableSessionID: "x", UnixNano: 1}); err == nil {
		t.Fatal("Write must reject an unsafe instance id")
	}
	if err := Write("", Entry{StableSessionID: "x", UnixNano: 1}); err == nil {
		t.Fatal("Write must reject an empty instance id")
	}
}

func TestPruneStaleRemovesOldKeepsFresh(t *testing.T) {
	useScratchDir(t)
	now := time.Now()
	old := Entry{StableSessionID: "old", UnixNano: now.Add(-48 * time.Hour).UnixNano()}
	fresh := Entry{StableSessionID: "fresh", UnixNano: now.Add(-1 * time.Hour).UnixNano()}
	if err := Write("old-inst", old); err != nil {
		t.Fatal(err)
	}
	if err := Write("fresh-inst", fresh); err != nil {
		t.Fatal(err)
	}
	n := PruneStale(24*time.Hour, now)
	if n != 1 {
		t.Fatalf("PruneStale deleted %d, want 1", n)
	}
	if _, ok := Read("old-inst"); ok {
		t.Fatal("stale entry should have been pruned")
	}
	if _, ok := Read("fresh-inst"); !ok {
		t.Fatal("fresh entry should have survived prune")
	}
}

func TestPruneStaleMissingDirIsZero(t *testing.T) {
	useScratchDir(t) // dir not created yet (no Write)
	if n := PruneStale(time.Hour, time.Now()); n != 0 {
		t.Fatalf("PruneStale on a missing dir = %d, want 0", n)
	}
}
