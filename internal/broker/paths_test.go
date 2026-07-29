package broker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mustSocketPath(t *testing.T) string {
	t.Helper()
	path, err := SocketPath()
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func mustPidFilePath(t *testing.T) string {
	t.Helper()
	path, err := PidFilePath()
	if err != nil {
		t.Fatal(err)
	}
	return path
}

// Path-resolution invariant (2026-05-09): SocketPath() and
// PidFilePath() MUST live in the same directory and MUST be deterministic
// across invocations regardless of the calling process's env. Two brokers
// with different XDG_RUNTIME_DIR ended up on different sockets, both
// polled Telegram, both 409'd, adapter conns scattered → claims landed on
// the wrong broker → messages fell to fallback even with valid claims.
func TestSocketAndPidFile_SameDirectory(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")
	sock := mustSocketPath(t)
	pid := mustPidFilePath(t)
	if filepath.Dir(sock) != filepath.Dir(pid) {
		t.Errorf("SocketPath dir %q != PidFilePath dir %q",
			filepath.Dir(sock), filepath.Dir(pid))
	}
}

func TestSocketPath_XDGRuntimeHonored(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", dir)
	got := mustSocketPath(t)
	want := filepath.Join(dir, "c3.sock")
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestSocketPath_NoXDG_FallsBackDeterministically(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")
	t.Setenv("HOME", t.TempDir()) // make sure HOME is unrelated
	got := mustSocketPath(t)
	if !strings.HasSuffix(got, "/c3.sock") {
		t.Errorf("got %q, want path ending in /c3.sock", got)
	}
	// Calling twice in the same env must return the same value.
	if got2 := mustSocketPath(t); got2 != got {
		t.Errorf("non-deterministic: %q vs %q", got, got2)
	}
}

func TestPidFilePath_XDGRuntimeHonored(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", dir)
	got := mustPidFilePath(t)
	want := filepath.Join(dir, "c3-broker.pid")
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestPidFilePath_NoXDG_FallsBackDeterministically(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")
	t.Setenv("HOME", t.TempDir())
	got := mustPidFilePath(t)
	if !strings.HasSuffix(got, "/c3-broker.pid") {
		t.Errorf("got %q, want path ending in /c3-broker.pid", got)
	}
	if got2 := mustPidFilePath(t); got2 != got {
		t.Errorf("non-deterministic: %q vs %q", got, got2)
	}
}

func TestPrivateRuntimeFallback_CreatesAndValidates0700Directory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c3-runtime")
	got, err := validateOrCreatePrivateRuntimeDir(path, uint32(os.Getuid()))
	if err != nil {
		t.Fatal(err)
	}
	if got != path {
		t.Fatalf("path=%q, want %q", got, path)
	}
	st, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !st.IsDir() || st.Mode().Perm() != 0700 {
		t.Fatalf("fallback created unsafe mode/type: %s %04o", st.Mode().Type(), st.Mode().Perm())
	}
}

func TestPrivateRuntimeFallback_RejectsSymlink(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "target")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(parent, "c3-runtime")
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if _, err := validateOrCreatePrivateRuntimeDir(path, uint32(os.Getuid())); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("pre-planted symlink was accepted: %v", err)
	}
}

func TestPrivateRuntimeFallback_RejectsNonDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c3-runtime")
	if err := os.WriteFile(path, []byte("not a directory"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := validateOrCreatePrivateRuntimeDir(path, uint32(os.Getuid())); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("pre-planted file was accepted: %v", err)
	}
}

func TestPrivateRuntimeFallback_RejectsWrongMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c3-runtime")
	if err := os.Mkdir(path, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := validateOrCreatePrivateRuntimeDir(path, uint32(os.Getuid())); err == nil || !strings.Contains(err.Error(), "want 0700") {
		t.Fatalf("world-accessible runtime directory was accepted: %v", err)
	}
}

func TestPrivateRuntimeFallback_RejectsWrongOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c3-runtime")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := validateOrCreatePrivateRuntimeDir(path, uint32(os.Getuid()+1)); err == nil || !strings.Contains(err.Error(), "owned by uid") {
		t.Fatalf("runtime directory owned by another uid was accepted: %v", err)
	}
}

func TestPrivateRuntimeFallback_PropagatesCreateFailure(t *testing.T) {
	parentFile := filepath.Join(t.TempDir(), "parent-file")
	if err := os.WriteFile(parentFile, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := validateOrCreatePrivateRuntimeDir(filepath.Join(parentFile, "c3-runtime"), uint32(os.Getuid())); err == nil || !strings.Contains(err.Error(), "create private runtime directory") {
		t.Fatalf("runtime fallback swallowed its directory-creation failure: %v", err)
	}
}

func TestEnsureParentDir(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "a", "b", "c", "file.txt")
	if err := ensureParentDir(target); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Dir(target)); err != nil {
		t.Errorf("parent dir not created: %v", err)
	}
}
