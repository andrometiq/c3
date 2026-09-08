//go:build !windows

package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/shimconfig"
)

func TestInstallClaudeShim_FreshInstall_CreatesSymlink(t *testing.T) {
	dir := t.TempDir()
	launcher := filepath.Join(dir, "claude-shim")
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\necho 'Usage: claude daemon [options]'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	installPath := filepath.Join(dir, "bin", "claude")

	if err := installClaudeShim(installPath, launcher, false); err != nil {
		t.Fatal(err)
	}
	got, err := os.Readlink(installPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != launcher {
		t.Fatalf("link target %q, want %q", got, launcher)
	}
}

func TestInstallClaudeShim_RefusesNonShimFile(t *testing.T) {
	dir := t.TempDir()
	launcher := filepath.Join(dir, "claude-shim")
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\necho 'Usage: claude daemon [options]'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	installPath := filepath.Join(dir, "claude")
	if err := os.WriteFile(installPath, []byte("real claude here"), 0o755); err != nil {
		t.Fatal(err)
	}

	err := installClaudeShim(installPath, launcher, false)
	if err == nil {
		t.Fatal("want refusal error, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to overwrite") {
		t.Fatalf("got %v, want refusal error", err)
	}
	// And it must NOT have been clobbered.
	data, _ := os.ReadFile(installPath)
	if string(data) != "real claude here" {
		t.Fatalf("file was modified: %q", string(data))
	}
}

func TestInstallClaudeShim_ForceOverwritesNonShimFile(t *testing.T) {
	dir := t.TempDir()
	launcher := filepath.Join(dir, "claude-shim")
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\necho 'Usage: claude daemon [options]'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	installPath := filepath.Join(dir, "claude")
	if err := os.WriteFile(installPath, []byte("real claude here"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := installClaudeShim(installPath, launcher, true); err != nil {
		t.Fatal(err)
	}
	got, err := os.Readlink(installPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != launcher {
		t.Fatalf("link target %q, want %q", got, launcher)
	}
}

func TestInstallClaudeShim_ReplacesExistingSymlink(t *testing.T) {
	// Isolate shim config so this test (which now triggers
	// EvalSymlinks-and-remember on the stale target) doesn't write to
	// the developer's real ~/.config/c3/claude-shim.json.
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	dir := t.TempDir()
	launcher := filepath.Join(dir, "claude-shim")
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\necho 'Usage: claude daemon [options]'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(dir, "old-target")
	if err := os.WriteFile(stale, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	installPath := filepath.Join(dir, "claude")
	if err := os.Symlink(stale, installPath); err != nil {
		t.Fatal(err)
	}

	if err := installClaudeShim(installPath, launcher, false); err != nil {
		t.Fatal(err)
	}
	got, err := os.Readlink(installPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != launcher {
		t.Fatalf("link target %q, want %q", got, launcher)
	}
}

func TestInstallClaudeShim_SymlinkToRealClaude_PersistsConfig(t *testing.T) {
	dir := t.TempDir()
	// Isolate XDG_CONFIG_HOME so the install writes the shim config
	// inside the test tempdir instead of $HOME/.config.
	xdg := filepath.Join(dir, "xdg")
	t.Setenv("XDG_CONFIG_HOME", xdg)

	launcher := filepath.Join(dir, "claude-shim")
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\necho 'Usage: claude daemon [options]'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Existing "real claude" the user installed (e.g. via npm/nvm).
	realClaude := filepath.Join(dir, "versions", "2.1.143", "claude")
	if err := os.MkdirAll(filepath.Dir(realClaude), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(realClaude, []byte("#!/bin/sh\necho real\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	installPath := filepath.Join(dir, "bin", "claude")
	if err := os.MkdirAll(filepath.Dir(installPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realClaude, installPath); err != nil {
		t.Fatal(err)
	}

	if err := installClaudeShim(installPath, launcher, false); err != nil {
		t.Fatalf("installClaudeShim: %v", err)
	}

	// Symlink now points at launcher.
	got, err := os.Readlink(installPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != launcher {
		t.Fatalf("link target %q, want %q", got, launcher)
	}

	// And the shim config records the resolved real-claude path.
	cfgPath, err := shimconfig.Path()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("shim config missing: %v", err)
	}
	var parsed shimconfig.File
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("parse shim config: %v", err)
	}
	wantResolved, _ := filepath.EvalSymlinks(realClaude)
	if parsed.RealClaude != wantResolved {
		t.Fatalf("real_claude in config = %q, want %q", parsed.RealClaude, wantResolved)
	}
}

// TestInstallClaudeShim_SymlinkAlreadyPointsAtLauncher_DoesNotRemove is
// the idempotency-short-circuit guard. When the existing symlink at
// installPath already resolves to launcher, we MUST NOT remove +
// recreate it — that opens a brief window where installPath doesn't
// exist on disk (race with concurrent `claude` invocations). The
// short-circuit keeps the inode stable. Closes report MINOR m6
// (2026-05-19).
func TestInstallClaudeShim_SymlinkAlreadyPointsAtLauncher_DoesNotRemove(t *testing.T) {
	dir := t.TempDir()
	xdg := filepath.Join(dir, "xdg")
	t.Setenv("XDG_CONFIG_HOME", xdg)

	launcher := filepath.Join(dir, "claude-shim")
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\necho 'Usage: claude daemon [options]'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	installPath := filepath.Join(dir, "claude")
	if err := os.Symlink(launcher, installPath); err != nil {
		t.Fatal(err)
	}

	// Capture inode + ctime before the install.
	beforeStat, err := os.Lstat(installPath)
	if err != nil {
		t.Fatal(err)
	}
	beforeSys, ok := beforeStat.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("Stat_t.Ino not available on this platform")
	}
	beforeIno := beforeSys.Ino

	if err := installClaudeShim(installPath, launcher, false); err != nil {
		t.Fatalf("installClaudeShim: %v", err)
	}

	// Symlink still points at launcher and the inode is unchanged —
	// proves we short-circuited rather than remove+recreate.
	got, err := os.Readlink(installPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != launcher {
		t.Fatalf("link target %q, want %q", got, launcher)
	}
	afterStat, err := os.Lstat(installPath)
	if err != nil {
		t.Fatal(err)
	}
	afterSys, ok := afterStat.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("Stat_t.Ino not available on this platform")
	}
	if afterSys.Ino != beforeIno {
		t.Fatalf("symlink inode changed (before=%d, after=%d) — short-circuit failed; remove+recreate happened",
			beforeIno, afterSys.Ino)
	}
}

func TestInstallClaudeShim_SymlinkAlreadyPointsAtLauncher_NoConfigWrite(t *testing.T) {
	dir := t.TempDir()
	xdg := filepath.Join(dir, "xdg")
	t.Setenv("XDG_CONFIG_HOME", xdg)

	launcher := filepath.Join(dir, "claude-shim")
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\necho 'Usage: claude daemon [options]'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	installPath := filepath.Join(dir, "claude")
	// Existing symlink already points at our launcher (re-running install).
	if err := os.Symlink(launcher, installPath); err != nil {
		t.Fatal(err)
	}

	if err := installClaudeShim(installPath, launcher, false); err != nil {
		t.Fatalf("installClaudeShim: %v", err)
	}

	// Final link still points at launcher.
	got, err := os.Readlink(installPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != launcher {
		t.Fatalf("link target %q, want %q", got, launcher)
	}

	// No config should have been written for a self-link.
	cfgPath, err := shimconfig.Path()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cfgPath); !os.IsNotExist(err) {
		t.Fatalf("shim config exists but shouldn't: stat err = %v", err)
	}
}

func TestInstallClaudeShim_AllowsOverwriteWhenSentinelPresent(t *testing.T) {
	dir := t.TempDir()
	launcher := filepath.Join(dir, "claude-shim")
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\necho 'Usage: claude daemon [options]'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	installPath := filepath.Join(dir, "claude")
	// Plant a regular file containing the shim sentinel — simulating a
	// prior shim install that was copied rather than symlinked.
	body := []byte("binary blob ...... " + shimSentinel + " ...... more bytes")
	if err := os.WriteFile(installPath, body, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := installClaudeShim(installPath, launcher, false); err != nil {
		t.Fatalf("expected sentinel detection to allow overwrite, got %v", err)
	}
	got, err := os.Readlink(installPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != launcher {
		t.Fatalf("link target %q, want %q", got, launcher)
	}
}

func TestUninstallClaudeShim_MissingPath_NoOp(t *testing.T) {
	dir := t.TempDir()
	installPath := filepath.Join(dir, "claude")
	removed, err := uninstallClaudeShim(installPath, false)
	if err != nil {
		t.Fatal(err)
	}
	if removed {
		t.Fatal("want removed=false for missing path")
	}
}

func TestUninstallClaudeShim_RemovesSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "launcher")
	if err := os.WriteFile(target, []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	installPath := filepath.Join(dir, "claude")
	if err := os.Symlink(target, installPath); err != nil {
		t.Fatal(err)
	}
	removed, err := uninstallClaudeShim(installPath, false)
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatal("want removed=true")
	}
	if _, err := os.Lstat(installPath); !os.IsNotExist(err) {
		t.Fatalf("file still present: %v", err)
	}
}

func TestUninstallClaudeShim_RefusesNonShimFile(t *testing.T) {
	dir := t.TempDir()
	installPath := filepath.Join(dir, "claude")
	if err := os.WriteFile(installPath, []byte("real claude"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := uninstallClaudeShim(installPath, false)
	if err == nil {
		t.Fatal("want refusal error, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to remove") {
		t.Fatalf("got %v, want refusal", err)
	}
	if _, err := os.Stat(installPath); err != nil {
		t.Fatalf("file vanished despite refusal: %v", err)
	}
}

func TestUninstallClaudeShim_RemovesSentinelFile(t *testing.T) {
	dir := t.TempDir()
	installPath := filepath.Join(dir, "claude")
	body := []byte("xxxxx " + shimSentinel + " yyyyy")
	if err := os.WriteFile(installPath, body, 0o755); err != nil {
		t.Fatal(err)
	}
	removed, err := uninstallClaudeShim(installPath, false)
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatal("want removed=true")
	}
	if _, err := os.Lstat(installPath); !os.IsNotExist(err) {
		t.Fatalf("file still present: %v", err)
	}
}

func TestUninstallClaudeShim_ForceRemovesNonShimFile(t *testing.T) {
	dir := t.TempDir()
	installPath := filepath.Join(dir, "claude")
	if err := os.WriteFile(installPath, []byte("real claude"), 0o755); err != nil {
		t.Fatal(err)
	}
	removed, err := uninstallClaudeShim(installPath, true)
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatal("want removed=true")
	}
}

func TestFileContainsSentinel_StraddlesChunkBoundary(t *testing.T) {
	// Build a file where shimSentinel straddles a 65536-byte chunk
	// boundary so we exercise the slop-overlap path in fileContainsSentinel.
	dir := t.TempDir()
	path := filepath.Join(dir, "f")
	half := len(shimSentinel) / 2
	prefix := make([]byte, 65536-half)
	for i := range prefix {
		prefix[i] = 'x'
	}
	suffix := []byte("yyyy")
	body := append(append(prefix, []byte(shimSentinel)...), suffix...)
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := fileContainsSentinel(path, shimSentinel)
	if err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Fatal("sentinel not detected across chunk boundary")
	}
}

// Build the actual shim, but resolve only a fake Claude under TempDir. Checking
// exact argv catches the original bug where the injected channel flag swallowed
// "daemon" and prevents a test from accidentally validating the real CLI.
func TestInstallClaudeShim_SelfTest(t *testing.T) {
	launcher := filepath.Join(t.TempDir(), "claude-shim")
	if out, err := exec.Command("go", "build", "-o", launcher, "../claude-shim").CombinedOutput(); err != nil {
		t.Fatalf("build shim: %v\n%s", err, out)
	}
	for _, tc := range []struct {
		name, daemon, version, previous, wantError string
	}{
		{name: "fresh", daemon: "echo 'Usage: claude daemon [options]'"},
		{name: "relative-path", daemon: "echo 'Usage: claude daemon [options]'"},
		{name: "replace-symlink", daemon: "echo 'Usage: claude daemon [options]'", previous: "symlink"},
		{name: "disabled", daemon: "echo 'Agent view is not enabled' >&2; exit 1"},
		{name: "disabled-hyphen", daemon: "echo 'Agent-view disabled'"},
		{name: "wrong-help", daemon: "echo 'Usage: claude [options]'; echo 'Try claude daemon'", wantError: "daemon --help"},
		{name: "daemon-exit", daemon: "echo 'Usage: claude daemon [options]'; exit 2", wantError: "daemon --help"},
		{name: "version-exit", daemon: "echo 'Usage: claude daemon [options]'", version: "exit 3", wantError: "--version"},
		{name: "restore-relative-symlink", daemon: "echo 'wrong dispatch'", previous: "symlink", wantError: "daemon --help"},
		{name: "restore-dangling-symlink", daemon: "echo 'wrong dispatch'", previous: "dangling", wantError: "daemon --help"},
		{name: "restore-forced-file", daemon: "echo 'wrong dispatch'", previous: "file", wantError: "daemon --help"},
		{name: "keep-existing-shim", daemon: "echo 'wrong dispatch'", previous: "shim", wantError: "daemon --help"},
		{name: "timeout", daemon: "while :; do :; done", wantError: "deadline exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("HOME", dir)
			t.Setenv("XDG_CONFIG_HOME", filepath.Join(dir, "config"))
			t.Setenv("C3_CLAUDE_REAL", "")
			realDir := filepath.Join(dir, "real")
			if err := os.MkdirAll(realDir, 0o755); err != nil {
				t.Fatal(err)
			}
			realClaude := filepath.Join(realDir, "claude")
			log := filepath.Join(dir, "calls")
			t.Setenv("C3_TEST_CALLS", log)
			t.Setenv("PATH", dir+string(os.PathListSeparator)+realDir)
			if tc.previous == "symlink" {
				// Replacing the only Claude link must resolve through saved config.
				t.Setenv("PATH", dir)
			}
			script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$C3_TEST_CALLS\"\n" +
				"if [ \"$#\" = 2 ] && [ \"$1\" = daemon ] && [ \"$2\" = --help ]; then\n" + tc.daemon + "\n" +
				"elif [ \"$#\" = 2 ] && [ \"$1\" = --dangerously-load-development-channels=plugin:c3@c3 ] && [ \"$2\" = --version ]; then\n" +
				"echo 'test version'\n" + tc.version + "\nelse\necho 'wrong argv' >&2; exit 99\nfi\n"
			if err := os.WriteFile(realClaude, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			installPath := filepath.Join(dir, "claude")
			if tc.name == "relative-path" {
				t.Chdir(dir)
				installPath = "claude"
			}
			previousTarget := ""
			switch tc.previous {
			case "symlink":
				previousTarget = "real/claude"
			case "dangling":
				previousTarget = "missing-claude"
			case "shim":
				previousTarget = launcher
			case "file":
				if err := os.WriteFile(installPath, []byte("previous executable"), 0o751); err != nil {
					t.Fatal(err)
				}
			}
			if previousTarget != "" {
				if err := os.Symlink(previousTarget, installPath); err != nil {
					t.Fatal(err)
				}
			}
			started := time.Now()
			err := installClaudeShim(installPath, launcher, tc.previous == "file")
			if time.Since(started) > 8*time.Second {
				t.Fatal("self-test exceeded timeout budget")
			}
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
				if target, err := os.Readlink(installPath); err != nil || target != launcher {
					t.Fatalf("installed link = %q, %v", target, err)
				}
				calls, err := os.ReadFile(log)
				if err != nil {
					t.Fatal(err)
				}
				if string(calls) != "daemon --help\n--dangerously-load-development-channels=plugin:c3@c3 --version\n" {
					t.Fatalf("self-test did not exercise both commands: %q", calls)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("error = %v, want %q", err, tc.wantError)
			}
			if previousTarget != "" {
				if target, err := os.Readlink(installPath); err != nil || target != previousTarget {
					t.Fatalf("restored link = %q, %v; want %q", target, err, previousTarget)
				}
			} else if tc.previous == "file" {
				data, err := os.ReadFile(installPath)
				if err != nil || string(data) != "previous executable" {
					t.Fatalf("restored file = %q, %v", data, err)
				}
				info, err := os.Stat(installPath)
				if err != nil || info.Mode().Perm() != 0o751 {
					t.Fatalf("restored mode: %v, %v", info, err)
				}
			} else if _, err := os.Lstat(installPath); !os.IsNotExist(err) {
				t.Fatalf("failed install left a link: %v", err)
			}
		})
	}
}

func TestInstallClaudeShim_RefusesDirectory(t *testing.T) {
	path := t.TempDir()
	if err := installClaudeShim(path, filepath.Join(t.TempDir(), "claude-shim"), true); err == nil {
		t.Fatal("expected refusal to replace a directory")
	}
	if info, err := os.Stat(path); err != nil || !info.IsDir() {
		t.Fatalf("directory was modified: %v, %v", info, err)
	}
}
