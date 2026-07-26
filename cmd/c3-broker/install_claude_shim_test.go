//go:build !windows

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/Andrometiq/c3/internal/shimconfig"
)

func TestInstallClaudeShim_FreshInstall_CreatesSymlink(t *testing.T) {
	dir := t.TempDir()
	launcher := filepath.Join(dir, "claude-shim")
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\n"), 0o755); err != nil {
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
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\n"), 0o755); err != nil {
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
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\n"), 0o755); err != nil {
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
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\n"), 0o755); err != nil {
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
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\n"), 0o755); err != nil {
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
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\n"), 0o755); err != nil {
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
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\n"), 0o755); err != nil {
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
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\n"), 0o755); err != nil {
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
