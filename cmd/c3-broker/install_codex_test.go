package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeLauncher creates a stand-in for C3's shipped `codex` launcher binary.
func writeLauncher(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
}

func launcherBodyIsC3(path string) bool {
	body, err := os.ReadFile(path)
	return err == nil && strings.Contains(string(body), "C3 LAUNCHER")
}

func TestEnsureCodexLauncher_InstallsStagedCopyWithoutClobberingRealCodex(t *testing.T) {
	dir := t.TempDir()
	launcher := filepath.Join(dir, "bin", "codex")
	staged := filepath.Join(dir, "libexec", "codex")
	if err := os.MkdirAll(filepath.Dir(staged), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(staged, []byte("C3 LAUNCHER v2"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := ensureCodexLauncher(launcher, staged, false, launcherBodyIsC3); err != nil {
		t.Fatalf("install staged launcher: %v", err)
	}
	if body, err := os.ReadFile(launcher); err != nil || string(body) != "C3 LAUNCHER v2" {
		t.Fatalf("launcher body=%q err=%v", body, err)
	}

	if err := os.WriteFile(launcher, []byte("REAL CODEX"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ensureCodexLauncher(launcher, staged, false, launcherBodyIsC3); err == nil {
		t.Fatal("non-C3 launcher must be refused without --force")
	}
	if body, err := os.ReadFile(launcher); err != nil || string(body) != "REAL CODEX" {
		t.Fatalf("real Codex changed on refusal: body=%q err=%v", body, err)
	}
	if err := ensureCodexLauncher(launcher, staged, true, launcherBodyIsC3); err != nil {
		t.Fatalf("--force refresh: %v", err)
	}
	if body, err := os.ReadFile(launcher); err != nil || string(body) != "C3 LAUNCHER v2" {
		t.Fatalf("forced launcher body=%q err=%v", body, err)
	}
}

func TestEnsureCodexLauncher_LiveC3LauncherIsAuthoritative(t *testing.T) {
	dir := t.TempDir()
	launcher := filepath.Join(dir, "bin", "codex")
	staged := filepath.Join(dir, "libexec", "codex")
	for path, body := range map[string]string{
		launcher: "C3 LAUNCHER v1",
		staged:   "C3 LAUNCHER v2",
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := ensureCodexLauncher(launcher, staged, false, launcherBodyIsC3); err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(launcher); err != nil || string(body) != "C3 LAUNCHER v1" {
		t.Fatalf("valid live launcher was overwritten by staged copy: body=%q err=%v", body, err)
	}
}

// REGRESSION: rc1's updater refreshed the live launcher binary but could not
// refresh the off-PATH staged copy. Re-running install-codex-shim must not copy
// that stale rc1 file back over the stable launcher.
func TestEnsureCodexLauncher_StaleStagedCopyCannotDowngradeLive(t *testing.T) {
	dir := t.TempDir()
	launcher := filepath.Join(dir, "bin", "codex")
	staged := filepath.Join(dir, "libexec", "codex")
	for path, body := range map[string]string{
		launcher: "C3 LAUNCHER stable",
		staged:   "C3 LAUNCHER rc1",
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := ensureCodexLauncher(launcher, staged, true, launcherBodyIsC3); err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(launcher); err != nil || string(body) != "C3 LAUNCHER stable" {
		t.Fatalf("stale staged launcher downgraded live stable launcher: body=%q err=%v", body, err)
	}
}

// Source install (GOBIN=~/go/bin): the launcher lives outside ~/.local/bin, so
// both targets get a symlink.
func TestInstallCodexShimsCreatesLocalAndNVMSymlinks(t *testing.T) {
	home := t.TempDir()
	launcher := filepath.Join(home, "go", "bin", "codex")
	writeLauncher(t, launcher)
	nvmBin := filepath.Join(home, ".nvm", "versions", "node", "v20.19.0", "bin")
	if err := os.MkdirAll(nvmBin, 0o755); err != nil {
		t.Fatal(err)
	}

	installed, err := installCodexShims(home, launcher, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(installed) != 2 {
		t.Fatalf("installed %d shims, want 2: %#v", len(installed), installed)
	}
	for _, path := range []string{
		filepath.Join(home, ".local", "bin", "codex"),
		filepath.Join(nvmBin, "codex"),
	} {
		target, err := os.Readlink(path)
		if err != nil {
			t.Fatalf("readlink %s: %v", path, err)
		}
		if target != launcher {
			t.Fatalf("%s -> %s, want %s", path, target, launcher)
		}
	}
}

// REGRESSION (v0.1.0 release audit): on the DEFAULT prebuilt install the nine
// binaries land in ~/.local/bin, so the launcher IS the first shim target. The
// installer used to os.Remove it and symlink it to itself — deleting C3's own
// codex binary and leaving the user's `codex` an ELOOP-broken self-link, with
// every nvm entry re-pointed at that broken link. It must skip the launcher.
func TestInstallCodexShims_PrebuiltLayout_NeverSelfLinks(t *testing.T) {
	home := t.TempDir()
	launcher := filepath.Join(home, ".local", "bin", "codex") // prebuilt layout
	writeLauncher(t, launcher)
	nvmBin := filepath.Join(home, ".nvm", "versions", "node", "v20.19.0", "bin")
	if err := os.MkdirAll(nvmBin, 0o755); err != nil {
		t.Fatal(err)
	}

	installed, err := installCodexShims(home, launcher, false)
	if err != nil {
		t.Fatalf("install must succeed on the prebuilt layout: %v", err)
	}

	// The launcher must survive, as a regular file — not deleted, not a symlink.
	fi, err := os.Lstat(launcher)
	if err != nil {
		t.Fatalf("C3's own codex launcher was DELETED: %v", err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		link, _ := os.Readlink(launcher)
		t.Fatalf("C3's codex launcher was replaced by a symlink -> %s (self-referential = ELOOP)", link)
	}
	if !fi.Mode().IsRegular() {
		t.Fatalf("launcher is no longer a regular file: mode=%v", fi.Mode())
	}

	for _, p := range installed {
		if absClean(p) == absClean(launcher) {
			t.Errorf("installer reported shimming the launcher itself: %s", p)
		}
	}

	// The nvm entry still gets its shim.
	nvmLink := filepath.Join(nvmBin, "codex")
	got, err := os.Readlink(nvmLink)
	if err != nil {
		t.Fatalf("nvm shim should still be installed: %v", err)
	}
	if got != launcher {
		t.Errorf("%s -> %s, want %s", nvmLink, got, launcher)
	}
}

// A regular file at a target is somebody's real codex binary; removing it
// destroys it. Refuse without --force, and leave it untouched.
func TestInstallCodexShims_RefusesToClobberRegularFile(t *testing.T) {
	home := t.TempDir()
	launcher := filepath.Join(home, "go", "bin", "codex")
	writeLauncher(t, launcher)

	realCodex := filepath.Join(home, ".local", "bin", "codex")
	if err := os.MkdirAll(filepath.Dir(realCodex), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(realCodex, []byte("REAL CODEX BINARY"), 0o755); err != nil {
		t.Fatal(err)
	}

	if _, err := installCodexShims(home, launcher, false); err == nil {
		t.Fatal("expected a refusal to overwrite a regular file")
	} else if !strings.Contains(err.Error(), "--force") {
		t.Errorf("refusal should tell the user about --force; got %v", err)
	}

	body, err := os.ReadFile(realCodex)
	if err != nil || string(body) != "REAL CODEX BINARY" {
		t.Errorf("the user's real codex binary must be left untouched; err=%v body=%q", err, body)
	}

	// --force replaces it.
	if _, err := installCodexShims(home, launcher, true); err != nil {
		t.Fatalf("--force should proceed: %v", err)
	}
	if got, err := os.Readlink(realCodex); err != nil || got != launcher {
		t.Errorf("--force should have installed the shim; got %q err=%v", got, err)
	}
}

// Re-running on an already-shimmed source install is a no-op, not a breakage.
func TestInstallCodexShims_Idempotent(t *testing.T) {
	home := t.TempDir()
	launcher := filepath.Join(home, "go", "bin", "codex")
	writeLauncher(t, launcher)

	if _, err := installCodexShims(home, launcher, false); err != nil {
		t.Fatal(err)
	}
	if _, err := installCodexShims(home, launcher, false); err != nil {
		t.Fatalf("second run must be safe: %v", err)
	}
	link := filepath.Join(home, ".local", "bin", "codex")
	if got, err := os.Readlink(link); err != nil || got != launcher {
		t.Errorf("shim should still point at the launcher; got %q err=%v", got, err)
	}
	if _, err := os.Stat(launcher); err != nil {
		t.Errorf("launcher must survive repeated installs: %v", err)
	}
}
