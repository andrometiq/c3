//go:build !windows

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestModernManagementCommandsBypassLauncher(t *testing.T) {
	for _, command := range []string{"queue", "agents", "archive", "unarchive", "delete", "doctor", "remote-control", "migrate-rollouts"} {
		if !shouldBypass([]string{command}) {
			t.Errorf("%s would incorrectly start an interactive bridge", command)
		}
	}
}

func TestFindAdapterPrefersReleaseBesideResolvedLauncher(t *testing.T) {
	root := t.TempDir()
	release := filepath.Join(root, "release")
	shimdir := filepath.Join(root, "bin")
	if err := os.MkdirAll(release, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(shimdir, 0755); err != nil {
		t.Fatal(err)
	}
	launcher := executableScript(t, release, "codex")
	want := executableScript(t, release, "c3-codex-adapter")
	executableScript(t, shimdir, "c3-codex-adapter")
	shim := filepath.Join(shimdir, "codex")
	if err := os.Symlink(launcher, shim); err != nil {
		t.Fatal(err)
	}
	t.Setenv("C3_CODEX_ADAPTER", "")
	t.Setenv("PATH", shimdir)
	got, err := findAdapter(shim)
	if err != nil || got != want {
		t.Fatalf("got %q, %v; want matching release %q", got, err, want)
	}
}

func TestStandaloneInstallDiscovery(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "packages", "standalone", "current", "bin")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	want := executableScript(t, dir, "codex")
	if got := standaloneCodexPath(root); got != want {
		t.Fatalf("standalone binary missed: %q", got)
	}
}

func TestExplicitRealCodexCannotPointBackToLauncher(t *testing.T) {
	path := executableScript(t, t.TempDir(), "codex")
	t.Setenv("C3_CODEX_REAL", path)
	if _, err := findRealCodex(path); err == nil {
		t.Fatal("recursive launcher accepted")
	}
}

func TestLauncherUsesRequestedWorkingDirectory(t *testing.T) {
	for _, args := range [][]string{{"-C", "project"}, {"--cd", "project"}, {"--cd=project"}, {"-Cproject"}} {
		if got := effectiveLauncherCWD(args, "/workspace"); got != "/workspace/project" || !hasCWDArg(args) {
			t.Fatalf("launcher/adapter directory disagrees with TUI for %v: %s", args, got)
		}
	}
}
