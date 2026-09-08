//go:build !windows

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
	testAdapterLookupProcess(t, true)
}

func TestFindAdapterBarePATHInvocationIgnoresHostileCWD(t *testing.T) {
	testAdapterLookupProcess(t, false)
}

func TestAdapterLookupHelper(t *testing.T) {
	if os.Getenv("C3_TEST_ADAPTER_LOOKUP") != "1" {
		return
	}
	got, err := findAdapter(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	if got != os.Getenv("C3_TEST_EXPECT_ADAPTER") {
		t.Fatalf("selected %q", got)
	}
}

func testAdapterLookupProcess(t *testing.T, sibling bool) {
	t.Helper()
	root := t.TempDir()
	release, pathdir, project := filepath.Join(root, "release"), filepath.Join(root, "bin"), filepath.Join(root, "project")
	for _, dir := range []string{release, pathdir, project} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Join(release, "codex")
	if err := os.WriteFile(launcher, data, 0755); err != nil {
		t.Fatal(err)
	}
	shim := filepath.Join(pathdir, "codex")
	if err := os.Symlink(launcher, shim); err != nil {
		t.Fatal(err)
	}
	want := executableScript(t, pathdir, "c3-codex-adapter")
	if sibling {
		want = executableScript(t, release, "c3-codex-adapter")
	}
	executableScript(t, project, "c3-codex-adapter")
	t.Setenv("PATH", pathdir)
	t.Setenv("C3_CODEX_ADAPTER", "")
	t.Setenv("C3_TEST_ADAPTER_LOOKUP", "1")
	t.Setenv("C3_TEST_EXPECT_ADAPTER", want)
	cmd := exec.Command("codex", "-test.run=^TestAdapterLookupHelper$")
	cmd.Args[0] = "codex"
	cmd.Dir = project
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("bare PATH invocation: %v: %s", err, strings.TrimSpace(string(output)))
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
