//go:build !windows

package main

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

// findRealCodex must:
//  1. Honor an explicit C3_CODEX_REAL env override.
//  2. Skip the wrapper path on PATH (don't recurse into ourselves).
//  3. Fall back to NVM's @openai/codex package script when nothing on PATH
//     is the real codex.
//
// Regression coverage for the resolution logic; ported from the deleted
// mvp/tests/test_codex_launcher.py.
func TestFindRealCodex_HonorsExplicitEnv(t *testing.T) {
	real := executableScript(t, t.TempDir(), "codex-real")
	t.Setenv("C3_CODEX_REAL", real)
	got, err := findRealCodex("/anything/codex")
	if err != nil {
		t.Fatalf("findRealCodex: %v", err)
	}
	if got != real {
		t.Errorf("got %q, want %q", got, real)
	}
}

func TestFindRealCodex_SkipsWrapperPath(t *testing.T) {
	root := t.TempDir()
	wrapperDir := filepath.Join(root, "wrapper")
	realDir := filepath.Join(root, "real")
	if err := os.MkdirAll(wrapperDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(realDir, 0o755); err != nil {
		t.Fatal(err)
	}
	wrapper := executableScript(t, wrapperDir, "codex")
	real := executableScript(t, realDir, "codex")

	t.Setenv("C3_CODEX_REAL", "")
	t.Setenv("PATH", wrapperDir+string(os.PathListSeparator)+realDir)

	got, err := findRealCodex(wrapper)
	if err != nil {
		t.Fatalf("findRealCodex: %v", err)
	}
	if got == wrapper {
		t.Error("findRealCodex returned the wrapper itself — recursion guard failed")
	}
	if got != real {
		t.Errorf("got %q, want real %q", got, real)
	}
}

func TestFindRealCodex_FallsBackToNVMPackageScript(t *testing.T) {
	root := t.TempDir()
	wrapperDir := filepath.Join(root, "wrapper")
	if err := os.MkdirAll(wrapperDir, 0o755); err != nil {
		t.Fatal(err)
	}
	wrapper := executableScript(t, wrapperDir, "codex")

	nvmBin := filepath.Join(root, ".nvm", "versions", "node", "v20.0.0", "lib", "node_modules", "@openai", "codex", "bin")
	if err := os.MkdirAll(nvmBin, 0o755); err != nil {
		t.Fatal(err)
	}
	nvmReal := executableScript(t, nvmBin, "codex.js")

	t.Setenv("C3_CODEX_REAL", "")
	t.Setenv("HOME", root)
	t.Setenv("PATH", wrapperDir) // wrapper-only PATH → forces NVM fallback

	got, err := findRealCodex(wrapper)
	if err != nil {
		t.Fatalf("findRealCodex: %v", err)
	}
	if got != nvmReal {
		t.Errorf("got %q, want NVM path %q", got, nvmReal)
	}
}

func TestFindRealCodex_NotFoundErrors(t *testing.T) {
	root := t.TempDir()
	t.Setenv("C3_CODEX_REAL", "")
	t.Setenv("HOME", root)
	t.Setenv("PATH", root) // empty PATH dir, no NVM
	if _, err := findRealCodex(filepath.Join(root, "codex")); err == nil {
		t.Error("expected error when codex is nowhere, got nil")
	}
}

func executableScript(t *testing.T, dir, name string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestShouldBypassNonInteractiveCommands(t *testing.T) {
	if !shouldBypass([]string{"exec", "echo", "hi"}) {
		t.Fatal("codex exec should bypass C3 launcher")
	}
	if !shouldBypass([]string{"--remote", "ws://127.0.0.1:8766"}) {
		t.Fatal("--remote should bypass C3 launcher")
	}
	if shouldBypass([]string{"resume"}) {
		t.Fatal("codex resume should stay bridged")
	}
}

func TestInferTopicNameUsesNearestClaudeMDWithSharedRootGuard(t *testing.T) {
	root := t.TempDir()
	shared := filepath.Join(root, "projects")
	project := filepath.Join(shared, "widget")
	if err := os.MkdirAll(project, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(shared, "CLAUDE.md"), []byte("# root\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := inferTopicName(shared, shared); got != "" {
		t.Fatalf("shared root topic = %q, want empty", got)
	}
	if got := inferTopicName(project, shared); got != "" {
		t.Fatalf("project without its own CLAUDE.md under shared root = %q, want empty", got)
	}
	if err := os.WriteFile(filepath.Join(project, "CLAUDE.md"), []byte("# project\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := inferTopicName(project, shared); got != "widget" {
		t.Fatalf("project topic = %q, want widget", got)
	}
}

func TestMCPConfigArgsPointAtGoCodexAdapter(t *testing.T) {
	args := mcpConfigArgs("/usr/local/bin/c3-codex-adapter", "ws://127.0.0.1:8766", "/tmp/work", "dm")
	joined := "\n" + joinArgs(args) + "\n"
	for _, want := range []string{
		`mcp_servers.c3_codex.command="/usr/local/bin/c3-codex-adapter"`,
		`mcp_servers.c3_codex.env.C3_CODEX_APP_SERVER_WS="ws://127.0.0.1:8766"`,
		`mcp_servers.c3_codex.env.C3_CODEX_CWD="/tmp/work"`,
		`mcp_servers.c3_codex.env.C3_CODEX_REMOTE_BRIDGE="1"`,
		`mcp_servers.c3_codex.env.C3_ATTACH_NAME="dm"`,
	} {
		if !contains(joined, want) {
			t.Fatalf("mcp args missing %s in:\n%s", want, joined)
		}
	}
	if contains(joined, "codex_stub.py") || contains(joined, "python3") {
		t.Fatalf("mcp args should not reference Python MVP stub:\n%s", joined)
	}
}

func TestChooseAppServerURLFallsForwardWhenDefaultOccupied(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	defaultURL := "ws://" + ln.Addr().String()
	got := chooseAppServerURL(defaultURL)
	if got == defaultURL {
		t.Fatalf("chooseAppServerURL reused occupied default %s", defaultURL)
	}
}

func joinArgs(args []string) string {
	out := ""
	for _, arg := range args {
		out += arg + "\n"
	}
	return out
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && index(s, sub) >= 0)
}

func index(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
