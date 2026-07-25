package codexlauncher

import (
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"
)

func TestIsC3BuildInfo(t *testing.T) {
	if !isC3BuildInfo(&debug.BuildInfo{Path: packagePath}) {
		t.Fatal("cmd/codex build info must identify the C3 launcher")
	}
	if isC3BuildInfo(&debug.BuildInfo{Path: "github.com/openai/codex"}) {
		t.Fatal("another codex executable must not identify as C3's launcher")
	}
	if isC3BuildInfo(nil) {
		t.Fatal("nil build info must not identify as C3's launcher")
	}
}

func TestIsC3RejectsArbitraryExecutable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if IsC3(path) {
		t.Fatal("an arbitrary executable named codex must not identify as C3's launcher")
	}
}
