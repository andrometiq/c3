package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Andrometiq/c3/internal/version"
)

var embeddedTTSRuntimeFiles = []string{
	"tts-handler.py",
	filepath.Join("tts-pkg", "tts.py"),
	filepath.Join("tts-pkg", "providers", "__init__.py"),
	filepath.Join("tts-pkg", "providers", "openrouter-gemini-tts.py"),
	filepath.Join("tts-pkg", "providers", "sarvam-bulbul-v3.py"),
	filepath.Join("tts-pkg", "providers", "elevenlabs-flash-v25.py"),
}

func withEmbeddedTTSTestPaths(t *testing.T, release, executable, home string) {
	t.Helper()
	oldVersion := version.Version
	oldExecutable := embeddedTTSExecutablePath
	oldHome := embeddedTTSUserHomeDir
	version.Version = release
	embeddedTTSExecutablePath = func() (string, error) { return executable, nil }
	embeddedTTSUserHomeDir = func() (string, error) { return home, nil }
	t.Cleanup(func() {
		version.Version = oldVersion
		embeddedTTSExecutablePath = oldExecutable
		embeddedTTSUserHomeDir = oldHome
	})
}

func writeCompleteTTSBundleAt(t *testing.T, bundle string) {
	t.Helper()
	for _, relative := range embeddedTTSRuntimeFiles {
		path := filepath.Join(bundle, relative)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("# runtime "+relative+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEmbeddedReleaseTTSRepairsMissingBundle(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "home")
	executable := filepath.Join(root, "bin", "c3-broker")
	withEmbeddedTTSTestPaths(t, "v0.1.0", executable, home)
	repaired, err := ensureEmbeddedReleaseTTS()
	if err != nil {
		t.Fatal(err)
	}
	if !repaired {
		t.Fatal("missing release TTS bundle was not repaired")
	}
	for _, relative := range embeddedTTSRuntimeFiles {
		if info, err := os.Stat(filepath.Join(home, ".local", "share", "c3", "plugins", "c3", "tts", relative)); err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
			t.Fatalf("runtime %s: info=%v err=%v", relative, info, err)
		}
	}
}

func TestEmbeddedReleaseTTSDevBuildDoesNotWriteHome(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "home")
	withEmbeddedTTSTestPaths(t, "", filepath.Join(root, "bin", "c3-broker"), home)
	repaired, err := ensureEmbeddedReleaseTTS()
	if err != nil || repaired {
		t.Fatalf("repaired=%v err=%v", repaired, err)
	}
	if _, err := os.Stat(home); !os.IsNotExist(err) {
		t.Fatalf("dev build mutated home: %v", err)
	}
}

func TestEmbeddedReleaseTTSPresentAdjacentBundleWins(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "home")
	executable := filepath.Join(root, "bin", "c3-broker")
	writeCompleteTTSBundleAt(t, filepath.Join(filepath.Dir(executable), "plugins", "c3", "tts"))
	withEmbeddedTTSTestPaths(t, "v0.1.0", executable, home)
	repaired, err := ensureEmbeddedReleaseTTS()
	if err != nil || repaired {
		t.Fatalf("repaired=%v err=%v", repaired, err)
	}
	if _, err := os.Stat(home); !os.IsNotExist(err) {
		t.Fatalf("authoritative adjacent bundle still caused a home write: %v", err)
	}
}
