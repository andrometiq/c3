package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Andrometiq/c3/internal/version"
)

var embeddedSTTRuntimeFiles = []string{
	"stt-handler.py",
	filepath.Join("stt-pkg", "stt.py"),
	filepath.Join("stt-pkg", "vocabulary.txt"),
	filepath.Join("stt-pkg", "providers", "gemini-3-flash-openrouter.py"),
	filepath.Join("stt-pkg", "providers", "soniox-stt-async-v5.py"),
	filepath.Join("stt-pkg", "providers", "elevenlabs-scribe-v2.py"),
	filepath.Join("stt-pkg", "providers", "sarvam-saaras-v3.py"),
}

func writeCompleteSTTBundleAt(t *testing.T, bundle string) {
	t.Helper()
	for _, relative := range embeddedSTTRuntimeFiles {
		path := filepath.Join(bundle, relative)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("# runtime "+relative+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func withEmbeddedSTTTestPaths(t *testing.T, release string, exe, home string) {
	t.Helper()
	oldVersion := version.Version
	oldExe := embeddedSTTExecutablePath
	oldHome := embeddedSTTUserHomeDir
	version.Version = release
	embeddedSTTExecutablePath = func() (string, error) { return exe, nil }
	embeddedSTTUserHomeDir = func() (string, error) { return home, nil }
	t.Cleanup(func() {
		version.Version = oldVersion
		embeddedSTTExecutablePath = oldExe
		embeddedSTTUserHomeDir = oldHome
	})
}

func TestEmbeddedReleaseSTT_RepairsRC1BinaryOnlyLayout(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "home")
	exe := filepath.Join(root, "bin", "c3-broker")
	withEmbeddedSTTTestPaths(t, "v0.1.0", exe, home)

	repaired, err := ensureEmbeddedReleaseSTT()
	if err != nil {
		t.Fatalf("repair binary-only release: %v", err)
	}
	if !repaired {
		t.Fatal("binary-only release layout was not repaired")
	}
	for _, relative := range embeddedSTTRuntimeFiles {
		path := filepath.Join(home, ".local", "share", "c3", "plugins", "c3", "stt", relative)
		if info, err := os.Stat(path); err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
			t.Fatalf("repaired runtime file %s: info=%v err=%v", relative, info, err)
		}
	}
}

func TestEmbeddedReleaseSTT_DevBuildNeverMutatesHome(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "home")
	withEmbeddedSTTTestPaths(t, "", filepath.Join(root, "bin", "c3-broker"), home)

	repaired, err := ensureEmbeddedReleaseSTT()
	if err != nil {
		t.Fatalf("dev build: %v", err)
	}
	if repaired {
		t.Fatal("dev build reported a release migration")
	}
	if _, err := os.Stat(home); !os.IsNotExist(err) {
		t.Fatalf("dev build mutated home: err=%v", err)
	}
}

func TestEmbeddedReleaseSTT_PresentAdjacentBundleIsAuthoritative(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "home")
	exe := filepath.Join(root, "bin", "c3-broker")
	writeCompleteSTTBundleAt(t, filepath.Join(filepath.Dir(exe), "plugins", "c3", "stt"))
	withEmbeddedSTTTestPaths(t, "v0.1.0", exe, home)

	repaired, err := ensureEmbeddedReleaseSTT()
	if err != nil {
		t.Fatal(err)
	}
	if repaired {
		t.Fatal("present executable-adjacent bundle must not be replaced")
	}
	if _, err := os.Stat(home); !os.IsNotExist(err) {
		t.Fatalf("authoritative adjacent bundle still caused a home write: err=%v", err)
	}
}

func TestEmbeddedReleaseSTT_PartialBundleDoesNotSuppressRepair(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "home")
	exe := filepath.Join(root, "bin", "c3-broker")
	bundle := filepath.Join(home, ".local", "share", "c3", "plugins", "c3", "stt")
	custom := filepath.Join(bundle, "stt-pkg", "providers", "custom.py")
	if err := os.MkdirAll(filepath.Dir(custom), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "stt-handler.py"), []byte("# incomplete\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(custom, []byte("# user provider\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	withEmbeddedSTTTestPaths(t, "v0.1.0", exe, home)

	repaired, err := ensureEmbeddedReleaseSTT()
	if err != nil {
		t.Fatalf("repair partial bundle: %v", err)
	}
	if !repaired {
		t.Fatal("partial bundle incorrectly suppressed repair")
	}
	for _, relative := range embeddedSTTRuntimeFiles {
		if _, err := os.Stat(filepath.Join(bundle, relative)); err != nil {
			t.Fatalf("repaired file %s: %v", relative, err)
		}
	}
	if body, err := os.ReadFile(custom); err != nil || string(body) != "# user provider\n" {
		t.Fatalf("repair lost custom provider: body=%q err=%v", body, err)
	}
}
