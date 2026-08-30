package updater

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
)

var expectedTTSRuntimeAssets = []string{
	"tts-handler.py",
	filepath.Join("tts-pkg", "tts.py"),
	filepath.Join("tts-pkg", "providers", "__init__.py"),
	filepath.Join("tts-pkg", "providers", "openrouter-gemini-tts.py"),
	filepath.Join("tts-pkg", "providers", "sarvam-bulbul-v3.py"),
	filepath.Join("tts-pkg", "providers", "elevenlabs-flash-v25.py"),
}

func TestValidateTTSBundleRequiresEveryRuntimeAsset(t *testing.T) {
	for _, missing := range expectedTTSRuntimeAssets {
		t.Run(filepath.ToSlash(missing), func(t *testing.T) {
			source := writeTTSBundle(t, t.TempDir(), "complete")
			if err := os.Remove(filepath.Join(source, missing)); err != nil {
				t.Fatal(err)
			}
			if err := ValidateTTSBundle(source); err == nil {
				t.Fatalf("bundle without %s passed validation", missing)
			}
		})
	}
}

func TestInstallTTSBundleFSPreservesCustomProviders(t *testing.T) {
	source := fstest.MapFS{}
	for _, name := range expectedTTSRuntimeAssets {
		source[filepath.ToSlash(name)] = &fstest.MapFile{Data: []byte("embedded-" + filepath.Base(name)), Mode: 0o644}
	}
	dest := t.TempDir()
	old := writeTTSBundle(t, dest, "old")
	custom := filepath.Join(old, "tts-pkg", "providers", "custom.py")
	if err := os.WriteFile(custom, []byte("custom"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := InstallTTSBundleFS(dest, source); err != nil {
		t.Fatalf("InstallTTSBundleFS: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(dest, ttsBundleRelativePath, "tts-handler.py")); err != nil || string(got) != "embedded-tts-handler.py" {
		t.Fatalf("installed handler=%q err=%v", got, err)
	}
	if got, err := os.ReadFile(custom); err != nil || string(got) != "custom" {
		t.Fatalf("custom provider=%q err=%v", got, err)
	}
}

func TestInstallTTSBundleRefusesSymlinkDestinationRoot(t *testing.T) {
	source := writeTTSBundle(t, t.TempDir(), "new")
	parent := t.TempDir()
	outside := t.TempDir()
	dest := filepath.Join(parent, "c3")
	if err := os.Symlink(outside, dest); err != nil {
		t.Fatal(err)
	}
	if err := InstallTTSBundle(dest, source); err == nil {
		t.Fatal("symlink destination root must be refused")
	}
}

func writeTTSBundle(t *testing.T, root, label string) string {
	t.Helper()
	dir := filepath.Join(root, ttsBundleRelativePath)
	for _, name := range expectedTTSRuntimeAssets {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(label+"-"+filepath.Base(name)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}
