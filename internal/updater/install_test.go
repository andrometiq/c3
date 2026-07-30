package updater

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
)

var expectedSTTRuntimeAssets = []string{
	"stt-handler.py",
	filepath.Join("stt-pkg", "stt.py"),
	filepath.Join("stt-pkg", "vocabulary.txt"),
	filepath.Join("stt-pkg", "providers", "gemini-3-flash-openrouter.py"),
	filepath.Join("stt-pkg", "providers", "soniox-stt-async-v5.py"),
	filepath.Join("stt-pkg", "providers", "elevenlabs-scribe-v2.py"),
	filepath.Join("stt-pkg", "providers", "sarvam-saaras-v3.py"),
}

// makeTarGz builds a gzip'd tarball at destPath containing entries (name→bytes)
// laid out under a single top-level dir topDir/ — mirroring the release tarball
// layout scripts/package.sh produces. Returns the raw tarball bytes too.
func makeTarGz(t *testing.T, destPath, topDir string, entries map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	// Top-level dir entry.
	if err := tw.WriteHeader(&tar.Header{Name: topDir + "/", Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	for name, data := range entries {
		if err := tw.WriteHeader(&tar.Header{
			Name:     topDir + "/" + name,
			Typeflag: tar.TypeReg,
			Mode:     0o755,
			Size:     int64(len(data)),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if destPath != "" {
		if err := os.WriteFile(destPath, buf.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return buf.Bytes()
}

func TestInstallBinaries_Success(t *testing.T) {
	src := t.TempDir()
	dest := t.TempDir()

	// Pre-seed dest with OLD binaries.
	for _, name := range BinaryNames {
		writeFile(t, dest, name, []byte("OLD "+name))
	}
	// Stage NEW sources.
	srcPaths := map[string]string{}
	for _, name := range BinaryNames {
		srcPaths[name] = writeFile(t, src, name, []byte("NEW "+name))
	}

	if err := InstallBinaries(dest, srcPaths); err != nil {
		t.Fatalf("install: %v", err)
	}
	for _, name := range BinaryNames {
		got, err := os.ReadFile(filepath.Join(dest, name))
		if err != nil {
			t.Fatalf("read installed %s: %v", name, err)
		}
		if string(got) != "NEW "+name {
			t.Errorf("%s = %q, want NEW", name, got)
		}
		fi, _ := os.Stat(filepath.Join(dest, name))
		if fi.Mode().Perm()&0o100 == 0 {
			t.Errorf("%s is not executable (mode %v)", name, fi.Mode())
		}
	}
	// No leftover temp files in dest.
	ents, _ := os.ReadDir(dest)
	for _, e := range ents {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Errorf("leftover temp file %s", e.Name())
		}
	}
}

func TestInstallBinaries_FailureMidwayLeavesOriginals(t *testing.T) {
	src := t.TempDir()
	dest := t.TempDir()

	// Pre-seed dest with OLD binaries.
	for _, name := range BinaryNames {
		writeFile(t, dest, name, []byte("OLD "+name))
	}
	// Stage NEW sources — but make ONE of them fail to stage (empty file, which
	// stageBinary rejects). This must abort in Phase 1, before any rename.
	srcPaths := map[string]string{}
	for i, name := range BinaryNames {
		if i == len(BinaryNames)-1 {
			srcPaths[name] = writeFile(t, src, name, []byte{}) // empty → staging error
			continue
		}
		srcPaths[name] = writeFile(t, src, name, []byte("NEW "+name))
	}

	if err := InstallBinaries(dest, srcPaths); err == nil {
		t.Fatal("install with an empty source must fail")
	}
	// Every original must be intact — no partial swap.
	for _, name := range BinaryNames {
		got, err := os.ReadFile(filepath.Join(dest, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if string(got) != "OLD "+name {
			t.Errorf("%s was modified to %q — originals must be untouched on failure", name, got)
		}
	}
	// And no temp files left behind.
	ents, _ := os.ReadDir(dest)
	for _, e := range ents {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Errorf("leftover temp file %s after failed install", e.Name())
		}
	}
}

func TestInstallBinaries_MissingSource(t *testing.T) {
	dest := t.TempDir()
	writeFile(t, dest, "c3-broker", []byte("OLD"))
	err := InstallBinaries(dest, map[string]string{"c3-broker": filepath.Join(t.TempDir(), "does-not-exist")})
	if err == nil {
		t.Fatal("missing source must error")
	}
	if got, _ := os.ReadFile(filepath.Join(dest, "c3-broker")); string(got) != "OLD" {
		t.Errorf("original clobbered to %q on missing-source failure", got)
	}
}

func TestInstallSTTBundle_InstallsVerifiedBundleWithoutFollowingDestinationSymlinks(t *testing.T) {
	src := writeSTTBundle(t, t.TempDir(), "new")
	dest := t.TempDir()
	old := writeSTTBundle(t, dest, "old")
	custom := filepath.Join(old, "stt-pkg", "providers", "my-custom-provider.py")
	if err := os.WriteFile(custom, []byte("custom"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := InstallSTTBundle(dest, src); err != nil {
		t.Fatalf("InstallSTTBundle: %v", err)
	}
	installed := filepath.Join(dest, sttBundleRelativePath, "stt-handler.py")
	if got, err := os.ReadFile(installed); err != nil || string(got) != "new-handler" {
		t.Fatalf("installed STT handler = %q, err=%v — updater copied binaries but omitted the release STT bundle", got, err)
	}
	if got, err := os.ReadFile(custom); err != nil || string(got) != "custom" {
		t.Fatalf("atomic bundle replacement lost the documented custom-provider extension: body=%q err=%v", got, err)
	}
	provider := filepath.Join(dest, sttBundleRelativePath, "stt-pkg", "providers", "soniox-stt-async-v5.py")
	if got, err := os.ReadFile(provider); err != nil || string(got) != "new-soniox-stt-async-v5.py" {
		t.Fatalf("installed STT provider = %q, err=%v — bundle replacement did not install the runnable provider set", got, err)
	}

	outside := t.TempDir()
	plugins := filepath.Join(dest, "plugins")
	if err := os.RemoveAll(plugins); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, plugins); err != nil {
		t.Fatal(err)
	}
	if err := InstallSTTBundle(dest, src); err == nil {
		t.Fatal("symlinked bundle parent must be refused — updater must not write release files outside the binary directory")
	}
}

func TestInstallSTTBundleFS_UsesVerifiedAtomicInstaller(t *testing.T) {
	source := fstest.MapFS{}
	for _, name := range expectedSTTRuntimeAssets {
		source[filepath.ToSlash(name)] = &fstest.MapFile{
			Data: []byte("embedded-" + filepath.Base(name)),
			Mode: 0o644,
		}
	}
	dest := t.TempDir()
	old := writeSTTBundle(t, dest, "old")
	custom := filepath.Join(old, "stt-pkg", "providers", "custom.py")
	if err := os.WriteFile(custom, []byte("custom"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := InstallSTTBundleFS(dest, source); err != nil {
		t.Fatalf("InstallSTTBundleFS: %v", err)
	}
	handler := filepath.Join(dest, sttBundleRelativePath, "stt-handler.py")
	if got, err := os.ReadFile(handler); err != nil || string(got) != "embedded-stt-handler.py" {
		t.Fatalf("installed embedded handler=%q err=%v", got, err)
	}
	if got, err := os.ReadFile(custom); err != nil || string(got) != "custom" {
		t.Fatalf("embedded repair lost custom provider: body=%q err=%v", got, err)
	}
}

func TestInstallSTTBundle_RefusesSymlinkDestinationRoot(t *testing.T) {
	src := writeSTTBundle(t, t.TempDir(), "new")
	parent := t.TempDir()
	outside := t.TempDir()
	dest := filepath.Join(parent, "c3")
	if err := os.Symlink(outside, dest); err != nil {
		t.Fatal(err)
	}
	if err := InstallSTTBundle(dest, src); err == nil {
		t.Fatal("symlink destination root must be refused")
	}
	if _, err := os.Stat(filepath.Join(outside, sttBundleRelativePath)); !os.IsNotExist(err) {
		t.Fatalf("installer followed destination-root symlink: err=%v", err)
	}
}

func TestValidateSTTBundle_RequiresEveryRuntimeAsset(t *testing.T) {
	for _, missing := range expectedSTTRuntimeAssets {
		t.Run(filepath.ToSlash(missing), func(t *testing.T) {
			src := writeSTTBundle(t, t.TempDir(), "complete")
			if err := os.Remove(filepath.Join(src, missing)); err != nil {
				t.Fatal(err)
			}
			if err := ValidateSTTBundle(src); err == nil {
				t.Fatalf("bundle without %s passed validation — updater could install an STT chain that cannot run", missing)
			}
		})
	}
}

func TestValidateSTTBundle_RejectsEmptyRuntimeAssets(t *testing.T) {
	for _, empty := range expectedSTTRuntimeAssets {
		t.Run(filepath.ToSlash(empty), func(t *testing.T) {
			src := writeSTTBundle(t, t.TempDir(), "complete")
			if err := os.WriteFile(filepath.Join(src, empty), nil, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := ValidateSTTBundle(src); err == nil {
				t.Fatalf("bundle with empty %s passed validation — updater advertised a runnable STT runtime that cannot run", empty)
			}
		})
	}
}

func writeSTTBundle(t *testing.T, root, label string) string {
	t.Helper()
	dir := filepath.Join(root, sttBundleRelativePath)
	for _, name := range expectedSTTRuntimeAssets {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		body := label + "-" + filepath.Base(name)
		switch name {
		case "stt-handler.py":
			body = label + "-handler"
		case filepath.Join("stt-pkg", "stt.py"):
			body = label + "-runner"
		}
		if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestExtractTarGz(t *testing.T) {
	work := t.TempDir()
	tarPath := filepath.Join(work, "rel.tar.gz")
	entries := map[string][]byte{}
	for _, name := range BinaryNames {
		entries[name] = []byte("bin-" + name)
	}
	makeTarGz(t, tarPath, "c3_v1.0.0_linux_amd64", entries)

	out := filepath.Join(work, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := extractTarGz(tarPath, out); err != nil {
		t.Fatalf("extract: %v", err)
	}
	pkg := filepath.Join(out, "c3_v1.0.0_linux_amd64")
	for _, name := range BinaryNames {
		got, err := os.ReadFile(filepath.Join(pkg, name))
		if err != nil {
			t.Fatalf("extracted %s: %v", name, err)
		}
		if string(got) != "bin-"+name {
			t.Errorf("%s = %q", name, got)
		}
	}
}

func TestExtractTarGz_RejectsTraversal(t *testing.T) {
	work := t.TempDir()
	tarPath := filepath.Join(work, "evil.tar.gz")
	// Hand-build a tarball with a traversal entry.
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	payload := []byte("pwned")
	_ = tw.WriteHeader(&tar.Header{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0o755, Size: int64(len(payload))})
	_, _ = tw.Write(payload)
	_ = tw.Close()
	_ = gz.Close()
	if err := os.WriteFile(tarPath, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(work, "out")
	_ = os.MkdirAll(out, 0o755)
	if err := extractTarGz(tarPath, out); err == nil {
		t.Fatal("path-traversal entry must be rejected")
	}
	if _, err := os.Stat(filepath.Join(work, "escape")); err == nil {
		t.Fatal("traversal wrote a file outside the destination")
	}
}
