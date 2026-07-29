package updater

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// maxBinaryBytes caps a single extracted binary so a malicious tarball can't
// exhaust the disk during extraction. C3 binaries are a few MiB; 200 MiB is
// far above any real one.
const maxBinaryBytes = 200 << 20

const (
	sttBundleRelativePath = "plugins/c3/stt"
	maxSTTBundleBytes     = 50 << 20
)

var requiredSTTBundleFiles = []string{
	"stt-handler.py",
	filepath.Join("stt-pkg", "stt.py"),
	filepath.Join("stt-pkg", "vocabulary.txt"),
	filepath.Join("stt-pkg", "providers", "gemini-3-flash-openrouter.py"),
	filepath.Join("stt-pkg", "providers", "soniox-stt-async-v5.py"),
	filepath.Join("stt-pkg", "providers", "elevenlabs-scribe-v2.py"),
	filepath.Join("stt-pkg", "providers", "sarvam-saaras-v3.py"),
}

// ExecutableDir returns the directory the currently-running executable lives in,
// with symlinks resolved (os.Executable then EvalSymlinks). This is the install
// target: swapping binaries here means the swap lands next to the binary that's
// running now.
//
// Self-update safety (Linux): replacing the file backing a RUNNING process is
// safe. os.Rename replaces the DIRECTORY ENTRY; the running process keeps its
// open inode (the old code) until it exits, and the new file gets a fresh inode.
// The next exec of the path — an adapter re-spawning c3-broker after the old one
// drains — picks up the new binary.
func ExecutableDir() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve own executable: %w", err)
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	return filepath.Dir(exe), nil
}

// InstallBinaries stages then installs the binaries in srcPaths (name → source
// path) into destDir. Each individual rename is atomic:
//
//	Phase 1 (stage): copy EVERY source into a private temp file inside destDir
//	  (same filesystem ⇒ the later rename is atomic). If ANY staging step fails
//	  — a missing/short source, a read/write/disk error — every temp file is
//	  removed and the ORIGINAL binaries are left completely untouched. No rename
//	  has happened yet, so a mid-staging failure can never leave a partial
//	  install.
//
//	Phase 2 (swap): rename each staged temp file onto its final name. Renames are
//	  atomic per-file on the same fs and the destDir was already proven writable
//	  by staging. A residual rename failure after earlier siblings were replaced
//	  can leave a partial install; the error is returned honestly for repair.
//
// destDir must exist and be writable. All sources should be pre-verified present
// by the caller (Update does this after checksum + extraction).
//
// Update refuses Windows before calling this function: Phase-2 os.Rename cannot
// replace a currently-running .exe, and swapping the other siblings first would
// leave a mixed-version install.
func InstallBinaries(destDir string, srcPaths map[string]string) error {
	staged, err := stageBinaries(destDir, srcPaths)
	if err != nil {
		return err
	}
	defer cleanupStagedBinaries(staged)
	return installStagedBinaries(staged)
}

// stageBinaries copies every source beside its final destination without
// changing any installed component. Update uses this separately so a bad or
// unreadable binary cannot be discovered only after the STT bundle changed.
func stageBinaries(destDir string, srcPaths map[string]string) (map[string]string, error) {
	if len(srcPaths) == 0 {
		return nil, fmt.Errorf("install: no binaries to install")
	}
	if err := dirWritable(destDir); err != nil {
		return nil, err
	}

	staged := map[string]string{} // finalPath → tmpPath

	// Phase 1 — stage all, or abort with originals untouched.
	for name, src := range srcPaths {
		final := filepath.Join(destDir, name)
		tmp, err := stageBinary(destDir, src)
		if err != nil {
			cleanupStagedBinaries(staged)
			return nil, fmt.Errorf("install: stage %s: %w", name, err)
		}
		staged[final] = tmp
	}
	return staged, nil
}

func cleanupStagedBinaries(staged map[string]string) {
	for _, tmp := range staged {
		_ = os.Remove(tmp)
	}
}

func installStagedBinaries(staged map[string]string) error {
	// Phase 2 — swap. destDir is proven writable and all temps are on its fs.
	var firstErr error
	for final, tmp := range staged {
		if err := os.Rename(tmp, final); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("install: rename into %s: %w", final, err)
			}
			_ = os.Remove(tmp)
		}
	}
	return firstErr
}

// InstallSTTBundle atomically replaces the Python STT bundle next to the C3
// binaries. The release archive is checksum-verified before this is called;
// these checks still refuse a malformed extracted layout or a symlinked install
// target that could redirect the copy outside the binary directory.
func InstallSTTBundle(destDir, srcDir string) error {
	if err := validateSTTBundle(srcDir); err != nil {
		return fmt.Errorf("install STT bundle: %w", err)
	}
	if err := dirWritable(destDir); err != nil {
		return err
	}
	parent := filepath.Join(destDir, "plugins", "c3")
	if err := ensureRealDir(filepath.Join(destDir, "plugins")); err != nil {
		return fmt.Errorf("install STT bundle: %w", err)
	}
	if err := ensureRealDir(parent); err != nil {
		return fmt.Errorf("install STT bundle: %w", err)
	}

	stage, err := os.MkdirTemp(parent, ".c3-stt-stage-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	stagedBundle := filepath.Join(stage, "stt")
	if err := copySTTBundle(srcDir, stagedBundle); err != nil {
		return fmt.Errorf("install STT bundle: stage: %w", err)
	}

	dest := filepath.Join(parent, "stt")
	backup := ""
	if info, err := os.Lstat(dest); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("install STT bundle: destination %s is not a real directory", dest)
		}
		if err := preserveCustomSTTProviders(dest, stagedBundle); err != nil {
			return fmt.Errorf("install STT bundle: preserve custom providers: %w", err)
		}
		backup, err = os.MkdirTemp(parent, ".c3-stt-backup-")
		if err != nil {
			return err
		}
		if err := os.Remove(backup); err != nil {
			return err
		}
		if err := os.Rename(dest, backup); err != nil {
			return err
		}
		defer os.RemoveAll(backup)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(stagedBundle, dest); err != nil {
		if backup != "" {
			_ = os.Rename(backup, dest)
		}
		return err
	}
	return nil
}

// preserveCustomSTTProviders carries the documented drop-in provider seam
// across an update. Shipped provider names in the new bundle win; any other
// regular *.py file is user extension state, not stale release data.
func preserveCustomSTTProviders(oldBundle, stagedBundle string) error {
	oldDir := filepath.Join(oldBundle, "stt-pkg", "providers")
	entries, err := os.ReadDir(oldDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	newDir := filepath.Join(stagedBundle, "stt-pkg", "providers")
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".py") {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing symlink %s", filepath.Join(oldDir, entry.Name()))
		}
		target := filepath.Join(newDir, entry.Name())
		if _, err := os.Lstat(target); err == nil {
			continue // the verified release owns this provider name
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := copyBoundedRegularFile(filepath.Join(oldDir, entry.Name()), target, maxSTTBundleBytes); err != nil {
			return err
		}
	}
	return nil
}

func copyBoundedRegularFile(src, dest string, limit int64) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("refusing non-regular file %s", src)
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, info.Mode().Perm())
	if err != nil {
		return err
	}
	n, copyErr := io.Copy(out, io.LimitReader(in, limit+1))
	closeErr := out.Close()
	if copyErr != nil {
		_ = os.Remove(dest)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(dest)
		return closeErr
	}
	if n > limit {
		_ = os.Remove(dest)
		return fmt.Errorf("%s exceeds %d-byte cap", src, limit)
	}
	return nil
}

func ensureRealDir(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return os.MkdirAll(path, 0o755)
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%s is not a real directory", path)
	}
	return nil
}

func validateSTTBundle(dir string) error {
	for _, name := range requiredSTTBundleFiles {
		path := filepath.Join(dir, name)
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("missing %s: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%s is not a regular file", path)
		}
		if info.Size() == 0 {
			return fmt.Errorf("%s is empty", path)
		}
	}
	return nil
}

func copySTTBundle(src, dest string) error {
	var copied int64
	return copySTTBundleFiles(src, dest, &copied)
}

func copySTTBundleFiles(src, dest string, copied *int64) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing symlink %s", filepath.Join(src, entry.Name()))
		}
		srcPath := filepath.Join(src, entry.Name())
		destPath := filepath.Join(dest, entry.Name())
		if entry.IsDir() {
			if err := copySTTBundleFiles(srcPath, destPath, copied); err != nil {
				return err
			}
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("refusing non-regular file %s", srcPath)
		}
		in, err := os.Open(srcPath)
		if err != nil {
			return err
		}
		out, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode().Perm())
		if err != nil {
			_ = in.Close()
			return err
		}
		n, copyErr := io.Copy(out, io.LimitReader(in, maxSTTBundleBytes-*copied+1))
		closeInErr := in.Close()
		closeOutErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeInErr != nil {
			return closeInErr
		}
		if closeOutErr != nil {
			return closeOutErr
		}
		*copied += n
		if *copied > maxSTTBundleBytes {
			return fmt.Errorf("STT bundle exceeds %d-byte cap", int64(maxSTTBundleBytes))
		}
	}
	return nil
}

// stageBinary copies src into a fresh temp file in destDir with mode 0755,
// returning the temp file's path. Same-fs placement means InstallBinaries can
// atomically rename it into place.
func stageBinary(destDir, src string) (string, error) {
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer in.Close()
	// Reject an empty source outright — an empty binary means a broken extraction
	// and must never be swapped in.
	if fi, err := in.Stat(); err != nil {
		return "", err
	} else if fi.Size() == 0 {
		return "", fmt.Errorf("source %s is empty", src)
	}

	tmp, err := os.CreateTemp(destDir, ".c3-update-*.tmp")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	if _, err := io.Copy(tmp, io.LimitReader(in, maxBinaryBytes+1)); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return "", err
	}
	if err := tmp.Chmod(0o755); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return "", err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	return tmpPath, nil
}

// dirWritable verifies destDir exists, is a directory, and accepts a temp file.
// Doing this up front turns a permission problem into a clean pre-swap error
// rather than a mid-install failure.
func dirWritable(destDir string) error {
	fi, err := os.Stat(destDir)
	if err != nil {
		return fmt.Errorf("install: dest dir %s: %w", destDir, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("install: dest %s is not a directory", destDir)
	}
	probe, err := os.CreateTemp(destDir, ".c3-write-probe-*")
	if err != nil {
		return fmt.Errorf("install: dest dir %s not writable: %w", destDir, err)
	}
	name := probe.Name()
	_ = probe.Close()
	_ = os.Remove(name)
	return nil
}

// extractTarGz extracts a .tar.gz into destDir. It defends against path-traversal
// (an entry whose cleaned path escapes destDir is rejected) and only writes
// regular files and directories — symlinks/devices/etc. in the archive are
// skipped. Extracted files get mode 0755 (they are executables). Caps each file
// at maxBinaryBytes.
func extractTarGz(tarballPath, destDir string) error {
	f, err := os.Open(tarballPath)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("extract: gzip: %w", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	cleanDest := filepath.Clean(destDir)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("extract: tar: %w", err)
		}
		// Reject path traversal: the joined+cleaned target must stay under destDir.
		target := filepath.Join(cleanDest, hdr.Name)
		if target != cleanDest && !strings.HasPrefix(target, cleanDest+string(os.PathSeparator)) {
			return fmt.Errorf("extract: entry %q escapes destination", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o755)
			if err != nil {
				return err
			}
			n, err := io.Copy(out, io.LimitReader(tr, maxBinaryBytes+1))
			cerr := out.Close()
			if err != nil {
				return err
			}
			if cerr != nil {
				return cerr
			}
			if n > maxBinaryBytes {
				return fmt.Errorf("extract: %q exceeds %d-byte cap", hdr.Name, int64(maxBinaryBytes))
			}
		default:
			// Skip symlinks, hardlinks, devices, fifos — a binary release tarball
			// contains only dirs + regular files; anything else is unexpected.
			continue
		}
	}
}
