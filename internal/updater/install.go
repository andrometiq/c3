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
	if len(srcPaths) == 0 {
		return fmt.Errorf("install: no binaries to install")
	}
	if err := dirWritable(destDir); err != nil {
		return err
	}

	staged := map[string]string{} // finalPath → tmpPath
	cleanup := func() {
		for _, tmp := range staged {
			_ = os.Remove(tmp)
		}
	}

	// Phase 1 — stage all, or abort with originals untouched.
	for name, src := range srcPaths {
		final := filepath.Join(destDir, name)
		tmp, err := stageBinary(destDir, src)
		if err != nil {
			cleanup()
			return fmt.Errorf("install: stage %s: %w", name, err)
		}
		staged[final] = tmp
	}

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
