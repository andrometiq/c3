package mappings

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

// Read parses the mappings.json file at path. Returns os.IsNotExist-friendly
// error if the file is missing.
func Read(path string) (*MappingsFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var mf MappingsFile
	if err := json.Unmarshal(data, &mf); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &mf, nil
}

// ReadWithBak reads AND validates the mappings file at path, with a one-
// generation backup fallback. It exists so a hand-edit that corrupts
// mappings.json no longer dead-ends the broker at startup the way it did before
// (every successful Write leaves a last-good path+".bak").
//
//   - Missing primary: returns the os.IsNotExist error UNCHANGED so the caller
//     can seed a skeleton (do not mask "no config" as "bad config").
//   - Primary parses AND validates: returns it, usedBak=false.
//   - Primary missing-fields / unparseable: if path+".bak" parses AND validates,
//     returns THAT with usedBak=true; otherwise returns the primary's original
//     error (usedBak=false) so the caller can surface it.
func ReadWithBak(path string) (mf *MappingsFile, usedBak bool, err error) {
	mf, err = Read(path)
	switch {
	case err != nil:
		if os.IsNotExist(err) {
			return nil, false, err // caller seeds a skeleton
		}
		// primary unparseable — fall through to the .bak attempt
	default:
		if verr := mf.Validate(); verr != nil {
			err = fmt.Errorf("validate %s: %w", path, verr)
			mf = nil
		} else {
			return mf, false, nil // primary good
		}
	}

	// Primary is present but bad (parse or validate). Try the last-good backup.
	primaryErr := err
	bmf, berr := Read(path + ".bak")
	if berr != nil {
		return nil, false, primaryErr // no usable backup — surface the primary error
	}
	if verr := bmf.Validate(); verr != nil {
		return nil, false, primaryErr // backup also bad — surface the primary error
	}
	return bmf, true, nil
}

// Write atomically rewrites the mappings file at path. The file is created
// (or replaced) at mode 0600 because it contains the bot token. Atomicity is
// achieved by writing to a sibling tempfile and then renaming.
//
// Backup-on-rewrite: if path already exists, its current contents are copied
// to path+".bak" (mode 0600) before the rename. Spec §4.3 retention rule:
// one generation only — successive rewrites overwrite the same .bak.
func Write(path string, mf *MappingsFile) error {
	data, err := json.MarshalIndent(mf, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal mappings: %w", err)
	}

	// Backup current file if present, BEFORE we touch the destination.
	if _, err := os.Stat(path); err == nil {
		if err := backupFile(path, path+".bak"); err != nil {
			return fmt.Errorf("backup %s -> %s.bak: %w", path, path, err)
		}
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".mappings.*.tmp")
	if err != nil {
		return fmt.Errorf("create tempfile: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write tempfile: %w", err)
	}
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("chmod tempfile: %w", err)
	}
	// fsync data to disk BEFORE the rename so a power loss between
	// rename and journal flush can't leave a zero-byte mappings.json.
	// Without this, ext4/xfs are allowed to commit the rename before
	// the inode's data blocks — recovery would lose the bot token.
	// (daemon.md §5.1)
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("fsync tempfile: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close tempfile: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		cleanup()
		return fmt.Errorf("rename %s -> %s: %w", tmpPath, path, err)
	}
	// fsync the parent directory so the rename itself is durable.
	// On a crash between rename and the dir's metadata flush, the
	// rename may be lost even though the new file's data persisted.
	if err := syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("fsync parent dir of %s: %w", path, err)
	}
	return nil
}

// syncDir fsyncs a directory file descriptor. Required after `rename(2)` so
// the kernel commits the directory entry, not just the inode data.
// Per POSIX/Linux this is a no-op for some filesystems but mandatory for
// ext4/xfs/btrfs guarantees.
func syncDir(dir string) error {
	// Windows disallows fsync on a directory handle (FlushFileBuffers on a
	// directory returns ERROR_ACCESS_DENIED). Directory-entry durability there
	// is the filesystem's (NTFS journal) responsibility, so skip it rather than
	// abort an otherwise-successful atomic write.
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// backupFile copies src to dst, mode 0600, overwriting dst if it exists. Uses
// io.Copy to avoid loading the whole file into memory unnecessarily, though
// mappings.json is tiny in practice.
func backupFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return nil
}
