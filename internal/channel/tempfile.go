package channel

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// TempPrefix starts the name of every attempt-owned temporary file C3 writes
// beside retained files. SweepStaleTemps removes only names with this prefix,
// so a completed file can never match.
const TempPrefix = ".c3tmp-"

// staleTempAge is how old a temporary must be before a sweep removes it. A live
// voice attempt holds its file for at most the fetch budget plus the STT
// deadline (well under an hour), so only files orphaned by a dead process go.
const staleTempAge = time.Hour

// SweepStaleTemps removes TempPrefix files in dir older than staleTempAge.
// Best-effort: a missing dir or a file that vanishes is not an error.
func SweepStaleTemps(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-staleTempAge)
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), TempPrefix) || entry.IsDir() {
			continue
		}
		if info, err := entry.Info(); err == nil && info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(dir, entry.Name()))
		}
	}
}

// CreateTemp creates dir (0700) if needed, sweeps its stale temporaries, and
// opens a fresh TempPrefix+pattern file the caller owns and must remove.
func CreateTemp(dir, pattern string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	SweepStaleTemps(dir)
	return os.CreateTemp(dir, TempPrefix+pattern)
}

// CopyToTemp copies the regular file src into a fresh CreateTemp file and
// returns its path and size. The copy has its own new mtime, and src is never
// modified. On error nothing is left behind.
func CopyToTemp(src, dir, pattern string) (path string, size int64, err error) {
	if info, err := os.Lstat(src); err != nil {
		return "", 0, err
	} else if !info.Mode().IsRegular() {
		return "", 0, fmt.Errorf("copy %s: not a regular file", src)
	}
	in, err := os.Open(src)
	if err != nil {
		return "", 0, err
	}
	defer in.Close()
	out, err := CreateTemp(dir, pattern)
	if err != nil {
		return "", 0, err
	}
	size, err = io.Copy(out, in)
	if closeErr := out.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(out.Name())
		return "", 0, fmt.Errorf("copy %s: %w", src, err)
	}
	return out.Name(), size, nil
}
