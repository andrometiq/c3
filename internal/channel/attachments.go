package channel

import (
	"fmt"
	"os"
	"path/filepath"
)

// AttachmentCacheDir is where the named channel keeps downloaded attachments:
// $XDG_CACHE_HOME/c3/<name>/attachments, with XDG_CACHE_HOME defaulting to
// $HOME/.cache. It only resolves the path; it creates and sweeps nothing.
func AttachmentCacheDir(name string) (string, error) {
	cache := os.Getenv("XDG_CACHE_HOME")
	if cache == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home: %w", err)
		}
		cache = filepath.Join(home, ".cache")
	}
	return filepath.Join(cache, "c3", name, "attachments"), nil
}

// ResolveAttachment returns path with symlinks resolved when it names a regular
// file directly inside dir (resolved too), so a link in dir that points
// elsewhere does not count. Both must be absolute; any doubt is false.
func ResolveAttachment(dir, path string) (string, bool) {
	if !filepath.IsAbs(dir) || !filepath.IsAbs(path) {
		return "", false
	}
	resolvedDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", false
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || filepath.Dir(resolved) != resolvedDir {
		return "", false
	}
	if info, err := os.Stat(resolved); err != nil || !info.Mode().IsRegular() {
		return "", false
	}
	return resolved, true
}
