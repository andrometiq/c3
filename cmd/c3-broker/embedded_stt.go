package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/Andrometiq/c3/internal/updater"
	"github.com/Andrometiq/c3/internal/version"
	sttassets "github.com/Andrometiq/c3/plugins/c3/stt"
)

var (
	embeddedSTTExecutablePath = os.Executable
	embeddedSTTUserHomeDir    = os.UserHomeDir
	embeddedSTTInstall        = func(dest string, source fs.FS) error {
		return updater.InstallSTTBundleFS(dest, source)
	}
)

// ensureEmbeddedReleaseSTT repairs the binary-only layout installed by the
// v0.1.0-rc1 updater. Dev/source builds never write into the user's home: the
// migration is armed only by a release version injected at link time.
//
// A handler beside c3-broker is authoritative. Otherwise the embedded,
// checksum-covered release runtime is installed into the documented user-data
// fallback. InstallSTTBundleFS supplies the same validation, no-symlink and
// atomic replacement semantics as a normal self-update and preserves custom
// providers already present in that fallback.
func ensureEmbeddedReleaseSTT() (bool, error) {
	if version.IsDev() {
		return false, nil
	}
	exe, err := embeddedSTTExecutablePath()
	if err != nil {
		return false, fmt.Errorf("resolve executable for embedded STT repair: %w", err)
	}
	if resolved, resolveErr := filepath.EvalSymlinks(exe); resolveErr == nil {
		exe = resolved
	}
	if updater.ValidateSTTBundle(filepath.Join(filepath.Dir(exe), "plugins", "c3", "stt")) == nil {
		return false, nil
	}

	home, err := embeddedSTTUserHomeDir()
	if err != nil || home == "" {
		if err == nil {
			err = fmt.Errorf("empty home directory")
		}
		return false, fmt.Errorf("resolve home for embedded STT repair: %w", err)
	}
	dest := filepath.Join(home, ".local", "share", "c3")
	if updater.ValidateSTTBundle(filepath.Join(dest, "plugins", "c3", "stt")) == nil {
		return false, nil
	}
	if err := embeddedSTTInstall(dest, sttassets.Runtime); err != nil {
		return false, err
	}
	return true, nil
}
