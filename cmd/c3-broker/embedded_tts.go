package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/Andrometiq/c3/internal/updater"
	"github.com/Andrometiq/c3/internal/version"
	ttsassets "github.com/Andrometiq/c3/plugins/c3/tts"
)

var (
	embeddedTTSExecutablePath = os.Executable
	embeddedTTSUserHomeDir    = os.UserHomeDir
	embeddedTTSInstall        = func(dest string, source fs.FS) error {
		return updater.InstallTTSBundleFS(dest, source)
	}
)

// ensureEmbeddedReleaseTTS materializes the embedded runtime for release
// builds when neither an executable-adjacent nor user-data bundle is complete.
func ensureEmbeddedReleaseTTS() (bool, error) {
	if version.IsDev() {
		return false, nil
	}
	executable, err := embeddedTTSExecutablePath()
	if err != nil {
		return false, fmt.Errorf("resolve executable for embedded TTS repair: %w", err)
	}
	if resolved, resolveErr := filepath.EvalSymlinks(executable); resolveErr == nil {
		executable = resolved
	}
	if updater.ValidateTTSBundle(filepath.Join(filepath.Dir(executable), "plugins", "c3", "tts")) == nil {
		return false, nil
	}

	home, err := embeddedTTSUserHomeDir()
	if err != nil || home == "" {
		if err == nil {
			err = fmt.Errorf("empty home directory")
		}
		return false, fmt.Errorf("resolve home for embedded TTS repair: %w", err)
	}
	dest := filepath.Join(home, ".local", "share", "c3")
	if updater.ValidateTTSBundle(filepath.Join(dest, "plugins", "c3", "tts")) == nil {
		return false, nil
	}
	if err := embeddedTTSInstall(dest, ttsassets.Runtime); err != nil {
		return false, err
	}
	return true, nil
}
