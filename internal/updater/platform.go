package updater

import (
	"fmt"
	"runtime"
	"strings"
)

// BinaryNames are the nine runnable binaries a release tarball carries,
// WITHOUT a platform extension. Update validates all nine, but only installs
// the opt-in `codex` launcher when a verified C3 launcher is already present at
// the destination. MUST stay in sync with
// scripts/package.sh's BINS list — if package.sh adds/removes a binary, update
// this too or an install will fail looking for a binary the tarball doesn't
// contain (or silently skip a new one). Resolve the on-disk filename via
// BinaryFileName so Windows binaries get their .exe suffix.
var BinaryNames = []string{
	"c3-broker",
	"c3-claude-adapter",
	"c3-codex-adapter",
	"c3-grok-adapter",
	"c3-agy-adapter",
	"c3-desktop-adapter",
	"codex",
	"claude-shim",
	"migrate-legacy",
}

// BinaryFileName returns the on-disk filename for a BinaryNames entry on the
// CURRENT platform. Windows binaries are built and shipped with a .exe suffix
// (scripts/package.sh appends it for GOOS=windows), so release lookup and
// install must resolve name+".exe" there; every other platform uses the bare
// name. Linux/macOS behaviour is unchanged (identity).
func BinaryFileName(name string) string {
	if runtime.GOOS == "windows" {
		return name + ".exe"
	}
	return name
}

// TarballName returns the release-asset filename for version on the CURRENT
// runtime platform, e.g. "c3_v1.0.0_linux_amd64.tar.gz". Mirrors package.sh's
// PKG naming: c3_<version>_<goos>_<goarch>.tar.gz.
func TarballName(version string) string {
	return TarballNameFor(version, runtime.GOOS, runtime.GOARCH)
}

// TarballNameFor is TarballName with an explicit platform (for tests).
func TarballNameFor(version, goos, goarch string) string {
	return fmt.Sprintf("c3_%s_%s_%s.tar.gz", version, goos, goarch)
}

// tarballDir returns the top-level directory name inside a release tarball,
// which package.sh sets to the tarball's basename without ".tar.gz" (PKG). The
// binaries live directly under this directory.
func tarballDir(tarballName string) string {
	return strings.TrimSuffix(tarballName, ".tar.gz")
}
