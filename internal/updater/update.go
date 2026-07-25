package updater

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"

	"github.com/karthikeyan5/c3/internal/codexlauncher"
	"github.com/karthikeyan5/c3/internal/version"
)

// Options configures an Update run. The zero value is usable: Client defaults to
// DefaultClient(), CurrentVersion to version.Current(), DestDir to the running
// executable's directory, and WorkDir to a fresh temp dir that is removed on
// return.
type Options struct {
	// CurrentVersion is the version to compare the latest release against. Empty
	// ⇒ version.Current(). A dev ("" / "dev") current makes Update a no-op.
	CurrentVersion string
	// Client is used for the release-API check. Downloads use their own
	// longer-timeout, proxy-aware client. nil ⇒ DefaultClient().
	Client *http.Client
	// DestDir is where the core binaries (plus a verified, already-present C3
	// Codex launcher) are installed. Empty ⇒ ExecutableDir() (the directory the
	// running binary lives in).
	DestDir string
	// WorkDir is a scratch directory for the download + extraction. Empty ⇒ a
	// fresh os.MkdirTemp that Update removes before returning. When set, the
	// caller owns cleanup (used by tests).
	WorkDir string
}

// Result reports the outcome of a check or update.
type Result struct {
	Checked         bool   // the latest-release API check completed
	CurrentVersion  string // the version we compared against
	LatestVersion   string // the latest release tag seen (may equal current)
	UpdateAvailable bool   // a strictly-newer, non-prerelease stable release exists
	Installed       bool   // binaries were downloaded, verified, and swapped
	Tarball         string // the asset that was installed (when Installed)
}

// CheckOnly performs the always-on availability check: query the latest release
// and report whether a newer STABLE version exists — downloading nothing. A dev
// build is reported as never-available (Checked stays false semantics: we skip
// the network entirely). Network/parse errors are returned for the caller to log
// (a failed check is never surfaced to the user).
func CheckOnly(ctx context.Context, current string, client *http.Client) (*Result, error) {
	res := &Result{CurrentVersion: current}
	if version.IsDevString(current) {
		// A dev build has no release identity to compare against — never nag.
		return res, nil
	}
	rel, err := FetchLatest(ctx, client)
	if err != nil {
		return res, err
	}
	res.Checked = true
	res.LatestVersion = rel.TagName
	res.UpdateAvailable = updatable(rel, current)
	return res, nil
}

// Update performs the full verify-then-swap update. It is a safe NO-OP
// (Installed=false, error=nil) when the current build is dev, when the latest
// release is not strictly newer, or when the latest is a prerelease/draft. On a
// real update it downloads the platform tarball + SHA256SUMS, verifies the
// checksum BEFORE touching anything, validates all nine shipped binaries, then
// replaces the eight core binaries in place. The optional cmd/codex launcher is
// replaced only when the existing destination is positively identified as C3's
// launcher; an absent or unrelated `codex` is never created or overwritten.
// Windows is refused before network or file work because
// replacing live .exe images can otherwise leave a mixed-version install.
func Update(ctx context.Context, opts Options) (*Result, error) {
	return updateForOS(ctx, opts, runtime.GOOS)
}

func updateForOS(ctx context.Context, opts Options, goos string) (*Result, error) {
	current := opts.CurrentVersion
	if current == "" {
		current = version.Current()
	}
	res := &Result{CurrentVersion: current}
	if version.IsDevString(current) {
		return res, nil // dev builds never self-update
	}
	if goos == "windows" {
		return res, fmt.Errorf("self-update is not supported on Windows: fully quit C3/your coding CLI, then download and re-extract the newer release tarball over the installed .exe files")
	}

	rel, err := FetchLatest(ctx, opts.Client)
	if err != nil {
		return res, err
	}
	res.Checked = true
	res.LatestVersion = rel.TagName
	if !updatable(rel, current) {
		return res, nil // equal/older, prerelease, or draft → no-op
	}
	res.UpdateAvailable = true

	// Resolve the asset for THIS platform and its checksum source.
	tarball := TarballName(rel.TagName)
	tarURL := rel.AssetURL(tarball)
	sumsURL := rel.AssetURL("SHA256SUMS")
	if tarURL == "" {
		return res, fmt.Errorf("release %s has no asset %q for this platform", rel.TagName, tarball)
	}
	if sumsURL == "" {
		return res, fmt.Errorf("release %s has no SHA256SUMS asset — refusing to install unverified", rel.TagName)
	}

	// Scratch workdir.
	work := opts.WorkDir
	if work == "" {
		work, err = os.MkdirTemp("", "c3-update-*")
		if err != nil {
			return res, err
		}
		defer os.RemoveAll(work)
	}

	// Download tarball + checksums.
	dl := downloadClientFn()
	tarPath := filepath.Join(work, tarball)
	sumsPath := filepath.Join(work, "SHA256SUMS")
	if err := downloadTo(ctx, dl, tarURL, tarPath); err != nil {
		return res, err
	}
	if err := downloadTo(ctx, dl, sumsURL, sumsPath); err != nil {
		return res, err
	}

	// Verify BEFORE anything is replaced.
	sumsData, err := os.ReadFile(sumsPath)
	if err != nil {
		return res, err
	}
	sums := ParseSHA256SUMS(sumsData)
	want, ok := sums[tarball]
	if !ok {
		return res, fmt.Errorf("SHA256SUMS has no entry for %s — refusing to install unverified", tarball)
	}
	if err := VerifyChecksum(tarPath, want); err != nil {
		return res, err // checksum mismatch → the download is corrupt/tampered; nothing swapped
	}

	// Extract and confirm every expected binary is present before swapping.
	extractDir := filepath.Join(work, "extracted")
	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		return res, err
	}
	if err := extractTarGz(tarPath, extractDir); err != nil {
		return res, err
	}
	pkgDir := filepath.Join(extractDir, tarballDir(tarball))
	srcPaths := map[string]string{}
	for _, name := range BinaryNames {
		// Resolve the platform filename (adds .exe on Windows). The map is keyed
		// by the on-disk name so InstallBinaries writes it back with that same
		// name — a Windows tarball ships c3-broker.exe and installs it as such.
		fname := BinaryFileName(name)
		p := filepath.Join(pkgDir, fname)
		if fi, statErr := os.Stat(p); statErr != nil || fi.IsDir() {
			return res, fmt.Errorf("release tarball missing binary %q", fname)
		}
		srcPaths[fname] = p
	}

	// Resolve the install target and swap.
	dest := opts.DestDir
	if dest == "" {
		dest, err = ExecutableDir()
		if err != nil {
			return res, err
		}
	}
	codexName := BinaryFileName("codex")
	if !codexlauncher.IsC3(filepath.Join(dest, codexName)) {
		delete(srcPaths, codexName)
	}
	if err := InstallBinaries(dest, srcPaths); err != nil {
		return res, err
	}
	res.Installed = true
	res.Tarball = tarball
	return res, nil
}

// updatable reports whether rel is a stable release strictly newer than current.
// Prereleases and drafts are never updatable, matching the "never auto-update to
// a prerelease" rule.
func updatable(rel *Release, current string) bool {
	if rel == nil {
		return false
	}
	if rel.Draft || rel.Prerelease || version.IsPrerelease(rel.TagName) {
		return false
	}
	return version.IsNewer(rel.TagName, current)
}
