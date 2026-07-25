// Package updater implements C3's self-update: query the latest GitHub release,
// download the platform tarball, verify it against the release's SHA256SUMS, and
// stage then replace the installed binaries in place.
//
// Design invariants (see docs/USAGE.md "Updating C3"):
//   - HTTPS only; checksum verification is mandatory before anything is replaced.
//   - Verify-then-swap: the running binaries are NEVER touched until the new
//     tarball has been fully downloaded, checksum-verified, and staged.
//   - Never downgrade (semver compare) and never install a prerelease.
//   - No signature infrastructure in v1 (checksum-only) — a signed-release path
//     is future work.
//
// The package is deliberately network-free at its seams so it is unit-testable:
// ParseRelease / ParseSHA256SUMS / VerifyChecksum / InstallBinaries / extractTarGz
// all operate on bytes or the local filesystem, and only FetchLatest / the
// download helpers touch the network.
package updater

// Release is the subset of the GitHub "latest release" API response C3 consumes.
// See https://docs.github.com/en/rest/releases/releases#get-the-latest-release.
type Release struct {
	TagName    string  `json:"tag_name"`
	Prerelease bool    `json:"prerelease"`
	Draft      bool    `json:"draft"`
	Assets     []Asset `json:"assets"`
}

// Asset is one uploaded release artifact (a platform tarball, or SHA256SUMS).
type Asset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	Size               int64  `json:"size"`
}

// AssetURL returns the download URL of the asset named exactly name, or "" when
// the release has no such asset.
func (r *Release) AssetURL(name string) string {
	if r == nil {
		return ""
	}
	for _, a := range r.Assets {
		if a.Name == name {
			return a.BrowserDownloadURL
		}
	}
	return ""
}
