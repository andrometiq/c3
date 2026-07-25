package updater

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// updateTestServer stands up a loopback TLS server that serves a GitHub-shaped
// latest-release payload plus the tarball + SHA256SUMS assets, and rewires the
// package seams (LatestReleaseURL, downloadClientFn) to point at it. It returns
// the tarball bytes for the current platform. Everything is torn down via
// t.Cleanup — no real network is touched.
func updateTestServer(t *testing.T, tag string, prerelease bool, corruptChecksum bool) {
	t.Helper()

	tarball := TarballName(tag)
	entries := map[string][]byte{}
	for _, name := range BinaryNames {
		entries[name] = []byte("NEW-" + name + "-" + tag)
	}
	tarBytes := makeTarGz(t, "", tarballDir(tarball), entries)

	sum := sha256Hex(tarBytes)
	if corruptChecksum {
		// Flip the digest so verification must fail.
		sum = sha256Hex([]byte("something else entirely"))
	}
	sumsBody := sum + "  " + tarball + "\n"

	mux := http.NewServeMux()
	var baseURL string
	mux.HandleFunc("/releases/latest", func(w http.ResponseWriter, r *http.Request) {
		rel := Release{
			TagName:    tag,
			Prerelease: prerelease,
			Assets: []Asset{
				{Name: tarball, BrowserDownloadURL: baseURL + "/dl/" + tarball},
				{Name: "SHA256SUMS", BrowserDownloadURL: baseURL + "/dl/SHA256SUMS"},
			},
		}
		_ = json.NewEncoder(w).Encode(rel)
	})
	mux.HandleFunc("/dl/"+tarball, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(tarBytes)
	})
	mux.HandleFunc("/dl/SHA256SUMS", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, sumsBody)
	})

	ts := httptest.NewTLSServer(mux)
	baseURL = ts.URL
	t.Cleanup(ts.Close)

	origURL := LatestReleaseURL
	origDL := downloadClientFn
	LatestReleaseURL = ts.URL + "/releases/latest"
	downloadClientFn = func() *http.Client { return ts.Client() }
	t.Cleanup(func() {
		LatestReleaseURL = origURL
		downloadClientFn = origDL
	})
	_ = runtime.GOOS
}

func seedOldBinaries(t *testing.T, dest string) {
	t.Helper()
	for _, name := range BinaryNames {
		writeFile(t, dest, name, []byte("OLD-"+name))
	}
}

func TestUpdate_InstallsNewerRelease(t *testing.T) {
	updateTestServer(t, "v9.9.9", false, false)
	dest := t.TempDir()
	seedOldBinaries(t, dest)

	res, err := Update(context.Background(), Options{
		CurrentVersion: "v1.0.0",
		Client:         trustingClient(t), // the loopback TLS server needs a trusting client
		DestDir:        dest,
		WorkDir:        t.TempDir(),
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !res.Installed {
		t.Fatalf("expected Installed=true, got %+v", res)
	}
	if res.LatestVersion != "v9.9.9" {
		t.Errorf("latest = %q", res.LatestVersion)
	}
	for _, name := range BinaryNames {
		got, _ := os.ReadFile(filepath.Join(dest, name))
		if string(got) != "NEW-"+name+"-v9.9.9" {
			t.Errorf("%s = %q, want NEW", name, got)
		}
	}
}

func TestUpdate_NoOpWhenNotNewer(t *testing.T) {
	updateTestServer(t, "v1.0.0", false, false)
	dest := t.TempDir()
	seedOldBinaries(t, dest)

	res, err := Update(context.Background(), Options{
		CurrentVersion: "v1.0.0", // equal → no-op
		Client:         trustingClient(t),
		DestDir:        dest,
		WorkDir:        t.TempDir(),
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if res.Installed {
		t.Error("equal version must not install")
	}
	for _, name := range BinaryNames {
		got, _ := os.ReadFile(filepath.Join(dest, name))
		if string(got) != "OLD-"+name {
			t.Errorf("%s clobbered on no-op: %q", name, got)
		}
	}
}

func TestUpdate_NoOpForPrerelease(t *testing.T) {
	updateTestServer(t, "v9.9.9-rc1", true, false)
	dest := t.TempDir()
	seedOldBinaries(t, dest)

	res, err := Update(context.Background(), Options{
		CurrentVersion: "v1.0.0",
		Client:         trustingClient(t),
		DestDir:        dest,
		WorkDir:        t.TempDir(),
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if res.Installed {
		t.Error("prerelease must never install")
	}
}

func TestUpdate_NoOpForDevBuild(t *testing.T) {
	updateTestServer(t, "v9.9.9", false, false)
	dest := t.TempDir()
	seedOldBinaries(t, dest)

	res, err := Update(context.Background(), Options{
		CurrentVersion: "dev",
		Client:         trustingClient(t),
		DestDir:        dest,
		WorkDir:        t.TempDir(),
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if res.Installed {
		t.Error("dev build must never self-update")
	}
}

func TestUpdateForOS_WindowsRefusesBeforeNetworkOrSwap(t *testing.T) {
	dest := t.TempDir()
	seedOldBinaries(t, dest)

	res, err := updateForOS(context.Background(), Options{
		CurrentVersion: "v1.0.0",
		DestDir:        dest,
	}, "windows")
	if err == nil {
		t.Fatal("Windows self-update must be refused before its per-file renames can leave a mixed-version install")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "windows") ||
		!strings.Contains(strings.ToLower(err.Error()), "re-extract") {
		t.Fatalf("refusal must give the Windows user an actionable manual path; got %v", err)
	}
	if res.Checked || res.Installed {
		t.Fatalf("Windows refusal must happen before network/download/install work: %+v", res)
	}
	for _, name := range BinaryNames {
		got, readErr := os.ReadFile(filepath.Join(dest, name))
		if readErr != nil || string(got) != "OLD-"+name {
			t.Fatalf("%s changed during Windows refusal: body=%q err=%v", name, got, readErr)
		}
	}
}

func TestUpdate_ChecksumMismatchLeavesOriginals(t *testing.T) {
	updateTestServer(t, "v9.9.9", false, true) // corrupt checksum
	dest := t.TempDir()
	seedOldBinaries(t, dest)

	res, err := Update(context.Background(), Options{
		CurrentVersion: "v1.0.0",
		Client:         trustingClient(t),
		DestDir:        dest,
		WorkDir:        t.TempDir(),
	})
	if err == nil {
		t.Fatal("checksum mismatch must return an error")
	}
	if res.Installed {
		t.Error("nothing must be installed on checksum mismatch")
	}
	for _, name := range BinaryNames {
		got, _ := os.ReadFile(filepath.Join(dest, name))
		if string(got) != "OLD-"+name {
			t.Errorf("%s clobbered despite checksum mismatch: %q", name, got)
		}
	}
}

func TestCheckOnly(t *testing.T) {
	updateTestServer(t, "v9.9.9", false, false)
	res, err := CheckOnly(context.Background(), "v1.0.0", trustingClient(t))
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !res.UpdateAvailable || res.LatestVersion != "v9.9.9" {
		t.Errorf("expected update available, got %+v", res)
	}

	// Dev build: never available, no network needed.
	devRes, err := CheckOnly(context.Background(), "dev", trustingClient(t))
	if err != nil {
		t.Fatalf("check dev: %v", err)
	}
	if devRes.UpdateAvailable {
		t.Error("dev build must report no update available")
	}
}

// trustingClient returns an http.Client that trusts the most-recently-created
// httptest TLS server. httptest servers each mint a cert; ts.Client() trusts it.
// We stash it on the package seam so this helper can retrieve it.
func trustingClient(t *testing.T) *http.Client {
	t.Helper()
	// downloadClientFn was rewired by updateTestServer to return the TLS server's
	// trusting client; reuse it for the API check too.
	return downloadClientFn()
}
