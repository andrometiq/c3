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
	updateTestServerFixture(t, tag, prerelease, corruptChecksum, "", "")
}

func updateTestServerMissingSTT(t *testing.T, tag string, prerelease bool, corruptChecksum bool, missingSTT string) {
	updateTestServerFixture(t, tag, prerelease, corruptChecksum, missingSTT, "")
}

func updateTestServerEmptyBinary(t *testing.T, tag, emptyBinary string) {
	updateTestServerFixture(t, tag, false, false, "", emptyBinary)
}

func updateTestServerFixture(t *testing.T, tag string, prerelease bool, corruptChecksum bool, missingSTT, emptyBinary string) {
	t.Helper()

	tarball := TarballName(tag)
	entries := map[string][]byte{}
	for _, name := range BinaryNames {
		entries[name] = []byte("NEW-" + name + "-" + tag)
	}
	if emptyBinary != "" {
		entries[emptyBinary] = nil
	}
	for _, name := range expectedSTTRuntimeAssets {
		entries[filepath.Join(sttBundleRelativePath, name)] = []byte("# new " + filepath.Base(name) + "\n")
	}
	entries[filepath.Join(sttBundleRelativePath, "stt-handler.py")] = []byte("# new handler\n")
	entries[filepath.Join(sttBundleRelativePath, "stt-pkg", "stt.py")] = []byte("# new runner\n")
	if missingSTT != "" {
		delete(entries, filepath.Join(sttBundleRelativePath, missingSTT))
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
		want := "NEW-" + name + "-v9.9.9"
		if name == "codex" {
			// The stand-in is not a verified C3 launcher. Updating C3 must not
			// overwrite an unrelated regular file merely because it is named
			// codex and happens to sit next to c3-broker.
			want = "OLD-codex"
		}
		if string(got) != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	if got, err := os.ReadFile(filepath.Join(dest, sttBundleRelativePath, "stt-handler.py")); err != nil || string(got) != "# new handler\n" {
		t.Fatalf("updated package STT handler = %q, err=%v — self-update installed binaries but omitted the Desktop/systemd STT bundle", got, err)
	}
}

func TestUpdate_DoesNotInstallAbsentCodexLauncher(t *testing.T) {
	updateTestServer(t, "v9.9.9", false, false)
	dest := t.TempDir()
	seedOldBinaries(t, dest)
	if err := os.Remove(filepath.Join(dest, "codex")); err != nil {
		t.Fatal(err)
	}

	res, err := Update(context.Background(), Options{
		CurrentVersion: "v1.0.0",
		Client:         trustingClient(t),
		DestDir:        dest,
		WorkDir:        t.TempDir(),
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !res.Installed {
		t.Fatalf("expected core binaries to install, got %+v", res)
	}
	if _, err := os.Lstat(filepath.Join(dest, "codex")); !os.IsNotExist(err) {
		t.Fatalf("update created the opt-in codex launcher: %v", err)
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

func TestUpdate_MissingSTTRuntimeAssetLeavesOriginals(t *testing.T) {
	missing := filepath.Join("stt-pkg", "providers", "soniox-stt-async-v5.py")
	updateTestServerMissingSTT(t, "v9.9.9", false, false, missing)
	dest := t.TempDir()
	seedOldBinaries(t, dest)
	oldBundle := writeSTTBundle(t, dest, "old")

	res, err := Update(context.Background(), Options{
		CurrentVersion: "v1.0.0",
		Client:         trustingClient(t),
		DestDir:        dest,
		WorkDir:        t.TempDir(),
	})
	if err == nil {
		t.Fatalf("release missing %s must be refused before installation", missing)
	}
	if res.Installed {
		t.Fatal("malformed STT release reported Installed=true")
	}
	for _, name := range BinaryNames {
		got, readErr := os.ReadFile(filepath.Join(dest, name))
		if readErr != nil || string(got) != "OLD-"+name {
			t.Fatalf("%s changed before STT bundle validation: body=%q err=%v", name, got, readErr)
		}
	}
	if got, readErr := os.ReadFile(filepath.Join(oldBundle, "stt-handler.py")); readErr != nil || string(got) != "old-handler" {
		t.Fatalf("old STT bundle changed before complete runtime validation: body=%q err=%v", got, readErr)
	}
}

func TestUpdate_UnstageableBinaryLeavesOldBundleAndBinariesUntouched(t *testing.T) {
	updateTestServerEmptyBinary(t, "v9.9.9", "c3-broker")
	dest := t.TempDir()
	seedOldBinaries(t, dest)
	oldBundle := writeSTTBundle(t, dest, "old")

	res, err := Update(context.Background(), Options{
		CurrentVersion: "v1.0.0",
		Client:         trustingClient(t),
		DestDir:        dest,
		WorkDir:        t.TempDir(),
	})
	if err == nil {
		t.Fatal("release with an empty binary must fail before any installed component changes")
	}
	if res.Installed {
		t.Fatal("release with an unstageable binary reported Installed=true")
	}
	for _, name := range BinaryNames {
		got, readErr := os.ReadFile(filepath.Join(dest, name))
		if readErr != nil || string(got) != "OLD-"+name {
			t.Fatalf("%s changed before every binary was staged: body=%q err=%v", name, got, readErr)
		}
	}
	if got, readErr := os.ReadFile(filepath.Join(oldBundle, "stt-handler.py")); readErr != nil || string(got) != "old-handler" {
		t.Fatalf("old STT bundle changed before every binary was staged: body=%q err=%v", got, readErr)
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
