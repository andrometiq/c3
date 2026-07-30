package updater

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The release tarball is produced by scripts/package.sh; the updater installs
// exactly the binaries it expects to find inside that tarball. Those two lists
// live in different files and different languages, and platform.go's comment on
// BinaryNames says they "MUST stay in sync" — but until this test, nothing
// enforced it.
//
// Why it is worth a test: both drift directions break users who have ALREADY
// installed C3, and the mechanism they would use to receive the fix is the very
// thing that broke.
//
//   - package.sh gains a binary, BinaryNames does not → `c3-broker update`
//     reports success while silently installing an incomplete release.
//   - BinaryNames gains one package.sh does not ship → the install fails
//     looking for a file the tarball never contained.
//
// These tests read the real packaging script rather than a copy, so they cannot
// pass against a stale duplicate of the list.

var (
	binsRe = regexp.MustCompile(`(?m)^BINS="([^"]*)"`)
	pkgRe  = regexp.MustCompile(`(?m)^PKG="([^"]*)"`)
)

// packageScript returns the contents of scripts/package.sh. `go test` runs with
// the package directory as its working directory, so the script sits two levels
// up.
func packageScript(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "scripts", "package.sh")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func TestBinaryNamesMatchPackageScript(t *testing.T) {
	src := packageScript(t)

	m := binsRe.FindStringSubmatch(src)
	if m == nil {
		t.Fatal(`scripts/package.sh: no BINS="..." assignment found. ` +
			`If the packaging script changed shape, update this test — do not delete it: ` +
			`it is the only thing keeping updater.BinaryNames honest.`)
	}
	shipped := strings.Fields(m[1])

	got := append([]string(nil), BinaryNames...)
	want := append([]string(nil), shipped...)
	sort.Strings(got)
	sort.Strings(want)

	if len(got) != len(want) {
		t.Errorf("binary-list length mismatch: updater.BinaryNames has %d, scripts/package.sh ships %d",
			len(BinaryNames), len(shipped))
	}

	inScript := map[string]bool{}
	for _, b := range shipped {
		inScript[b] = true
	}
	inUpdater := map[string]bool{}
	for _, b := range BinaryNames {
		inUpdater[b] = true
	}

	for _, b := range BinaryNames {
		if !inScript[b] {
			t.Errorf("updater.BinaryNames lists %q but scripts/package.sh does NOT ship it — "+
				"an update would fail looking for a binary the tarball does not contain", b)
		}
	}
	for _, b := range shipped {
		if !inUpdater[b] {
			t.Errorf("scripts/package.sh ships %q but updater.BinaryNames does NOT list it — "+
				"an update would silently install an incomplete release", b)
		}
	}
}

func TestPackageScriptShipsSTTBundle(t *testing.T) {
	src := packageScript(t)
	for _, command := range []string{
		`cp "$ROOT/plugins/c3/stt/stt-handler.py" "$DEST/plugins/c3/stt/"`,
		`cp "$ROOT/plugins/c3/stt/stt-pkg/stt.py" "$ROOT/plugins/c3/stt/stt-pkg/vocabulary.txt" "$DEST/plugins/c3/stt/stt-pkg/"`,
		`cp "$ROOT/plugins/c3/stt/stt-pkg/providers/"*.py "$DEST/plugins/c3/stt/stt-pkg/providers/"`,
	} {
		if !strings.Contains(src, command) {
			t.Fatalf("scripts/package.sh does not ship %q — binary-only Desktop/Antigravity/Grok/systemd releases will resolve no handler", command)
		}
	}
	for _, instruction := range []string{
		"keep plugins/c3/stt beside the installed binaries",
		"runtime asset, not source-only data",
	} {
		if !strings.Contains(src, instruction) {
			t.Fatalf("release manifest does not tell binary installers to preserve the STT runtime: missing %q", instruction)
		}
	}
}

func TestPackageScriptShipsGrokPluginAndThirdPartyNotices(t *testing.T) {
	src := packageScript(t)
	for _, command := range []string{
		`cp "$ROOT/plugins/c3-grok/.mcp.json" "$ROOT/plugins/c3-grok/plugin.json" "$ROOT/plugins/c3-grok/README.md" "$DEST/plugins/c3-grok/"`,
		`cp "$ROOT/plugins/c3-grok/hooks/hooks.json" "$DEST/plugins/c3-grok/hooks/"`,
		`go list -m -f`,
		`"$DEST/THIRD_PARTY_LICENSES/MODULES.txt"`,
		`LICENSE 'LICENSE.*' 'LICENSE-*' COPYING 'COPYING.*' 'COPYING-*' NOTICE 'NOTICE.*' 'NOTICE-*'`,
		`has no top-level LICENSE/COPYING/NOTICE file`,
	} {
		if !strings.Contains(src, command) {
			t.Fatalf("scripts/package.sh is missing release contract %q", command)
		}
	}
}

func TestPrebuiltInstallCopiesEveryPackagedRuntime(t *testing.T) {
	for _, doc := range []string{"INSTALL.md", filepath.Join("docs", "INSTALL.md")} {
		data, err := os.ReadFile(filepath.Join("..", "..", doc))
		if err != nil {
			t.Fatalf("read %s: %v", doc, err)
		}
		text := string(data)
		for _, command := range []string{
			`cp -R "${pkg}/plugins/c3/stt/." ~/.local/bin/plugins/c3/stt/`,
			`cp -R "${pkg}/plugins/c3-grok/." ~/.local/bin/plugins/c3-grok/`,
		} {
			if !strings.Contains(text, command) {
				t.Fatalf("%s prebuilt install omits %q", doc, command)
			}
		}
	}
}

func TestReleasePipelineFailsClosedOnPartialPlatformSet(t *testing.T) {
	makefile, err := os.ReadFile(filepath.Join("..", "..", "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(makefile), "@set -eu; for p in $(PLATFORMS)") {
		t.Fatal("Makefile dist loop is not fail-fast; a non-final package failure could be masked")
	}

	workflow, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "release.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(workflow)
	for _, platform := range []string{
		"linux_amd64", "linux_arm64",
		"darwin_amd64", "darwin_arm64",
		"windows_amd64", "windows_arm64",
	} {
		needle := `"dist/c3_${TAG}_` + platform + `.tar.gz"`
		if strings.Count(text, needle) != 2 {
			t.Fatalf("release workflow must name %s exactly in verification and publication", platform)
		}
	}
	for _, contract := range []string{
		`[ "$(find dist -maxdepth 1 -type f -name '*.tar.gz' | wc -l)" -eq 6 ]`,
		`[ "$(awk 'NF {n++} END {print n+0}' dist/SHA256SUMS)" -eq 6 ]`,
		`[ "$actual_names" = "$expected_names" ]`,
		`gh release create "$TAG" "$@" dist/SHA256SUMS`,
	} {
		if !strings.Contains(text, contract) {
			t.Fatalf("release workflow is missing fail-closed contract %q", contract)
		}
	}
	if strings.Contains(text, "gh release create \"$TAG\" \\\n            dist/*.tar.gz") {
		t.Fatal("release publication still uses a wildcard that permits a partial platform set")
	}
}

// TestTarballNameMatchesPackageScript pins the release-asset filename. The
// updater downloads an asset by exact name; if package.sh's PKG pattern and
// TarballNameFor ever disagree, every `c3-broker update` fails to find its
// asset in an otherwise perfectly good release.
func TestTarballNameMatchesPackageScript(t *testing.T) {
	src := packageScript(t)

	m := pkgRe.FindStringSubmatch(src)
	if m == nil {
		t.Fatal(`scripts/package.sh: no PKG="..." assignment found; cannot verify the release-asset naming contract`)
	}

	// Render the script's shell pattern with concrete values, the same way the
	// script would, then compare against what the updater asks GitHub for.
	shell := m[1]
	repl := strings.NewReplacer(
		"${VERSION}", "v9.9.9",
		"${GOOS}", "plan9",
		"${GOARCH}", "riscv64",
	)
	wantStem := repl.Replace(shell)
	if strings.Contains(wantStem, "$") {
		t.Fatalf("scripts/package.sh PKG=%q contains a variable this test does not know how to expand; "+
			"teach it the new variable rather than weakening the check", shell)
	}

	want := wantStem + ".tar.gz"
	if got := TarballNameFor("v9.9.9", "plan9", "riscv64"); got != want {
		t.Errorf("release-asset name contract broken:\n  updater.TarballNameFor = %q\n  scripts/package.sh     = %q\n"+
			"The updater downloads assets by exact name; a mismatch makes every update fail.", got, want)
	}
}

// TestMakefileBuildsEveryShippedBinary guards the third copy of the list. `make
// build` is what a contributor runs locally, and a binary that ships in the
// release but is missing from `make build` produces a local tree that cannot
// reproduce the release (this drifted once already: claude-shim shipped in the
// tarball but was absent from the Makefile).
func TestMakefileBuildsEveryShippedBinary(t *testing.T) {
	src := packageScript(t)
	m := binsRe.FindStringSubmatch(src)
	if m == nil {
		t.Fatal(`scripts/package.sh: no BINS="..." assignment found`)
	}
	shipped := strings.Fields(m[1])

	data, err := os.ReadFile(filepath.Join("..", "..", "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	mk := string(data)

	for _, b := range shipped {
		needle := fmt.Sprintf("$(BIN_DIR)/%s ", b)
		if !strings.Contains(mk, needle) {
			t.Errorf("Makefile's build target does not build %q, but scripts/package.sh ships it — "+
				"`make build` cannot reproduce the release", b)
		}
	}
}
