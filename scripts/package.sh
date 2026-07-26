#!/usr/bin/env sh
# package.sh — build the C3 binaries for one platform and bundle a release tarball.
#
# Usage: scripts/package.sh <goos> <goarch> <version> <outdir>
#   e.g. scripts/package.sh linux amd64 v1.0.0 dist
#
# Produces: <outdir>/c3_<version>_<goos>_<goarch>.tar.gz
# Each tarball contains the nine compiled binaries, LICENSE, and a MANIFEST.txt.
# Pure-Go cross-compile (CGO disabled), so every target builds on any host.
#
# Shared by .github/workflows/release.yml and the Makefile `dist` target so the
# packaging logic lives in exactly one place.
set -eu

GOOS="${1:?usage: package.sh <goos> <goarch> <version> <outdir>}"
GOARCH="${2:?usage: package.sh <goos> <goarch> <version> <outdir>}"
VERSION="${3:?usage: package.sh <goos> <goarch> <version> <outdir>}"
OUTDIR="${4:?usage: package.sh <goos> <goarch> <version> <outdir>}"

# Repo root = parent of this script's directory.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Every main package under cmd/ that ships as a runnable binary.
BINS="c3-broker c3-claude-adapter c3-codex-adapter c3-grok-adapter c3-agy-adapter c3-desktop-adapter codex claude-shim migrate-legacy"

# Go package path whose Version var the auto-updater reads; injected at build
# time via -ldflags -X so a release binary knows its own version (dev builds,
# built without this, report "dev" and never auto-update). Must stay in sync
# with internal/version.
VERSIONPKG="github.com/Andrometiq/c3/internal/version.Version"

# sha256 helper that works on both Linux (sha256sum) and macOS (shasum).
sha256() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | awk '{print $1}'
	else
		shasum -a 256 "$1" | awk '{print $1}'
	fi
}

PKG="c3_${VERSION}_${GOOS}_${GOARCH}"
STAGE="$(mktemp -d)"
DEST="$STAGE/$PKG"
mkdir -p "$DEST"

COMMIT="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
GOVER="$(cd "$ROOT" && go version | awk '{print $3}')"
BUILT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Windows executables need the .exe suffix — `go build -o <name>` writes the
# name verbatim (it only appends .exe when -o names a directory), so without
# this the Windows tarball would ship non-runnable, extension-less binaries.
EXT=""
[ "$GOOS" = "windows" ] && EXT=".exe"

echo "==> building $PKG"
for b in $BINS; do
	CGO_ENABLED=0 GOOS="$GOOS" GOARCH="$GOARCH" \
		go -C "$ROOT" build -trimpath -ldflags "-s -w -X ${VERSIONPKG}=${VERSION}" \
		-o "$DEST/$b$EXT" "./cmd/$b"
done

cp "$ROOT/LICENSE" "$DEST/LICENSE"

# MANIFEST.txt — provenance + per-binary checksums + install hint.
{
	echo "C3 — Command, Control, Communications"
	echo "version:   $VERSION"
	echo "platform:  ${GOOS}/${GOARCH}"
	echo "commit:    $COMMIT"
	echo "built:     $BUILT (UTC)"
	echo "toolchain: $GOVER (CGO disabled)"
	echo
	echo "binaries (sha256):"
	for b in $BINS; do
		printf '  %s  %s\n' "$(sha256 "$DEST/$b$EXT")" "$b$EXT"
	done
	echo
	echo "Install: place these binaries on your PATH (e.g. ~/.local/bin),"
	echo "then follow INSTALL.md. C3's /c3:build rebuilds them from source if needed."
} >"$DEST/MANIFEST.txt"

mkdir -p "$OUTDIR"
OUTDIR="$(cd "$OUTDIR" && pwd)"
TARBALL="$OUTDIR/$PKG.tar.gz"
tar -czf "$TARBALL" -C "$STAGE" "$PKG"
rm -rf "$STAGE"

echo "    $(sha256 "$TARBALL")  $PKG.tar.gz"
