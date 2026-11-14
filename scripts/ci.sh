#!/bin/sh

# One source of truth for the local commit gate and GitHub Actions.
# Keep every release-platform compile check here so a local green commit means
# the hosted CI workflow will exercise the same commands.
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

# Run on the Go version go.mod's go line names, which is what hosted CI
# installs (setup-go reads it), so a newer local Go can't pass what CI fails
# (Go 1.25 vs 1.27 json decoding, 2026-10). The go command downloads that
# toolchain once if it is missing; gofmt comes from the same toolchain.
go_version=$(sed -n 's/^go \([0-9][0-9.]*\).*/\1/p' go.mod)
case $go_version in
*.*.*) ;;
*.*) go_version="$go_version.0" ;; # "go 1.N" names the language; the release is 1.N.0
*)
	echo "scripts/ci.sh: cannot read the go version from go.mod" >&2
	exit 1
	;;
esac
export GOTOOLCHAIN="go${go_version}"
if ! go_banner=$(go version); then
	echo "scripts/ci.sh: Go ${go_version} toolchain is not available (offline and not cached?)" >&2
	exit 1
fi
echo "==> go ${go_version} (${go_banner})"

echo "==> formatting"
unformatted=$("$(go env GOROOT)/bin/gofmt" -l .)
if [ -n "$unformatted" ]; then
	echo "These files are not gofmt-clean:" >&2
	printf '%s\n' "$unformatted" >&2
	echo "Run: gofmt -w <files>" >&2
	exit 1
fi

echo "==> build"
go build ./...

echo "==> vet"
go vet ./...

echo "==> cross-compile release targets"
for target in \
	linux/amd64 \
	linux/arm64 \
	darwin/amd64 \
	darwin/arm64 \
	windows/amd64 \
	windows/arm64
do
	echo "  -> $target"
	GOOS=${target%/*} GOARCH=${target#*/} CGO_ENABLED=0 go build ./...
done

echo "==> vet windows sources and tests"
GOOS=windows go vet ./...

echo "==> test"
go test ./...

# The live-matrix Python suites (verdict core, sanitize, extractor-replay,
# hostdriver) are slow (the extractor-replay suite alone runs minutes), so run
# them in hosted CI and on demand, not on every local commit.
if [ "${CI:-}" = "true" ] || [ "${RUN_PY_TESTS:-}" = "1" ]; then
	echo "==> python live-matrix suites"
	python3 -m unittest discover -s scripts/live-matrix -p 'test_*.py'
fi

echo "==> CI gate passed"
