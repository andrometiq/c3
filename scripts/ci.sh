#!/bin/sh

# One source of truth for the local commit gate and GitHub Actions.
# Keep every release-platform compile check here so a local green commit means
# the hosted CI workflow will exercise the same commands.
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

echo "==> formatting"
unformatted=$(gofmt -l .)
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

echo "==> CI gate passed"
