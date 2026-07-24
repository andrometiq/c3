.PHONY: build test clean install dist

BIN_DIR := bin
DIST_DIR := dist
# Release version: pass VERSION=v1.0.0 explicitly (the release workflow does), or
# fall back to git describe ONLY when the checkout sits exactly on a tag. Any
# other source build stays an uninjected dev build — a pseudo-version like
# v1.0.0-5-gSHA would sort as a prerelease OLDER than the latest release, arming
# the update checker with a false "update available" that /c3:update would then
# DOWNGRADE. Dev builds disable the checker entirely (version.IsDev).
VERSION ?= $(shell git describe --tags --exact-match 2>/dev/null)
PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64
# Inject the build version so a binary knows its own release identity (the
# auto-updater compares it against the latest GitHub release). Kept in sync with
# scripts/package.sh's VERSIONPKG. Empty VERSION ⇒ no injection ⇒ dev build.
VERSION_LDFLAGS := $(if $(VERSION),-X github.com/karthikeyan5/c3/internal/version.Version=$(VERSION),)

build:
	@mkdir -p $(BIN_DIR)
	go build -ldflags "$(VERSION_LDFLAGS)" -o $(BIN_DIR)/c3-broker ./cmd/c3-broker
	go build -ldflags "$(VERSION_LDFLAGS)" -o $(BIN_DIR)/c3-claude-adapter ./cmd/c3-claude-adapter
	go build -ldflags "$(VERSION_LDFLAGS)" -o $(BIN_DIR)/c3-codex-adapter ./cmd/c3-codex-adapter
	go build -ldflags "$(VERSION_LDFLAGS)" -o $(BIN_DIR)/c3-grok-adapter ./cmd/c3-grok-adapter
	go build -ldflags "$(VERSION_LDFLAGS)" -o $(BIN_DIR)/c3-agy-adapter ./cmd/c3-agy-adapter
	go build -ldflags "$(VERSION_LDFLAGS)" -o $(BIN_DIR)/c3-desktop-adapter ./cmd/c3-desktop-adapter
	go build -ldflags "$(VERSION_LDFLAGS)" -o $(BIN_DIR)/codex ./cmd/codex
	go build -ldflags "$(VERSION_LDFLAGS)" -o $(BIN_DIR)/claude-shim ./cmd/claude-shim
	go build -ldflags "$(VERSION_LDFLAGS)" -o $(BIN_DIR)/migrate-legacy ./cmd/migrate-legacy

test:
	go test ./...

clean:
	rm -rf $(BIN_DIR)

install:
	go install -ldflags "$(VERSION_LDFLAGS)" ./cmd/...

# Cross-compile every platform into $(DIST_DIR)/ as release tarballs + SHA256SUMS.
# Mirrors what .github/workflows/release.yml runs on a v* tag, for local testing.
dist:
	@rm -rf $(DIST_DIR) && mkdir -p $(DIST_DIR)
	@for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; \
		sh scripts/package.sh $$os $$arch $(VERSION) $(DIST_DIR); \
	done
	@cd $(DIST_DIR) && { sha256sum *.tar.gz > SHA256SUMS 2>/dev/null || shasum -a 256 *.tar.gz > SHA256SUMS; }
	@echo "built $(DIST_DIR)/:"; ls -1 $(DIST_DIR)
