// Package sttassets embeds the release STT runtime in c3-broker.
//
// The v0.1.0-rc1 updater installs only binaries. Keeping the runtime in the
// broker lets the first stable restart repair that binary-only layout without
// changing the frozen IPC or relying on the discarded release archive.
package sttassets

import "embed"

// Runtime contains only production runtime files: no tests, caches, or bakeoff
// tooling. The updater validates the required set before installing it.
//
//go:embed stt-handler.py stt-pkg/stt.py stt-pkg/vocabulary.txt stt-pkg/providers/*.py
var Runtime embed.FS
