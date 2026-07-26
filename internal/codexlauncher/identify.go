// Package codexlauncher identifies C3's opt-in Codex launcher binary.
package codexlauncher

import (
	"debug/buildinfo"
	"runtime/debug"
)

const packagePath = "github.com/Andrometiq/c3/cmd/codex"

// IsC3 reports whether path is a Go executable built from C3's cmd/codex
// package. It is deliberately stricter than "a file named codex": install and
// update paths use this check before replacing a regular file that may be the
// user's real Codex CLI.
func IsC3(path string) bool {
	info, err := buildinfo.ReadFile(path)
	return err == nil && isC3BuildInfo(info)
}

func isC3BuildInfo(info *debug.BuildInfo) bool {
	return info != nil && info.Path == packagePath
}
