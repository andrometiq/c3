//go:build !windows

package web

import (
	"syscall"
	"testing"
)

// makeTestFIFO creates a named pipe for the unsupported-media test. Windows
// has no FIFOs, so the Windows twin reports false and the case is skipped.
func makeTestFIFO(t *testing.T, path string) bool {
	t.Helper()
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	return true
}
