//go:build windows

package web

import "testing"

// makeTestFIFO reports false on Windows: there are no FIFOs to create, so the
// FIFO rejection case is exercised only on unix builds.
func makeTestFIFO(t *testing.T, path string) bool {
	t.Helper()
	return false
}
