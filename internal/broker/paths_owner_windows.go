//go:build windows

package broker

import "os"

// The private-/tmp ownership check is unreachable on Windows: runtimeDir uses
// LOCALAPPDATA (or the user's local AppData fallback) and returns before the
// Unix fallback path. Keep the unsupported answer explicit if that changes.
func runtimeDirOwnerUID(os.FileInfo) (uint32, bool) {
	return 0, false
}
