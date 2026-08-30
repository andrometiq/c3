//go:build windows

package web

// openNoFollow is a no-op on Windows (no O_NOFOLLOW); the fd re-Stat
// IsRegular check after open is the guard on this platform.
const openNoFollow = 0
