//go:build !windows

package web

import "syscall"

// openNoFollow makes the final open refuse a symlink at the last path
// component. Windows has no O_NOFOLLOW; the fd re-Stat IsRegular check after
// open covers both platforms.
const openNoFollow = syscall.O_NOFOLLOW
