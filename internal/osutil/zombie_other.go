//go:build !linux && !darwin

package osutil

// IsZombie has no process-state source here, so a zombie keeps reading as alive
// to whatever existence check the caller made, as before.
func IsZombie(int) bool { return false }
