//go:build !linux && !darwin

// Package peercred reads the kernel's record of who is at the other end of a
// connected Unix socket.
package peercred

import (
	"errors"
	"net"
)

// VerifySameUser always fails here: the peer can't be verified, so callers
// that need it fail closed.
func VerifySameUser(net.Conn) error {
	return errors.New("peercred: peer verification unavailable on this platform")
}
