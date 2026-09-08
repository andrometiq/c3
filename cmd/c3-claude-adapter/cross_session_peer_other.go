//go:build !linux && !darwin

package main

import (
	"errors"
	"net"
)

const crossSessionPeerPIDAvailable = false

func validateCrossSessionPeer(net.Conn, int) error {
	return errors.New("cross-session peer PID verification unavailable on this platform")
}
