//go:build linux || darwin

package main

import (
	"errors"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

const crossSessionPeerPIDAvailable = true

// Validate the connected descriptor, not a second pathname lookup. fstat checks
// the local endpoint's type/owner; it cannot identify the listener inode on
// Linux. Kernel peer credentials bind this connection to the owning host even
// if a same-user process substituted the pathname after validation.
func validateCrossSessionPeer(conn net.Conn, hostPID int) error {
	c, ok := conn.(*net.UnixConn)
	if !ok {
		return errors.New("cross-session connected socket unavailable")
	}
	raw, err := c.SyscallConn()
	if err != nil {
		return errors.New("cross-session connected socket unavailable")
	}
	valid := false
	err = raw.Control(func(fd uintptr) {
		var stat unix.Stat_t
		if unix.Fstat(int(fd), &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFSOCK || stat.Uid != uint32(os.Getuid()) {
			return
		}
		pid, uid, err := crossSessionPeerIdentity(int(fd))
		valid = err == nil && crossSessionCredentialsMatch(hostPID, pid, uid)
	})
	if err != nil || !valid {
		return errors.New("cross-session connected peer is not the owning host")
	}
	return nil
}

func crossSessionCredentialsMatch(hostPID, peerPID int, peerUID uint32) bool {
	return hostPID > 1 && peerPID == hostPID && peerUID == uint32(os.Getuid())
}
