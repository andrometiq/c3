//go:build linux || darwin

// Package peercred reads the kernel's record of who is at the other end of a
// connected Unix socket.
package peercred

import (
	"errors"
	"net"
	"os"
)

// VerifySameUser fails unless conn is a Unix socket whose peer runs as this
// process's user. It checks the connected descriptor, not the socket's path,
// so a substituted path can't pass.
func VerifySameUser(conn net.Conn) error {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return errors.New("peercred: not a unix socket")
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return err
	}
	isSameUser := false
	err = raw.Control(func(fd uintptr) {
		_, uid, err := Identity(int(fd))
		isSameUser = err == nil && uid == uint32(os.Getuid())
	})
	if err != nil || !isSameUser {
		return errors.New("peercred: peer is not this user")
	}
	return nil
}
