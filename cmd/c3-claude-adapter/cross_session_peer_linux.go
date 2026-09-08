package main

import "golang.org/x/sys/unix"

func crossSessionPeerIdentity(fd int) (int, uint32, error) {
	cred, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return 0, 0, err
	}
	return int(cred.Pid), cred.Uid, nil
}
