package main

import "golang.org/x/sys/unix"

func crossSessionPeerIdentity(fd int) (int, uint32, error) {
	pid, err := unix.GetsockoptInt(fd, unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	if err != nil {
		return 0, 0, err
	}
	cred, err := unix.GetsockoptXucred(fd, unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	if err != nil {
		return 0, 0, err
	}
	// Darwin xucred ABI version is 0; x/sys exposes the struct but not the macro.
	if cred.Version != 0 {
		return 0, 0, unix.EINVAL
	}
	return pid, cred.Uid, nil
}
