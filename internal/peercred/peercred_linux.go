package peercred

import "golang.org/x/sys/unix"

// Identity returns the pid and uid of the process at the other end of the
// connected Unix socket fd, as recorded by the kernel.
func Identity(fd int) (int, uint32, error) {
	cred, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return 0, 0, err
	}
	return int(cred.Pid), cred.Uid, nil
}
