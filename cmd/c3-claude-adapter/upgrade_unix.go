//go:build linux || darwin

package main

import (
	"errors"
	"io"
	"os"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/sys/unix"
)

const upgradeSupported = true

func upgradeStdio(a *adapter) (mcp.Transport, error) {
	fd := int(os.Stdin.Fd())
	if err := unix.SetNonblock(fd, true); err != nil {
		return nil, err
	}
	t := &upgradeTransport{output: os.Stdout}
	t.readAvailable = func(p []byte) (int, error) {
		n, err := unix.Read(fd, p)
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		if n == 0 {
			return 0, io.EOF
		}
		return n, nil
	}
	a.upgrade.wire = t
	return t, nil
}
func upgradeExec(path string, args, env []string) error { return syscall.Exec(path, args, env) }
