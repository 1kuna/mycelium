//go:build unix

package myce

import (
	"net"
	"syscall"
)

// RawConn uses the standard runtime poller, but returns after one successful
// nonblocking write. net.Conn.Write itself loops until the entire buffer has
// been sent and hides the progress needed to renew the transfer idle deadline.
func fileWriteSome(conn net.Conn, p []byte) (int, error) {
	socket, ok := conn.(syscall.Conn)
	if !ok {
		return conn.Write(p[:min(len(p), 8192)])
	}
	raw, err := socket.SyscallConn()
	if err != nil {
		return 0, err
	}
	var n int
	var writeErr error
	err = raw.Write(func(fd uintptr) bool {
		for {
			n, writeErr = syscall.Write(int(fd), p)
			if writeErr == syscall.EINTR {
				continue
			}
			return writeErr != syscall.EAGAIN && writeErr != syscall.EWOULDBLOCK
		}
	})
	if err != nil {
		return 0, err
	}
	return max(n, 0), writeErr
}
