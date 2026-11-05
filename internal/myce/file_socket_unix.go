//go:build unix

package myce

import (
	"io"
	"net"
	"syscall"
)

// Only an operation that actually waited for socket readiness measures wire
// service time. A successful nonblocking copy measures a kernel buffer, not
// network throughput, and must never teach an idle deadline.
func fileSocketSome(conn net.Conn, p []byte, write bool) (int, error, bool) {
	socket, ok := conn.(syscall.Conn)
	if !ok {
		if write {
			n, e := conn.Write(p[:min(len(p), 8192)])
			return n, e, true
		}
		n, e := conn.Read(p)
		return n, e, true
	}
	raw, err := socket.SyscallConn()
	if err != nil {
		return 0, err, false
	}
	var n int
	var operationErr error
	waited := false
	operation := func(fd uintptr) bool {
		for {
			if write {
				n, operationErr = syscall.Write(int(fd), p)
			} else {
				n, operationErr = syscall.Read(int(fd), p)
			}
			if operationErr == syscall.EINTR {
				continue
			}
			if operationErr == syscall.EAGAIN || operationErr == syscall.EWOULDBLOCK {
				waited = true
				return false
			}
			return true
		}
	}
	if write {
		err = raw.Write(operation)
	} else {
		err = raw.Read(operation)
	}
	if err != nil {
		return 0, err, waited
	}
	if !write && n == 0 && operationErr == nil {
		operationErr = io.EOF
	}
	return max(n, 0), operationErr, waited
}
