//go:build !unix

package myce

import "net"

// Production targets use readiness observations. Opaque connections retain
// owner cancellation; their buffering cannot establish wire service time.
func fileSocketSome(conn net.Conn, p []byte, write bool) (int, error, bool) {
	if write {
		n, e := conn.Write(p[:min(len(p), 8192)])
		return n, e, false
	}
	n, e := conn.Read(p)
	return n, e, false
}
