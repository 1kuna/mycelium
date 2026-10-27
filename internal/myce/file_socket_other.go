//go:build !unix

package myce

import "net"

// Production release targets are Unix. Other injected connections retain the
// ordinary net.Conn partial-write contract.
func fileWriteSome(conn net.Conn, p []byte) (int, error) {
	return conn.Write(p[:min(len(p), 8192)])
}
