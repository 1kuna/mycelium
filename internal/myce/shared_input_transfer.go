package myce

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// Input transfers renew the socket deadline on progress, allowing arbitrarily
// slow healthy transfers. The idle allowance includes two full hash passes at
// 100 MiB/s for the store's verification before headers arrive (#218).
func (c Client) inputTransfer(ref FileRef) (Client, func()) {
	idle := max(60*time.Second, 30*time.Second+time.Duration(float64(ref.SizeBytes)/(100<<20)*2*float64(time.Second)))
	hc := http.Client{}
	if c.HTTP != nil {
		hc = *c.HTTP
	}
	hc.Timeout = 0
	base := hc.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	if transport, ok := base.(*http.Transport); ok {
		tr := transport.Clone()
		dial := tr.DialContext
		if dial == nil {
			dial = (&net.Dialer{Timeout: idle, KeepAlive: 30 * time.Second}).DialContext
		}
		tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := dial(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return &inputConn{Conn: conn, idle: idle}, nil
		}
		tr.ForceAttemptHTTP2 = false
		tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		tr.ResponseHeaderTimeout = idle
		tr.TLSHandshakeTimeout = idle
		hc.Transport = tr
		c.HTTP = &hc
		return c, tr.CloseIdleConnections
	}
	// Injected/custom transports retain their own I/O semantics.
	c.HTTP = &hc
	return c, func() {}
}

type inputConn struct {
	net.Conn
	idle time.Duration
}

func (c *inputConn) Read(p []byte) (int, error) {
	if err := c.SetReadDeadline(time.Now().Add(c.idle)); err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}
func (c *inputConn) Write(p []byte) (int, error) {
	if err := c.SetWriteDeadline(time.Now().Add(c.idle)); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}
