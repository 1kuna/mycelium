package myce

import (
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

type shutdownPipeListener struct {
	conn     net.Conn
	accepted bool
	closed   chan struct{}
	once     sync.Once
}

func (l *shutdownPipeListener) Accept() (net.Conn, error) {
	if !l.accepted {
		l.accepted = true
		return l.conn, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}
func (l *shutdownPipeListener) Close() error   { l.once.Do(func() { close(l.closed) }); return nil }
func (l *shutdownPipeListener) Addr() net.Addr { return l.conn.LocalAddr() }

type shutdownObservedConn struct {
	net.Conn
	writing   chan struct{}
	writeOnce sync.Once
}

func (c *shutdownObservedConn) Write(p []byte) (int, error) {
	if c.writing != nil {
		c.writeOnce.Do(func() { close(c.writing) })
	}
	return c.Conn.Write(p)
}

// Cancellation must wake actual socket I/O, not just Context.Done. net.Pipe
// removes TCP buffering from the reproduction: an unread response cannot make
// progress, and an unfinished request body cannot complete on its own.
func TestQueueShutdownInterruptsSocketIO(t *testing.T) {
	for _, phase := range []string{"body", "response"} {
		t.Run(phase, func(t *testing.T) {
			q, _ := fixture(t)
			peer, conn := net.Pipe()
			defer peer.Close()
			defer conn.Close()
			entered := make(chan struct{})
			observed := &shutdownObservedConn{Conn: conn}
			listener := &shutdownPipeListener{conn: observed, closed: make(chan struct{})}
			defer listener.Close()
			server := &http.Server{ConnContext: QueueConnectionContext, Handler: q.shutdownRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if phase == "body" {
					close(entered)
					_, _ = io.Copy(io.Discard, r.Body)
				} else {
					_, _ = w.Write(make([]byte, 8192))
				}
			}))}
			if phase == "response" {
				observed.writing = entered
			}
			served := make(chan error, 1)
			go func() { served <- server.Serve(listener) }()
			defer server.Close()
			request := "GET / HTTP/1.1\r\nHost: fixture.invalid\r\n\r\n"
			if phase == "body" {
				request = "POST / HTTP/1.1\r\nHost: fixture.invalid\r\nContent-Length: 100\r\n\r\n"
			}
			if _, err := io.WriteString(peer, request); err != nil {
				t.Fatal(err)
			}
			<-entered
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := q.ShutdownServers(ctx, server); err != nil {
				t.Fatal("socket I/O retained shutdown", err)
			}
			if err := <-served; err != http.ErrServerClosed {
				t.Fatal(err)
			}
		})
	}
}
