package myce

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"net/http"
	"time"
)

type queueConnectionKey struct{}

// QueueConnectionContext gives shutdown access to the underlying socket. TLS
// must not send another encrypted record after an interrupted response write.
func QueueConnectionContext(ctx context.Context, conn net.Conn) context.Context {
	if secured, ok := conn.(*tls.Conn); ok {
		conn = secured.NetConn()
	}
	return context.WithValue(ctx, queueConnectionKey{}, conn)
}

func (q *Queue) initShutdown() {
	q.shutdownOnce.Do(func() {
		q.shutdownContext, q.shutdownCancel = context.WithCancel(context.Background())
	})
}

// Both API transports share the fence. Cancel accepted requests before waiting
// for HTTP shutdown: long polls, uploads and slow readers must not prolong a
// queue restart. Transactions already committing retain their atomic boundary;
// workers keep their durable reports and retry against the replacement queue.
func (q *Queue) shutdownRequests(next http.Handler) http.Handler {
	q.initShutdown()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if q.stopping.Load() {
			respond(w, nil, queueStopping())
			return
		}
		ctx, cancel := context.WithCancel(r.Context())
		interrupted := make(chan struct{})
		stop := context.AfterFunc(q.shutdownContext, func() {
			defer close(interrupted)
			// Close the raw socket before waking the handler. An expired TLS
			// write deadline can discard an encrypted record after advancing its
			// sequence; tls.Conn.Close then sends close_notify at the next
			// sequence, causing a genuine bad-record-MAC refusal at the client.
			if conn, ok := r.Context().Value(queueConnectionKey{}).(net.Conn); ok {
				_ = conn.Close()
			} else {
				// Embedded handlers without QueueConnectionContext can interrupt
				// reads safely. ShutdownServers owns their bounded write drain.
				controller := http.NewResponseController(w)
				_ = controller.SetReadDeadline(time.Now()) // Socket I/O only.
			}
			cancel()
		})
		defer func() {
			if !stop() {
				<-interrupted
			}
			cancel()
		}()
		// Shutdown can race registration; do not begin work after its fence.
		if q.stopping.Load() {
			respond(w, nil, queueStopping())
			return
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func queueStopping() error {
	return &HTTPError{Status: 503, Code: "queue_stopping", Message: "queue is shutting down; retry the request"}
}

// ShutdownServers drains every queue API transport concurrently within the
// caller's stop budget. A deadline closes sockets, never kills the queue or
// declares an unfinished transaction committed. Close still joins q.mu/storage.
func (q *Queue) ShutdownServers(ctx context.Context, servers ...*http.Server) error {
	q.BeginShutdown()
	log.Printf("event=queue_shutdown phase=api_draining transports=%d", len(servers))
	done := make(chan error, len(servers))
	for _, server := range servers {
		go func() {
			err := server.Shutdown(ctx)
			if err != nil {
				err = errors.Join(err, server.Close())
			}
			done <- err
		}()
	}
	var err error
	for range servers {
		err = errors.Join(err, <-done)
	}
	log.Printf("event=queue_shutdown phase=api_closed clean=%t", err == nil)
	return err
}
