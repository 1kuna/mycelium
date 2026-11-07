package myce

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"
)

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
			cancel()
			// Context cancellation alone cannot unblock a request body read or
			// response write. Transport deadlines interrupt both without a kill.
			controller := http.NewResponseController(w)
			now := time.Now() // Socket I/O only; never a scheduling decision.
			_ = controller.SetReadDeadline(now)
			_ = controller.SetWriteDeadline(now)
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
