package myce

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Filesystem syscalls cannot be cancelled. Keep at most one outstanding call
// per operation, and never wait for NAS I/O while holding the queue lock.
type checkpointIO[T any] struct {
	mu      sync.Mutex
	key     string
	pending *checkpointIOResult[T]
	timeout time.Duration
}
type checkpointIOResult[T any] struct {
	done  chan struct{}
	value T
	err   error
}

func (n *checkpointIO[T]) run(key string, fn func() (T, error)) (T, error) {
	n.mu.Lock()
	var zero T
	if n.pending != nil && n.key != key {
		select {
		case <-n.pending.done:
			n.pending = nil
		default:
			n.mu.Unlock()
			return zero, errors.New("NAS unreachable: previous filesystem call is still blocked")
		}
	}
	if n.pending == nil {
		n.key = key
		r := &checkpointIOResult[T]{done: make(chan struct{})}
		n.pending = r
		go func() { r.value, r.err = fn(); close(r.done) }()
	}
	pending := n.pending
	n.mu.Unlock()
	// This is an I/O budget, not a scheduling deadline; grace still uses Clock.
	timeout := n.timeout
	if timeout == 0 {
		timeout = 25 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	select {
	case <-pending.done:
		n.mu.Lock()
		if n.pending == pending {
			n.pending = nil
		}
		n.mu.Unlock()
		return pending.value, pending.err
	case <-ctx.Done():
		return zero, errors.New("NAS unreachable: filesystem operation timed out")
	}
}

func needsCheckpoint(j *Job) bool { return j.Spec.Priority != "production" || j.Spec.Resumable }
