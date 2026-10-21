package myce

import (
	"context"
	"errors"
	"sync"
	"time"
)

// networkFilesystemBudget is the 250 ms warm-network allowance used by worker
// preparation/heartbeat (#63, bc2684b). Onboarding applies it per filesystem
// operation, outside service publication and queue locks.
const networkFilesystemBudget = 250 * time.Millisecond

var errCheckpointIOTimeout = errors.New("NAS unreachable: filesystem operation timed out")
var errCheckpointIOPending = errors.New("NAS unreachable: previous filesystem call is still blocked")

// Filesystem syscalls cannot be cancelled. Keep at most one outstanding call
// per operation, and never wait for NAS I/O while holding the queue lock.
type checkpointIO[T any] struct {
	mu      sync.Mutex
	key     string
	pending *checkpointIOResult[T]
	timeout time.Duration
}
type checkpointIOResult[T any] struct {
	discard bool // A timed-out/cancelled result is never reusable.
	done    chan struct{}
	value   T
	err     error
}

func (n *checkpointIO[T]) run(key string, fn func() (T, error)) (T, error) {
	// Durable worker operations consume late completion rather than repeat
	// side effects. Context-bound validation checks must instead run afresh.
	return n.runBounded(context.Background(), key, fn, false)
}

func (n *checkpointIO[T]) runContext(parent context.Context, key string, fn func() (T, error)) (T, error) {
	return n.runBounded(parent, key, fn, true)
}

func (n *checkpointIO[T]) runBounded(parent context.Context, key string, fn func() (T, error), discardOnTimeout bool) (T, error) {
	var zero T
	timeout := n.timeout
	if timeout == 0 {
		timeout = 25 * time.Millisecond
	}
	// I/O budgets use real deadlines; queue scheduling and grace still use Clock.
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	for {
		if err := context.Cause(ctx); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return zero, errCheckpointIOTimeout
			}
			return zero, err
		}
		n.mu.Lock()
		if n.pending != nil && (n.key != key || n.pending.discard) {
			select {
			case <-n.pending.done:
				n.pending = nil
			default:
				if n.key != key || discardOnTimeout && n.pending.discard {
					n.mu.Unlock()
					return zero, errCheckpointIOPending
				}
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
		select {
		case <-pending.done:
			n.mu.Lock()
			discard := pending.discard
			if n.pending == pending {
				n.pending = nil
			}
			n.mu.Unlock()
			if discard {
				continue
			}
			// A completion racing cancellation must not be accepted as success.
			if err := context.Cause(ctx); err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return zero, errCheckpointIOTimeout
				}
				return zero, err
			}
			return pending.value, pending.err
		case <-ctx.Done():
			n.mu.Lock()
			pending.discard = pending.discard || discardOnTimeout
			n.mu.Unlock()
			err := context.Cause(ctx)
			if errors.Is(err, context.DeadlineExceeded) {
				return zero, errCheckpointIOTimeout
			}
			return zero, err
		}
	}
}

func needsCheckpoint(j *Job) bool { return j.Spec.Priority != "production" || j.Spec.Resumable }
