package myce

import (
	"context"
	"errors"
	"sync"
	"time"
)

// NAS budgets share one policy for SMB/NFS/autofs. Background observations must
// return within a heartbeat; availability retains a successful observation while
// a new probe makes filesystem-operation progress. Validation bounds each syscall.
const (
	heartbeatFilesystemBudget = 750 * time.Millisecond
	// Ten seconds bounds a stalled operation above the observed 6.074-second
	// maximum; qualification under supported share load remains outstanding.
	networkFilesystemBudget        = 10 * time.Second
	checkpointPreparationBudget    = networkFilesystemBudget
	onboardingMountInventoryBudget = 5 * time.Second       // standalone selection, including dry runs
	localObservationBudget         = 25 * time.Millisecond // local caches/inventory, never NAS
)

var errCheckpointIOTimeout = errors.New("NAS unreachable: filesystem operation timed out")

// Pending preserves the worker's timeout diagnostic, while distinguishing an
// unfinished observation from a completed failure for inventory refresh.
var errCheckpointIOPending = errors.New("NAS unreachable: filesystem operation timed out")

// Filesystem syscalls cannot be cancelled. Keep at most one outstanding call
// per operation, and never wait for NAS I/O while holding the queue lock.
type checkpointIO[T any] struct {
	mu      sync.Mutex
	key     string
	pending *checkpointIOResult[T]
	timeout time.Duration // Negative: the sequence context owns per-operation deadlines.
}
type checkpointIOResult[T any] struct {
	discard    bool // A timed-out/cancelled result is never reusable.
	done       chan struct{}
	progressed time.Time
	value      T
	err        error
}

// Final result publication must not overtake a still-writing live checkpoint
// publication. Its allocation sample and cleanup precede attempt completion.
func (n *checkpointIO[T]) inFlight() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.pending == nil {
		return false
	}
	select {
	case <-n.pending.done:
		return false
	default:
		return true
	}
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
		timeout = localObservationBudget
	}
	// I/O budgets use real deadlines; queue scheduling and grace still use Clock.
	var ctx context.Context
	var cancel context.CancelFunc
	if timeout < 0 {
		ctx, cancel = context.WithCancel(parent)
	} else {
		ctx, cancel = context.WithTimeout(parent, timeout)
	}
	defer cancel()
	for {
		if err := context.Cause(ctx); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				if !discardOnTimeout {
					return zero, errCheckpointIOPending
				}
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
			r := &checkpointIOResult[T]{done: make(chan struct{}), progressed: time.Now()}
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
			if err := context.Cause(ctx); discardOnTimeout && err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					if !discardOnTimeout {
						return zero, errCheckpointIOPending
					}
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
				if !discardOnTimeout {
					return zero, errCheckpointIOPending
				}
				return zero, errCheckpointIOTimeout
			}
			return zero, err
		}
	}
}

func needsCheckpoint(j *Job) bool { return j.Spec.Priority != "production" || j.Spec.Resumable }

// Background availability uses the installer's measured per-operation stall
// backstop: successful CIFS create/unlink calls took 17.73s/23.22s (#234).
// Completed operations renew liveness; a progressing sequence has no total cap.
// A stuck call fences admission after 30s, costing up to 30s of retained success.
// The 750ms control wait and single outstanding sequence remain unchanged.
const checkpointAvailabilityStallBudget = onboardingFilesystemStallBudget

type checkpointAvailability struct {
	checkpointIO[bool]
	available   bool // protected by checkpointIO.mu
	observedKey string
}

func (n *checkpointAvailability) run(key string, fn func() (bool, error)) (bool, error) {
	return n.runProgress(key, func(func()) (bool, error) { return fn() })
}

func (n *checkpointAvailability) runProgress(key string, fn func(func()) (bool, error)) (bool, error) {
	value, err := n.checkpointIO.run(key, func() (bool, error) {
		progress := func() {
			n.mu.Lock()
			n.pending.progressed = time.Now()
			n.mu.Unlock()
		}
		value, err := fn(progress)
		n.mu.Lock()
		n.available, n.observedKey = value && err == nil, key
		n.mu.Unlock()
		return value, err
	})
	if !errors.Is(err, errCheckpointIOPending) {
		return value, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.available && n.observedKey == key && n.key == key && n.pending != nil && time.Since(n.pending.progressed) < checkpointAvailabilityStallBudget {
		return true, nil
	}
	return false, err
}
