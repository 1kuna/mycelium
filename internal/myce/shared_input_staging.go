package myce

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"
)

type sharedInputStaging struct {
	done     chan struct{}
	progress atomic.Int64
}

func sharedInputProgress(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if progress, ok := ctx.Value(ioProgressKey{}).(func()); ok {
		progress()
	}
	return ctx.Err()
}

func sharedInputPath(ctx context.Context, root, relative string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	path, err := confinedPath(root, relative)
	if err == nil {
		err = sharedInputProgress(ctx)
	}
	return path, err
}

// The existing admission lock bounds builders. Retain a single outstanding
// source operation after withdrawal too, so retries cannot pile up CIFS calls.
// No process runs here. Cancellation releases the attempt's use/build/admission
// locks; the late writer owns only its private, unpublished partial directory.
func (c *SharedInputCache) stageInput(parent context.Context, populate func(context.Context, string) error, publish func(string) error) error {
	if err := parent.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	if c.staging != nil {
		select {
		case <-c.staging.done:
		default:
			c.mu.Unlock()
			return errCheckpointIOPending
		}
	}
	b := &sharedInputStaging{done: make(chan struct{})}
	c.staging = b
	c.mu.Unlock()

	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	progress := func() { b.progress.Store(c.cache.Clock.Now().UnixNano()) }
	progress()
	ctx = context.WithValue(ctx, ioProgressKey{}, progress)
	// A random build key uses the existing orphan-partial cleanup protocol.
	// Its lock protects only disposable scratch, never execution admission.
	stageKey := identityHash(ID())
	stage := filepath.Join(c.cache.Root, stageKey+".partial")
	result, consumed := make(chan error), make(chan struct{})
	completed := false
	defer func() {
		close(consumed)
		if completed {
			<-b.done
		}
	}()
	go func() {
		defer close(b.done)
		defer os.Remove(filepath.Join(c.cache.Root, "locks", stageKey+".build"))
		lock, err := c.cache.lock(ctx, stageKey+".build", syscall.LOCK_EX)
		if err == nil {
			defer unlockEnvironment(lock)
			defer removeSharedEntry(stage)
			err = os.Mkdir(stage, 0700)
			if err == nil {
				err = populate(ctx, stage)
			}
		}
		select {
		case result <- err:
			// Publication/cleanup must finish before the writer removes scratch.
			<-consumed
		case <-ctx.Done():
		}
	}()
	// Reuse the measured 30s operation-stall bound (#306/#308). Completed
	// metadata operations and copied bytes renew progress; there is no total cap.
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-result:
			completed = true
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err != nil {
				return err
			}
			return publish(stage)
		case <-tick.C:
			if c.cache.Clock.Now().Sub(time.Unix(0, b.progress.Load())) >= checkpointAvailabilityStallBudget {
				return errCheckpointIOTimeout
			}
		}
	}
}
