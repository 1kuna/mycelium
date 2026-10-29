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
	done      chan struct{}
	ctx       context.Context
	withdrawn bool
	progress  atomic.Int64
}

func sharedInputProgress(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if progress, ok := ctx.Value(ioProgressKey{}).(func()); ok {
		progress()
	}
	return nil
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

// Active independent inputs retain s055's concurrency. Once a source is
// withdrawn, refuse new source operations until its private writer joins. This
// bounds detached work by the attempts already admitted, without serializing I/O.
// Publication belongs only to the caller; no process runs in a detached writer.
func (c *SharedInputCache) stageInput(parent context.Context, populate func(context.Context, string) error, publish func(string) error) error {
	if err := parent.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	c.mu.Lock()
	for pending := range c.staging {
		select {
		case <-pending.done:
			delete(c.staging, pending)
		default:
			if pending.withdrawn || pending.ctx.Err() != nil {
				c.mu.Unlock()
				cancel()
				return errCheckpointIOPending
			}
		}
	}
	b := &sharedInputStaging{done: make(chan struct{}), ctx: ctx}
	if c.staging == nil {
		c.staging = map[*sharedInputStaging]struct{}{}
	}
	c.staging[b] = struct{}{}
	c.mu.Unlock()
	defer func() {
		// Serialize withdrawal (including stall) with registration before returning
		// control to an actor that releases the original use/build/execution locks.
		c.mu.Lock()
		b.withdrawn = true
		cancel()
		c.mu.Unlock()
	}()
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
