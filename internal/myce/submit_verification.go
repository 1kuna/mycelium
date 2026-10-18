package myce

import (
	"context"
	"time"
)

const submitVerificationTimeout = 30 * time.Second

// Filesystem syscalls cannot be interrupted by context cancellation. Use one
// standard semaphore slot per queue so a stuck read/fsync cannot accumulate
// goroutines doing I/O. Each caller can cancel acquisition and waiting; the slot
// stays charged until the syscall returns. No queue mutation runs in that worker.
func verifySubmissionInputs(ctx context.Context, store *FileStore, manifest FileManifest, slot chan struct{}) error {
	select {
	case slot <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-slot
		return err
	}
	done := make(chan error, 1)
	go func() {
		// The existing verifier only needs the immutable file-store snapshot.
		verifier := Queue{files: store}
		err := verifier.verifyManifest(ctx, manifest)
		<-slot
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
