package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
		err := verifySubmissionManifest(ctx, store, manifest)
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

// The lifecycle fence held by SubmitContext protects verified bytes through
// commit; live-job references then protect them under ordinary retention.
func verifySubmissionManifest(ctx context.Context, store *FileStore, manifest FileManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manifest.validate(); err != nil {
		return err
	}
	if store == nil {
		return &HTTPError{Status: 503, Code: "input_blob_unavailable", Message: "queue file store unavailable; retry after storage recovery"}
	}
	for name, ref := range manifest {
		if err := store.verify(ctx, ref); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			reason := stagingErrorName(err)
			code, status := "input_blob_unavailable", 503
			if errors.Is(err, os.ErrNotExist) {
				code, status = "input_blob_missing", 409
			} else if reason == "identity_mismatch" {
				code, status = "input_blob_corrupt", 409
			}
			return &HTTPError{Status: status, Code: code, Message: fmt.Sprintf("input=%q blob=%s operation=submit_verify error=%s; upload the original input again before submitting", name, ref.SHA256, reason)}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := store.syncDir(filepath.Join(store.root, "blobs")); err != nil {
		return &HTTPError{Status: 503, Code: "input_blob_unavailable", Message: "input verification durability barrier failed: " + stagingErrorName(err)}
	}
	return ctx.Err()
}
