package myce

import (
	"context"
	"errors"
	"time"
)

// StoragePreflightFailure contains only fixed root/operation names and numbers.
// Paths and OS error text must never reach fleet receipts or status responses.
// Errno is zero for logical refusals and stalls without a completed syscall.
type StoragePreflightFailure struct {
	Root      string        `json:"root"`
	Operation string        `json:"operation"`
	Errno     int           `json:"errno"`
	Elapsed   time.Duration `json:"elapsed_ns"`
	TimedOut  bool          `json:"timed_out,omitempty"`
}

type storageRootKey struct{}

func storageRootContext(ctx context.Context, root string) context.Context {
	return context.WithValue(ctx, storageRootKey{}, root)
}
func storageContextRoot(ctx context.Context) string {
	root, _ := ctx.Value(storageRootKey{}).(string)
	return root
}

type storagePreflightError struct {
	detail StoragePreflightFailure
	err    error
}

func (e *storagePreflightError) Error() string { return e.err.Error() }
func (e *storagePreflightError) Unwrap() error { return e.err }

func storageCheckError(root, operation string, elapsed time.Duration, err error) error {
	if err == nil {
		return nil
	}
	detail := StoragePreflightFailure{Root: root, Operation: operation, Elapsed: elapsed}
	if detail.Root == "" {
		detail.Root = "nas"
	}
	var prior *storagePreflightError
	if errors.As(err, &prior) {
		detail = prior.detail
		if root != "" {
			detail.Root = root
		}
	} else {
		detail.Operation, detail.Errno = storageProbeFailureDetails(operation, err)
		detail.TimedOut = errors.Is(err, errCheckpointIOTimeout) || errors.Is(err, context.DeadlineExceeded)
		if errors.Is(err, errCheckpointIOPending) {
			detail.Operation = "pending"
		}
	}
	return &storagePreflightError{detail: detail, err: err}
}

// Preserve the installer's public refusal, with evidence from the failing call.
func storageRefusal(refusal, evidence error) error {
	var detail *storagePreflightError
	if errors.As(evidence, &detail) {
		return &storagePreflightError{detail: detail.detail, err: refusal}
	}
	return refusal
}

func storageFailureDetail(err error) *StoragePreflightFailure {
	var detail *storagePreflightError
	if !errors.As(err, &detail) {
		return nil
	}
	return sanitizeStorageFailure(&detail.detail)
}

// Revalidate even local receipts: neither replay nor acknowledgements may
// project arbitrary strings supplied by a damaged/untrusted receipt or helper.
func sanitizeStorageFailure(detail *StoragePreflightFailure) *StoragePreflightFailure {
	if detail == nil {
		return nil
	}
	out := *detail
	switch out.Root {
	case "state", "environment_cache", "model_cache", "file_cache", "queue_state", "queue_uploads", "queue_files", "queue_joining", "nas", "checkpoint", "model", "environment":
	default:
		out.Root = "unknown"
	}
	out.Operation, _ = storageProbeFailureDetails(out.Operation, nil)
	if out.Errno < 0 {
		out.Errno = 0
	}
	if out.Elapsed < 0 {
		out.Elapsed = 0
	}
	return &out
}

func preflightFleetStorage(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) error {
	started := time.Now()
	roots, err := c.StateRoots()
	if err != nil {
		return storageCheckError("state", "resolve", time.Since(started), err)
	}
	for i, root := range roots {
		name := "queue_state"
		if i == 0 {
			name = "state"
		} else if c.Role == "queue" {
			name = []string{"state", "queue_state", "queue_uploads", "queue_uploads", "queue_files", "queue_joining"}[i]
		} else if c.Role == "worker" {
			name = []string{"state", "environment_cache", "model_cache", "file_cache"}[i]
		}
		started := time.Now()
		if err := CheckStateRoots(root); err != nil {
			return storageCheckError(name, "format", time.Since(started), err)
		}
	}
	started = time.Now()
	if err := prepareInstallNASMode(ctx, c, command, true); err != nil {
		return storageCheckError("", "filesystem", time.Since(started), err)
	}
	return nil
}

func fleetStorageFailure(code string, detail *StoragePreflightFailure) *StoragePreflightFailure {
	if code != "fleet_storage_preflight_failed" {
		return nil
	}
	return sanitizeStorageFailure(detail)
}

func storageRootRefusal(ctx context.Context, operation string, refusal, evidence error) error {
	if evidence == nil {
		evidence = refusal
	}
	return storageRefusal(refusal, storageCheckError(storageContextRoot(ctx), operation, 0, evidence))
}
