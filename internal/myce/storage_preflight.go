package myce

import (
	"context"
	"errors"
	"log"
	"syscall"
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
	var prior *storagePreflightError
	if errors.As(err, &prior) {
		detail = prior.detail
		if detail.Root == "" {
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

func preflightFleetStorageResult(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error), fs *onboardingStorageFS) (*StoragePreflightFailure, error) {
	started := time.Now()
	roots, err := c.StateRoots()
	if err != nil {
		return nil, storageCheckError("state", "resolve", time.Since(started), err)
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
			return nil, storageCheckError(name, "format", time.Since(started), err)
		}
	}
	return prepareInstallStorage(ctx, c, command, true, fs)
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

// Older queues reject unknown acknowledgement fields. A new queue advertises
// this additive diagnostic capability in the instruction; receipts retain it
// regardless of the queue version, without retrying a refused acknowledgement.
func fleetReceiptStorageFailure(r fleetInstallReceipt) *StoragePreflightFailure {
	if r.Phase != "failed" || !r.Instruction.StorageDiagnostics {
		return nil
	}
	return fleetStorageFailure(fleetFailureCode(r.Failure), r.StorageFailure)
}

// Transient reachability says nothing about the installed binary or service.
// Keep definitive path, permission, namespace and filesystem errors fatal.
func installStorageOutcome(ctx context.Context, err error) (*StoragePreflightFailure, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	detail := storageFailureDetail(err)
	if detail == nil {
		return nil, err
	}
	transient := detail.TimedOut || detail.Operation == "pending"
	switch syscall.Errno(detail.Errno) {
	case syscall.EIO, syscall.ETIMEDOUT, syscall.ENETUNREACH, syscall.EHOSTUNREACH, syscall.ENOTCONN, syscall.ECONNRESET, syscall.ESTALE:
		transient = true
	}
	if !transient {
		return nil, err
	}
	log.Printf("warning: install storage unavailable root=%s operation=%s errno=%d elapsed=%s timed_out=%t; runtime storage admission remains active", detail.Root, detail.Operation, detail.Errno, detail.Elapsed, detail.TimedOut)
	return detail, nil
}

const installStorageReceiptFile = "install-storage.json"

type installStorageReceipt struct {
	Warning *StoragePreflightFailure `json:"storage_warning,omitempty"`
}

func prepareInstallStorage(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error), preflight bool, fs *onboardingStorageFS) (*StoragePreflightFailure, error) {
	var err error
	if fs != nil {
		copy := *fs
		copy.removeCreated = preflight
		_, err = prepareOnboardingStorageFS(ctx, c, false, command, copy)
	} else {
		err = prepareInstallNASMode(ctx, c, command, preflight)
	}
	warning, err := installStorageOutcome(ctx, err)
	var typed *PreflightError
	if err != nil && ctx.Err() == nil && !errors.As(err, &typed) {
		err = storageRefusal(preflightError("nas_unreachable", "storage", "storage validation failed; check the selected roots and permissions"), err)
	}
	// The CLI's first observation may own an outstanding syscall. Retain its
	// precise diagnostic when the second check can only report "pending".
	if warning != nil && warning.Operation == "pending" && c.storageWarning != nil {
		warning = sanitizeStorageFailure(c.storageWarning)
	}
	return warning, err
}

func fleetReceiptStorageWarning(r fleetInstallReceipt) *StoragePreflightFailure {
	if !r.Instruction.StorageWarnings {
		return nil
	}
	return sanitizeStorageFailure(r.StorageWarning)
}
