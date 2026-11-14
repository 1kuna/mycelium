package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
)

// Probe the nearest existing ancestor before mkdir: an unmounted network root
// must never silently become a local checkpoint/environment root.
func validateLocalModelVolume(ctx context.Context, abort context.CancelCauseFunc, path string) (string, error) {
	for {
		info, err := onboardingFSOperation(ctx, abort, func() (os.FileInfo, error) { return os.Lstat(path) }, "local-model-ancestor")
		if errors.Is(err, os.ErrNotExist) {
			parent := filepath.Dir(path)
			if parent != path {
				path = parent
				continue
			}
		}
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", storageRootRefusal(ctx, "local-model-ancestor", preflightError("model_root_not_local", "storage", "model root outside the selected NAS must use an accessible local filesystem"), err)
		}
		local, err := onboardingFSOperation(ctx, abort, func() (bool, error) { return modelRootLocalFilesystem(path), nil }, "local-model-filesystem")
		if err != nil || !local {
			return "", storageRootRefusal(ctx, "local-model-filesystem", preflightError("model_root_not_local", "storage", "model root outside the selected NAS must use an accessible local filesystem"), err)
		}
		disk, err := onboardingFSOperation(ctx, abort, func() (DiskReading, error) { return workerDiskProbe(path) }, "local-model-headroom")
		if err != nil || disk.TotalBytes == 0 || disk.FreeBytes <= diskFreeFloor(disk.TotalBytes) {
			return "", storageRootRefusal(ctx, "local-model-headroom", preflightError("model_root_headroom", "storage", "local model volume lacks measured free space above the disk floor"), err)
		}
		return path, nil
	}
}

// Both plan-only and real install paths establish the same local-root
// selection/headroom facts. Plan-only never creates directories or probes writes.
func ValidateOnboardingModelSelection(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) error {
	if c.ModelRoot == "" || withinNAS(c.NAS, c.ModelRoot) {
		return nil
	}
	if !filepath.IsAbs(c.ModelRoot) {
		return preflightError("model_root_not_local", "storage", "model root must be absolute")
	}
	roots, err := MountedNASRoots(ctx, c.Detection.OS, command)
	if err != nil {
		return err
	}
	if mount := onboardingMount(c, roots); mount != "" && withinNAS(mount, c.ModelRoot) {
		return nil
	}
	ioCtx, abort := context.WithCancelCause(ctx)
	defer abort(nil)
	_, err = onboardingNASIO.runContext(ioCtx, "local-model-selection:"+c.ModelRoot, func() (bool, error) { _, err := validateLocalModelVolume(ioCtx, abort, c.ModelRoot); return false, err })
	if errors.Is(err, errCheckpointIOTimeout) || errors.Is(err, errCheckpointIOPending) {
		return preflightError("nas_unresponsive", "storage", "local model filesystem validation stalled or is still pending")
	}
	return err
}
