package myce

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// MountedNASRoots retains only mount paths, never source addresses/options
// (which can contain credentials). Mounting belongs to the operator or OS.
func MountedNASRoots(ctx context.Context, osname string, command func(context.Context, string, ...string) ([]byte, error)) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bounded, cancel := context.WithTimeout(ctx, onboardingMountInventoryBudget)
	defer cancel()
	roots, err := mountedNASRoots(bounded, osname, command)
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return nil, context.Canceled
	}
	if bounded.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
		return nil, preflightError("nas_unresponsive", "storage", fmt.Sprintf("NAS mount inventory exceeded the %s budget; restore the mount and retry", onboardingMountInventoryBudget))
	}
	return roots, err
}

type nasMount struct{ root, fingerprint string }

func mountedNASRoots(ctx context.Context, osname string, command func(context.Context, string, ...string) ([]byte, error)) ([]string, error) {
	mounts, err := mountedNASInventory(ctx, osname, command)
	roots := make([]string, 0, len(mounts))
	for _, mount := range mounts {
		roots = append(roots, mount.root)
	}
	slices.Sort(roots)
	return slices.Compact(roots), err
}

func mountedNASInventory(ctx context.Context, osname string, command func(context.Context, string, ...string) ([]byte, error)) ([]nasMount, error) {
	if command == nil {
		return nil, preflightError("nas_unreachable", "storage", "mount inventory runner missing")
	}
	var b []byte
	var err error
	if osname == "linux" {
		b, err = command(ctx, "cat", "/proc/mounts")
	} else if osname == "darwin" {
		b, err = command(ctx, "/sbin/mount")
	} else {
		return nil, preflightError("nas_unsupported", "storage", "NAS mount detection supports Linux and macOS")
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, preflightError("nas_unreachable", "storage", "cannot inspect existing NAS mounts; mount the shared NAS through the OS")
	}
	mounts := []nasMount{}
	for _, line := range strings.Split(string(b), "\n") {
		var path, kind, source string
		if osname == "linux" {
			f := strings.Fields(line)
			if len(f) < 4 {
				continue
			}
			source, path, kind = f[0], f[1], f[2]
			path = strings.NewReplacer(`\040`, " ", `\011`, "\t", `\134`, `\`).Replace(path)
		} else {
			src, rest, ok := strings.Cut(line, " on ")
			source = src
			if !ok {
				continue
			}
			i := strings.LastIndex(rest, " (")
			if i < 0 {
				continue
			}
			path = rest[:i]
			kind = strings.TrimSpace(strings.Split(rest[i+2:], ",")[0])
			kind = strings.TrimSuffix(kind, ")")
		}
		if networkMountType(kind) && filepath.IsAbs(path) && path != "/" {
			mounts = append(mounts, nasMount{filepath.Clean(path), nasSourceFingerprint(source, kind)})
		}
	}
	return mounts, nil
}
func withinNAS(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && (rel == "." || filepath.IsLocal(rel))
}

// networkMountType accepts OS-managed automounts and known network FUSE
// subtypes, never generic/local FUSE (for example encfs). An autofs path is
// triggered only inside the bounded filesystem validation below.
func networkMountType(kind string) bool {
	return slices.Contains([]string{"nfs", "nfs4", "cifs", "smbfs", "smb3", "autofs", "fuse.sshfs", "fuse.rclone", "fuse.smbnetfs"}, kind)
}

func onboardingMount(c ServiceConfig, roots []string) string {
	mount := ""
	for _, root := range roots {
		if withinNAS(root, c.NAS) && (mount == "" || len(root) > len(mount)) {
			mount = root
		}
	}
	return mount
}
func nasNotConfigured(roots []string) error {
	return preflightError("nas_not_configured", "storage", fmt.Sprintf("configured/default NAS is absent; mount it through the OS or select --nas PATH; candidate mounts: %q", roots))
}

// ResolveOnboardingStorage resolves selection before displaying either a dry
// or real install plan. Explicit/saved paths are selections, not proof of I/O.
// A fresh invitation selects exactly one mount matching the cluster identity.
func ResolveOnboardingStorage(ctx context.Context, c ServiceConfig, discover bool, command func(context.Context, string, ...string) ([]byte, error), identity ...NASIdentity) (ServiceConfig, error) {
	if !discover && c.NAS != "" {
		return c, nil
	}
	if len(identity) > 0 && identity[0].Fingerprint != "" {
		return resolveClusterNAS(ctx, c, command, identity[0])
	}
	roots, err := MountedNASRoots(ctx, c.Detection.OS, command)
	if err != nil {
		return c, err
	}
	if c.NAS == "" || onboardingMount(c, roots) == "" {
		return c, nasNotConfigured(roots)
	}
	return c, nil
}

type onboardingStorageFS struct {
	removeCreated bool // Preflight removes only directories it created, inside the owned I/O sequence.
	lstat         func(string) (os.FileInfo, error)
	mkdir         func(string, os.FileMode) error
	probe         func(string) error
	probeContext  func(context.Context, context.CancelCauseFunc, string) error
}

var onboardingNASIO = checkpointIO[bool]{timeout: -1} // Each operation cancels the sequence on stall; no aggregate cap.

// PrepareOnboardingStorage validates an already resolved selection. One checkpointIO
// guards the complete filesystem sequence, including cleanup. A blocked syscall
// may outlive the caller, but no second sequence is launched while it is pending.
func PrepareOnboardingStorage(ctx context.Context, c ServiceConfig, discover bool, command func(context.Context, string, ...string) ([]byte, error)) (ServiceConfig, error) {
	return prepareOnboardingStorageFS(ctx, c, discover, command, onboardingStorageFS{lstat: os.Lstat, mkdir: os.Mkdir, probeContext: probeNASDirectoryContext})
}
func prepareOnboardingStorageFS(ctx context.Context, c ServiceConfig, discover bool, command func(context.Context, string, ...string) ([]byte, error), fs onboardingStorageFS) (ServiceConfig, error) {
	if err := ctx.Err(); err != nil {
		return c, err
	}
	roots, err := MountedNASRoots(ctx, c.Detection.OS, command)
	if err != nil {
		return c, err
	}
	mount := onboardingMount(c, roots)
	if mount == "" {
		if discover || c.NAS == "" {
			return c, nasNotConfigured(roots)
		}
		return c, preflightError("nas_not_mounted", "storage", "selected NAS is not mounted; restore the OS mount or supply --nas PATH")
	}
	ioCtx, abort := context.WithCancelCause(ctx)
	defer abort(nil)
	_, err = onboardingNASIO.runContext(ioCtx, c.NAS+"\x00"+c.Checkpoints+"\x00"+c.ModelRoot+"\x00"+c.EnvironmentRoot, func() (bool, error) {
		return false, validateOnboardingStorageFS(ioCtx, abort, c, mount, fs)
	})
	if errors.Is(ctx.Err(), context.Canceled) {
		return c, context.Canceled
	}
	if errors.Is(err, errCheckpointIOTimeout) || errors.Is(err, errCheckpointIOPending) || errors.Is(err, context.DeadlineExceeded) {
		return c, preflightError("nas_unresponsive", "storage", fmt.Sprintf("NAS validation stalled beyond the %s filesystem-operation budget or another validation is still pending; restore the mount and retry", networkFilesystemBudget))
	}
	if err != nil {
		return c, err
	}
	c.RequireMountedNAS = true
	return c, nil
}

// The operation stays in the sequence goroutine even after its deadline. The
// outer guard therefore retains ownership until an uninterruptible syscall
// returns, preventing retries from duplicating blocked I/O.
func onboardingFSOperation[T any](ctx context.Context, abort context.CancelCauseFunc, fn func() (T, error), operation ...string) (T, error) {
	var zero T
	if err := context.Cause(ctx); err != nil {
		return zero, err
	}
	name := "filesystem"
	if len(operation) > 0 {
		name = operation[0]
	}
	opCtx, cancel := context.WithTimeout(ctx, networkFilesystemBudget)
	stop := context.AfterFunc(opCtx, func() {
		if errors.Is(opCtx.Err(), context.DeadlineExceeded) {
			log.Printf("NAS validation operation=%s stalled budget=%s", name, networkFilesystemBudget)
			abort(errCheckpointIOTimeout)
		}
	})
	defer cancel()
	defer stop()
	started := time.Now()
	value, err := fn()
	log.Printf("NAS validation operation=%s completed elapsed=%s success=%t", name, time.Since(started), err == nil && opCtx.Err() == nil)
	if errors.Is(opCtx.Err(), context.DeadlineExceeded) {
		abort(errCheckpointIOTimeout)
	}
	if cause := context.Cause(ctx); cause != nil {
		return value, cause
	}
	return value, err
}
func onboardingFSError(ctx context.Context, abort context.CancelCauseFunc, fn func() error, operation ...string) error {
	_, err := onboardingFSOperation(ctx, abort, func() (bool, error) { return false, fn() }, operation...)
	return err
}
func validateOnboardingStorageFS(ctx context.Context, abort context.CancelCauseFunc, c ServiceConfig, mount string, fs onboardingStorageFS) (result error) {
	var created []string
	defer func() {
		for i := len(created) - 1; i >= 0; i-- {
			// Cleanup stays in the owned sequence, even after caller cancellation.
			cleanup, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
			result = errors.Join(result, onboardingFSError(cleanup, func(err error) { abort(err); cancel(err) }, func() error { return os.Remove(created[i]) }))
			cancel(nil)
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	// Check the mount and every ancestor before mkdir. In particular, a missing
	// mountpoint is never created on the local disk.
	if st, e := onboardingFSOperation(ctx, abort, func() (os.FileInfo, error) { return fs.lstat(mount) }, "mount-metadata"); e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return preflightError("nas_unreachable", "storage", "mounted NAS is unavailable; restore the OS mount")
	}
	for _, path := range []string{c.NAS, c.Checkpoints, c.ModelRoot, c.EnvironmentRoot} {
		if !filepath.IsAbs(path) || !withinNAS(mount, path) {
			return preflightError("nas_root_outside_mount", "storage", "checkpoint, model and environment roots must stay on the mounted NAS")
		}
		rel, _ := filepath.Rel(mount, path)
		at := mount
		for _, part := range strings.Split(rel, string(filepath.Separator)) {
			if part == "." {
				continue
			}
			at = filepath.Join(at, part)
			if err := ctx.Err(); err != nil {
				return err
			}
			st, e := onboardingFSOperation(ctx, abort, func() (os.FileInfo, error) { return fs.lstat(at) }, "ancestor-metadata")
			if errors.Is(e, os.ErrNotExist) {
				if err := ctx.Err(); err != nil {
					return err
				}
				if e = onboardingFSError(ctx, abort, func() error {
					err := fs.mkdir(at, 0700)
					if err == nil && fs.removeCreated {
						created = append(created, at)
					}
					return err
				}, "mkdir"); e != nil {
					return preflightError("nas_unreachable", "storage", "NAS roots cannot be created; check reachability and write permission")
				}
			} else if e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
				return preflightError("nas_root_invalid", "storage", "NAS roots require accessible real directories, without symlinks")
			}
		}
	}
	for i, path := range []string{c.Checkpoints, c.ModelRoot, c.EnvironmentRoot} {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("NAS validation root=%s started", []string{"checkpoint", "model", "environment"}[i])
		started := time.Now()
		var err error
		if fs.probeContext != nil {
			err = fs.probeContext(ctx, abort, path)
		} else {
			err = onboardingFSError(ctx, abort, func() error { return fs.probe(path) })
		}
		if err != nil {
			return preflightError("nas_unreachable", "storage", "NAS write/sync/rename probe failed; restore mount reachability and permissions")
		}
		log.Printf("NAS validation root=%s completed elapsed=%s", []string{"checkpoint", "model", "environment"}[i], time.Since(started))
	}
	return ctx.Err()
}

func probeNASDirectory(dir string) error {
	ctx, abort := context.WithCancelCause(context.Background())
	defer abort(nil)
	return probeNASDirectoryContext(ctx, abort, dir)
}
func probeNASDirectoryContext(ctx context.Context, abort context.CancelCauseFunc, dir string) error {
	f, err := onboardingFSOperation(ctx, abort, func() (*os.File, error) { return os.CreateTemp(dir, ".myce-install-probe-") }, "create")
	if f != nil && err != nil {
		_ = f.Close()
	}
	if err != nil {
		return err
	}
	path := f.Name()
	renamed := path + ".renamed"
	// Cleanup also gets a bound. If cancellation prevents cleanup, retain the
	// disposable probe for operator recovery rather than launching more NAS I/O.
	defer func() {
		_ = onboardingFSError(ctx, abort, func() error { return os.Remove(path) }, "delete")
		_ = onboardingFSError(ctx, abort, func() error { return os.Remove(renamed) }, "delete")
	}()
	writeErr := onboardingFSError(ctx, abort, func() error { _, err := f.WriteString("myce NAS write and rename probe\n"); return err }, "write")
	syncErr := onboardingFSError(ctx, abort, f.Sync, "fsync")
	// Close the owned descriptor even on timeout; it remains inside the single
	// outstanding sequence and cannot cause a second probe to be launched.
	closeErr := onboardingFSError(ctx, abort, f.Close, "close")
	if ctx.Err() != nil {
		_ = f.Close()
	}
	if err = errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	if err = onboardingFSError(ctx, abort, func() error { return os.Rename(path, renamed) }, "rename"); err != nil {
		return err
	}
	return onboardingFSError(ctx, abort, func() error { return os.Remove(renamed) }, "delete")
}
