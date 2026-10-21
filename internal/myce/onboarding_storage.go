package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// MountedNASRoots retains only mount paths, never source addresses/options
// (which can contain credentials). Mounting belongs to the operator or OS.
func MountedNASRoots(ctx context.Context, osname string, command func(context.Context, string, ...string) ([]byte, error)) ([]string, error) {
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
		return nil, preflightError("nas_unreachable", "storage", "cannot inspect existing NAS mounts; mount the shared NAS through the OS")
	}
	roots := []string{}
	for _, line := range strings.Split(string(b), "\n") {
		var path, kind string
		if osname == "linux" {
			f := strings.Fields(line)
			if len(f) < 4 {
				continue
			}
			path, kind = f[1], f[2]
			path = strings.NewReplacer(`\040`, " ", `\011`, "\t", `\134`, `\`).Replace(path)
		} else {
			_, rest, ok := strings.Cut(line, " on ")
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
			roots = append(roots, filepath.Clean(path))
		}
	}
	slices.Sort(roots)
	return slices.Compact(roots), nil
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
// or real install plan. Explicit/saved paths are selections, not proof of I/O;
// only the documented default may be selected without --nas on a fresh plan.
func ResolveOnboardingStorage(ctx context.Context, c ServiceConfig, discover bool, command func(context.Context, string, ...string) ([]byte, error)) (ServiceConfig, error) {
	if !discover && c.NAS != "" {
		return c, nil
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
	lstat func(string) (os.FileInfo, error)
	mkdir func(string, os.FileMode) error
	probe func(string) error
}

var onboardingNASIO = checkpointIO[bool]{timeout: networkFilesystemBudget}

// PrepareOnboardingStorage never adopts discovered candidates. One checkpointIO
// guards the complete filesystem sequence, including cleanup. A blocked syscall
// may outlive the caller, but no second sequence is launched while it is pending.
func PrepareOnboardingStorage(ctx context.Context, c ServiceConfig, discover bool, command func(context.Context, string, ...string) ([]byte, error)) (ServiceConfig, error) {
	return prepareOnboardingStorageFS(ctx, c, discover, command, onboardingStorageFS{os.Lstat, os.Mkdir, probeNASDirectory})
}
func prepareOnboardingStorageFS(ctx context.Context, c ServiceConfig, discover bool, command func(context.Context, string, ...string) ([]byte, error), fs onboardingStorageFS) (ServiceConfig, error) {
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
	ioCtx, cancel := context.WithTimeout(ctx, networkFilesystemBudget)
	defer cancel()
	_, err = onboardingNASIO.run(c.NAS+"\x00"+c.Checkpoints+"\x00"+c.ModelRoot+"\x00"+c.EnvironmentRoot, func() (bool, error) {
		return false, validateOnboardingStorageFS(ioCtx, c, mount, fs)
	})
	if errors.Is(err, errCheckpointIOTimeout) || errors.Is(err, errCheckpointIOPending) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return c, preflightError("nas_unresponsive", "storage", "NAS validation exceeded the 250 ms network-filesystem budget or was cancelled; restore the mount and retry")
	}
	if err != nil {
		return c, err
	}
	c.RequireMountedNAS = true
	return c, nil
}
func validateOnboardingStorageFS(ctx context.Context, c ServiceConfig, mount string, fs onboardingStorageFS) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Check the mount and every ancestor before mkdir. In particular, a missing
	// mountpoint is never created on the local disk.
	if st, e := fs.lstat(mount); e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
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
			st, e := fs.lstat(at)
			if errors.Is(e, os.ErrNotExist) {
				if err := ctx.Err(); err != nil {
					return err
				}
				if e = fs.mkdir(at, 0700); e != nil {
					return preflightError("nas_unreachable", "storage", "NAS roots cannot be created; check reachability and write permission")
				}
			} else if e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
				return preflightError("nas_root_invalid", "storage", "NAS roots require accessible real directories, without symlinks")
			}
		}
	}
	for _, path := range []string{c.Checkpoints, c.ModelRoot, c.EnvironmentRoot} {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := fs.probe(path); err != nil {
			return preflightError("nas_unreachable", "storage", "NAS write/sync/rename probe failed; restore mount reachability and permissions")
		}
	}
	return ctx.Err()
}

func probeNASDirectory(dir string) error {
	f, err := os.CreateTemp(dir, ".myce-install-probe-")
	if err != nil {
		return err
	}
	path := f.Name()
	renamed := path + ".renamed"
	defer os.Remove(path)
	defer os.Remove(renamed)
	_, writeErr := f.WriteString("myce NAS write and rename probe\n")
	if err = errors.Join(writeErr, f.Sync(), f.Close()); err != nil {
		return err
	}
	if err = os.Rename(path, renamed); err != nil {
		return err
	}
	return os.Remove(renamed)
}
