package myce

import (
	"context"
	"errors"
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
		if slices.Contains([]string{"nfs", "nfs4", "cifs", "smbfs", "smb3"}, kind) && filepath.IsAbs(path) && path != "/" {
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

// PrepareOnboardingStorage is the mandatory NAS-mode seam before tool bootstrap
// or service publication. discover is true only for a fresh default NAS plan;
// an upgrade or explicit path must never select a different filesystem.
func PrepareOnboardingStorage(ctx context.Context, c ServiceConfig, discover bool, command func(context.Context, string, ...string) ([]byte, error)) (ServiceConfig, error) {
	roots, err := MountedNASRoots(ctx, c.Detection.OS, command)
	if err != nil {
		return c, err
	}
	mount := ""
	for _, root := range roots {
		if withinNAS(root, c.NAS) && (mount == "" || len(root) > len(mount)) {
			mount = root
		}
	}
	if mount == "" && discover && len(roots) == 1 {
		old := c.NAS
		c.NAS = roots[0]
		mount = roots[0]
		for _, pair := range []struct {
			path *string
			name string
		}{{&c.Checkpoints, "checkpoints"}, {&c.ModelRoot, "models"}, {&c.EnvironmentRoot, "environments"}} {
			if *pair.path == filepath.Join(old, "mycelium", pair.name) {
				*pair.path = filepath.Join(c.NAS, "mycelium", pair.name)
			}
		}
	}
	if mount == "" {
		code := "nas_not_mounted"
		if discover && len(roots) > 1 {
			code = "nas_mount_ambiguous"
		}
		return c, preflightError(code, "storage", "shared NAS is not unambiguously mounted; mount it through the OS and supply --nas; no local substitute was created")
	}
	// Check the mount and every ancestor before mkdir. In particular, a missing
	// mountpoint is never created on the local disk.
	if st, e := os.Lstat(mount); e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return c, preflightError("nas_unreachable", "storage", "mounted NAS is unavailable; restore the OS mount")
	}
	for _, path := range []string{c.NAS, c.Checkpoints, c.ModelRoot, c.EnvironmentRoot} {
		if !filepath.IsAbs(path) || !withinNAS(mount, path) {
			return c, preflightError("nas_root_outside_mount", "storage", "checkpoint, model and environment roots must stay on the mounted NAS")
		}
		rel, _ := filepath.Rel(mount, path)
		at := mount
		for _, part := range strings.Split(rel, string(filepath.Separator)) {
			if part == "." {
				continue
			}
			at = filepath.Join(at, part)
			st, e := os.Lstat(at)
			if errors.Is(e, os.ErrNotExist) {
				if e = os.Mkdir(at, 0700); e != nil {
					return c, preflightError("nas_unreachable", "storage", "NAS roots cannot be created; check reachability and write permission")
				}
			} else if e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
				return c, preflightError("nas_root_invalid", "storage", "NAS roots require accessible real directories, without symlinks")
			}
		}
	}
	for _, path := range []string{c.Checkpoints, c.ModelRoot, c.EnvironmentRoot} {
		if ctx.Err() != nil {
			return c, ctx.Err()
		}
		if err := probeNASDirectory(path); err != nil {
			return c, preflightError("nas_unreachable", "storage", "NAS write/sync/rename probe failed; restore mount reachability and permissions")
		}
	}
	c.RequireMountedNAS = true
	return c, nil
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
