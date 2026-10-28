//go:build linux || darwin

package myce

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Declarations are operator configuration, not authored job paths. Resolve
// symlinks before validating or installing a rule, and require an existing
// regular file or tree rather than a wildcard or a broad storage root.
func (w Worker) nativeDeclaredRoots(spool string) (read, execute []string, err error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, nil, err
	}
	forbidden := []string{string(filepath.Separator), home, w.JobFileRoot, filepath.Join(home, ".local/share/mycelium")}
	if c := w.ServiceConfig; c != nil {
		forbidden = append(forbidden, c.NAS)
		if c.Config != "" {
			forbidden = append(forbidden, filepath.Dir(filepath.Dir(c.Config)))
		}
	}
	resolve := func(paths []string) ([]string, error) {
		var roots []string
		for _, path := range paths {
			if !filepath.IsAbs(path) || strings.ContainsAny(path, "\x00\r\n*?[]~") {
				return nil, errors.New("native_isolation_grant_invalid: declare an absolute literal file or directory")
			}
			root, err := filepath.EvalSymlinks(path)
			if err != nil {
				return nil, fmt.Errorf("native_isolation_grant_invalid: %w", err)
			}
			info, err := os.Stat(root)
			if err != nil || !info.IsDir() && !info.Mode().IsRegular() {
				return nil, errors.New("native_isolation_grant_invalid: grant must name a regular file or directory")
			}
			for _, path := range forbidden {
				if path == "" {
					continue
				}
				protected, err := nativeResolvedPath(path)
				if err != nil {
					return nil, err
				}
				if nativePathContains(root, protected) {
					return nil, errors.New("native_isolation_grant_too_broad: filesystem, home, NAS and installed Mycelium roots cannot be declared")
				}
			}
			roots = append(roots, root)
		}
		if err := w.validateNativeReadGrants(roots, spool); err != nil {
			return nil, err
		}
		return roots, nil
	}
	read, err = resolve(w.NativeReadRoots)
	if err == nil {
		execute, err = resolve(w.NativeExecuteRoots)
	}
	return read, execute, err
}

func nativePathContains(root, child string) bool {
	rel, err := filepath.Rel(root, child)
	return err == nil && (rel == "." || filepath.IsLocal(rel))
}

// Landlock grants are additive: a read-only declaration must not inherit
// execute or write permission from another runtime or attempt grant.
func nativeReadOnlyOverlap(read, other []string) error {
	for _, root := range read {
		if root == "" {
			continue
		}
		root, err := nativeResolvedPath(root)
		if err != nil {
			return err
		}
		for _, path := range other {
			if path == "" {
				continue
			}
			path, err := nativeResolvedPath(path)
			if err != nil {
				return err
			}
			if nativePathContains(root, path) || nativePathContains(path, root) {
				return errors.New("native_isolation_grant_overlap: read-only root overlaps an executable or writable root")
			}
		}
	}
	return nil
}

// Shared runtime read grants must not exempt protected descendants. Reject an
// operator layout that places worker authority or sibling storage below a
// runtime root, including roots supplied through loader configuration.
func (w Worker) validateNativeReadGrants(read []string, spool string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	// Storage ancestors contain legitimate cache/model subtrees. Private
	// content trees permit neither an enclosing grant nor a grant inside them.
	type protectedPath struct {
		path          string
		allowChildren bool
	}
	protected := []protectedPath{
		{spool, true},
		{w.JobFileRoot, true},
		{filepath.Join(home, ".local/share/mycelium"), true},
		{filepath.Join(spool, "job-files"), false},
		{filepath.Join(spool, "checkpoints"), false},
		{filepath.Join(spool, "file-checkpoints"), false},
		{w.CheckpointRoot, false},
		{w.SecretRoot, false},
		{filepath.Join(home, ".config"), false},
		{filepath.Join(home, ".ssh"), false},
		{filepath.Join(home, "Library/Keychains"), false},
	}
	if runtime.GOOS == "linux" {
		protected = append(protected, protectedPath{"/sys/fs/cgroup", false})
	}
	if c := w.ServiceConfig; c != nil {
		protected = append(protected, protectedPath{c.State, true})
		for _, path := range []string{c.Config, c.Credentials, c.EnvironmentFile, c.JoiningFile} {
			protected = append(protected, protectedPath{path, false})
		}
	}
	contains := func(root, child string) bool {
		rel, err := filepath.Rel(root, child)
		return err == nil && (rel == "." || filepath.IsLocal(rel))
	}
	for _, grant := range read {
		if grant == "" {
			continue
		}
		grant, err := nativeResolvedPath(grant)
		if err != nil {
			return err
		}
		for _, private := range protected {
			if private.path == "" {
				continue
			}
			path, err := nativeResolvedPath(private.path)
			if err != nil {
				return err
			}
			if contains(grant, path) || !private.allowChildren && contains(path, grant) {
				return errors.New("native_isolation_runtime_overlap: runtime read root overlaps protected worker storage; install state and credentials outside runtime roots")
			}
		}
	}
	return nil
}

// Resolve existing ancestors too, so an absent credential leaf cannot evade
// layout validation through an installation symlink.
func nativeResolvedPath(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	ancestor, suffix := path, ""
	for {
		if resolved, err := filepath.EvalSymlinks(ancestor); err == nil {
			return filepath.Join(resolved, suffix), nil
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return path, nil
		}
		suffix = filepath.Join(filepath.Base(ancestor), suffix)
		ancestor = parent
	}
}
