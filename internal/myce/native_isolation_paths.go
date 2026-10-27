//go:build linux || darwin

package myce

import (
	"errors"
	"os"
	"path/filepath"
)

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
		{w.CheckpointRoot, false},
		{w.SecretRoot, false},
		{filepath.Join(home, ".config"), false},
		{filepath.Join(home, ".ssh"), false},
		{filepath.Join(home, "Library/Keychains"), false},
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
