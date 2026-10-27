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
	protected := []string{spool, filepath.Join(spool, "job-files"), filepath.Join(spool, "checkpoints"), w.CheckpointRoot, w.JobFileRoot, w.SecretRoot,
		filepath.Join(home, ".local/share/mycelium"), filepath.Join(home, ".config"), filepath.Join(home, ".ssh")}
	if c := w.ServiceConfig; c != nil {
		protected = append(protected, c.Config, c.Credentials, c.EnvironmentFile, c.JoiningFile, c.State)
	}
	for _, grant := range read {
		if grant == "" {
			continue
		}
		grant, err := nativeResolvedPath(grant)
		if err != nil {
			return err
		}
		for _, path := range protected {
			if path == "" {
				continue
			}
			path, err := nativeResolvedPath(path)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(grant, path)
			if err == nil && (rel == "." || filepath.IsLocal(rel)) {
				return errors.New("native_isolation_runtime_overlap: runtime read root contains protected worker storage; install state and credentials outside runtime roots")
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
