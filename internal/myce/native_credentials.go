//go:build linux || darwin

package myce

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
)

// #1309: plain and declared-unsandboxed native jobs keep the worker user's
// ordinary file access (MY-25), except for the fleet credential, installed
// authority and SSH keys. deny lists those paths (existing, resolved, none
// inside another). allow lists the descendants of a denied path that jobs
// still use: this worker's state (their own attempt files), caches, tools and
// executables. Every co-installed role (a queue beside this worker) is denied.
func (w Worker) nativeCredentialPaths(spool string, attempt []string) (deny, allow []string, err error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, nil, err
	}
	roots := []string{filepath.Join(home, ".local", "share", "mycelium")}
	files := []string{filepath.Join(home, ".ssh"), w.SecretRoot}
	addConfig := func(c ServiceConfig, path string) {
		files = append(files, path, c.Credentials, c.EnvironmentFile, c.JoiningFile)
	}
	if c := w.ServiceConfig; c != nil {
		addConfig(*c, c.Config)
		if c.Config != "" {
			roots = append(roots, filepath.Dir(filepath.Dir(c.Config)))
		}
	}
	for _, root := range slices.Clone(roots) {
		configs, _ := filepath.Glob(filepath.Join(root, "*", "config.json"))
		for _, path := range configs {
			var c ServiceConfig
			if data, err := os.ReadFile(path); err == nil && json.Unmarshal(data, &c) == nil {
				addConfig(c, path)
			}
		}
	}
	for _, path := range append(roots, files...) {
		if path == "" {
			continue
		}
		resolved, err := nativeResolvedPath(path)
		if err != nil {
			return nil, nil, err
		}
		if _, err := os.Lstat(resolved); err == nil && !slices.Contains(deny, resolved) {
			deny = append(deny, resolved)
		}
	}
	// A path inside another denied path adds nothing, and on Linux its
	// ancestors must never include a denied directory.
	deny = slices.DeleteFunc(deny, func(path string) bool {
		return slices.ContainsFunc(deny, func(other string) bool { return other != path && nativePathContains(other, path) })
	})
	slices.Sort(deny)

	candidates := append([]string{spool, w.StateDir, w.CacheRoot, w.ModelRoot, w.EnvironmentRoot, w.ExtensionRoot, w.CheckpointRoot, w.JobFileRoot, w.FileCacheRoot}, attempt...)
	if c := w.ServiceConfig; c != nil {
		candidates = append(candidates, c.State, c.ModelCache, c.EnvironmentCache)
	}
	if w.environmentCache != nil {
		candidates = append(candidates, w.environmentCache.Root)
	}
	candidates = append(candidates, w.NativeReadRoots...)
	candidates = append(candidates, w.NativeExecuteRoots...)
	candidates = append(candidates, w.nativeSharedInputs...)
	candidates = append(candidates, w.nativeModelInputs...)
	if executable, err := os.Executable(); err == nil {
		candidates = append(candidates, executable)
	}
	for _, root := range roots {
		candidates = append(candidates, filepath.Join(root, "tools"))
		// Installed binaries (myce and its retained versions) are not secrets.
		entries, _ := filepath.Glob(filepath.Join(root, "*", "*"))
		for _, path := range entries {
			if info, err := os.Lstat(path); err == nil && info.Mode().IsRegular() && info.Mode()&0111 != 0 {
				candidates = append(candidates, path)
			}
		}
	}
	for _, path := range candidates {
		if path == "" {
			continue
		}
		resolved, err := nativeResolvedPath(path)
		if err != nil {
			return nil, nil, err
		}
		inside := slices.ContainsFunc(deny, func(d string) bool { return nativePathContains(d, resolved) })
		// A grant that encloses a denied path would re-expose it.
		encloses := slices.ContainsFunc(deny, func(d string) bool { return nativePathContains(resolved, d) })
		if _, err := os.Lstat(resolved); err == nil && inside && !encloses && !slices.Contains(allow, resolved) {
			allow = append(allow, resolved)
		}
	}
	slices.Sort(allow)
	return deny, allow, nil
}
