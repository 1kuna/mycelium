package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func (c *FileCache) budgetCandidates(ctx context.Context) ([]diskBudgetCandidate, error) {
	entries, err := os.ReadDir(c.Root)
	if err != nil {
		return nil, err
	}
	var out []diskBudgetCandidate
	for _, entry := range entries {
		hash := strings.TrimSuffix(entry.Name(), ".partial")
		if (FileRef{SHA256: hash}).validate() != nil {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		path := filepath.Join(c.Root, entry.Name())
		out = append(out, diskBudgetCandidate{path: path, used: info.ModTime(), evict: func(ctx context.Context) (bool, error) {
			if !c.mu.TryLock() {
				return false, nil
			}
			defer c.mu.Unlock()
			cache, key, err := c.collectionLockStorage()
			if err != nil {
				return false, err
			}
			lock, err := cache.tryLock(key)
			if err != nil || lock == nil {
				return false, err
			}
			defer unlockEnvironment(lock)
			current, err := os.Lstat(path)
			if err != nil {
				return false, err
			}
			if !os.SameFile(info, current) || !current.ModTime().Equal(info.ModTime()) {
				return false, nil
			}
			// HEAD validates size and identity at the authoritative file store.
			ref := FileRef{SHA256: hash, SizeBytes: current.Size()}
			if strings.HasSuffix(path, ".partial") {
				// A partial's size is not the authoritative blob size; retain it
				// unless a complete sibling supplies that identity.
				return false, nil
			}
			if err = c.Client.HasFile(ctx, ref); err != nil {
				return false, err
			}
			return true, os.Remove(path)
		}})
	}
	return out, nil
}

// Local-only provenance accompanies the existing integrity record. This is
// never sent in worker status; a missing old record preserves the last copy.
type sharedRecoverySource struct {
	Input   SharedInput       `json:"input"`
	Root    string            `json:"root,omitempty"`
	Sources map[string]string `json:"sources,omitempty"`
	Model   *ModelSnapshot    `json:"model,omitempty"`
	// Fetched from the model hub at an exact revision: re-fetchable on demand.
	Hub bool `json:"hub,omitempty"`
}

func (c *SharedInputCache) recoverable(ctx context.Context, source *sharedRecoverySource, victims ...string) error {
	if source == nil {
		return errors.New("cache source unknown")
	}
	if source.Hub && source.Model != nil {
		return nil
	}
	s := source.Input
	if s.ManifestFile != "" && len(s.Manifest) == 0 {
		path, err := c.nasPath(source.Root, s.ManifestFile)
		if err != nil {
			return err
		}
		if err = verifyFile(ctx, path, s.ref()); err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if digestBytes(data) != s.SHA256 {
			return errors.New("manifest source changed")
		}
		if err = json.Unmarshal(data, &s.Manifest); err != nil {
			return err
		}
		if len(s.Manifest) == 0 {
			return errors.New("empty manifest source")
		}
	}
	if len(s.Manifest) > 0 {
		for name, ref := range s.Manifest {
			path := source.Sources[name]
			if path == "" {
				var err error
				path, err = c.nasPath(source.Root, filepath.ToSlash(filepath.Join(s.NAS, name)))
				if err != nil {
					return err
				}
			}
			if !independentCacheSource(path, victims...) {
				return errors.New("source is the cache copy")
			}
			if err := verifyFile(ctx, path, ref); err != nil {
				return err
			}
		}
		return nil
	}
	if s.NAS != "" {
		path, err := c.nasPath(source.Root, s.NAS)
		if err != nil {
			return err
		}
		if !independentCacheSource(path, victims...) {
			return errors.New("source is the cache copy")
		}
		return verifyFile(ctx, path, s.ref())
	}
	return c.Client.HasFile(ctx, s.ref())
}

func (c *SharedInputCache) budgetCandidates(ctx context.Context) ([]diskBudgetCandidate, error) {
	entries, err := os.ReadDir(c.cache.Root)
	if err != nil {
		return nil, err
	}
	var out []diskBudgetCandidate
	for _, entry := range entries {
		key := entry.Name()
		if entry.IsDir() && strings.HasSuffix(key, ".partial") {
			stageKey := strings.TrimSuffix(key, ".partial")
			if (FileRef{SHA256: stageKey}).validate() != nil {
				continue
			}
			path := filepath.Join(c.cache.Root, key)
			info, err := os.Stat(filepath.Join(path, "resume"))
			if err != nil {
				continue
			}
			out = append(out, diskBudgetCandidate{path: path, used: info.ModTime(), evict: func(ctx context.Context) (bool, error) {
				build, err := c.cache.tryLock(stageKey + ".build")
				if err != nil || build == nil {
					return false, err
				}
				defer unlockEnvironment(build)
				data, err := os.ReadFile(filepath.Join(path, "resume"))
				if err != nil {
					return false, err
				}
				key := string(data)
				if (FileRef{SHA256: key}).validate() != nil {
					return false, errors.New("unknown partial owner")
				}
				use, err := c.cache.tryLock(key + ".use")
				if err != nil || use == nil {
					return false, err
				}
				defer unlockEnvironment(use)
				pinned, err := sharedInputPinned(c.cache.Root, key)
				if err != nil || pinned {
					return false, err
				}
				data, err = os.ReadFile(filepath.Join(path, "recovery.json"))
				if err != nil {
					return false, err
				}
				var recovery sharedRecoverySource
				if err = json.Unmarshal(data, &recovery); err != nil {
					return false, err
				}
				if err = c.recoverable(ctx, &recovery, path); err != nil {
					return false, err
				}
				if c.budget != nil {
					referenced, err := c.queueReferenced(ctx, key, &recovery)
					if err != nil || referenced {
						return false, err
					}
				}
				gate, err := c.cache.tryLock("admission")
				if err != nil || gate == nil {
					return false, err
				}
				defer unlockEnvironment(gate)
				return true, removeSharedEntry(path)
			}})
			continue
		}
		if !entry.IsDir() || (FileRef{SHA256: key}).validate() != nil {
			continue
		}
		path := filepath.Join(c.cache.Root, key)
		info, err := os.Stat(filepath.Join(path, "ready.json"))
		if err != nil {
			continue
		}
		out = append(out, diskBudgetCandidate{path: path, used: info.ModTime(), evict: func(ctx context.Context) (bool, error) {
			use, err := c.cache.tryLock(key + ".use")
			if err != nil || use == nil {
				return false, err
			}
			defer unlockEnvironment(use)
			build, err := c.cache.tryLock(key + ".build")
			if err != nil || build == nil {
				return false, err
			}
			defer unlockEnvironment(build)
			pinned, err := sharedInputPinned(c.cache.Root, key)
			if err != nil || pinned {
				return false, err
			}
			current, err := os.Stat(filepath.Join(path, "ready.json"))
			if err != nil {
				return false, err
			}
			if !os.SameFile(info, current) || !info.ModTime().Equal(current.ModTime()) {
				return false, nil
			}
			data, err := os.ReadFile(filepath.Join(path, "ready.json"))
			if err != nil {
				return false, err
			}
			var record sharedEntry
			if err = json.Unmarshal(data, &record); err != nil {
				return false, err
			}
			if record.Recovery == nil {
				data, err = os.ReadFile(filepath.Join(path, "recovery.json"))
				if err != nil {
					return false, err
				}
				if err = json.Unmarshal(data, &record.Recovery); err != nil {
					return false, err
				}
			}
			if err = c.recoverable(ctx, record.Recovery, path); err != nil {
				return false, err
			}
			if c.budget != nil {
				referenced, err := c.queueReferenced(ctx, key, record.Recovery)
				if err != nil || referenced {
					return false, err
				}
			}
			gate, err := c.cache.tryLock("admission")
			if err != nil || gate == nil {
				return false, err
			}
			defer unlockEnvironment(gate)
			if err = removeSharedEntry(path); err != nil {
				return false, err
			}
			c.mu.Lock()
			delete(c.verified, key)
			c.mu.Unlock()
			return true, nil
		}})
	}
	return out, nil
}

func (c *EnvironmentCache) budgetCandidates(ctx context.Context) ([]diskBudgetCandidate, error) {
	entries, err := c.evictionCandidates("")
	if err != nil {
		return nil, err
	}
	var out []diskBudgetCandidate
	for _, entry := range entries {
		key := entry.Key
		out = append(out, diskBudgetCandidate{path: filepath.Join(c.Root, key), used: c.Clock.Now().Add(-entry.Age), evict: func(ctx context.Context) (bool, error) {
			if c.references == nil {
				return false, errors.New("environment references unavailable")
			}
			var gate *os.File
			defer func() { unlockEnvironment(gate) }()
			removed, err := c.evict(key, "filesystem_pressure", func() (bool, error) {
				referenced, err := c.references(ctx, key)
				if err != nil || referenced {
					return false, err
				}
				gate, err = c.tryLock("admission")
				return gate != nil, err
			})
			if removed && err == nil {
				_, err = c.purgeRetiredKey(key)
			}
			return removed, err
		}})
	}
	// Use package managers' supported pruning, under their existing exclusive
	// download lock. Never unlink shared wheels behind active builders.
	out = append(out, diskBudgetCandidate{path: filepath.Join(c.Root, ".downloads"), evict: func(ctx context.Context) (bool, error) {
		lock, err := c.tryLock("downloads")
		if err != nil || lock == nil {
			return false, err
		}
		defer unlockEnvironment(lock)
		run := c.Run
		if run == nil {
			run = c.runBuild
		}
		env := append(environmentList(buildEnvironmentFrom(os.Environ())), "UV_CACHE_DIR="+c.downloadCache("uv"), "PIXI_CACHE_DIR="+c.downloadCache("pixi"))
		for _, tool := range []string{"uv", "pixi"} {
			if _, err := os.Stat(c.downloadCache(tool)); os.IsNotExist(err) {
				continue
			}
			args := []string{"uv", "cache", "prune", "--ci"}
			if tool == "pixi" {
				args = []string{"pixi", "clean", "cache", "--yes"}
			}
			if err := run(ctx, args, c.Root, env, io.Discard, io.Discard); err != nil {
				return false, err
			}
		}
		return true, nil
	}})
	return out, nil
}

func (c *ModelCache) budgetCandidates(ctx context.Context) ([]diskBudgetCandidate, error) {
	entries, err := os.ReadDir(c.Root)
	if err != nil {
		return nil, err
	}
	var out []diskBudgetCandidate
	for _, entry := range entries {
		key := strings.TrimSuffix(entry.Name(), ".partial")
		m, ok := c.knownModels.Load(key)
		if !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(c.Root, entry.Name())
		out = append(out, diskBudgetCandidate{path: path, used: info.ModTime(), evict: func(ctx context.Context) (bool, error) {
			if !c.mu.TryLock() {
				return false, nil
			}
			defer c.mu.Unlock()
			if c.active[key] != nil || c.Protected != nil && c.Protected(key) {
				return false, nil
			}
			model := m.(Model)
			if model.FileStore {
				if c.FileClient == nil {
					return false, errors.New("model source unavailable")
				}
				if len(model.Files) == 0 {
					err = c.FileClient.HasFile(ctx, FileRef{model.SHA256, model.SizeBytes})
				} else {
					for _, file := range model.Files {
						if err = c.FileClient.HasFile(ctx, FileRef{file.SHA256, file.SizeBytes}); err != nil {
							break
						}
					}
				}
			} else {
				source, e := c.source(model)
				err = e
				if err == nil && !independentCacheSource(source, path) {
					return false, errors.New("source is the cache copy")
				}
				if err == nil {
					err = verifyModelContext(ctx, source, model)
				}
			}
			if err != nil {
				return false, err
			}
			current, err := os.Lstat(path)
			if err != nil {
				return false, err
			}
			if !os.SameFile(info, current) || !current.ModTime().Equal(info.ModTime()) {
				return false, nil
			}
			if err = os.RemoveAll(path); err != nil {
				return false, err
			}
			c.verified.Delete(key)
			c.progress.Delete(key)
			return true, nil
		}})
	}
	return out, nil
}

func independentCacheSource(source string, victims ...string) bool {
	resolved, err := filepath.EvalSymlinks(source)
	if err != nil {
		return false
	}
	for _, victim := range victims {
		root, err := filepath.EvalSymlinks(victim)
		if err != nil || withinNAS(root, resolved) {
			return false
		}
	}
	return true
}
