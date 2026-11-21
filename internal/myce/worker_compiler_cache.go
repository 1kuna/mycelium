package myce

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Go's content-addressed build cache is rebuildable. The worker owns this
// location; no cleanup ever targets HOME/.cache or an authored GOCACHE.
// Shared use locks plus durable exact-attempt pins survive a worker restart.
type workerCompilerCache struct {
	cache    EnvironmentCache
	external atomic.Bool
}

func (c *workerCompilerCache) externalUsage(ctx context.Context) (int64, error) {
	if c == nil {
		return 0, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return 0, err
	}
	root := filepath.Join(home, ".cache", "go-build")
	if runtime.GOOS == "darwin" {
		root = filepath.Join(home, "Library", "Caches", "go-build")
	}
	return collectionAllocatedBytes(ctx, root)
}

func (c *workerCompilerCache) acquire(ctx context.Context, s *workerStore, j Job) (string, func(), error) {
	if c == nil {
		return "", func() {}, nil
	}
	if !nativeUnsandboxed(&j) || j.Spec.Run.Env["GOCACHE"] != "" {
		c.external.Store(true)
		return "", func() {}, nil
	}
	if err := c.cache.initStorage(); err != nil {
		return "", nil, err
	}
	key := identityHash("go-build")
	lock, err := c.cache.lock(ctx, key+".use", syscall.LOCK_SH)
	if err != nil {
		return "", nil, err
	}
	unpin, err := s.pinSharedInput(c.cache.Root, key)
	if err != nil {
		unlockEnvironment(lock)
		return "", nil, err
	}
	path := filepath.Join(c.cache.Root, key)
	var once sync.Once
	release := func() {
		once.Do(func() { at := c.cache.Clock.Now(); _ = os.Chtimes(path, at, at); unpin(); unlockEnvironment(lock) })
	}
	if err = os.MkdirAll(path, 0700); err != nil {
		release()
		return "", nil, err
	}
	return path, release, nil
}

func (c *workerCompilerCache) budgetCandidates(ctx context.Context) ([]diskBudgetCandidate, error) {
	key := identityHash("go-build")
	path := filepath.Join(c.cache.Root, key)
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, nil
	}
	return []diskBudgetCandidate{{path: path, used: info.ModTime(), trim: func(ctx context.Context, enough func() (bool, error), limit uint64) error {
		lock, err := c.cache.tryLock(key + ".use")
		if err != nil || lock == nil {
			return err
		}
		defer unlockEnvironment(lock)
		pinned, err := sharedInputPinned(c.cache.Root, key)
		if err != nil || pinned {
			return err
		}
		current, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !os.SameFile(info, current) || !info.ModTime().Equal(current.ModTime()) {
			return nil
		}
		return trimCompilerObjects(ctx, path, current, enough, limit)
	}}}, nil
}

// Go stores independent action and data files in 256 hex shards. Trim only
// known regular objects. Gate tooling can keep auxiliary state beside them.
func trimCompilerObjects(ctx context.Context, path string, info fs.FileInfo, enough func() (bool, error), limit uint64) error {
	root, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer root.Close()
	current, err := root.Stat(".")
	if err != nil || !os.SameFile(info, current) {
		return err
	}
	device := current.Sys().(*syscall.Stat_t).Dev
	type object struct {
		path string
		used time.Time
		info fs.FileInfo
	}
	var objects []object
	for shard := range 256 {
		name := fmt.Sprintf("%02x", shard)
		dir, err := root.Lstat(name)
		if err != nil || !dir.IsDir() || dir.Sys().(*syscall.Stat_t).Dev != device {
			continue
		}
		entries, err := fs.ReadDir(root.FS(), name)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			file := entry.Name()
			if len(file) != 66 || file[:2] != name || file[64:] != "-a" && file[64:] != "-d" ||
				(FileRef{SHA256: file[:64]}).validate() != nil || !entry.Type().IsRegular() {
				continue
			}
			stat, err := entry.Info()
			if err == nil && stat.Mode().IsRegular() && stat.Sys().(*syscall.Stat_t).Dev == device {
				objects = append(objects, object{filepath.Join(name, file), stat.ModTime(), stat})
			}
		}
	}
	sort.SliceStable(objects, func(i, j int) bool { return objects[i].used.Before(objects[j].used) })
	var removed uint64
	for _, entry := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if enough, err := enough(); enough || err != nil {
			return err
		}
		if limit > 0 {
			info, err := root.Lstat(entry.path)
			if err != nil || !os.SameFile(entry.info, info) || !info.ModTime().Equal(entry.used) {
				continue
			}
			stat := info.Sys().(*syscall.Stat_t)
			if stat.Nlink != 1 || stat.Blocks <= 0 {
				continue
			}
			bytes := uint64(stat.Blocks) * 512
			if bytes > limit-removed {
				continue
			}
			if err := root.Remove(entry.path); err == nil {
				removed += bytes
				if removed >= limit {
					return nil
				}
			}
		} else {
			_ = root.Remove(entry.path)
		}
	}
	return nil
}
