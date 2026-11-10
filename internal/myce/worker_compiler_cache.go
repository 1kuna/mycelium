package myce

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
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
	return []diskBudgetCandidate{{path: path, used: info.ModTime(), evict: func(ctx context.Context) (bool, error) {
		lock, err := c.cache.tryLock(key + ".use")
		if err != nil || lock == nil {
			return false, err
		}
		defer unlockEnvironment(lock)
		pinned, err := sharedInputPinned(c.cache.Root, key)
		if err != nil || pinned {
			return false, err
		}
		current, err := os.Lstat(path)
		if err != nil {
			return false, err
		}
		if !os.SameFile(info, current) || !info.ModTime().Equal(current.ModTime()) {
			return false, nil
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return true, os.RemoveAll(path)
	}}}, nil
}
