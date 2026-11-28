package myce

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
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

func (c *workerCompilerCache) budgetCandidates(ctx context.Context) (diskBudgetCandidateSet, error) {
	key := identityHash("go-build")
	path := filepath.Join(c.cache.Root, key)
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return diskBudgetCandidateSet{}, nil
	}
	if err != nil || !info.IsDir() {
		return diskBudgetCandidateSet{}, err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return diskBudgetCandidateSet{}, err
	}
	close := func() { _ = root.Close() }
	parent, err := os.Stat(c.cache.Root)
	if err != nil {
		close()
		return diskBudgetCandidateSet{}, err
	}
	current, err := root.Stat(".")
	if err != nil || !os.SameFile(info, current) || parent.Sys().(*syscall.Stat_t).Dev != current.Sys().(*syscall.Stat_t).Dev {
		close()
		return diskBudgetCandidateSet{}, err
	}

	// This cutoff only filters the inventory. Each unlink takes a fresh lock
	// and checks pins again, including attempts that start during the pass.
	cutoff, err := c.trimCutoff(key)
	if err != nil {
		close()
		return diskBudgetCandidateSet{}, err
	}
	candidates, err := c.compilerObjects(ctx, root, current, key, cutoff)
	return diskBudgetCandidateSet{entries: candidates, close: close}, err
}

func (c *workerCompilerCache) trimCutoff(key string) (time.Time, error) {
	lock, err := c.cache.tryLock(key + ".use")
	if err != nil {
		return time.Time{}, err
	}
	defer unlockEnvironment(lock)
	return compilerTrimCutoff(c.cache.Root, key, c.cache.Clock.Now(), lock == nil)
}

// compilerCacheGrace keeps what the latest builds just wrote, as the gate's own
// cache trim does. goCacheUseRefresh is Go's mtimeInterval: a cache lookup
// refreshes an entry's mtime only when it is older than an hour.
const (
	compilerCacheGrace = 10 * time.Minute
	goCacheUseRefresh  = time.Hour
)

// Objects modified before the returned time may go. An idle cache (exclusive
// use lock held, no pins) keeps only the grace period. While attempts hold the
// cache, every object one of them looked up has an mtime no older than the
// oldest pin's creation minus Go's refresh interval, so older objects are ones
// no current attempt has used. Go's own cache trim relies on the same mtime
// refresh while other go commands run. A long attempt therefore protects only
// what it could have touched, not the whole cache. A holder that has locked but
// not yet pinned started now. Unreadable pins protect everything.
func compilerTrimCutoff(root, key string, now time.Time, locked bool) (time.Time, error) {
	since, inUse := now, locked
	entries, err := os.ReadDir(filepath.Join(root, "pins", key))
	if err != nil && !os.IsNotExist(err) {
		return time.Time{}, err
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return time.Time{}, err
		}
		inUse = true
		if info.ModTime().Before(since) {
			since = info.ModTime()
		}
	}
	if inUse {
		since = since.Add(-goCacheUseRefresh)
	}
	return since.Add(-compilerCacheGrace), nil
}

// Go stores independent action and data files in 256 hex shards. Trim only
// known regular objects. Gate tooling can keep auxiliary state beside them.
func (c *workerCompilerCache) compilerObjects(ctx context.Context, root *os.Root, info fs.FileInfo, key string, cutoff time.Time) ([]diskBudgetCandidate, error) {
	device := info.Sys().(*syscall.Stat_t).Dev
	var objects []diskBudgetCandidate
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
				return nil, err
			}
			file := entry.Name()
			if len(file) != 66 || file[:2] != name || file[64:] != "-a" && file[64:] != "-d" ||
				(FileRef{SHA256: file[:64]}).validate() != nil || !entry.Type().IsRegular() {
				continue
			}
			info, err := entry.Info()
			if err != nil || !info.Mode().IsRegular() || !info.ModTime().Before(cutoff) {
				continue
			}
			stat := info.Sys().(*syscall.Stat_t)
			if stat.Dev != device || stat.Nlink != 1 || stat.Blocks <= 0 {
				continue
			}
			rel := filepath.Join(name, file)
			objects = append(objects, diskBudgetCandidate{
				path: filepath.Join(root.Name(), rel), used: info.ModTime(), bytes: uint64(stat.Blocks) * 512,
				evict: func(context.Context) (bool, error) {
					return c.evictCompilerObject(root, key, rel, info)
				},
			})
		}
	}
	return objects, nil
}

func (c *workerCompilerCache) evictCompilerObject(root *os.Root, key, path string, info fs.FileInfo) (bool, error) {
	lock, err := c.cache.tryLock(key + ".use")
	if err != nil {
		return false, err
	}
	defer unlockEnvironment(lock)
	cutoff, err := compilerTrimCutoff(c.cache.Root, key, c.cache.Clock.Now(), lock == nil)
	if err != nil || !info.ModTime().Before(cutoff) {
		return false, err
	}
	opened, err := root.Stat(".")
	if err != nil {
		return false, err
	}
	live, err := os.Lstat(root.Name())
	if err != nil || !os.SameFile(opened, live) {
		return false, err
	}
	current, err := root.Lstat(path)
	if err != nil || !os.SameFile(info, current) || !info.ModTime().Equal(current.ModTime()) ||
		current.Sys().(*syscall.Stat_t).Nlink != 1 {
		return false, err
	}
	if err := root.Remove(path); err != nil {
		return false, err
	}
	return true, nil
}
