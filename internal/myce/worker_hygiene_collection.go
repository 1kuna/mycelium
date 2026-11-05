package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
)

type CacheCollection struct {
	Bytes     int64  `json:"bytes"`
	Limit     int64  `json:"limit"`
	Reclaimed int64  `json:"reclaimed"`
	Busy      bool   `json:"busy,omitempty"`
	Protected bool   `json:"protected,omitempty"`
	Error     string `json:"error,omitempty"`
}

type WorkerHygieneCollection struct {
	At           time.Time       `json:"at"`
	NextAt       time.Time       `json:"next_at"`
	FileCache    CacheCollection `json:"file_cache"`
	Environments CacheCollection `json:"environments"`
	StagingError string          `json:"staging_error,omitempty"`
}

type workerHygieneCollector struct {
	mu     sync.Mutex
	w      Worker
	report WorkerHygieneCollection
}

func (c *workerHygieneCollector) snapshot() *WorkerHygieneCollection {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.report
	return &r
}

// Called only by the existing serialized retention lane, never a heartbeat or
// queue transaction. Releases can request retention more often than a minute;
// expensive inventory uses at most the existing minute cadence and 10% duty.
func (c *workerHygieneCollector) collect(ctx context.Context) {
	started := c.w.Clock.Now()
	c.mu.Lock()
	due := !started.Before(c.report.NextAt)
	c.mu.Unlock()
	if !due {
		return
	}
	r := WorkerHygieneCollection{At: started}
	if c.w.fileCache != nil {
		r.FileCache = c.w.fileCache.collect(ctx)
	}
	if c.w.environmentCache != nil {
		r.Environments = c.w.environmentCache.collectBudget(ctx, func(ctx context.Context, key string) (bool, error) {
			var state struct {
				Referenced bool `json:"referenced"`
			}
			err := c.w.Client.Call(ctx, "GET", "/api/environments/"+key+"/reference", nil, &state)
			return state.Referenced, err
		})
	}
	if c.w.ServiceConfig != nil {
		if err := collectFailedFleetStaging(ctx, *c.w.ServiceConfig, fleetCommand); err != nil {
			r.StagingError = "installer_staging_retained"
		}
	}
	// A measured expensive sweep backs off instead of continuously scanning a
	// large cache. The tenfold period budgets at most 10% of time for hygiene.
	r.NextAt = c.w.Clock.Now().Add(max(time.Minute, c.w.Clock.Now().Sub(started)*10))
	c.mu.Lock()
	c.report = r
	c.mu.Unlock()
}

func (c *FileCache) collectionLock(ctx context.Context) (*os.File, error) {
	cache, key, err := c.collectionLockStorage()
	if err != nil {
		return nil, err
	}
	return cache.lock(ctx, key, syscall.LOCK_EX)
}

func (c *FileCache) collectionLockStorage() (*EnvironmentCache, string, error) {
	if err := PrepareStateRoots(c.Root); err != nil {
		return nil, "", err
	}
	root, err := filepath.EvalSymlinks(c.Root)
	if err != nil {
		return nil, "", err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, "", err
	}
	parent := filepath.Dir(root)
	locks := filepath.Join(parent, "locks")
	if err := os.MkdirAll(locks, 0700); err != nil {
		return nil, "", err
	}
	info, err := os.Lstat(locks)
	if err != nil {
		return nil, "", err
	}
	if !info.IsDir() {
		return nil, "", errors.New("cache lock root is not a real directory")
	}
	return &EnvironmentCache{Root: parent}, "file-cache-" + identityHash(root), nil
}

func (c *FileCache) collect(ctx context.Context) (r CacheCollection) {
	r.Limit = c.Limit
	if !c.mu.TryLock() {
		r.Busy = true
		return r
	}
	defer c.mu.Unlock()
	cache, key, err := c.collectionLockStorage()
	if err != nil {
		r.Error = "file_cache_collect_failed"
		return r
	}
	lock, err := cache.tryLock(key)
	if err != nil {
		r.Error = "file_cache_collect_failed"
		return r
	}
	if lock == nil {
		r.Busy = true
		return r
	}
	defer unlockEnvironment(lock)
	entries, err := os.ReadDir(c.Root)
	if err != nil {
		r.Error = "file_cache_collect_failed"
		return r
	}
	type victim struct {
		name string
		info os.FileInfo
	}
	var victims []victim
	for _, entry := range entries {
		if entry.Name() == FormatMarker || entry.IsDir() && (entry.Name() == "shared-inputs" || entry.Name() == "model-snapshots") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			r.Error = "file_cache_collect_failed"
			return r
		}
		// Only complete content-addressed cache copies are deletion candidates.
		// Unknown files, symlinks and partial downloads remain untouched.
		if !info.Mode().IsRegular() {
			r.Protected = true
			continue
		}
		r.Bytes += info.Size()
		if (FileRef{SHA256: entry.Name()}).validate() != nil {
			r.Protected = true
			continue
		}
		victims = append(victims, victim{entry.Name(), info})
	}
	sort.Slice(victims, func(i, j int) bool { return victims[i].info.ModTime().Before(victims[j].info.ModTime()) })
	for _, v := range victims {
		if r.Bytes <= r.Limit {
			break
		}
		if ctx.Err() != nil {
			r.Error = "file_cache_collect_interrupted"
			return r
		}
		path := filepath.Join(c.Root, v.name)
		info, err := os.Lstat(path)
		if err != nil || !os.SameFile(v.info, info) || !info.Mode().IsRegular() {
			r.Error = "file_cache_collect_changed"
			return r
		}
		if err := os.Remove(path); err != nil {
			r.Error = "file_cache_collect_failed"
			return r
		}
		r.Bytes -= info.Size()
		r.Reclaimed += info.Size()
	}
	if r.Bytes > r.Limit {
		r.Protected = true
	}
	return r
}

// Reuse the existing eviction primitive and local use/build locks, but obtain
// authoritative queue references immediately before each budget eviction.
func (c *EnvironmentCache) collectBudget(ctx context.Context, references func(context.Context, string) (bool, error)) (r CacheCollection) {
	if err := c.init(); err != nil {
		r.Error = "environment_collect_failed"
		return r
	}
	r.Limit = c.Limit
	gate, err := c.tryLock("admission")
	if err != nil {
		r.Error = "environment_collect_failed"
		return r
	}
	if gate == nil {
		r.Busy = true
		return r
	}
	defer unlockEnvironment(gate)
	r.Bytes, err = c.DiskUsage()
	if err != nil {
		r.Error = "environment_collect_failed"
		return r
	}
	if r.Bytes <= r.Limit {
		return r
	}
	if references == nil {
		r.Error = "environment_references_unavailable"
		return r
	}
	candidates, err := c.evictionCandidates("")
	if err != nil {
		r.Error = "environment_collect_failed"
		return r
	}
	for _, v := range candidates {
		if r.Bytes <= r.Limit {
			break
		}
		if ctx.Err() != nil {
			r.Error = "environment_collect_interrupted"
			return r
		}
		if !c.ready(v.Key) {
			r.Protected = true
			continue
		}
		removed, err := c.evict(v.Key, "automatic_budget", func() (bool, error) {
			if !c.ready(v.Key) {
				return false, nil
			}
			referenced, err := references(ctx, v.Key)
			return !referenced, err
		})
		if err != nil {
			r.Error = "environment_references_unavailable"
			return r
		}
		if !removed {
			r.Protected = true
			continue
		}
		if _, err = c.purgeRetiredKey(v.Key); err != nil {
			r.Error = "environment_collect_failed"
			return r
		}
		after, err := c.DiskUsage()
		if err != nil {
			r.Error = "environment_collect_failed"
			return r
		}
		r.Reclaimed += max(0, r.Bytes-after)
		r.Bytes = after
	}
	if r.Bytes > r.Limit {
		r.Protected = true
	}
	return r
}

// The durable fleet receipt proves ownership of exactly one staging path.
// Never scan arbitrary binaries, recovery archives, rollback copies or unknown
// staging. The updater's transaction lock and helper-absence check both bind.
func collectFailedFleetStaging(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) error {
	unlock, err := commandLock(c.State, "fleet-update.lock", false)
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return nil
	}
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := os.Lstat(filepath.Join(c.State, fleetUpdateReceiptFile)); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	r, err := readFleetReceipt(filepath.Join(c.State, fleetUpdateReceiptFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if r.Phase != "failed" {
		return nil
	}
	// Config mismatch or a frozen recovery receipt belongs to its owner.
	_, hash, err := fleetConfig(c.Config)
	if err != nil || hash != r.ConfigSHA256 {
		return errors.New("installer state changed")
	}
	absent, err := fleetHelperAbsent(ctx, c, r.Instruction, command)
	if err != nil {
		return err
	}
	if !absent {
		return nil
	}
	path := fleetStagingPath(c, r.Instruction)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("installer staging is not a regular file")
	}
	return os.Remove(path)
}
