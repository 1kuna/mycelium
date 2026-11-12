package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
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
	Pending   bool   `json:"pending,omitempty"`
	Error     string `json:"error,omitempty"`
}

type WorkerHygieneCollection struct {
	DiskBudget             []DiskBudgetVolume      `json:"disk_budget,omitempty"`
	Scratch                *AttemptFilesCollection `json:"scratch,omitempty"`
	CompilerWarning        string                  `json:"compiler_warning,omitempty"`
	UnmanagedCompilerBytes int64                   `json:"unmanaged_compiler_bytes,omitempty"`
	At                     time.Time               `json:"at"`
	NextAt                 time.Time               `json:"next_at"`
	FileCache              CacheCollection         `json:"file_cache"`
	Environments           CacheCollection         `json:"environments"`
	StagingError           string                  `json:"staging_error,omitempty"`
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
	c.w.reclaimPendingDisk(ctx)
	r.DiskBudget = c.w.diskBudget.collect(ctx)
	if c.w.compilerCache != nil && c.w.compilerCache.external.Load() {
		r.CompilerWarning = "external_compiler_cache_unmanaged"
	}
	if c.w.compilerCache != nil {
		if bytes, err := c.w.compilerCache.externalUsage(ctx); err == nil && bytes > 0 {
			r.UnmanagedCompilerBytes = bytes
			r.CompilerWarning = "external_compiler_cache_unmanaged"
		}
	}
	// Refresh while idle as well as at admission: freeing unrelated disk must
	// wake cache-blocked placement without a worker restart or a new attempt.
	if c.w.sharedInputs != nil {
		c.w.sharedInputs.refreshCapacity()
	}
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

func (w Worker) environmentReferenced(ctx context.Context, key string) (bool, error) {
	var state struct {
		Referenced bool `json:"referenced"`
	}
	err := w.Client.Call(ctx, "GET", "/api/environments/"+key+"/reference", nil, &state)
	return state.Referenced, err
}

func (c *workerHygieneCollector) recordScratch(report AttemptFilesCollection) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.report.Scratch = &report
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
	if !c.mu.TryLock() {
		r.Busy = true
		return r
	}
	defer c.mu.Unlock()
	r.Limit = c.Limit
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
	explicit := c.Limit
	if c.automatic {
		explicit = 0
	}
	r.Limit, _, err = inputCacheBudget(c.Root, r.Bytes, explicit, c.diskProbe, c.budget.foreignHeadroom(c.Root))
	if err != nil {
		r.Error = "file_cache_collect_failed"
		return r
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
		if c.budget != nil {
			if err := c.Client.HasFile(ctx, FileRef{SHA256: v.name, SizeBytes: info.Size()}); err != nil {
				r.Protected = true
				continue
			}
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

// Inventory and reference requests never hold admission. Each candidate keeps
// its existing use/build fences; only the final atomic retirement tries admission.
func (c *EnvironmentCache) collectBudget(ctx context.Context, references func(context.Context, string) (bool, error)) (r CacheCollection) {
	if err := c.init(); err != nil {
		r.Error = "environment_collect_failed"
		return r
	}
	r.Limit = c.Limit
	var err error
	r.Bytes, err = c.collectionUsage(ctx)
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
	before := r.Bytes
	changed, estimatedEnough := false, false
	defer func() {
		if changed {
			after, err := c.collectionUsage(ctx)
			if err != nil {
				r.Error = "environment_collect_interrupted"
				return
			}
			r.Bytes, r.Reclaimed = after, max(0, before-after)
		}
		if r.Bytes > r.Limit {
			if estimatedEnough {
				r.Pending = true
			} else {
				r.Protected = true
			}
		}
	}()
	for _, v := range candidates {
		if r.Bytes <= r.Limit {
			estimatedEnough = true
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
		// Per-prefix allocated bytes can overestimate the saving when prefixes share
		// inodes. Stop conservatively, then recount the union once after the batch;
		// any residual budget work waits for the next measured sweep. This avoids
		// traversing every remaining environment after each individual eviction.
		size, err := collectionAllocatedBytes(ctx, filepath.Join(c.Root, v.Key))
		if err != nil {
			r.Error = "environment_collect_failed"
			return r
		}
		var admission *os.File
		removed, err := c.evict(v.Key, "automatic_budget", func() (bool, error) {
			if !c.ready(v.Key) {
				return false, nil
			}
			referenced, err := references(ctx, v.Key)
			if err != nil || referenced {
				return false, err
			}
			if err := ctx.Err(); err != nil {
				return false, err
			}
			admission, err = c.tryLock("admission")
			if err == nil && admission == nil {
				r.Busy = true
			}
			return admission != nil, err
		})
		unlockEnvironment(admission)
		if err != nil {
			r.Error = "environment_references_unavailable"
			return r
		}
		if !removed {
			r.Protected = true
			continue
		}
		changed = true
		if _, err = c.purgeRetiredKey(v.Key); err != nil {
			r.Error = "environment_collect_failed"
			return r
		}
		r.Bytes = max(0, r.Bytes-size)
	}
	if r.Bytes <= r.Limit {
		estimatedEnough = true
	}
	return r
}

// Preserve the existing budget's allocated-block/inode union, with cancellation
// between metadata operations. Downloads retain their separate cache policy.
func (c *EnvironmentCache) collectionUsage(ctx context.Context) (int64, error) {
	entries, err := os.ReadDir(c.Root)
	if err != nil {
		return 0, err
	}
	paths := []string{filepath.Join(c.Root, ".Trash")}
	for _, d := range entries {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if d.IsDir() && (len(d.Name()) == 64 || strings.HasSuffix(d.Name(), ".partial")) {
			paths = append(paths, filepath.Join(c.Root, d.Name()))
		}
	}
	return collectionAllocatedBytes(ctx, paths...)
}

type collectionDeviceKey struct{}

func collectionAllocatedBytes(ctx context.Context, paths ...string) (int64, error) {
	seen := map[[2]uint64]bool{}
	var size int64
	for _, root := range paths {
		err := filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
			if e := ctx.Err(); e != nil {
				return e
			}
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			info, err := d.Info()
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				return errors.New("environment disk accounting unavailable")
			}
			if device, limited := ctx.Value(collectionDeviceKey{}).(uint64); limited && uint64(stat.Dev) != device {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			id := [2]uint64{uint64(stat.Dev), uint64(stat.Ino)}
			if seen[id] && d.IsDir() {
				return filepath.SkipDir
			}
			if !seen[id] {
				size += stat.Blocks * 512
				seen[id] = true
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	return size, nil
}

// The durable fleet receipt proves ownership of exactly one staging path.
// Never scan arbitrary binaries, recovery archives, rollback copies or unknown
// staging. A failed receipt is written by a helper that then exits, and every
// helper holds the update lock while it runs, so holding the lock proves none
// is writing.
func collectFailedFleetStaging(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) (result error) {
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
	superseded := collectSupersededFleetStaging(c, fleetStagingPath(c, r.Instruction))
	defer func() { result = errors.Join(result, superseded) }()
	if r.Phase != "failed" {
		return nil
	}
	// Config mismatch or a frozen recovery receipt belongs to its owner.
	_, hash, err := fleetConfig(c.Config)
	if err != nil || hash != r.ConfigSHA256 {
		return errors.New("installer state changed")
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

var fleetStagingName = regexp.MustCompile(`^\.myce-fleet-[0-9a-f]{24}\.partial$`)

// A receipt naming a different operation proves this install moved on: an
// earlier operation's partial is rebuildable from its frozen release and has no
// writer, because helpers write staging only while holding the update lock the
// caller holds. Only a binary directory that also holds this install's config
// is exclusively this install's; a shared directory could hold another
// install's live staging, so it is never scanned. Links, directories and other
// names are never candidates.
func collectSupersededFleetStaging(c ServiceConfig, current string) error {
	dir := filepath.Dir(c.Binary)
	if filepath.Clean(dir) != filepath.Clean(filepath.Dir(c.Config)) {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var result error
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if !fleetStagingName.MatchString(entry.Name()) || path == current || !entry.Type().IsRegular() {
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	return result
}

// Worker diagnostics are untrusted even when the stock collector emits codes.
// Sanitize only the detached public view, preserving queue-owned evidence.
func publicWorkerCollection(m *Machine) {
	if m.HygieneCollection == nil {
		return
	}
	r := *m.HygieneCollection
	code := func(value, fallback string, allowed ...string) string {
		if value == "" || slices.Contains(allowed, value) {
			return value
		}
		return fallback
	}
	r.FileCache.Error = code(r.FileCache.Error, "file_cache_collect_failed", "file_cache_collect_failed", "file_cache_collect_interrupted", "file_cache_collect_changed")
	r.Environments.Error = code(r.Environments.Error, "environment_collect_failed", "environment_collect_failed", "environment_collect_interrupted", "environment_references_unavailable")
	r.StagingError = code(r.StagingError, "installer_staging_retained", "installer_staging_retained")
	r.CompilerWarning = code(r.CompilerWarning, "external_compiler_cache_unmanaged", "external_compiler_cache_unmanaged")
	r.DiskBudget = slices.Clone(r.DiskBudget)
	for i := range r.DiskBudget {
		v := &r.DiskBudget[i]
		if !sha256Name.MatchString(v.ID) {
			v.ID = ""
		}
		v.Roots = slices.Clone(v.Roots)
		for j := range v.Roots {
			v.Roots[j] = code(v.Roots[j], "unknown", "attempts", "files", "shared_inputs", "model_snapshots", "environments", "models", "compiler")
		}
		v.Warning = code(v.Warning, "disk_inventory_unavailable", "foreign_disk_growth", "disk_inventory_unavailable")
		v.DeclineCause = code(v.DeclineCause, "", "owned_disk_growth", "foreign_disk_growth")
	}
	if r.Scratch != nil {
		s := *r.Scratch
		s.Reasons = map[string]int64{}
		for reason, bytes := range r.Scratch.Reasons {
			name := code(reason, "unknown", "unknown_job", "unresolved_custody", "parked_storage_custody", "pending_delivery", "unique_unreleased", "file_retention", "unknown_folder", "active", "pending_replay", "queue_unavailable", "local_custody", "collection_failed")
			s.Reasons[name] += bytes
		}
		r.Scratch = &s
	}
	m.HygieneCollection = &r
}
