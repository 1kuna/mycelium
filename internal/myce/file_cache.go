package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// FileCache contains only reproducible store copies. Attempt folders hold private
// copies, so commands cannot mutate shared blobs and cache eviction cannot race use.
type FileCache struct {
	budget    *workerDiskBudget
	Root      string
	Limit     int64
	Clock     Clock
	Client    Client
	automatic bool
	diskProbe func(string) (DiskReading, error)
	mu        sync.Mutex
}

func (c *FileCache) copyTo(ctx context.Context, ref FileRef, dest string) (stageErr error) {
	if err := ref.validate(); err != nil {
		return err
	}
	finish := stagingMeasure(ctx, stageDiskBudget)
	defer func() { finish(stageErr) }()
	operation := "cache_admission"
	defer func() { stageErr = withStagingCause("", ref, operation, stageErr) }()
	// Reserve before any cache mutex: pressure collection only tries cache locks.
	release, err := c.budget.reserve(ctx, c.Root, uint64(ref.SizeBytes), 0)
	if err != nil {
		return err
	}
	defer release()
	releaseCopy, err := c.budget.reserve(ctx, filepath.Dir(dest), uint64(ref.SizeBytes), 0)
	if err != nil {
		return err
	}
	defer releaseCopy()
	operation = "cache_init"
	finish(nil)
	finish = stagingMeasure(ctx, stageCacheLock)
	c.mu.Lock()
	finish(nil)
	finish = stagingMeasure(ctx, stageCacheLookup)
	defer c.mu.Unlock()
	if err := ref.validate(); err != nil {
		return err
	}
	if !c.automatic && c.Limit <= 0 {
		return errors.New("file exceeds local cache limit")
	}
	if err := PrepareStateRoots(c.Root); err != nil {
		return err
	}
	finish(nil)
	finish = stagingMeasure(ctx, stageCollectionLock)
	lock, err := c.collectionLock(ctx)
	if err != nil {
		return err
	}
	defer unlockEnvironment(lock)
	finish(nil)
	finish = stagingMeasure(ctx, stageCacheLookup)
	root, err := filepath.EvalSymlinks(c.Root)
	if err != nil {
		return err
	}
	p := filepath.Join(root, ref.SHA256)
	if verifyFile(ctx, p, ref) == nil {
		stagingCount(ctx, "hit")
		now := c.Clock.Now()
		if err = os.Chtimes(p, now, now); err != nil {
			return err
		}
		finish(nil)
		finish = stagingMeasure(ctx, stagePlacement)
		operation = "copy"
		return copyJobFileContext(ctx, p, dest)
	}
	stagingCount(ctx, "miss")
	finish(nil)
	finish = func(error) {}
	// Never replace corrupt cache bytes as if they were complete. They are disposable
	// only after the durable authoritative copy has been checked.
	operation = "cache_verify"
	if err = c.Client.recoverInputTransfer(ctx, ref, operation, func(one Client) error { return one.HasFile(ctx, ref) }); err != nil {
		return err
	}
	finish = stagingMeasure(ctx, stageCacheEviction)
	if _, err = os.Lstat(p); err == nil {
		if err = os.Remove(p); err != nil {
			return err
		}
	}
	partial := p + ".partial"
	type victim struct {
		name string
		size int64
		used int64
	}
	var victims []victim
	operation = "cache_scan"
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	used := int64(0)
	prefix := int64(0)
	for _, entry := range entries {
		if entry.Name() == FormatMarker || (entry.Name() == "shared-inputs" || entry.Name() == "model-snapshots") && entry.IsDir() {
			continue
		}
		info, e := entry.Info()
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() {
			return errors.New("invalid cache entry")
		}
		used += info.Size()
		if entry.Name() == ref.SHA256+".partial" {
			prefix = info.Size()
			continue
		}
		victims = append(victims, victim{entry.Name(), info.Size(), info.ModTime().UnixNano()})
	}
	if prefix > ref.SizeBytes {
		return errors.New("file partial exceeds declared size")
	}
	explicit := c.Limit
	if c.automatic {
		explicit = 0
	}
	limit, free, err := inputCacheBudget(root, used, explicit, c.diskProbe, c.budget.foreignHeadroom(root))
	if err != nil {
		return err
	}
	need := ref.SizeBytes - prefix
	sort.Slice(victims, func(i, j int) bool {
		if victims[i].used == victims[j].used {
			return victims[i].name < victims[j].name
		}
		return victims[i].used < victims[j].used
	})
	for _, v := range victims {
		if used+need <= limit && free >= need {
			break
		}
		hash := strings.TrimSuffix(v.name, ".partial")
		if (FileRef{SHA256: hash}).validate() != nil {
			return errors.New("unknown file in cache; refusing eviction")
		}
		// These are disposable cache copies, never checkpoint/result custody.
		// Each attempt owns a private materialization. Queue expiry or a
		// transient HEAD failure for an unrelated victim must not block new
		// inputs. The requested blob was verified above before any eviction.
		operation = "cache_evict"
		if err = os.Remove(filepath.Join(root, v.name)); err != nil {
			return err
		}
		stagingCount(ctx, "evicted")
		used -= v.size
		limit, free, err = inputCacheBudget(root, used, explicit, c.diskProbe, c.budget.foreignHeadroom(root))
		if err != nil {
			return err
		}
	}
	operation = "cache_admission"
	if used+need > limit || free < need {
		return &stagingCapacityError{required: need, available: max(0, min(limit-used, free)), limit: limit, capacity: "file_cache"}
	}
	finish(nil)
	finish = func(error) {}
	operation = "cache_download"
	if err = c.Client.recoverInputTransfer(ctx, ref, operation, func(one Client) error { return one.DownloadFile(ctx, ref, partial) }); err != nil {
		return err
	}
	finish = stagingMeasure(ctx, stageCachePublish)
	operation = "cache_publish"
	if err = os.Rename(partial, p); err != nil {
		return err
	}
	if err = syncWorkerDir(root); err != nil {
		return err
	}
	now := c.Clock.Now()
	if err = os.Chtimes(p, now, now); err != nil {
		return err
	}
	finish(nil)
	finish = stagingMeasure(ctx, stagePlacement)
	operation = "copy"
	return copyJobFileContext(ctx, p, dest)
}

func validateJobFiles(s Spec) error {
	if err := validateSharedInputs(s); err != nil {
		return err
	}
	if err := validateModelSnapshots(s); err != nil {
		return err
	}
	if err := s.Files.validate(); err != nil {
		return err
	}
	if len(s.Files) > 0 && (len(s.Inputs) > 0 || s.Run.Workdir != "") {
		return errors.New("stored files use a private working folder; cannot mix NAS inputs or workdir")
	}
	all := FileManifest{}
	for n, f := range s.Files {
		all[n] = f
	}
	for n, p := range s.Outputs {
		if !validOutputDeclaration(n) || !safeRelative(p) {
			return errors.New("outputs require clean relative names, folders (dir/) or globs, and prefixes")
		}
		if kind, _ := outputKind(n); kind != outputFile {
			if err := validateOutputPatternSpec(s); err != nil {
				return err
			}
			continue
		}
		if _, ok := all[n]; ok {
			return errors.New("input and output names overlap")
		}
		all[n] = FileRef{SHA256: strings.Repeat("0", 64)}
	}
	return all.validate()
}
