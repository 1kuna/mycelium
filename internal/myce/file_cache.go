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
	budgets *cacheBudgets
	Root    string
	Limit   int64
	Clock   Clock
	Client  Client
	mu      sync.Mutex
}

func (c *FileCache) copyTo(ctx context.Context, ref FileRef, dest string) (stageErr error) {
	operation := "cache_init"
	defer func() { stageErr = withStagingCause("", ref, operation, stageErr) }()
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ref.validate(); err != nil {
		return err
	}
	if c.Clock == nil {
		c.Clock = RealClock{}
	}
	if c.budgets == nil {
		c.budgets = newCacheBudgets()
	}
	if c.Limit < 0 || c.Limit > 0 && ref.SizeBytes > c.Limit {
		return errors.New("file exceeds local cache limit")
	}
	if err := PrepareStateRoots(c.Root); err != nil {
		return err
	}
	lock, err := c.collectionLock(ctx)
	if err != nil {
		return err
	}
	defer unlockEnvironment(lock)
	root, err := filepath.EvalSymlinks(c.Root)
	if err != nil {
		return err
	}
	p := filepath.Join(root, ref.SHA256)
	if verifyFile(ctx, p, ref) == nil {
		c.budgets.touch(c.Root, ref.SHA256, ref.SizeBytes, c.Clock.Now())
		now := c.Clock.Now()
		if err = os.Chtimes(p, now, now); err != nil {
			return err
		}
		operation = "copy"
		return c.materialize(ctx, p, dest, ref.SizeBytes)
	}
	// Never replace corrupt cache bytes as if they were complete. They are disposable
	// only after the durable authoritative copy has been checked.
	operation = "cache_verify"
	if err = c.Client.HasFile(ctx, ref); err != nil {
		return err
	}
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
	free, err := diskFree(root)
	if err != nil {
		return err
	}
	limit, err := c.budgets.limit(c.Root, used, c.Limit, c.Clock.Now())
	if err != nil {
		return err
	}
	need := ref.SizeBytes - prefix
	sort.Slice(victims, func(i, j int) bool {
		hi, hj := c.budgets.recent(c.Root, victims[i].name, c.Clock.Now()), c.budgets.recent(c.Root, victims[j].name, c.Clock.Now())
		if hi != hj {
			return !hi
		}
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
		used -= v.size
		free, err = diskFree(root)
		if err != nil {
			return err
		}
	}
	operation = "cache_admission"
	if used+need > limit || free < need {
		return errors.New("insufficient local file cache space")
	}
	reservation, err := c.budgets.reserve(c.Root, need, c.Clock)
	if err != nil {
		return err
	}
	defer reservation.release()
	copyCtx, releaseCopy, err := c.budgets.copy(ctx, c.Root, c.Clock, 0)
	if err != nil {
		return err
	}
	defer releaseCopy()
	ctx = copyCtx
	operation = "cache_download"
	if err = c.Client.DownloadFile(ctx, ref, partial); err != nil {
		return err
	}
	operation = "cache_publish"
	if err = os.Rename(partial, p); err != nil {
		return err
	}
	if err = syncWorkerDir(root); err != nil {
		return err
	}
	c.budgets.touch(c.Root, ref.SHA256, ref.SizeBytes, c.Clock.Now())
	now := c.Clock.Now()
	if err = os.Chtimes(p, now, now); err != nil {
		return err
	}
	operation = "copy"
	releaseCopy()
	reservation.release()
	return c.materialize(ctx, p, dest, ref.SizeBytes)
}

// A private attempt copy can live on a different volume from its cache. Charge
// its destination separately; the cache's completed download no longer owns a
// copy slot, so a single-stream volume cannot deadlock on nested acquisition.
func (c *FileCache) materialize(ctx context.Context, source, dest string, size int64) error {
	root := filepath.Dir(dest)
	if _, err := c.budgets.limit(root, 0, 0, c.Clock.Now()); err != nil {
		return err
	}
	held, err := c.budgets.reserve(root, size, c.Clock)
	if err != nil {
		return err
	}
	defer held.release()
	copyCtx, release, err := c.budgets.copy(ctx, root, c.Clock, 0)
	if err != nil {
		return err
	}
	defer release()
	return copyJobFileContext(copyCtx, source, dest)
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
	if err := validateDockerFiles(s); err != nil {
		return err
	}
	all := FileManifest{}
	for n, f := range s.Files {
		all[n] = f
	}
	for n, p := range s.Outputs {
		if !safeRelative(n) || !safeRelative(p) {
			return errors.New("outputs require clean relative names and prefixes")
		}
		if _, ok := all[n]; ok {
			return errors.New("input and output names overlap")
		}
		all[n] = FileRef{SHA256: strings.Repeat("0", 64)}
	}
	return all.validate()
}
