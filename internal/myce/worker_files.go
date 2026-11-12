package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
)

func (w *Worker) initFileTransfers(ctx context.Context, s *workerStore) error {
	w.fileCheckpoints = s.state.Job != nil && s.state.Job.FileTransport
	root := w.FileCacheRoot
	if root == "" {
		root = filepath.Join(s.dir, "file-cache")
	}
	limit := w.FileCacheLimit
	if limit < 0 {
		return errors.New("file cache limit must be positive")
	}
	// Positive EnvironmentCache Limit initializes storage/locks only; automatic
	// input admission computes the effective budget on every scan.
	w.fileCache = &FileCache{Root: root, Limit: limit, automatic: limit == 0, Clock: w.Clock, Client: w.Client}
	w.fileCache.budget = w.diskBudget
	w.fileCache.diskProbe = w.DiskProbe
	w.sharedInputs = &SharedInputCache{cache: EnvironmentCache{Root: filepath.Join(root, "shared-inputs"), Limit: max(1, limit), Clock: w.Clock}, automatic: limit == 0, Client: w.Client}
	w.sharedInputs.budget = w.diskBudget
	w.sharedInputs.diskProbe = w.DiskProbe
	w.sharedInputs.refreshCapacity()
	// Model snapshots are tens of gigabytes, so they share the model cache's
	// size setting rather than the small file cache's.
	models := w.CacheLimit
	if models == 0 {
		models = defaultModelCacheBytes
	}
	w.modelSnapshots = &SharedInputCache{cache: EnvironmentCache{Root: filepath.Join(root, "model-snapshots"), Limit: models, Clock: w.Clock}, Client: w.Client}
	w.modelSnapshots.budget = w.diskBudget
	w.modelSnapshots.diskProbe = w.DiskProbe
	if err := w.modelSnapshots.cache.init(); err != nil {
		return err
	}
	w.modelIndex = &modelSnapshotIndex{}
	w.modelIndex.load(w.modelSnapshots)
	if w.diskBudget != nil {
		w.diskBudget.add(diskBudgetRoot{name: "files", path: root, candidates: w.fileCache.budgetCandidates})
		w.diskBudget.add(diskBudgetRoot{name: "shared_inputs", path: w.sharedInputs.cache.Root, candidates: w.sharedInputs.budgetCandidates})
		w.diskBudget.add(diskBudgetRoot{name: "model_snapshots", path: w.modelSnapshots.cache.Root, candidates: w.modelSnapshots.budgetCandidates})
	}
	if w.fileCheckpoints {
		if w.CheckpointRoot != "" {
			return errors.New("NAS checkpoint-root cannot be combined with queue file-store checkpoints")
		}
		s.state.CheckpointRoot = filepath.Join(s.dir, "file-checkpoints")
		if err := os.MkdirAll(s.state.CheckpointRoot, 0700); err != nil {
			return err
		}
		if err := s.save(); err != nil {
			return err
		}
	}
	return nil
}
