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
	if limit < 0 || w.CacheLimit < 0 {
		return errors.New("cache limits must be nonnegative")
	}
	budgets := newCacheBudgets()
	w.fileCache = &FileCache{Root: root, Limit: limit, Clock: w.Clock, Client: w.Client, budgets: budgets}
	w.sharedInputs = &SharedInputCache{cache: EnvironmentCache{Root: filepath.Join(root, "shared-inputs"), Limit: limit, Clock: w.Clock, budgets: budgets}, Client: w.Client}
	w.sharedInputs.refreshCapacity()
	w.modelSnapshots = &SharedInputCache{cache: EnvironmentCache{Root: filepath.Join(root, "model-snapshots"), Limit: w.CacheLimit, Clock: w.Clock, budgets: budgets}, Client: w.Client}
	budgets.register(w.fileCache.Root, func(ctx context.Context) { w.fileCache.collect(ctx) })
	budgets.register(w.sharedInputs.cache.Root, w.sharedInputs.collectAdaptive)
	budgets.register(w.modelSnapshots.cache.Root, w.modelSnapshots.collectAdaptive)
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
