package myce

import (
	"context"
	"errors"
	"fmt"
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
	if limit == 0 {
		var err error
		limit, err = AutoEnvironmentCacheLimit(root)
		if err != nil {
			return fmt.Errorf("file cache auto-size: %w", err)
		}
	}
	if limit <= 0 {
		return errors.New("file cache limit must be positive")
	}
	w.fileCache = &FileCache{Root: root, Limit: limit, Clock: w.Clock, Client: w.Client}
	w.sharedInputs = &SharedInputCache{cache: EnvironmentCache{Root: filepath.Join(root, "shared-inputs"), Limit: limit, Clock: w.Clock}, Client: w.Client}
	w.sharedInputs.refreshCapacity()
	// Model snapshots are tens of gigabytes, so they share the model cache's
	// size setting rather than the small file cache's.
	models := w.CacheLimit
	if models == 0 {
		models = defaultModelCacheBytes
	}
	w.modelSnapshots = &SharedInputCache{cache: EnvironmentCache{Root: filepath.Join(root, "model-snapshots"), Limit: models, Clock: w.Clock}, Client: w.Client}
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
