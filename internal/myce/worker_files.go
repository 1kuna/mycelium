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
	if limit == 0 {
		limit = 10 << 30
	}
	if limit <= 0 {
		return errors.New("file cache limit must be positive")
	}
	w.fileCache = &FileCache{Root: root, Limit: limit, Clock: w.Clock, Client: w.Client}
	w.sharedInputs = &SharedInputCache{cache: EnvironmentCache{Root: filepath.Join(root, "shared-inputs"), Limit: limit, Clock: w.Clock}, Client: w.Client}
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
