package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const environmentBuildStallWindow = 5 * time.Minute

var errEnvironmentBuildStalled = errors.New("environment build stalled: no log or download-cache growth for 5 minutes (one retry permitted)")

func (c *EnvironmentCache) downloadCache(tool string) string {
	return filepath.Join(c.Root, ".downloads", tool)
}

// Package managers own cache contents and locking. Never remove wheel files by
// hand: uv's standard prune preserves installed hardlinks/clones, unlike symlinks.
// Builds share a lock; pruning waits for every installer to finish.
func (c *EnvironmentCache) pruneDownloads(ctx context.Context, dir string, env []string, out io.Writer, run func(context.Context, []string, string, []string, io.Writer, io.Writer) error) error {
	gate, err := c.lock(ctx, "downloads", syscall.LOCK_EX)
	if err != nil {
		return err
	}
	defer unlockEnvironment(gate)
	for _, tool := range []string{"uv", "pixi"} {
		if err = os.MkdirAll(c.downloadCache(tool), 0700); err != nil {
			return err
		}
	}
	root := filepath.Join(c.Root, ".downloads")
	size, err := environmentSize(root)
	if err != nil {
		return err
	}
	if size <= c.DownloadLimit {
		return nil
	}
	fmt.Fprintf(out, "download cache: %d bytes exceeds separate %d-byte limit; pruning\n", size, c.DownloadLimit)
	for _, tool := range []string{"uv", "pixi"} {
		n, err := environmentSize(c.downloadCache(tool))
		if err != nil {
			return err
		}
		if n == 0 {
			continue
		}
		args := []string{"uv", "cache", "prune", "--ci"}
		if tool == "pixi" {
			args = []string{"pixi", "clean", "cache", "--yes"}
		}
		if err = run(ctx, args, dir, env, out, out); err != nil {
			return fmt.Errorf("%s cache prune: %w", tool, err)
		}
	}
	size, err = environmentSize(root)
	if err != nil {
		return err
	}
	if size > c.DownloadLimit {
		return fmt.Errorf("download cache remains %d bytes after standard pruning; limit %d", size, c.DownloadLimit)
	}
	return nil
}
