package myce

import (
	"context"
	"encoding/json"
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

// downloadNeedFile records the largest download-cache growth one build caused.
const downloadNeedFile = "download-need.json"

type downloadNeed struct {
	Bytes int64 `json:"bytes"`
}

// downloadLimit is twice the largest measured single build's downloads: room
// for that environment's packages plus one more build of the same size (its
// next lock revision) before standard pruning, so a rebuild never re-downloads
// and re-unpacks what it just fetched (#1480). Before any measurement there is
// no separate limit. Real disk pressure still prunes downloads through the
// worker disk floor (MY-34). An explicit DownloadLimit overrides measurement.
func (c *EnvironmentCache) downloadLimit() (int64, bool) {
	if c.DownloadLimit > 0 {
		return c.DownloadLimit, true
	}
	var need downloadNeed
	b, err := os.ReadFile(filepath.Join(c.Root, downloadNeedFile))
	if err != nil || json.Unmarshal(b, &need) != nil || need.Bytes <= 0 {
		return 0, false
	}
	return 2 * need.Bytes, true
}

// recordDownloadNeed keeps the largest growth of the shared download cache
// across one build's installation. Concurrent builders can only inflate it,
// which errs toward keeping downloads; the caller holds the shared downloads
// lock, so no prune runs in between.
func (c *EnvironmentCache) recordDownloadNeed(ctx context.Context, before int64) error {
	after, err := environmentSize(filepath.Join(c.Root, ".downloads"))
	if err != nil || after <= before {
		return err
	}
	gate, err := c.lock(ctx, "download-need", syscall.LOCK_EX)
	if err != nil {
		return err
	}
	defer unlockEnvironment(gate)
	var need downloadNeed
	if b, err := os.ReadFile(filepath.Join(c.Root, downloadNeedFile)); err == nil {
		_ = json.Unmarshal(b, &need)
	}
	if after-before <= need.Bytes {
		return nil
	}
	return saveWorkerJSON(c.Root, downloadNeedFile, downloadNeed{Bytes: after - before})
}

// Package managers own cache contents and locking. Never remove wheel files by
// hand: uv's standard prune preserves installed hardlinks/clones, unlike symlinks.
// Builds share a lock; pruning waits for every installer to finish. Prunes run
// in the download cache itself: pixi reads a project manifest from its working
// directory and refuses a uv environment's pyproject.toml (#1480).
func (c *EnvironmentCache) pruneDownloads(ctx context.Context, env []string, out io.Writer, run func(context.Context, []string, string, []string, io.Writer, io.Writer) error) error {
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
	limit, limited := c.downloadLimit()
	if !limited || size <= limit {
		return nil
	}
	fmt.Fprintf(out, "download cache: %d bytes exceeds separate %d-byte limit; pruning\n", size, limit)
	// npm's content-addressed cache has no partial prune; clearing it under the
	// exclusive downloads lock is `npm cache clean --force` without needing npm.
	if err = os.RemoveAll(c.downloadCache("npm")); err != nil {
		return fmt.Errorf("npm cache prune: %w", err)
	}
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
		if err = run(ctx, args, root, env, out, out); err != nil {
			return fmt.Errorf("%s cache prune: %w", tool, err)
		}
	}
	size, err = environmentSize(root)
	if err != nil {
		return err
	}
	if size > limit {
		return fmt.Errorf("download cache remains %d bytes after standard pruning; limit %d", size, limit)
	}
	return nil
}
