package myce

import (
	"errors"
	"os"
	"path/filepath"
)

// AutoEnvironmentCacheLimit uses the cache filesystem, including its nearest
// existing ancestor on fresh installs. It reserves 20 GiB and never allocates.
func AutoEnvironmentCacheLimit(root string) (int64, error) {
	if root == "" {
		return 0, errors.New("environment cache root is required")
	}
	for {
		free, err := diskFree(root)
		if err == nil {
			return environmentCacheSize(free)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
		parent := filepath.Dir(root)
		if parent == root {
			return 0, err
		}
		root = parent
	}
}
func environmentCacheSize(free int64) (int64, error) {
	const floor, ceiling, reserve = int64(4 << 30), int64(80 << 30), int64(20 << 30)
	if free < reserve+floor {
		return 0, errors.New("environment cache auto-size requires 24 GiB free; supply an explicit --environment-cache-bytes limit")
	}
	return min(max(free/4, floor), ceiling, free-reserve), nil
}

// An explicit flag wins. Only the exact retired 20 GiB default migrates.
func ResolveEnvironmentCacheSizing(c *ServiceConfig, explicit bool) error {
	return resolveEnvironmentCacheSizing(c, explicit, AutoEnvironmentCacheLimit)
}

func resolveEnvironmentCacheSizing(c *ServiceConfig, explicit bool, size func(string) (int64, error)) error {
	if c.Role != "worker" || explicit || c.EnvironmentCacheBytes != 0 && c.EnvironmentCacheBytes != 20<<30 {
		return nil
	}
	limit, err := size(c.EnvironmentCache)
	if err != nil {
		return err
	}
	c.EnvironmentCacheBytes = limit
	return nil
}
