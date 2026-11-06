package myce

import (
	"errors"
	"os"
)

// AutoEnvironmentCacheLimit reports current physical headroom. Runtime caches
// keep zero as automatic and remeasure admission rather than saving this value.
func AutoEnvironmentCacheLimit(root string) (int64, error) {
	if root == "" {
		return 0, errors.New("environment cache root is required")
	}
	disk, err := workerDiskProbe(root)
	if err != nil {
		return 0, err
	}
	return environmentCacheSize(cacheBytes(disk.FreeBytes))
}
func environmentCacheSize(free int64) (int64, error) {
	return max(0, free-int64(os.Getpagesize())), nil
}

// An explicit flag wins. Only the exact retired 20 GiB default migrates.
func ResolveEnvironmentCacheSizing(c *ServiceConfig, explicit bool) error {
	return resolveEnvironmentCacheSizing(c, explicit, AutoEnvironmentCacheLimit)
}

func resolveEnvironmentCacheSizing(c *ServiceConfig, explicit bool, size func(string) (int64, error)) error {
	if c.Role != "worker" || explicit || c.EnvironmentCacheBytes != 0 && c.EnvironmentCacheBytes != 20<<30 {
		return nil
	}
	// Preserve automatic intent in generated services. An old implicit value can
	// migrate, but an explicitly supplied positive limit is never reinterpreted.
	c.EnvironmentCacheBytes = 0
	return nil
}
