package myce

import (
	"errors"
	"fmt"
	"math"
)

// AutoEnvironmentCacheLimit uses the cache filesystem, including its nearest
// existing ancestor on fresh installs. It reserves 20 GiB and never allocates.
// It reads through the worker disk probe so synthetic test hosts cover it.
func AutoEnvironmentCacheLimit(root string) (int64, error) {
	if root == "" {
		return 0, errors.New("environment cache root is required")
	}
	reading, err := workerDiskProbe(root)
	if err != nil {
		return 0, err
	}
	return environmentCacheSize(int64(reading.FreeBytes))
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

// devEnvironmentCacheStart is a dev cache's first limit before anything is
// cached: fake and test environments take kilobytes to megabytes.
const devEnvironmentCacheStart = int64(1 << 30)

// needLimit sizes a dev cache from need: the larger of the current limit, the
// bytes needed now (cached environments plus the one being admitted) and the
// small start size, never past the host's room. Room is what the cache already
// holds plus free space above the volume's disk floor, the same floor that
// protects every worker's host. host describes that measurement for refusals.
func (c *EnvironmentCache) needLimit(limit, need int64) (int64, string, error) {
	used, err := c.DiskUsage()
	if err != nil {
		return 0, "", err
	}
	disk, err := workerDiskProbe(c.Root)
	if err != nil {
		return 0, "", err
	}
	floor := diskFreeFloor(disk.TotalBytes)
	room := used
	if disk.FreeBytes > floor {
		room += int64(min(disk.FreeBytes-floor, math.MaxInt64/2))
	}
	host := fmt.Sprintf("; host has %d bytes free with a %d-byte disk floor", disk.FreeBytes, floor)
	// A session starting with its volume already at the floor refuses up front;
	// a running one keeps what it holds and refuses each new environment.
	if limit == 0 && disk.FreeBytes <= floor || room <= 0 {
		return 0, host, fmt.Errorf("environment cache %s has no room%s; free space on that volume", c.Root, host)
	}
	return min(max(limit, need, used, devEnvironmentCacheStart), room), host, nil
}
