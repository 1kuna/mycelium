package myce

import "math"

// Automatic input caches use current physical headroom, rather than a fraction
// frozen at startup. Occupancy is added back once: eviction exchanges used
// blocks for free blocks without shrinking the allowance. Other writers' current
// use is already excluded by filesystem free space; foreignHeadroom keeps room
// for their measured swings and growth (see foreignDiskUse). The existing disk
// floor protects control state, logs and filesystem metadata.
func inputCacheBudget(root string, used, explicit int64, probe func(string) (DiskReading, error), foreignHeadroom uint64) (limit, free int64, err error) {
	if probe == nil {
		probe = workerDiskProbe
	}
	disk, err := probe(root)
	if err != nil {
		return 0, 0, err
	}
	floor := addDiskBytes(diskFreeFloor(disk.TotalBytes), foreignHeadroom)
	if disk.FreeBytes > floor {
		free = int64(min(uint64(math.MaxInt64), disk.FreeBytes-floor))
	}
	limit = explicit
	if explicit == 0 {
		limit = int64(min(uint64(math.MaxInt64), addDiskBytes(uint64(max(0, used)), uint64(free))))
	}
	return limit, free, nil
}
