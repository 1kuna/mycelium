package myce

import "context"

// Retention uses the same compiler use lock, pins and object recognition as
// worker admission. Only the compiler cache participates in queue relief.
func (b *workerDiskBudget) trimForFileStore(ctx context.Context, cache *workerCompilerCache, volume string, pressure workerFileStorePressure) {
	if pressure.ShortfallBytes == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	path := cache.cache.Root
	disk, err := b.probe(path)
	if err != nil || disk.Volume != volume || disk.FreeBytes >= pressure.TargetFreeBytes {
		return
	}
	// Recheck statfs between unlinks. Concurrent writes cannot enlarge this
	// one measured trim.
	enough := func() (bool, error) {
		disk, err := b.probe(path)
		return disk.Volume != volume || disk.FreeBytes >= pressure.TargetFreeBytes, err
	}
	limit := min(pressure.ShortfallBytes, pressure.TargetFreeBytes-disk.FreeBytes)
	// statfs can increase only by a complete allocation block.
	if disk.BlockBytes > 0 && limit%disk.BlockBytes != 0 {
		limit = addDiskBytes(limit, disk.BlockBytes-limit%disk.BlockBytes)
	}
	candidates, err := cache.budgetCandidates(ctx)
	if err != nil {
		return
	}
	for _, candidate := range candidates {
		_ = candidate.trim(ctx, enough, limit)
	}
}
