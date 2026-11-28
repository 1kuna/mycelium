package myce

import (
	"context"
	"math/bits"
	"time"
)

type fileStoreDiskSample struct {
	at   time.Time
	disk DiskReading
}

// Net free-space loss measures concurrent allocations on the same volume,
// including writers outside the store. Project that rate over one ordinary
// hygiene interval. Short samples cannot magnify a one-off burst.
func (b *workerDiskBudget) measureFileStoreWrites(disk DiskReading) uint64 {
	now := b.clock.Now()
	old := b.fileStoreSample
	b.fileStoreSample = fileStoreDiskSample{now, disk}
	elapsed := now.Sub(old.at)
	if old.disk.Volume != disk.Volume || old.disk.TotalBytes != disk.TotalBytes ||
		elapsed <= 0 || disk.FreeBytes >= old.disk.FreeBytes {
		return 0
	}
	hi, lo := bits.Mul64(old.disk.FreeBytes-disk.FreeBytes, uint64(workerHygieneMinInterval))
	slack, _ := bits.Div64(hi, lo, uint64(max(workerHygieneMinInterval, elapsed)))
	return slack
}

// Retention uses the same compiler use lock, pins and object recognition as
// worker admission. Only the compiler cache participates in queue relief.
func (b *workerDiskBudget) trimForFileStore(ctx context.Context, cache *workerCompilerCache, volume string, pressure workerFileStorePressure, writeSlack uint64) {
	if pressure.ShortfallBytes == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	path := cache.cache.Root
	disk, err := b.probe(path)
	if err != nil || disk.Volume != volume || disk.FreeBytes >= pressure.TargetFreeBytes || pressure.TargetFreeBytes > disk.TotalBytes {
		return
	}
	limit := min(pressure.ShortfallBytes, pressure.TargetFreeBytes-disk.FreeBytes)
	target := pressure.TargetFreeBytes
	if pressure.BlockedWrite {
		writeSlack = min(writeSlack, disk.TotalBytes-target)
		target += writeSlack
		limit += writeSlack // The compiler trim can only reclaim eligible objects.
	}
	// Recheck statfs between unlinks. Concurrent writes cannot enlarge this
	// one measured trim.
	enough := func() (bool, error) {
		disk, err := b.probe(path)
		return disk.Volume != volume || disk.FreeBytes >= target, err
	}
	set, err := cache.budgetCandidates(ctx)
	if err != nil {
		if set.close != nil {
			set.close()
		}
		return
	}
	if set.close != nil {
		defer set.close()
	}
	sortDiskCandidates(set.entries, false)
	_ = evictDiskCandidates(ctx, set.entries, enough, roundDiskShortfall(limit, disk.BlockBytes), false)
	// Start the next rate sample after our own removals, so this trim
	// cannot hide allocations made between hygiene passes.
	if after, err := b.probe(path); err == nil && after.Volume == volume && after.TotalBytes == disk.TotalBytes {
		b.fileStoreSample = fileStoreDiskSample{b.clock.Now(), after}
	}
}
