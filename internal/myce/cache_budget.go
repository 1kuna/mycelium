package myce

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// cacheBudgets accounts for all cache kinds on each physical filesystem. Only
// metadata probes run here, outside both this mutex and the queue mutex. A
// reservation is charged in full until its writer joins: double counting bytes
// already written is preferable to promising the same free blocks twice.
// A zero authored limit means borrowing available capacity, never a saved size.
type cacheBudgets struct {
	mu           sync.Mutex
	hostRoot     string
	probe        func(string) (DiskReading, error)
	allocation   func([]string) (int64, error)
	volumes      map[string]*cacheVolume
	reservations map[*cacheReservation]struct{}
	reuse        map[string]cacheReuse
	copies       map[string]*copyGovernor
	maintenance  map[string]func(context.Context)
}
type cacheVolume struct {
	at                     time.Time
	free, managed          int64
	horizon                time.Duration
	drain                  float64
	domains                map[string]cacheDemand
	control, controlGrowth int64
	floor                  int64
}
type cacheDemand struct{ used, hot int64 }
type cacheReuse struct {
	at       time.Time
	interval time.Duration
	bytes    int64
}
type cacheReservation struct {
	ledger  *cacheBudgets
	volume  string
	bytes   int64
	started time.Time
	clock   Clock
	host    *os.File
	once    sync.Once
}

func newCacheBudgets() *cacheBudgets {
	return &cacheBudgets{probe: workerDiskProbe, allocation: cacheFootprint, volumes: map[string]*cacheVolume{}, reservations: map[*cacheReservation]struct{}{}, reuse: map[string]cacheReuse{}, copies: map[string]*copyGovernor{}}
}
func cacheBytes(n uint64) int64 { return int64(min(n, uint64(math.MaxInt64))) }
func cacheFloatBytes(n float64) int64 {
	if n >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(max(0, n))
}
func cacheSum(a, b int64) int64 { return a + min(max(0, b), math.MaxInt64-a) }
func (b *cacheBudgets) touch(root, key string, bytes int64, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := root + "/" + key
	r := b.reuse[id]
	if !r.at.IsZero() && now.After(r.at) {
		r.interval = now.Sub(r.at)
	}
	r.at, r.bytes = now, bytes
	b.reuse[id] = r
}
func (b *cacheBudgets) recent(root, key string, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.reuse[root+"/"+key]
	return r.interval > 0 && now.Sub(r.at) < r.interval
}
func (b *cacheBudgets) forget(root, key string) {
	b.mu.Lock()
	delete(b.reuse, root+"/"+key)
	b.mu.Unlock()
}

// sample predicts external consumption over the longest observed sampling or
// writer interval. Managed growth is removed from that trend. The bootstrap is
// one OS page: Bavail already excludes the filesystem's reserved blocks; a page
// is the smallest control-write allocation we can justify before measurements.
func (b *cacheBudgets) sample(root string, used int64, now time.Time) (DiskReading, error) {
	disk, err := b.probe(root)
	if err != nil {
		return disk, err
	}
	return b.sampleReading(root, used, now, disk)
}
func (b *cacheBudgets) sampleReading(root string, used int64, now time.Time, disk DiskReading) (DiskReading, error) {
	if disk.TotalBytes == 0 {
		return disk, errors.New("disk capacity unavailable")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	v := b.volumes[disk.Volume]
	if v == nil {
		v = &cacheVolume{domains: map[string]cacheDemand{}}
		b.volumes[disk.Volume] = v
	}
	hot := int64(0)
	for id, r := range b.reuse {
		if filepath.Dir(id) == root && r.interval > 0 && now.Sub(r.at) < r.interval {
			hot = cacheSum(hot, r.bytes)
		}
	}
	v.domains[root] = cacheDemand{max(0, used), min(max(0, used), hot)}
	managed := int64(0)
	for _, d := range v.domains {
		managed = cacheSum(managed, d.used)
	}

	v.at, v.free, v.managed = now, cacheBytes(disk.FreeBytes), managed
	return disk, nil
}
func (b *cacheBudgets) floorLocked(v *cacheVolume) int64 {
	return v.floor
}
func (b *cacheBudgets) pendingLocked(volume string) int64 {
	n := int64(0)
	for r := range b.reservations {
		if r.volume == volume {
			n = cacheSum(n, r.bytes)
		}
	}
	return n
}
func (b *cacheBudgets) limit(root string, used, explicit int64, now time.Time) (int64, error) {
	disk, err := b.probe(root)
	if err != nil {
		return 0, err
	}
	return b.limitReading(root, used, explicit, now, disk)
}
func (b *cacheBudgets) limitReading(root string, used, explicit int64, now time.Time, disk DiskReading) (int64, error) {
	if _, err := b.sampleReading(root, used, now, disk); err != nil {
		return 0, err
	}
	host, err := b.hostView(disk.Volume)
	if err != nil {
		return 0, err
	}
	defer host.close()
	b.mu.Lock()
	defer b.mu.Unlock()
	v := b.volumes[disk.Volume]
	host.sample(disk, now, max(v.control, v.controlGrowth))
	if err = host.save(); err != nil {
		return 0, err
	}
	v.drain, v.horizon = host.state.Drain, host.state.Horizon
	floor := host.floor(disk)
	v.floor = floor
	spare := v.free - cacheSum(floor, cacheSum(max(0, host.pending-host.ownPending(root)), max(0, host.demand-host.demandRoots[root])))
	limit := cacheSum(max(0, used), max(0, spare))
	if spare < 0 {
		// Reclaim cold bytes across domains before applying pressure to the reused
		// working set. No domain gets an independent fraction of the same free disk.
		cold := int64(0)
		for _, d := range v.domains {
			cold = cacheSum(cold, d.used-d.hot)
		}
		d := v.domains[root]
		debt := float64(-spare)
		reclaim := float64(0)
		if cold > 0 {
			reclaim = min(debt, float64(cold)) * float64(d.used-d.hot) / float64(cold)
		}
		if debt > float64(cold) && v.managed > cold {
			reclaim += (debt - float64(cold)) * float64(d.hot) / float64(v.managed-cold)
		}
		limit = max(0, used-int64(math.Ceil(min(float64(used), reclaim))))
	}
	if explicit > 0 {
		limit = min(limit, explicit)
	}
	return limit, nil
}
func (b *cacheBudgets) reserve(root string, bytes int64, clock Clock, paths ...string) (*cacheReservation, error) {
	return b.reserveClaim(root, bytes, clock, false, paths...)
}
func (b *cacheBudgets) reserveClaim(root string, bytes int64, clock Clock, adopt bool, paths ...string) (*cacheReservation, error) {
	disk, err := b.probe(root)
	if err != nil {
		return nil, err
	}
	host, err := b.hostView(disk.Volume)
	if err != nil {
		return nil, err
	}
	defer host.close()
	// Allocation observations precede this physical read. Pending blocks remain
	// charged across processes even before any payload pathname exists.
	disk, err = b.probe(root)
	if err != nil {
		return nil, err
	}
	host.sample(disk, clock.Now(), 0)
	charge := cacheWriteCharge(disk, bytes)
	free := cacheBytes(disk.FreeBytes) - cacheSum(host.floor(disk), host.pending)
	if bytes < 0 || !adopt && charge > free {
		return nil, errors.New("insufficient shared filesystem cache headroom")
	}
	initial, err := b.allocation(paths)
	if err != nil {
		return nil, err
	}
	f, err := host.claim(hostCacheClaim{Root: root, Bytes: charge, Paths: paths, Initial: initial, Observed: initial})
	if err != nil {
		return nil, err
	}
	if err = host.save(); err != nil {
		f.Close()
		return nil, err
	}
	r := &cacheReservation{ledger: b, volume: disk.Volume, bytes: charge, started: clock.Now(), clock: clock, host: f}
	b.mu.Lock()
	b.reservations[r] = struct{}{}
	b.mu.Unlock()
	return r, nil
}
func (r *cacheReservation) release() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		b := r.ledger
		// Observe final owned allocations before dropping the kernel claim. A later
		// sample cannot mistake a completed download for external disk consumption.
		host, err := b.hostView(r.volume)
		if err == nil {
			host.state.Horizon = max(host.state.Horizon, r.clock.Now().Sub(r.started))
			// Clearing the record prevents future unrelated writes at a reused path
			// from being credited to a dead operation.
			err = writeCacheRecord(r.host, hostCacheClaim{})
			if err == nil {
				_ = host.save()
			}
			host.close()
		}
		if err == nil {
			r.host.Close()
		}
		b.mu.Lock()
		delete(b.reservations, r)
		b.mu.Unlock()
	})
}

// One controller per destination volume observes the actual source/network and
// local-write path together. Different cache kinds cannot independently saturate
// the same disk. All streams finish before the slot is released.
func (b *cacheBudgets) copy(ctx context.Context, root string, clock Clock, explicit int) (context.Context, func(), error) {
	started := clock.Now()
	disk, err := b.probe(root)
	latency := clock.Now().Sub(started)
	if err != nil {
		return ctx, nil, err
	}
	b.mu.Lock()
	g := b.copies[disk.Volume]
	if g == nil {
		g = &copyGovernor{clock: clock}
		b.copies[disk.Volume] = g
	}
	b.mu.Unlock()
	g.control(latency)
	if waiting, ok := ctx.Value(cacheAdmissionKey{}).(func(bool)); ok {
		waiting(true)
		defer waiting(false)
	}
	stream, err := g.acquire(ctx, explicit)
	if err != nil {
		return ctx, nil, err
	}
	return context.WithValue(ctx, cacheCopyProgressKey{}, stream.progress), stream.release, nil
}

type cacheCopyProgressKey struct{}
type cacheAdmissionKey struct{}

func withoutCopyProgress(ctx context.Context) context.Context {
	return context.WithValue(ctx, cacheCopyProgressKey{}, func(int64) {})
}

// The fixed SQLite/WAL/SHM set supplies measured control-write growth without
// traversing payloads. Keep room for another observed growth burst; inability to
// read these files never grants extra capacity.
func (s *FileStore) observeControl(ctx context.Context, volume string) error {
	if s.controlFiles.database == "" {
		return nil
	}
	seen := map[[2]uint64]bool{}
	total := uint64(0)
	for _, file := range []string{s.controlFiles.database, s.controlFiles.wal, s.controlFiles.shared} {
		n, err := storageAllocation(ctx, file, true, seen)
		if err != nil {
			return err
		}
		total = addDiskBytes(total, n)
	}
	s.budgets.mu.Lock()
	defer s.budgets.mu.Unlock()
	v := s.budgets.volumes[volume]
	if v == nil {
		v = &cacheVolume{domains: map[string]cacheDemand{}}
		s.budgets.volumes[volume] = v
	}
	n := cacheBytes(total)
	if v.control > 0 {
		v.controlGrowth = max(v.controlGrowth, n-v.control)
	}
	v.control = n
	return nil
}
