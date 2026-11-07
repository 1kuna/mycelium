package myce

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// cacheBudgets learns reuse and coordinates all cache kinds with a host-local
// filesystem ledger. Allocation scans and physical probes never hold q.mu.
// Kernel-held claims retain pending blocks until their writer joins.
// A zero authored limit means borrowing available capacity, never a saved size.
type cacheBudgets struct {
	mu           sync.Mutex
	hostRoot     string
	probe        func(string) (DiskReading, error)
	allocation   func([]string) (int64, error)
	helpers      func(int) ([]int, error)
	volumes      map[string]*cacheVolume
	reservations map[*cacheReservation]struct{}
	reuse        map[string]cacheReuse
	copies       map[string]*copyGovernor
	maintenance  map[string]func(context.Context)
}
type cacheVolume struct {
	at            time.Time
	free, managed int64
	horizon       time.Duration
	drain         float64
	domains       map[string]cacheDemand
	floor         int64
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
	mu      sync.Mutex
	closing bool
}

func newCacheBudgets() *cacheBudgets {
	return &cacheBudgets{probe: workerDiskProbe, allocation: cacheFootprint, helpers: workerGroupMembers, volumes: map[string]*cacheVolume{}, reservations: map[*cacheReservation]struct{}{}, reuse: map[string]cacheReuse{}, copies: map[string]*copyGovernor{}}
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

// Inventories describe eviction demand, independently of the host ledger
// that measures owned allocations and physical free-space changes.
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
func (b *cacheBudgets) limit(root string, used, explicit int64, now time.Time) (int64, error) {
	disk, err := b.probe(root)
	if err != nil {
		return 0, err
	}
	return b.limitReading(root, used, explicit, now, disk)
}
func (b *cacheBudgets) limitReading(root string, used, explicit int64, now time.Time, disk DiskReading) (int64, error) {
	host, err := b.hostView(disk.Volume)
	if err != nil {
		return 0, err
	}
	defer host.close()
	fresh, err := b.probe(root)
	if err != nil {
		return 0, err
	}
	if fresh.Volume != disk.Volume {
		return 0, errors.New("cache volume changed during capacity observation")
	}
	disk = fresh
	if _, err = b.sampleReading(root, used, now, disk); err != nil {
		return 0, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	v := b.volumes[disk.Volume]
	host.sample(disk, now, 0)
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
			reclaim = min(debt, float64(d.used-d.hot))
		}
		if debt > float64(cold) && v.managed > cold {
			reclaim += min(debt-float64(cold), float64(d.hot))
		}
		limit = max(0, used-int64(math.Ceil(min(float64(used), reclaim))))
	}
	if explicit > 0 {
		limit = min(limit, explicit)
	}
	return limit, nil
}
func (b *cacheBudgets) reserve(root string, bytes int64, clock Clock, paths ...string) (*cacheReservation, error) {
	return b.reserveClaim(root, bytes, clock, "", "", paths...)
}
func (b *cacheBudgets) reserveClaim(root string, bytes int64, clock Clock, kind, custody string, paths ...string) (*cacheReservation, error) {
	disk, err := b.probe(root)
	if err != nil {
		return nil, err
	}
	host, err := b.hostView(disk.Volume)
	if err != nil {
		return nil, err
	}
	defer host.close()
	if custody != "" {
		if path := host.persistent[custody]; path != "" {
			f, err := os.OpenFile(path, os.O_RDWR, 0600)
			if err != nil {
				return nil, err
			}
			// Durable upload custody does not depend on this descriptor's lock.
			// A same-root store replacement may still hold the previous handle.
			var claim hostCacheClaim
			if err = readCacheRecord(f, &claim); err != nil {
				f.Close()
				return nil, err
			}
			if bytes < 0 || cacheWriteCharge(disk, bytes) > claim.Bytes {
				f.Close()
				return nil, errors.New("upload length exceeds its durable capacity promise")
			}
			r := &cacheReservation{ledger: b, volume: disk.Volume, bytes: claim.Bytes, clock: clock, host: f}
			b.mu.Lock()
			b.reservations[r] = struct{}{}
			b.mu.Unlock()
			return r, nil
		}
	}
	// Allocation observations precede this physical read. Pending blocks remain
	// charged across processes even before any payload pathname exists.
	disk, err = b.probe(root)
	if err != nil {
		return nil, err
	}
	host.sample(disk, clock.Now(), 0)
	charge := cacheWriteCharge(disk, bytes)
	prospective, err := json.Marshal(hostCacheClaim{Kind: kind, Custody: custody, Root: root, Bytes: charge, Paths: paths})
	if err != nil {
		return nil, err
	}
	host.recordBytes = max(host.recordBytes, int64(len(prospective)))
	free := cacheBytes(disk.FreeBytes) - cacheSum(host.floor(disk), host.pending)
	if bytes < 0 || charge > free {
		return nil, errors.New("insufficient shared filesystem cache headroom")
	}
	initial, err := b.allocation(paths)
	if err != nil {
		return nil, err
	}
	f, err := host.claim(hostCacheClaim{Kind: kind, Custody: custody, Root: root, Bytes: charge, Paths: paths, Initial: initial})
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
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.host == nil {
		return
	}
	r.closing = true
	b := r.ledger
	// A failed scan/publication retains the descriptor and map ownership. Idle
	// maintenance retries; neither sync.Once nor garbage collection owns release.
	host, err := b.hostView(r.volume)
	if err != nil {
		return
	}
	defer host.close()
	if err = host.save(); err != nil {
		return
	}
	if err = writeCacheRecord(r.host, hostCacheClaim{}); err != nil {
		return
	}
	delete(host.paths, r.host.Name())
	if err = host.resetOwned(); err != nil {
		return
	}
	if err = host.save(); err != nil {
		return
	}
	r.host.Close()
	r.host = nil
	b.mu.Lock()
	delete(b.reservations, r)
	b.mu.Unlock()
}

// Queue shutdown relinquishes its handle, not a resumable upload's promise.
func (r *cacheReservation) detach() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.host != nil {
		r.host.Close()
		r.host = nil
	}
	r.ledger.mu.Lock()
	delete(r.ledger.reservations, r)
	r.ledger.mu.Unlock()
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
	g.mu.Lock()
	g.measureControl = func() (time.Duration, error) {
		start := clock.Now()
		_, err := b.probe(root)
		return clock.Now().Sub(start), err
	}
	g.mu.Unlock()
	g.control(latency)
	defer cacheWait(ctx)()
	stream, err := g.acquire(ctx, explicit)
	if err != nil {
		return ctx, nil, err
	}
	return context.WithValue(ctx, cacheCopyProgressKey{}, stream.progress), stream.release, nil
}

type cacheCopyProgressKey struct{}
type cacheAdmissionKey struct{}
type cacheReservationKey struct{}

func withoutCopyProgress(ctx context.Context) context.Context {
	return context.WithValue(ctx, cacheCopyProgressKey{}, func(int64) {})
}

func cacheWait(ctx context.Context) func() {
	waiting, ok := ctx.Value(cacheAdmissionKey{}).(func(bool))
	if !ok {
		return func() {}
	}
	waiting(true)
	var once sync.Once
	return func() { once.Do(func() { waiting(false) }) }
}
