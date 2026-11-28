package myce

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
)

// The worker has one ledger, keyed by physical filesystem rather than cache.
// Reservations charge the full remaining write until its writer joins. Bytes
// already reflected in statfs may therefore be counted twice during a transfer;
// this deliberately trades temporary admission for avoiding overcommit.
// No method is called under q.mu or from a heartbeat. Candidate callbacks use
// nonblocking cache locks, so admission never waits for another budget owner.
type workerDiskBudget struct {
	mu              sync.Mutex
	clock           Clock
	probe           func(string) (DiskReading, error)
	roots           []diskBudgetRoot
	reservations    map[string]uint64
	previous        map[string]DiskBudgetVolume
	controlGrowth   map[string]uint64
	controlPrevious map[string]uint64
	declines        map[string]diskDecline
	cacheFloorRetry map[string]time.Time
	inventoryDir    string
	fileStoreSample fileStoreDiskSample // Accessed only by the serialized hygiene lane.
	// Read by cache sizing under cache locks, so it has its own short lock
	// rather than waiting behind a reclaim that holds mu.
	foreignMu sync.Mutex
	foreign   map[string]foreignDiskUse
}

// Other users' disk use on one volume: total minus free minus the allocated
// bytes Mycelium owns there (the queue's own store counts as another user).
// Their headroom is the larger of the room to return to their highest observed
// use and their fastest observed growth between two collections, the cadence at
// which this worker can evict. Below their peak the first covers a return to it;
// at the peak the second covers one more interval of growth. Both are measured
// since this worker started; on a host nobody else writes to, the headroom stays
// zero. A large permanent deletion by another user reserves at most the space it
// freed, so it never shrinks Mycelium below where it was before.
type foreignDiskUse struct{ current, peak, peakGrowth uint64 }

// diskDecline is one continuous free-space decline on a volume: it starts at
// the first collection whose free space falls below the previous one and
// ends at any collection back at or above its starting free space. It is
// in memory only; a restart begins a new episode.
type diskDecline struct {
	since     time.Time
	free      uint64
	ownedFrom int64
}

func (f foreignDiskUse) headroom() uint64 {
	return max(f.peak-min(f.peak, f.current), f.peakGrowth)
}

// Zero for an unmeasured volume or a worker without a budget.
func (b *workerDiskBudget) foreignHeadroom(path string) uint64 {
	if b == nil {
		return 0
	}
	r, err := b.probe(path)
	if err != nil {
		return 0
	}
	return b.foreignHeadroomFor(r.Volume)
}

func (b *workerDiskBudget) foreignHeadroomFor(volume string) uint64 {
	b.foreignMu.Lock()
	defer b.foreignMu.Unlock()
	return b.foreign[volume].headroom()
}

type diskBudgetFloorKey struct{}

type diskBudgetRoot struct {
	name, path string
	candidates func(context.Context) (diskBudgetCandidateSet, error)
	// Rebuildable entries that participate in the cache floor.
	cacheFloor bool
}

type diskBudgetCandidateSet struct {
	entries []diskBudgetCandidate
	close   func()
}

type diskBudgetCandidate struct {
	path  string
	used  time.Time
	class diskCacheClass
	evict func(context.Context) (bool, error)
	// Files have a cheap allocation estimate, trees are measured on demand.
	bytes uint64
	tree  bool
	bulk  bool // Package-manager pruning has no per-entry LRU or size bound.
}

type diskCacheClass uint8

const (
	// Private blobs and Go objects can be refetched or rebuilt independently.
	disposableDiskCache diskCacheClass = iota
	transferDiskCache                  // A complete verified shared input or model copy.
	preparedDiskCache                  // An installed environment requires a complete rebuild.
)

// Public evidence contains role names and opaque volume IDs, never host paths.
type DiskBudgetVolume struct {
	ID                     string    `json:"id"`
	At                     time.Time `json:"at"`
	Roots                  []string  `json:"roots"`
	FreeBytes              uint64    `json:"free_bytes"`
	FloorBytes             uint64    `json:"floor_bytes"`
	ReservedBytes          uint64    `json:"reserved_bytes"`
	OwnedBytes             int64     `json:"owned_bytes"`
	ReclaimedBytes         uint64    `json:"reclaimed_bytes"`
	UnexplainedGrowthBytes uint64    `json:"unexplained_growth_bytes"`
	ExhaustionSeconds      *float64  `json:"exhaustion_seconds,omitempty"`
	// The current continuous decline's age and whether bytes outside
	// Mycelium's own allocation explain most of it. Sent only to queues
	// advertising workerDiskTrendProtocol.
	DecliningSeconds   float64 `json:"declining_seconds,omitempty"`
	DeclineCause       string  `json:"decline_cause,omitempty"`
	Pressure           bool    `json:"pressure,omitempty"`
	Warning            string  `json:"warning,omitempty"`
	ControlGrowthBytes uint64  `json:"control_growth_bytes"`
	// Headroom kept free for other users, and its basis (foreignDiskUse).
	// Sent only to queues advertising workerDiskHeadroomProtocol.
	ForeignBytes           uint64 `json:"foreign_bytes,omitempty"`
	ForeignPeakBytes       uint64 `json:"foreign_peak_bytes,omitempty"`
	ForeignPeakGrowthBytes uint64 `json:"foreign_peak_growth_bytes,omitempty"`
	ForeignHeadroomBytes   uint64 `json:"foreign_headroom_bytes,omitempty"`
}

func newWorkerDiskBudget(clock Clock, probe func(string) (DiskReading, error)) *workerDiskBudget {
	if probe == nil {
		probe = workerDiskProbe
	}
	return &workerDiskBudget{clock: clock, probe: probe, reservations: map[string]uint64{}, previous: map[string]DiskBudgetVolume{}, controlGrowth: map[string]uint64{}, controlPrevious: map[string]uint64{}}
}

func (b *workerDiskBudget) add(root diskBudgetRoot) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.roots = append(b.roots, root)
}

func (b *workerDiskBudget) rootSnapshot() []diskBudgetRoot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]diskBudgetRoot(nil), b.roots...)
}

func (b *workerDiskBudget) reserve(ctx context.Context, root string, need uint64, floor uint64) (func(), error) {
	if b == nil {
		return func() {}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Waiting for the ledger and reclaiming local caches is admission work, not a
	// stalled transfer. A staging caller's 30s stall budget (#306/#308) watches
	// storage I/O; one environment tree or compiler cache delete can take longer,
	// and an unrenewed pulse here timed out a 9.3 GB input with no byte copied
	// on every retry. Cancellation still stops reclaim between candidates.
	defer keepIOProgress(ctx)()
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.probe(root)
	if err != nil || r.TotalBytes == 0 {
		return nil, errors.New("disk_headroom: filesystem measurement unavailable")
	}
	// A model-copy policy is a condition on this admission, not physical
	// headroom for every writer on the volume. Share pending bytes, but derive
	// other admissions and collection floors from their own live measurements.
	floor = max(floor, addDiskBytes(addDiskBytes(diskFreeFloor(r.TotalBytes), b.controlGrowth[r.Volume]), b.foreignHeadroomFor(r.Volume)))
	b.trimCacheFloor(ctx, root, r, need)
	if err = b.reclaim(ctx, r.Volume, root, need, floor); err != nil {
		return nil, err
	}
	b.reservations[r.Volume] = addDiskBytes(b.reservations[r.Volume], need)
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.reservations[r.Volume] -= need
			b.mu.Unlock()
		})
	}, nil
}

// The pulse cadence is well inside the stall budget so a watcher sampling at any
// moment sees a recent renewal. Tests shorten it.
var ioProgressKeepalive = time.Second

// keepIOProgress renews a staging caller's progress until the returned stop runs.
// Without a progress hook in ctx it does nothing.
func keepIOProgress(ctx context.Context) func() {
	progress, ok := ctx.Value(ioProgressKey{}).(func())
	if !ok {
		return func() {}
	}
	progress()
	done, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		tick := time.NewTicker(ioProgressKeepalive)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
				progress()
			}
		}
	}()
	return func() { close(done); <-joined; progress() }
}

func (b *workerDiskBudget) reclaim(ctx context.Context, volume, path string, need, floor uint64) error {
	enough := func() (bool, error) {
		r, err := b.probe(path)
		if err != nil {
			return false, err
		}
		demand := addDiskBytes(addDiskBytes(b.reservations[volume], need), floor)
		return r.FreeBytes >= demand, nil
	}
	if ok, err := enough(); ok || err != nil {
		return err
	}
	candidates, close := b.evictionCandidates(ctx, volume, false)
	defer close()
	sortDiskCandidates(candidates, true)
	if err := evictDiskCandidates(ctx, candidates, enough, 0, false); err != nil {
		return err
	}
	if ok, err := enough(); ok || err != nil {
		return err
	}
	r, err := b.probe(path)
	if err != nil {
		return err
	}
	protected := addDiskBytes(b.reservations[volume], floor)
	available := r.FreeBytes - min(r.FreeBytes, protected)
	return &stagingCapacityError{required: int64(min(need, math.MaxInt64)), available: int64(min(available, math.MaxInt64)), limit: int64(min(r.TotalBytes-min(r.TotalBytes, floor), math.MaxInt64)), capacity: "disk_free_floor"}
}

// cacheFreeFloorBytes is the free space Mycelium's rebuildable caches leave on
// their volume so the host can still install and update software (maintainer,
// 2026-11-26: about 40 GiB on every fleet Mac). On a volume under 160 GiB a
// quarter of it is the floor instead, so the caches stay usable there; never
// below the admission floor. Admission does not refuse work because of it.
const cacheFreeFloorBytes = 40 << 30

func cacheFreeFloor(total uint64) uint64 {
	return max(min(cacheFreeFloorBytes, total/4), diskFreeFloor(total))
}

// evictionCandidates inventories a single volume and borrows any open cache
// descriptors until the caller finishes the pass.
func (b *workerDiskBudget) evictionCandidates(ctx context.Context, volume string, cacheFloor bool) ([]diskBudgetCandidate, func()) {
	var candidates []diskBudgetCandidate
	var closing []func()
	for _, root := range b.roots {
		if root.candidates == nil || cacheFloor && !root.cacheFloor {
			continue
		}
		r, err := b.probe(root.path)
		if err != nil || r.Volume != volume {
			continue
		}
		set, err := root.candidates(ctx)
		if set.close != nil {
			closing = append(closing, set.close)
		}
		if err != nil {
			continue // Unknown/unavailable sources stay protected.
		}
		candidates = append(candidates, set.entries...)
	}
	return candidates, func() {
		for _, close := range closing {
			close()
		}
	}
}

func sortDiskCandidates(candidates []diskBudgetCandidate, byCost bool) {
	sort.Slice(candidates, func(i, j int) bool {
		if byCost && candidates[i].class != candidates[j].class {
			return candidates[i].class < candidates[j].class
		}
		if candidates[i].used.Equal(candidates[j].used) {
			return candidates[i].path < candidates[j].path
		}
		return candidates[i].used.Before(candidates[j].used)
	})
}

// With a measured shortfall, cap the total attempted allocation per pass.
// Whole entries may supply the final increment when they cannot be split.
// Go's independent objects keep the strict cap for queue file-store relief.
func evictDiskCandidates(ctx context.Context, candidates []diskBudgetCandidate, enough func() (bool, error), limit uint64, wholeEntries bool) error {
	bounded := limit > 0
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		if ok, err := enough(); ok || err != nil {
			return err
		}
		bytes := candidate.bytes
		if bounded {
			if candidate.bulk {
				continue
			}
			if candidate.tree {
				n, err := cacheEntryAllocation(ctx, candidate.path)
				if err != nil {
					continue
				}
				bytes = uint64(n)
			}
			if bytes == 0 || !wholeEntries && bytes > limit {
				continue
			}
		}
		removed, _ := candidate.evict(ctx)
		if bounded && removed {
			limit -= min(bytes, limit)
			if limit == 0 {
				break
			}
		}
	}
	return nil
}

func roundDiskShortfall(limit, block uint64) uint64 {
	if block > 0 && limit%block != 0 {
		return addDiskBytes(limit, block-limit%block)
	}
	return limit
}

// trimCacheFloor leaves one budget for rebuildable entries on each volume.
// Forecast space already promised to writers and measured competing use;
// a full filesystem block above the floor keeps an idle pass proactive at
// the boundary. If protected entries prevent trimming, ordinary admission
// still uses its separate physical floor. The caller holds b.mu.
func (b *workerDiskBudget) trimCacheFloor(ctx context.Context, path string, r DiskReading, need uint64) {
	floor := max(cacheFreeFloor(r.TotalBytes), addDiskBytes(diskFreeFloor(r.TotalBytes), b.controlGrowth[r.Volume]))
	target := addDiskBytes(floor, max(r.BlockBytes, b.foreignHeadroomFor(r.Volume)))
	target = min(r.TotalBytes, addDiskBytes(target, b.reservations[r.Volume]))
	if need <= r.TotalBytes-target {
		target += need
	}
	if r.FreeBytes >= target || b.clock.Now().Before(b.cacheFloorRetry[r.Volume]) {
		return
	}
	limit := roundDiskShortfall(target-r.FreeBytes, r.BlockBytes)
	enough := func() (bool, error) {
		d, err := b.probe(path)
		return d.Volume != r.Volume || d.FreeBytes >= target, err
	}
	candidates, close := b.evictionCandidates(ctx, r.Volume, true)
	defer close()
	sortDiskCandidates(candidates, false)
	_ = evictDiskCandidates(ctx, candidates, enough, limit, true)
	if ok, err := enough(); !ok || err != nil {
		if b.cacheFloorRetry == nil {
			b.cacheFloorRetry = map[string]time.Time{}
		}
		b.cacheFloorRetry[r.Volume] = b.clock.Now().Add(workerHygieneMinInterval)
	} else {
		delete(b.cacheFloorRetry, r.Volume)
	}
}

// A fresh statfs shortfall can use the retention lane without waiting on the
// adaptive backoff of its full allocation inventory.
func (b *workerDiskBudget) reclaimPressure(ctx context.Context) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	seen := map[string]bool{}
	for _, root := range b.roots {
		r, err := b.probe(root.path)
		if err != nil || r.TotalBytes == 0 || seen[r.Volume] {
			continue
		}
		seen[r.Volume] = true
		b.trimCacheFloor(ctx, root.path, r, 0)
		floor := addDiskBytes(addDiskBytes(diskFreeFloor(r.TotalBytes), b.controlGrowth[r.Volume]), b.foreignHeadroomFor(r.Volume))
		if r.FreeBytes < addDiskBytes(floor, b.reservations[r.Volume]) {
			_ = b.reclaim(ctx, r.Volume, root.path, 0, floor)
		}
	}
}

// The existing retention lane owns inventory. Free-space deltas are compared
// with an inode-union allocation ledger; deletion savings always come from a
// fresh statfs reading rather than the sum of hardlinked candidate sizes.
func (b *workerDiskBudget) observeForeign(volume string, current, growth uint64) foreignDiskUse {
	b.foreignMu.Lock()
	defer b.foreignMu.Unlock()
	if b.foreign == nil {
		b.foreign = map[string]foreignDiskUse{}
	}
	f := b.foreign[volume]
	f.current, f.peak, f.peakGrowth = current, max(f.peak, current), max(f.peakGrowth, growth)
	b.foreign[volume] = f
	return f
}

func (b *workerDiskBudget) collect(ctx context.Context) []DiskBudgetVolume {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	groups := map[string][]diskBudgetRoot{}
	for _, root := range b.roots {
		r, err := b.probe(root.path)
		if err == nil && r.TotalBytes > 0 {
			groups[r.Volume] = append(groups[r.Volume], root)
		}
	}
	var out []DiskBudgetVolume
	for volume, roots := range groups {
		r, err := b.probe(roots[0].path)
		if err != nil {
			continue
		}
		before := r.FreeBytes
		b.trimCacheFloor(ctx, roots[0].path, r, 0)
		base := addDiskBytes(diskFreeFloor(r.TotalBytes), b.controlGrowth[volume])
		floor := addDiskBytes(base, b.foreignHeadroomFor(volume))
		_ = b.reclaim(ctx, volume, roots[0].path, 0, floor)
		r, err = b.probe(roots[0].path)
		if err != nil {
			continue
		}
		v := DiskBudgetVolume{ID: identityHash(volume), At: b.clock.Now(), FreeBytes: r.FreeBytes, FloorBytes: floor, ReservedBytes: b.reservations[volume], ControlGrowthBytes: b.controlGrowth[volume]}
		if r.FreeBytes > before {
			v.ReclaimedBytes = r.FreeBytes - before
		}
		paths := []string{}
		for _, root := range roots {
			v.Roots = append(v.Roots, root.name)
			paths = append(paths, filepath.Clean(root.path))
		}
		inventoryCtx := ctx
		ancestor := roots[0].path
		for {
			info, e := os.Stat(ancestor)
			if e == nil {
				if stat, ok := info.Sys().(*syscall.Stat_t); ok {
					inventoryCtx = context.WithValue(ctx, collectionDeviceKey{}, uint64(stat.Dev))
				}
				break
			}
			if !os.IsNotExist(e) || filepath.Dir(ancestor) == ancestor {
				break
			}
			ancestor = filepath.Dir(ancestor)
		}
		v.OwnedBytes, err = collectionAllocatedBytes(inventoryCtx, paths...)
		if err != nil {
			v.Warning = "disk_inventory_unavailable"
			out = append(out, v)
			continue
		}
		if old, ok := b.previous[volume]; ok && v.At.After(old.At) {
			// Signed growth: Mycelium cleanup can hide a simultaneous foreign write.
			growth := float64(old.FreeBytes) - float64(v.FreeBytes) - float64(v.OwnedBytes-old.OwnedBytes)
			if growth > 0 {
				v.UnexplainedGrowthBytes = uint64(min(float64(math.MaxInt64), growth))
				v.Warning = "foreign_disk_growth"
			}
		}
		foreign := b.observeForeign(volume, r.TotalBytes-min(r.TotalBytes, addDiskBytes(r.FreeBytes, uint64(max(0, v.OwnedBytes)))), v.UnexplainedGrowthBytes)
		v.ForeignBytes, v.ForeignPeakBytes, v.ForeignPeakGrowthBytes, v.ForeignHeadroomBytes = foreign.current, foreign.peak, foreign.peakGrowth, foreign.headroom()
		if next := addDiskBytes(base, v.ForeignHeadroomBytes); next > v.FloorBytes {
			// New foreign evidence raised the headroom: evict for it now rather
			// than at the next collection. Keep this sample's free/owned pair so
			// the next growth comparison stays consistent.
			b.trimCacheFloor(ctx, roots[0].path, r, 0)
			_ = b.reclaim(ctx, volume, roots[0].path, 0, next)
			v.FloorBytes = next
		}
		if old, ok := b.previous[volume]; ok && v.At.After(old.At) {
			b.observeDecline(volume, &v, old) // Against the final floor.
		}
		v.Pressure = r.FreeBytes < addDiskBytes(v.FloorBytes, v.ReservedBytes)
		b.previous[volume] = v
		if b.inventoryDir != "" {
			if control, err := b.probe(b.inventoryDir); err == nil && control.Volume == volume {
				entries, err := os.ReadDir(b.inventoryDir)
				if err == nil {
					var paths []string
					for _, entry := range entries {
						if entry.Type().IsRegular() {
							paths = append(paths, filepath.Join(b.inventoryDir, entry.Name()))
						}
					}
					if size, err := collectionAllocatedBytes(ctx, paths...); err == nil {
						if old, ok := b.controlPrevious[volume]; ok {
							b.controlGrowth[volume] = uint64(max(0, size-int64(old)))
						}
						b.controlPrevious[volume] = uint64(size)
					}
				}
			}
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if b.inventoryDir != "" {
		type localRoot struct {
			Name string `json:"role"`
			Path string `json:"path"`
		}
		inventory := struct {
			Volumes []DiskBudgetVolume `json:"volumes"`
			Roots   []localRoot        `json:"roots"`
		}{Volumes: out}
		for _, root := range b.roots {
			inventory.Roots = append(inventory.Roots, localRoot{root.name, root.path})
		}
		_ = saveWorkerJSON(b.inventoryDir, "disk-inventory.json", inventory)
	}
	return out
}

// observeDecline measures the episode's average consumption rate, not one
// collection step, so a brief stall or burst does not swing the forecast.
// exhaustion_seconds is the time to the free floor at that rate.
func (b *workerDiskBudget) observeDecline(volume string, v *DiskBudgetVolume, old DiskBudgetVolume) {
	if b.declines == nil {
		b.declines = map[string]diskDecline{}
	}
	d, ok := b.declines[volume]
	if !ok || v.FreeBytes >= d.free {
		delete(b.declines, volume)
		if old.FreeBytes <= v.FreeBytes {
			return
		}
		d = diskDecline{since: old.At, free: old.FreeBytes, ownedFrom: old.OwnedBytes}
		b.declines[volume] = d
	}
	elapsed := v.At.Sub(d.since).Seconds()
	if elapsed <= 0 {
		return
	}
	consumed := float64(d.free - v.FreeBytes)
	seconds := float64(v.FreeBytes-min(v.FreeBytes, v.FloorBytes)) / (consumed / elapsed)
	v.ExhaustionSeconds, v.DecliningSeconds = &seconds, elapsed
	v.DeclineCause = "owned_disk_growth"
	if owned := float64(v.OwnedBytes - d.ownedFrom); consumed-owned > owned {
		v.DeclineCause = "foreign_disk_growth"
	}
}
