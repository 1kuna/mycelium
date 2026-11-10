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
	floors          map[string]map[*byte]uint64
	controlGrowth   map[string]uint64
	controlPrevious map[string]uint64
	inventoryDir    string
}

type diskBudgetFloorKey struct{}

type diskBudgetRoot struct {
	name, path string
	candidates func(context.Context) ([]diskBudgetCandidate, error)
}

type diskBudgetCandidate struct {
	path  string
	used  time.Time
	evict func(context.Context) (bool, error)
}

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
	Pressure               bool      `json:"pressure,omitempty"`
	Warning                string    `json:"warning,omitempty"`
	ControlGrowthBytes     uint64    `json:"control_growth_bytes"`
}

func newWorkerDiskBudget(clock Clock, probe func(string) (DiskReading, error)) *workerDiskBudget {
	if probe == nil {
		probe = workerDiskProbe
	}
	return &workerDiskBudget{clock: clock, probe: probe, reservations: map[string]uint64{}, previous: map[string]DiskBudgetVolume{}, floors: map[string]map[*byte]uint64{}, controlGrowth: map[string]uint64{}, controlPrevious: map[string]uint64{}}
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
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.probe(root)
	if err != nil || r.TotalBytes == 0 {
		return nil, errors.New("disk_headroom: filesystem measurement unavailable")
	}
	floor = max(floor, addDiskBytes(diskFreeFloor(r.TotalBytes), b.controlGrowth[r.Volume]))
	if err = b.reclaim(ctx, r.Volume, root, need, floor); err != nil {
		return nil, err
	}
	b.reservations[r.Volume] = addDiskBytes(b.reservations[r.Volume], need)
	token := new(byte)
	if b.floors[r.Volume] == nil {
		b.floors[r.Volume] = map[*byte]uint64{}
	}
	b.floors[r.Volume][token] = floor
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.reservations[r.Volume] -= need
			delete(b.floors[r.Volume], token)
			b.mu.Unlock()
		})
	}, nil
}

func (b *workerDiskBudget) reclaim(ctx context.Context, volume, path string, need, floor uint64) error {
	for _, active := range b.floors[volume] {
		floor = max(floor, active)
	}
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
	var candidates []diskBudgetCandidate
	for _, root := range b.roots {
		r, err := b.probe(root.path)
		if err != nil || r.Volume != volume || root.candidates == nil {
			continue
		}
		entries, err := root.candidates(ctx)
		if err != nil {
			continue
		} // Unknown/unavailable sources stay protected.
		candidates = append(candidates, entries...)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].used.Equal(candidates[j].used) {
			return candidates[i].path < candidates[j].path
		}
		return candidates[i].used.Before(candidates[j].used)
	})
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, _ = candidate.evict(ctx) // A failed proof never permits deletion.
		if ok, err := enough(); ok || err != nil {
			return err
		}
	}
	r, err := b.probe(path)
	if err != nil {
		return err
	}
	protected := addDiskBytes(b.reservations[volume], floor)
	available := r.FreeBytes - min(r.FreeBytes, protected)
	return &stagingCapacityError{required: int64(min(need, math.MaxInt64)), available: int64(min(available, math.MaxInt64)), limit: int64(min(r.TotalBytes-min(r.TotalBytes, floor), math.MaxInt64)), capacity: "disk_free_floor"}
}

// The existing retention lane owns inventory. Free-space deltas are compared
// with an inode-union allocation ledger; deletion savings always come from a
// fresh statfs reading rather than the sum of hardlinked candidate sizes.
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
		floor := addDiskBytes(diskFreeFloor(r.TotalBytes), b.controlGrowth[volume])
		for _, active := range b.floors[volume] {
			floor = max(floor, active)
		}
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
		v.Pressure = r.FreeBytes < addDiskBytes(v.FloorBytes, v.ReservedBytes)
		if old, ok := b.previous[volume]; ok && v.At.After(old.At) {
			// Signed growth: Mycelium cleanup can hide a simultaneous foreign write.
			growth := float64(old.FreeBytes) - float64(v.FreeBytes) - float64(v.OwnedBytes-old.OwnedBytes)
			if growth > 0 {
				v.UnexplainedGrowthBytes = uint64(min(float64(math.MaxInt64), growth))
				v.Warning = "foreign_disk_growth"
			}
			if old.FreeBytes > v.FreeBytes {
				seconds := float64(v.FreeBytes-min(v.FreeBytes, v.FloorBytes)) / (float64(old.FreeBytes-v.FreeBytes) / v.At.Sub(old.At).Seconds())
				v.ExhaustionSeconds = &seconds
			}
		}
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
