package myce

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Local, same-account processes coordinate pending blocks with kernel-owned
// locks. These are not leases: death releases a claim, no elapsed time grants
// ownership, and no other process's payload is removed. Inactive scalar slots
// are reused. The coordination directory contains no payload or credentials.
type hostCacheState struct {
	Controls                     map[string]int64
	ControlNeeds                 map[string]int64
	At                           time.Time
	Free, Written, SampleWritten int64
	Horizon                      time.Duration
	Drain                        float64
	Control                      int64
}
type hostCacheClaim struct {
	ControlGrowth     int64
	ControlPage       int64
	ControlUnit       int64
	Kind              string
	Root              string
	Bytes             int64
	Paths             []string
	Initial, Observed int64
}
type hostCacheView struct {
	valid           bool
	state           hostCacheState
	pending, demand int64
	demandRoots     map[string]int64
	pendingRoots    map[string]int64
	file            *os.File
	slot            *os.File
	directory       string
}

func (b *cacheBudgets) hostDirectory(volume string) (string, error) {
	root := b.hostRoot
	if root == "" {
		var err error
		root, err = os.UserCacheDir()
		if err != nil {
			return "", err
		}
		host, err := os.Hostname()
		if err != nil {
			return "", err
		}
		root = filepath.Join(root, "mycelium", "cache-capacity", identityHash(host))
	}
	return filepath.Join(root, identityHash(volume)), nil
}
func cacheFootprint(paths []string) (int64, error) {
	seen := map[[2]uint64]bool{}
	total := int64(0)
	for _, root := range paths {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			st, ok := info.Sys().(*syscall.Stat_t)
			if !ok || st.Blocks < 0 {
				return errors.New("cache allocation unavailable")
			}
			id := [2]uint64{uint64(st.Dev), uint64(st.Ino)}
			if !seen[id] {
				seen[id] = true
				total = cacheSum(total, cacheBytes(uint64(st.Blocks)*512))
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}
func writeCacheRecord(f *os.File, v any) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		return err
	}
	return f.Truncate(int64(len(data)))
}
func readCacheRecord(f *os.File, v any) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	err := json.NewDecoder(f).Decode(v)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// The filesystem mutex protects only admission metadata, outside q.mu. Probe
// after measuring claims, so the same blocks cannot be credited as both written
// bytes and old free space. Malformed accounting refuses admission.
func (b *cacheBudgets) hostView(volume string) (*hostCacheView, error) {
	dir, err := b.hostDirectory(volume)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "state"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	v := &hostCacheView{file: f, directory: dir, demandRoots: map[string]int64{}, pendingRoots: map[string]int64{}}
	fail := func(err error) (*hostCacheView, error) { v.close(); return nil, err }
	if err = readCacheRecord(f, &v.state); err != nil {
		return fail(err)
	}
	v.valid = true
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fail(err)
	}
	controls := map[string]int64{}
	needs := map[string]int64{}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".claim") {
			continue
		}
		claim, err := os.OpenFile(filepath.Join(dir, entry.Name()), os.O_RDWR, 0600)
		if err != nil {
			return fail(err)
		}
		lockErr := syscall.Flock(int(claim.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		active := errors.Is(lockErr, syscall.EWOULDBLOCK) || errors.Is(lockErr, syscall.EAGAIN)
		if lockErr != nil && !active {
			claim.Close()
			return fail(lockErr)
		}
		var c hostCacheClaim
		if err = readCacheRecord(claim, &c); err == nil {
			var used int64
			used, err = b.allocation(c.Paths)
			if err == nil {
				// Signed net allocation includes interrupted writes and owned scratch
				// removal, independently of asynchronously refreshed cache inventories.
				if c.Kind == "control" {
					if active {
						c.ControlGrowth = max(c.ControlGrowth, used-c.Observed)
						controls[c.Root] = used
						wal, allocationErr := b.allocation(c.Paths[1:])
						if allocationErr != nil {
							err = allocationErr
						} else {
							// SQLite's WAL-index region is 32 KiB; a WAL starts with a
							// 32-byte header and a 24-byte frame header plus one DB page.
							bootstrap := cacheSum(cacheRound(32<<10, c.ControlUnit), cacheRound(cacheSum(c.ControlPage, 32+24), c.ControlUnit))
							needs[c.Root] = max(needs[c.Root], wal, c.ControlGrowth, bootstrap)
						}
					}
				} else {
					v.state.Written += used - c.Observed
				}
				c.Observed = used
				if active {
					if c.Kind == "control" {
						// Controls reserve their next measured rewrite, not payload blocks.
					} else if c.Kind == "demand" {
						v.demand = cacheSum(v.demand, c.Bytes)
						v.demandRoots[c.Root] = cacheSum(v.demandRoots[c.Root], c.Bytes)
					} else {
						remaining := max(0, c.Bytes-max(0, used-c.Initial))
						v.pending = cacheSum(v.pending, remaining)
						v.pendingRoots[c.Root] = cacheSum(v.pendingRoots[c.Root], remaining)
					}
				}
				if !active {
					c = hostCacheClaim{}
				}
				if err == nil {
					err = writeCacheRecord(claim, c)
				}
			}
		}
		if !active && v.slot == nil {
			v.slot = claim
		} else {
			claim.Close()
		}
		if err != nil {
			return fail(err)
		}
	}
	for root, current := range controls {
		if previous, ok := v.state.Controls[root]; ok {
			v.state.Written += current - previous
		}
	}
	v.state.Controls, v.state.ControlNeeds = controls, needs
	v.state.Control = 0
	for _, n := range needs {
		v.state.Control = cacheSum(v.state.Control, n)
	}
	return v, nil
}
func (v *hostCacheView) close() {
	if v.valid {
		_ = v.save()
	}
	if v.slot != nil {
		v.slot.Close()
	}
	if v.file != nil {
		syscall.Flock(int(v.file.Fd()), syscall.LOCK_UN)
		v.file.Close()
	}
}
func (v *hostCacheView) save() error { return writeCacheRecord(v.file, v.state) }
func (v *hostCacheView) claim(c hostCacheClaim) (*os.File, error) {
	f := v.slot
	if f != nil {
		v.slot = nil
	} else {
		entries, err := os.ReadDir(v.directory)
		if err != nil {
			return nil, err
		}
		// At most one slot per simultaneously live claim, plus the state file.
		f, err = os.OpenFile(filepath.Join(v.directory, strconv.Itoa(len(entries))+".claim"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
			f.Close()
			return nil, err
		}
	}
	err := writeCacheRecord(f, c)
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
func (v *hostCacheView) sample(disk DiskReading, now time.Time, control int64) {
	s := &v.state
	if !s.At.IsZero() && now.After(s.At) {
		interval := now.Sub(s.At)
		loss := (s.Free - cacheBytes(disk.FreeBytes)) - (s.Written - s.SampleWritten)
		s.Horizon = interval
		s.Drain = float64(max(0, loss)) / interval.Seconds()
	}
	s.At, s.Free, s.SampleWritten = now, cacheBytes(disk.FreeBytes), s.Written
	s.Control = max(s.Control, control)
}
func cacheAllocationUnit(d DiskReading) int64 {
	// Synthetic probes may omit an allocation unit; 512 bytes is the allocation
	// counter's physical sector unit, not a host-specific memory page size.
	return max(512, cacheBytes(d.AllocationBytes))
}
func (v *hostCacheView) floor(d DiskReading) int64 {
	// Four separate control objects may need a fresh allocation: a capacity
	// record, its owner claim, publication metadata, and its parent directory.
	// Queue control also reserves a rewrite of its observed SQLite/WAL/SHM set.
	return cacheSum(cacheSum(4*cacheAllocationUnit(d), v.state.Control), cacheFloatBytes(v.state.Drain*v.state.Horizon.Seconds()))
}
func cacheWriteCharge(d DiskReading, bytes int64) int64 {
	unit := cacheAllocationUnit(d)
	if bytes < 0 {
		return bytes
	}
	rounded := bytes
	if remainder := bytes % unit; remainder > 0 {
		rounded = cacheSum(bytes, unit-remainder)
	}
	// Payload publication also allocates ready/verification records and a parent
	// directory; the logical payload's final block is rounded independently.
	return cacheSum(rounded, 3*unit)
}

// Demand is observable by all cache domains, including other local processes.
// It grants no write authority and survives exactly as long as its owner lock.
func (b *cacheBudgets) borrow(ctx context.Context, root string, bytes int64) (func(), error) {
	defer cacheWait(ctx)()
	disk, err := b.probe(root)
	if err != nil {
		return nil, err
	}
	v, err := b.hostView(disk.Volume)
	if err != nil {
		return nil, err
	}
	f, err := v.claim(hostCacheClaim{Kind: "demand", Root: root, Bytes: cacheWriteCharge(disk, bytes)})
	if err == nil {
		err = v.save()
	}
	v.close()
	if err != nil {
		if f != nil {
			f.Close()
		}
		return nil, err
	}
	b.maintain(ctx)
	var once sync.Once
	return func() { once.Do(func() { f.Close() }) }, nil
}

// An unmeasured native installer has no declared size. Explore alone, fencing
// all remaining allocatable blocks from sibling writers until its process joins.
// The existing build monitor cancels on measured pressure; this is admission
// coordination, not an OS quota for an arbitrary native executable.
func (b *cacheBudgets) explore(root string, clock Clock, paths ...string) (*cacheReservation, error) {
	disk, err := b.probe(root)
	if err != nil {
		return nil, err
	}
	host, err := b.hostView(disk.Volume)
	if err != nil {
		return nil, err
	}
	defer host.close()
	disk, err = b.probe(root)
	if err != nil {
		return nil, err
	}
	host.sample(disk, clock.Now(), 0)
	if host.pending > 0 {
		return nil, errors.New("environment sizing waits for active volume writers")
	}
	bytes := cacheBytes(disk.FreeBytes) - host.floor(disk)
	if bytes <= 0 {
		return nil, errors.New("environment sizing has no filesystem headroom")
	}
	initial, err := b.allocation(paths)
	if err != nil {
		return nil, err
	}
	f, err := host.claim(hostCacheClaim{Kind: "explore", Root: root, Bytes: bytes, Paths: paths, Initial: initial, Observed: initial})
	if err != nil {
		return nil, err
	}
	if err = host.save(); err != nil {
		f.Close()
		return nil, err
	}
	r := &cacheReservation{ledger: b, volume: disk.Volume, bytes: bytes, started: clock.Now(), clock: clock, host: f}
	b.mu.Lock()
	b.reservations[r] = struct{}{}
	b.mu.Unlock()
	return r, nil
}

func (v *hostCacheView) ownPending(root string) int64 {
	n := int64(0)
	for scope, bytes := range v.pendingRoots {
		if root == scope || strings.HasPrefix(root, scope+string(filepath.Separator)) {
			n = cacheSum(n, bytes)
		}
	}
	return n
}

// Each file has its own rounded data allocation and inode/entry allowance.
// Directory allocations are counted once per distinct ancestor. This prevents
// many tiny files from being admitted as a single packed logical byte string.
func cacheManifestCharge(disk DiskReading, files FileManifest) int64 {
	unit := cacheAllocationUnit(disk)
	dirs := map[string]bool{".": true}
	total := 3 * unit // ready, verification record, publication parent
	for name, ref := range files {
		rounded := ref.SizeBytes
		if remainder := rounded % unit; remainder > 0 {
			rounded = cacheSum(rounded, unit-remainder)
		}
		total = cacheSum(total, cacheSum(rounded, unit))
		if name == "." {
			continue
		}
		for dir := filepath.Dir(name); dir != "."; dir = filepath.Dir(dir) {
			dirs[dir] = true
		}
	}
	for range dirs {
		total = cacheSum(total, unit)
	}
	return total
}
func (r *cacheReservation) ensureCharge(root string, charge int64) error {
	b := r.ledger
	host, err := b.hostView(r.volume)
	if err != nil {
		return err
	}
	defer host.close()
	var claim hostCacheClaim
	if err = readCacheRecord(r.host, &claim); err != nil {
		return err
	}
	if charge <= claim.Bytes {
		return nil
	}
	disk, err := b.probe(root)
	if err != nil {
		return err
	}
	if disk.Volume != r.volume {
		return errors.New("cache volume changed during reservation")
	}
	available := cacheBytes(disk.FreeBytes) - cacheSum(host.floor(disk), host.pending)
	if charge-claim.Bytes > available {
		return errors.New("insufficient filesystem allocation space for cache manifest")
	}
	claim.Bytes = charge
	if err = writeCacheRecord(r.host, claim); err != nil {
		return err
	}
	if err = host.save(); err != nil {
		return err
	}
	b.mu.Lock()
	r.bytes = charge
	b.mu.Unlock()
	return nil
}

func (r *cacheReservation) addPath(path string) error {
	host, err := r.ledger.hostView(r.volume)
	if err != nil {
		return err
	}
	defer host.close()
	var claim hostCacheClaim
	if err = readCacheRecord(r.host, &claim); err != nil {
		return err
	}
	size, err := r.ledger.allocation([]string{path})
	if err != nil {
		return err
	}
	claim.Paths = append(claim.Paths, path)
	claim.Initial = cacheSum(claim.Initial, size)
	claim.Observed = cacheSum(claim.Observed, size)
	return writeCacheRecord(r.host, claim)
}
func (r *cacheReservation) ensureMetadata(root string, bytes int64) error {
	host, err := r.ledger.hostView(r.volume)
	if err != nil {
		return err
	}
	var claim hostCacheClaim
	err = readCacheRecord(r.host, &claim)
	host.close()
	if err != nil {
		return err
	}
	used, err := r.ledger.allocation(claim.Paths)
	if err != nil {
		return err
	}
	disk, err := r.ledger.probe(root)
	if err != nil {
		return err
	}
	return r.ensureCharge(root, cacheSum(max(0, used-claim.Initial), cacheWriteCharge(disk, bytes)))
}

func (b *cacheBudgets) trackControl(root string, clock Clock, paths ...string) (*cacheReservation, error) {
	disk, err := b.probe(root)
	if err != nil {
		return nil, err
	}
	host, err := b.hostView(disk.Volume)
	if err != nil {
		return nil, err
	}
	defer host.close()
	page, err := sqliteControlPage(paths[0])
	if err != nil {
		return nil, err
	}
	initial, err := b.allocation(paths)
	if err != nil {
		return nil, err
	}
	f, err := host.claim(hostCacheClaim{Kind: "control", Root: root, Paths: paths, Initial: initial, Observed: initial, ControlPage: page, ControlUnit: cacheAllocationUnit(disk)})
	if err != nil {
		return nil, err
	}
	return &cacheReservation{ledger: b, volume: disk.Volume, started: clock.Now(), clock: clock, host: f}, nil
}

func sqliteControlPage(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var header [18]byte
	if _, err = io.ReadFull(f, header[:]); err != nil {
		return 0, err
	}
	if string(header[:16]) != "SQLite format 3\x00" {
		return 0, errors.New("SQLite control format unavailable")
	}
	page := int64(binary.BigEndian.Uint16(header[16:]))
	if page == 1 {
		page = 65536
	}
	if page < 512 || page > 65536 || page&(page-1) != 0 {
		return 0, errors.New("SQLite page geometry unavailable")
	}
	return page, nil
}

func cacheRound(bytes, unit int64) int64 {
	unit = max(512, unit)
	if remainder := bytes % unit; remainder > 0 {
		return cacheSum(bytes, unit-remainder)
	}
	return bytes
}
