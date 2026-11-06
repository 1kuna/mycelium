package myce

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Local, same-account processes coordinate pending blocks with kernel-owned
// locks and durable custody records. No elapsed time grants ownership, and
// no other process's payload is removed. Inactive scalar slots
// are reused. The coordination directory contains no payload or credentials.
type hostCacheState struct {
	Controls                     map[string]int64
	Owned                        int64
	ControlNeeds                 map[string]int64
	At                           time.Time
	Free, Written, SampleWritten int64
	Horizon                      time.Duration
	Drain                        float64
	Control                      int64
}
type hostCacheClaim struct {
	Custody     string
	Helpers     []int
	ControlPage int64
	ControlUnit int64
	Kind        string
	Root        string
	Bytes       int64
	Paths       []string
	Initial     int64
}
type hostCacheView struct {
	ledger          *cacheBudgets
	paths           map[string][]string
	valid           bool
	state           hostCacheState
	pending, demand int64
	demandRoots     map[string]int64
	pendingRoots    map[string]int64
	persistent      map[string]string
	file            *os.File
	slot            *os.File
	directory       string
	recordBytes     int64
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

// Locks live on stable inodes; payload records use fsynced atomic replacement.
// Two independently published copies recover a damaged publication without
// confusing JSON truncation with an empty/released obligation.
func writeCacheRecord(f *os.File, v any) error {
	dir, name := filepath.Dir(f.Name()), filepath.Base(f.Name())
	if err := saveWorkerJSON(dir, name+".previous", v); err != nil {
		return err
	}
	return saveWorkerJSON(dir, name+".json", v)
}
func readCacheRecord(f *os.File, v any) error {
	var failures []error
	missing := 0
	for _, suffix := range []string{".json", ".previous"} {
		data, err := os.ReadFile(f.Name() + suffix)
		if os.IsNotExist(err) {
			missing++
			continue
		}
		if err == nil {
			err = json.Unmarshal(data, v)
		}
		if err == nil {
			return nil
		}
		failures = append(failures, err)
	}
	if missing == 2 {
		return nil // Newly created lock, never a partially written payload record.
	}
	return errors.Join(failures...)
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
	v := &hostCacheView{ledger: b, paths: map[string][]string{}, file: f, directory: dir, demandRoots: map[string]int64{}, pendingRoots: map[string]int64{}, persistent: map[string]string{}}
	fail := func(err error) (*hostCacheView, error) { v.close(); return nil, err }
	if err = readCacheRecord(f, &v.state); err != nil {
		return fail(err)
	}
	v.valid = true
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fail(err)
	}
	var allPaths []string
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
			// Upload custody is durable. Builders may outlive their parent in a
			// separate session; only an empty exact recorded session proves exit.
			custody := c.Kind == "upload"
			if !active && custody && len(c.Paths) >= 2 {
				// Durable custody follows resumable tus authority, not a dead
				// queue handle. Completed/fenced removal can precede scalar
				// retirement; no remaining pair means no writer can resume it.
				custody = false
				for _, path := range c.Paths[:2] {
					if _, statErr := os.Lstat(path); !os.IsNotExist(statErr) {
						custody = true // Unknown/permission failures retain custody.
					}
				}
			}
			if !active && c.Kind == "explore" {
				for _, group := range c.Helpers {
					members, inspectErr := b.helpers(group)
					if inspectErr != nil || len(members) > 0 {
						custody = true
						break
					}
				}
			}
			live := active || custody
			if c.Kind == "upload" && live {
				v.persistent[c.Custody] = claim.Name()
			}
			encoded, encodeErr := json.Marshal(c)
			if encodeErr != nil {
				claim.Close()
				return fail(encodeErr)
			}
			v.recordBytes = max(v.recordBytes, int64(len(encoded)))
			allPaths = append(allPaths, c.Paths...)
			if live {
				v.paths[claim.Name()] = c.Paths
				if c.Kind == "control" {
					used, allocationErr := b.allocation(c.Paths)
					if allocationErr != nil {
						err = allocationErr
					} else {
						previous, seen := v.state.Controls[c.Root]
						if !seen {
							previous = used
						}
						controls[c.Root] = used
						wal, allocationErr := b.allocation(c.Paths[1:])
						if allocationErr != nil {
							err = allocationErr
						} else {
							bootstrap := cacheSum(cacheRound(32<<10, c.ControlUnit), cacheRound(cacheSum(c.ControlPage, 32+24), c.ControlUnit))
							needs[c.Root] = max(needs[c.Root], wal, max(0, used-previous), bootstrap)
						}
					}
				} else if c.Kind == "demand" {
					v.demand = cacheSum(v.demand, c.Bytes)
					v.demandRoots[c.Root] = cacheSum(v.demandRoots[c.Root], c.Bytes)
				} else {
					// Withhold the whole measured commitment through completion.
					// Allocation at a shared/reused pathname cannot discharge a
					// different writer's still-pending bytes.
					v.pending = cacheSum(v.pending, c.Bytes)
					v.pendingRoots[c.Root] = cacheSum(v.pendingRoots[c.Root], c.Bytes)
				}
			} else {
				c = hostCacheClaim{}
				err = writeCacheRecord(claim, c)
			}
		}
		if !active && c.Kind == "" && v.slot == nil {
			v.slot = claim
		} else {
			claim.Close()
		}
		if err != nil {
			return fail(err)
		}
	}
	// A single inode-union census prevents two claims from crediting the same
	// allocation twice. Removing a record changes the ownership baseline, not
	// physical writes; completed data can remain at its published destination.
	owned, err := b.allocation(allPaths)
	if err != nil {
		return fail(err)
	}
	v.state.Written += owned - v.state.Owned
	if err := v.resetOwned(); err != nil {
		return fail(err)
	}
	v.state.Controls, v.state.ControlNeeds = controls, needs
	v.state.Control = 0
	for _, n := range needs {
		v.state.Control = cacheSum(v.state.Control, n)
	}
	return v, nil
}
func (v *hostCacheView) resetOwned() error {
	var paths []string
	for _, owned := range v.paths {
		paths = append(paths, owned...)
	}
	used, err := v.ledger.allocation(paths)
	if err == nil {
		v.state.Owned = used
	}
	return err
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
		var err error
		// At most one scalar slot per live writer or durable obligation. Reuse
		// inactive slots; random names avoid collisions with record sidecars.
		f, err = os.OpenFile(filepath.Join(v.directory, ID()+".claim"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
			f.Close()
			return nil, err
		}
	}
	encoded, err := json.Marshal(c)
	if err == nil {
		v.recordBytes = max(v.recordBytes, int64(len(encoded)))
		err = writeCacheRecord(f, c)
	}
	if err == nil {
		v.paths[f.Name()] = c.Paths
		err = v.resetOwned()
	}
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
	// Each of the state and owner records needs two synced copies plus an
	// atomic-publication temporary. Two stable lock inodes and the parent also
	// need allocations. Encoded sizes scale this control requirement with the
	// actual scalar/path working set instead of assuming one memory page.
	state, _ := json.Marshal(v.state)
	claim, _ := json.Marshal(hostCacheClaim{})
	unit := cacheAllocationUnit(d)
	stateBytes := cacheRound(int64(len(state)), unit)
	claimBytes := cacheRound(max(int64(len(claim)), v.recordBytes), unit)
	metadata := cacheSum(3*unit, cacheSum(cacheSum(stateBytes, cacheSum(stateBytes, stateBytes)), cacheSum(claimBytes, cacheSum(claimBytes, claimBytes))))
	return cacheSum(cacheSum(metadata, v.state.Control), cacheFloatBytes(v.state.Drain*v.state.Horizon.Seconds()))
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
	f, err := host.claim(hostCacheClaim{Kind: "explore", Root: root, Bytes: bytes, Paths: paths, Initial: initial})
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

// The existing command supervisor calls this gate after persisting its process
// session and before releasing the payload. A parent crash can never leave an
// executing builder without a durable session receipt in the capacity ledger.
func (r *cacheReservation) recordHelper(group int) error {
	if group <= 0 {
		return errors.New("builder storage custody lacks a process session")
	}
	host, err := r.ledger.hostView(r.volume)
	if err != nil {
		return err
	}
	defer host.close()
	var claim hostCacheClaim
	if err = readCacheRecord(r.host, &claim); err != nil {
		return err
	}
	for _, previous := range claim.Helpers {
		if previous == group {
			return nil
		}
	}
	claim.Helpers = append(claim.Helpers, group)
	return writeCacheRecord(r.host, claim)
}

type cacheBuildClaimKey struct{}

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
	if err := writeCacheRecord(r.host, claim); err != nil {
		return err
	}
	host.paths[r.host.Name()] = claim.Paths
	return host.resetOwned()
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
	f, err := host.claim(hostCacheClaim{Kind: "control", Root: root, Paths: paths, Initial: initial, ControlPage: page, ControlUnit: cacheAllocationUnit(disk)})
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
