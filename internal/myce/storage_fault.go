package myce

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type storageFaultContextKey struct{}

func (w Worker) storageIOContext(ctx context.Context) context.Context {
	// Nested readers and staging wrappers can return the same OS error again.
	// Keep syscall identity while visiting every sibling of a joined error.
	var mu sync.Mutex
	seen := map[error]bool{}
	observe := func(err error) {
		storageErrorLeaves(err, func(leaf error) {
			if _, _, ok := storageIOFault(leaf); !ok {
				return
			}
			mu.Lock()
			duplicate := seen[leaf]
			seen[leaf] = true
			mu.Unlock()
			if !duplicate {
				w.observeStorageError(leaf)
			}
		})
	}
	return context.WithValue(ctx, storageFaultContextKey{}, observe)
}

func observeStorageContext(ctx context.Context, err error) {
	if observe, ok := ctx.Value(storageFaultContextKey{}).(func(error)); ok && err != nil {
		observe(err)
	}
}

// Preserve the path/operation wrapper as a leaf. Unwrap aggregation and
// contextual wrappers without letting the first ordinary error hide a sibling.
func storageErrorLeaves(err error, visit func(error)) {
	if err == nil {
		return
	}
	switch err.(type) {
	case *os.PathError, *os.LinkError:
		visit(err)
		return
	}
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		for _, child := range e.Unwrap() {
			storageErrorLeaves(child, visit)
		}
	case interface{ Unwrap() error }:
		storageErrorLeaves(e.Unwrap(), visit)
	}
}

// StorageFault is private worker evidence. It contains no path, payload or
// filesystem error text. Admission uses the existing mount health projection.
type StorageFault struct {
	Root        string    `json:"root"`
	Operation   string    `json:"operation"`
	Errno       int       `json:"errno"`
	Class       string    `json:"class"`
	At          time.Time `json:"at"`
	Generation  uint64    `json:"generation"`
	Data        bool      `json:"data"`
	Active      bool      `json:"active"`
	CacheBypass string    `json:"cache_bypass,omitempty"`
}

// Bad paths, permissions, producer identities, cancellation and local cache
// errors are not evidence that a mounted NAS has failed. In particular, a bad
// authored digest must not quarantine other jobs' storage.
func storageIOFault(err error) (*os.PathError, syscall.Errno, bool) {
	p, ok := err.(*os.PathError)
	var number syscall.Errno
	if !ok {
		link, ok := err.(*os.LinkError)
		if !ok || link.Op != "rename" {
			return nil, 0, false
		}
		p = &os.PathError{Op: link.Op, Path: link.New, Err: link.Err}
	}
	if p.Op == "read" && errors.Is(p.Err, io.ErrUnexpectedEOF) {
		return p, 0, true
	}
	if !errors.As(p.Err, &number) {
		return nil, 0, false
	}
	switch number {
	case syscall.EIO, syscall.EINVAL, syscall.EAGAIN, syscall.ESTALE, syscall.ENOTCONN, syscall.ETIMEDOUT, syscall.EBUSY:
	default:
		return nil, 0, false
	}
	switch p.Op {
	case "read", "open", "stat", "lstat", "readdirent", "readdir", "close", "write", "sync", "rename", "remove", "mkdir":
		return p, number, true
	}
	return nil, 0, false
}

// The OS error retains the exact syscall's path and operation through wrappers.
// Attribution requires a configured/observed network backing, never a host name
// or an error from a local destination beside a NAS source.
// One backing owns the data latch and its recovery counter. Roles contain
// only a backing identity; they never receive copies of this state.
type storageBacking struct {
	fault         StorageFault
	owner, source string
	successes     int
	cycle         uint64
}
type storageRoot struct{ path, backing string }

func (w Worker) storageRoots() map[string]storageRoot {
	paths := map[string]string{"checkpoint": w.CheckpointRoot, "job-files": w.JobFileRoot, "model": w.ModelRoot, "model-cache": w.CacheRoot, "environment-source": w.EnvironmentSourceRoot, "environment": w.EnvironmentRoot, "file-cache": w.FileCacheRoot, "extension": w.ExtensionRoot}
	if w.ServiceConfig != nil {
		paths["nas"] = w.ServiceConfig.NAS
	}
	if w.storageHealth != nil {
		w.storageHealth.mu.Lock()
		if p := w.storageHealth.mounts["checkpoint"]; p != nil {
			paths["checkpoint"] = p.path
		}
		w.storageHealth.mu.Unlock()
	}
	backings := w.storageRoleBackings(paths)
	roots := map[string]storageRoot{}
	for role, path := range paths {
		if path != "" {
			roots[role] = storageRoot{path: path, backing: backings[role]}
		}
	}
	return roots
}

func (w Worker) observeStorageError(err error) {
	if w.storageHealth == nil || err == nil {
		return
	}
	roots := w.storageRoots()
	m := w.storageHealth
	m.mu.Lock()
	defer m.mu.Unlock()
	m.storageRoots = roots
	storageErrorLeaves(err, func(leaf error) {
		e, _, ok := storageIOFault(leaf)
		if !ok || !filepath.IsAbs(e.Path) {
			return
		}
		role := ""
		// Attribution chooses one configured source role, never a fan-out set.
		for _, candidate := range []string{"checkpoint", "job-files"} {
			r := roots[candidate]
			if r.backing != "" && withinNAS(r.path, e.Path) && (role == "" || len(r.path) > len(roots[role].path)) {
				role = candidate
			}
		}
		if role != "" {
			m.storageFault(role, leaf, m.clock.Now(), false)
		}
	})
}

func (w Worker) storageRoleBackings(roots map[string]string) map[string]string {
	expected := map[string]string{}
	if w.rootMonitor != nil {
		w.rootMonitor.mu.Lock()
		for role, mount := range w.rootMonitor.expected {
			expected[role] = mount
		}
		w.rootMonitor.mu.Unlock()
	}
	if w.ServiceConfig != nil && w.ServiceConfig.NAS != "" {
		for role, path := range roots {
			if path != "" && withinNAS(w.ServiceConfig.NAS, path) && len(w.ServiceConfig.NAS) > len(expected[role]) {
				expected[role] = filepath.Clean(w.ServiceConfig.NAS)
			}
		}
	}
	return expected
}

// Called only under m.mu. Owned I/O, probe errors and recovery failures all
// enter this one transition; a sibling cannot retain an older generation.
func (m *hostHealthMonitor) storageFault(role string, err error, at time.Time, proof bool) {
	root := m.storageRoots[role]
	if root.backing == "" {
		return
	}
	observed := false
	storageErrorLeaves(err, func(leaf error) {
		event, errno, ok := storageIOFault(leaf)
		if !ok {
			return
		}
		// A probe's inventory error is not an error on its network root. The
		// recovery source may belong to a sibling role on this same backing.
		attributed := false
		for _, r := range m.storageRoots {
			if r.backing == root.backing && filepath.IsAbs(event.Path) && withinNAS(r.path, event.Path) {
				attributed = true
				break
			}
		}
		if !attributed {
			return
		}
		observed = true
		m.latchStorageFault(role, event, errno, at, err)
	})
	if !observed && proof {
		// Verification, bypass and premature EOF errors need no invented errno.
		// They invalidate the same candidate as a syscall fault.
		m.latchStorageFault(role, nil, 0, at, err)
	}
}

func (m *hostHealthMonitor) latchStorageFault(role string, event *os.PathError, errno syscall.Errno, at time.Time, err error) {
	root := m.storageRoots[role]
	if m.storageBackings == nil {
		m.storageBackings = map[string]*storageBacking{}
	}
	state := m.storageBackings[root.backing]
	if state == nil {
		state = &storageBacking{}
		m.storageBackings[root.backing] = state
	}
	data := state.fault.Active && state.fault.Data
	source := state.source
	operation, class := "reopen", "verification_failed"
	if event != nil {
		operation, class = event.Op, "filesystem_io"
		if errno == 0 {
			class = "incomplete_read"
		}
		if event.Op == "read" {
			data, source = true, event.Path
		}
	}
	// Keep one owner until recovery, including across repeated sibling faults.
	if !state.fault.Active {
		state.owner = role
		for _, candidate := range []string{"checkpoint", "nas"} {
			if m.mounts[candidate] != nil && m.storageRoots[candidate].backing == root.backing {
				state.owner = candidate
				break
			}
		}
	}
	state.fault = StorageFault{Root: role, Operation: operation, Errno: int(errno), Class: class, At: at, Generation: state.fault.Generation + 1, Data: data, Active: true}
	state.source, state.successes = source, 0
	// Do not reset the offset cycle: consecutive attempts rotate even when a
	// newer failure resets the candidate. Generation and time fence results.
	if errors.Is(err, errStorageCacheBypass) {
		state.fault.CacheBypass = "unavailable"
	}
	if m.mounts[state.owner] == nil {
		owner := m.storageRoots[state.owner]
		m.mounts[state.owner] = &hostMountProbe{root: state.owner, path: owner.path, clock: m.clock, probe: healthMountAccess("nas"), storage: m}
	}
}

func (p *hostMountProbe) backingFault() *storageBacking {
	if p.storage == nil {
		return nil
	}
	return p.storage.storageBackings[p.storage.storageRoots[p.root].backing]
}
func (p *hostMountProbe) faultGeneration() uint64 {
	if state := p.backingFault(); state != nil {
		return state.fault.Generation
	}
	return 0
}

func (m *hostHealthMonitor) storageResult(p *hostMountProbe, r hostMountResult, now time.Time) bool {
	if r.err != nil {
		m.storageFault(p.root, r.err, now, r.dataProof)
		return true
	}
	state := p.backingFault()
	if state == nil || !state.fault.Active {
		return true
	}
	fresh := r.generation == state.fault.Generation && p.started.After(state.fault.At)
	if state.owner == p.root && fresh && (!state.fault.Data || r.dataProof) {
		state.successes++
		if r.dataProof {
			state.fault.CacheBypass = "requested"
		}
		if state.successes >= hostHealthConfirmation {
			state.fault.Active = false
		}
	}
	return fresh && (!r.dataProof || state.owner == p.root)
}

// Only the data latch is shared. Existing role metadata/advisory/stall policy
// stays in hostMountProbe; every role projects the backing latch over it.
func (m *hostHealthMonitor) projectStorageFaults(sample *HostHealthSample) {
	for role, root := range m.storageRoots {
		state := m.storageBackings[root.backing]
		if state == nil || !state.fault.Active {
			continue
		}
		found := false
		for i := range sample.Mounts {
			if sample.Mounts[i].Root == role {
				sample.Mounts[i].Reason = "mount_probe_failed"
				sample.Mounts[i].Adverse = true
				found = true
			}
		}
		if !found {
			sample.Mounts = append(sample.Mounts, HostMountHealth{Root: role, Reason: "mount_probe_failed", Adverse: true})
		}
	}
}

// A fresh open/read/close followed by a second open/read/close tests data and
// reopen access without requiring a writable sentinel on read-only shares.
// At most one OS page per read bounds fault-time I/O. Each completion rotates
// its offset; both opens request client cache bypass before re-reading that
// offset. Stable metadata and equal nonempty bytes verify consistency, not the
// producer's full identity. Normal staging still verifies the declared digest.
var errStorageCacheBypass = errors.New("cache_bypass: unavailable")

func verifyStorageReopen(path string, cycle uint64, progress func(), open func(string) (fs.File, error), bypass func(fs.File, int64, int64) error) error {
	var offset, length int64
	read := func() (data []byte, info fs.FileInfo, err error) {
		f, err := open(path)
		progress()
		if err != nil {
			return nil, nil, err
		}
		defer func() { err = errors.Join(err, f.Close()); progress() }()
		info, err = f.Stat()
		progress()
		if err != nil {
			return nil, nil, err
		}
		if !info.Mode().IsRegular() || info.Size() <= 0 {
			return nil, nil, errors.New("data recovery requires nonempty regular source")
		}
		// For sub-page files rotate between disjoint portions when possible.
		// A one-byte file has only one possible nonempty offset.
		span := min(int64(os.Getpagesize()), max(int64(1), info.Size()/2))
		offset = int64(cycle%uint64((info.Size()+span-1)/span)) * span
		length = min(span, info.Size()-offset)
		if err = bypass(f, offset, length); err != nil {
			return nil, nil, errors.Join(errStorageCacheBypass, err)
		}
		progress()
		seeker, ok := f.(io.Seeker)
		if !ok {
			return nil, nil, errors.New("data recovery source cannot seek")
		}
		if _, err = seeker.Seek(offset, io.SeekStart); err != nil {
			return nil, nil, err
		}
		progress()
		data = make([]byte, length)
		_, err = io.ReadFull(f, data)
		progress()
		return data, info, err
	}
	a, ai, err := read()
	if err != nil {
		return err
	}
	b, bi, err := read()
	if err != nil {
		return err
	}
	if ai.Size() != bi.Size() || ai.Mode() != bi.Mode() || !ai.ModTime().Equal(bi.ModTime()) || !bytes.Equal(a, b) {
		return errors.New("data changed during recovery read")
	}
	return nil
}

var workerStorageReopen = func(path string, cycle uint64, progress func()) error {
	return verifyStorageReopen(path, cycle, progress, func(path string) (fs.File, error) { return os.Open(path) }, storageCacheBypass)
}
