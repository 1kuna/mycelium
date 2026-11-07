package myce

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"time"
)

type storageFaultContextKey struct{}

func (w Worker) storageIOContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, storageFaultContextKey{}, w.observeStorageError)
}

func observeStorageContext(ctx context.Context, err error) {
	if observe, ok := ctx.Value(storageFaultContextKey{}).(func(error)); ok && err != nil {
		observe(err)
	}
}

func observeStorageMetadata(ctx context.Context, err error) {
	var p *os.PathError
	// contextReader observes read errors before cancellation and before the
	// same error unwinds through verification/staging wrappers.
	if errors.As(err, &p) && p.Op == "read" {
		return
	}
	observeStorageContext(ctx, err)
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
	var p *os.PathError
	var number syscall.Errno
	if !errors.As(err, &p) {
		var link *os.LinkError
		if !errors.As(err, &link) || link.Op != "rename" {
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
func (w Worker) observeStorageError(err error) {
	if w.storageHealth == nil {
		return
	}
	p, number, ok := storageIOFault(err)
	if !ok || !filepath.IsAbs(p.Path) {
		return
	}
	roots := map[string]string{"checkpoint": w.CheckpointRoot, "job-files": w.JobFileRoot}
	w.storageHealth.mu.Lock()
	if p := w.storageHealth.mounts["checkpoint"]; p != nil {
		roots["checkpoint"] = p.path
	}
	w.storageHealth.mu.Unlock()
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
			if path != "" && withinNAS(w.ServiceConfig.NAS, path) {
				expected[role] = w.ServiceConfig.NAS
			}
		}
	}
	backing := ""
	for role, path := range roots {
		if path != "" && expected[role] != "" && withinNAS(path, p.Path) && len(expected[role]) > len(backing) {
			backing = expected[role]
		}
	}
	if backing == "" {
		return
	}
	w.storageHealth.observeStorageFault(p, number, backing, roots)
}

func (m *hostHealthMonitor) observeStorageFault(e *os.PathError, number syscall.Errno, backing string, roots map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	at := m.clock.Now()
	// A single backing mount can serve checkpoint and input roles. Their
	// existing lanes receive the same observation, not a whole-host drain.
	for role, path := range roots {
		if path == "" || !withinNAS(backing, path) {
			continue
		}
		p := m.mounts[role]
		if p == nil {
			p = &hostMountProbe{root: role, path: path, clock: m.clock, probe: healthMountAccess("nas")}
			m.mounts[role] = p
		}
		p.recordStorageFault(e, number, at)
	}
	if p := m.mounts["nas"]; p != nil && withinNAS(backing, p.path) {
		p.recordStorageFault(e, number, at)
	}
	// Publish to the same history immediately, before another local admission.
	s := HostHealthSample{At: at}
	if n := len(m.health.History); n > 0 {
		s = m.health.History[n-1]
		s.At = at
	}
	s.Mounts = nil
	for _, p := range m.mounts {
		v := p.last
		v.Root, v.Reason, v.Adverse = p.root, p.reason, p.reason != ""
		s.Mounts = append(s.Mounts, v)
	}
	m.health.observe(s)
}

// Called only with the health monitor mutex held (or by its single sampler).
func (p *hostMountProbe) recordStorageFault(e *os.PathError, number syscall.Errno, at time.Time) {
	data := e.Op == "read"
	if p.fault != nil && p.fault.Active {
		data = data || p.fault.Data
	}
	var generation uint64 = 1
	if p.fault != nil {
		generation = p.fault.Generation + 1
	}
	class := "filesystem_io"
	if number == 0 {
		class = "incomplete_read"
	}
	p.fault = &StorageFault{Root: p.root, Operation: e.Op, Errno: int(number), Class: class, At: at, Generation: generation, Data: data, Active: true}
	if e.Op == "read" {
		p.recoverySource = e.Path // Private local custody; never part of StorageFault/wire.
	}
	p.recoveryCycle = 0
	p.reason, p.candidate, p.consecutive = "mount_probe_failed", "", 0
	p.last.Reason, p.last.Adverse = p.reason, true
	p.nextProbe = time.Time{}
}

func (p *hostMountProbe) faultGeneration() uint64 {
	if p.fault == nil {
		return 0
	}
	return p.fault.Generation
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

// Snapshot private typed evidence separately from the versioned wire history.
func (m *hostHealthMonitor) storageFaultDisk() (map[string]StorageFault, map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	faults, sources := map[string]StorageFault{}, map[string]string{}
	for role, p := range m.mounts {
		if p.fault != nil {
			faults[role] = *p.fault
			sources[role] = p.recoverySource
		}
	}
	return faults, sources
}

func restoreStorageFault(p *hostMountProbe, f StorageFault, source string) {
	p.fault, p.recoverySource = &f, source
	if f.Active {
		p.reason = "mount_probe_failed"
		p.last = HostMountHealth{Root: p.root, Reason: p.reason, Adverse: true}
	}
}

// Keep direct role projection in the existing dependency resolver.
func storageFaultRole(role string) bool {
	return slices.Contains([]string{"checkpoint", "job-files"}, role)
}
