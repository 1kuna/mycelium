package myce

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Storage probes overwrite one fixed file per host and purpose and never delete
// it. A NAS recycle bin keeps every SMB delete, so a create-then-remove probe
// left one deleted file behind per probe; at fleet check-in rates that grew a
// single bin folder past the point where listing it stalls the share (#1545).
// Overwriting in place proves the same writable/synced access without adding
// directory entries after the first probe. A fresh name per probe would also
// sidestep a client handle stuck on the fixed file (#2176), but it would hide
// that stuck handle and bring back the recycle-bin growth, so the fixed file
// stays and a write that never reaches the store fails the probe instead.

const storageProbePrefix = "myce storage probe "

// Successful NAS operations have taken 17.73s/23.22s (#234); the existing
// per-operation stall allowance is 30s. Ten allowances between idle probes
// bounds worst-observed probe duty below 10%, instead of one per heartbeat.
// Real I/O failures and local OS mount inventory still fence immediately.
// Cost: an otherwise idle failed share can remain cached for five minutes.
const checkpointMetadataInterval = 10 * checkpointAvailabilityStallBudget

var storageProbeHost = sync.OnceValue(func() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "host"
	}
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '.' {
			return r
		}
		return '_'
	}, host)
})

// storageProbePath names the fixed probe file for this host and purpose.
// Purposes keep independent probes (queue control, queue file store, worker,
// install) from truncating each other's file mid-check.
func storageProbePath(dir, prefix, purpose string) string {
	return filepath.Join(dir, prefix+storageProbeHost()+"-"+purpose)
}

// storageProbeOps lets bounded callers wrap each filesystem call.
type storageProbeOps struct {
	open   func(path string) (*os.File, error)
	run    func(operation string, fn func() error) error
	report func()
	// bypass drops cached pages before the read-back; nil uses the platform
	// call (Linux fadvise DONTNEED, macOS F_NOCACHE).
	bypass func(file fs.File, offset, length int64) error
}

func plainStorageProbeOps(progress func()) storageProbeOps {
	if progress == nil {
		progress = func() {}
	}
	return storageProbeOps{
		open:   openStorageProbe,
		run:    func(_ string, fn func() error) error { return fn() },
		report: progress,
	}
}

func openStorageProbe(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
}

// probeStorageFile truncates and rewrites path in place with a fresh nonce,
// fsyncs and closes it, then reopens it, drops the client's cached pages and
// reads the nonce back. Any failed step reports the store unavailable.
//
// The write alone proves nothing on a network filesystem: on a Linux CIFS
// mount the kernel accepted every probe write into the page cache, discarded
// each write-back (`No writable handle in writepages ret=-9`) and served the
// read-back from that cache, so the probe passed for 15 hours while the NAS
// copy stayed empty (#2174). fsync surfaces a failed write-back as an error;
// reading the nonce past the page cache proves the store holds this probe's
// bytes rather than an earlier probe's. One lock per path serializes this
// process's probes of the same file (health sampling and check-in admission
// share the worker's), so a concurrent truncate never reads as an outage.
func probeStorageFile(path string, ops storageProbeOps) (err error) {
	lock, _ := storageProbeLocks.LoadOrStore(path, new(sync.Mutex))
	lock.(*sync.Mutex).Lock()
	defer lock.(*sync.Mutex).Unlock()
	operation := "open"
	defer func() {
		if err != nil {
			logStorageProbeFailure(operation, err)
		}
	}()
	content := storageProbeRecord()
	f, err := ops.open(path)
	ops.report()
	if err != nil {
		if f != nil {
			_ = f.Close()
		}
		return err
	}
	operation = "write"
	err = ops.run("write", func() error { _, err := f.WriteString(content); return err })
	ops.report()
	if err == nil {
		operation = "fsync"
		err = ops.run("fsync", f.Sync)
		ops.report()
	}
	// Close the owned descriptor even after a failed write or a cancelled
	// bounded sequence that skipped the close operation.
	closed := false
	if err == nil {
		operation = "close"
	}
	err = errors.Join(err, ops.run("close", func() error { closed = true; return f.Close() }))
	if !closed {
		_ = f.Close()
	}
	ops.report()
	if err != nil {
		return err
	}
	operation = "read"
	bypass := ops.bypass
	if bypass == nil {
		bypass = storageCacheBypass
	}
	var stored string
	err = ops.run("read", func() error {
		r, err := os.Open(path)
		if err != nil {
			return err
		}
		// Platforms without a cache-bypass call still rely on fsync above.
		if err = bypass(r, 0, int64(len(content))); errors.Is(err, errStorageCacheBypass) {
			err = nil
		}
		if err == nil {
			buf := make([]byte, len(content))
			var n int
			n, err = io.ReadFull(r, buf)
			stored = string(buf[:n])
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				err = nil // A short store is reported by the comparison below.
			}
		}
		return errors.Join(err, r.Close())
	})
	ops.report()
	if err == nil && stored != content {
		operation = "verify"
		err = errStorageProbeMismatch
	}
	return err
}

var (
	storageProbeLocks       sync.Map // probe path -> *sync.Mutex
	errStorageProbeMismatch = errors.New("storage probe read back different bytes than it wrote")
)

// storageProbeRecord is fixed-length probe content with a per-probe nonce.
func storageProbeRecord() string {
	var nonce [8]byte
	_, _ = rand.Read(nonce[:]) // crypto/rand.Read never returns an error.
	return storageProbePrefix + hex.EncodeToString(nonce[:]) + "\n"
}

// Log only fixed operation names and numeric errno, never filesystem error
// text: PathError contains private mount paths and may contain credentials.
func storageProbeFailureDetails(operation string, err error) (string, int) {
	var path *os.PathError
	if errors.As(err, &path) {
		operation = path.Op
	} else {
		var link *os.LinkError
		var call *os.SyscallError
		if errors.As(err, &link) {
			operation = link.Op
		} else if errors.As(err, &call) {
			operation = call.Syscall
		}
	}
	switch operation {
	case "open", "stat", "lstat", "write", "fsync", "sync", "close", "read", "rename",
		"create", "mkdir", "remove", "readdirent", "mount-metadata", "ancestor-metadata",
		"filesystem", "verify", "containment", "format", "resolve", "inventory", "pending",
		"local-model-ancestor", "local-model-filesystem", "local-model-headroom":
	default:
		operation = "unknown"
	}
	var errno syscall.Errno
	errors.As(err, &errno)
	return operation, int(errno)
}

func logStorageProbeFailure(operation string, err error) {
	operation, errno := storageProbeFailureDetails(operation, err)
	log.Printf("storage probe failed operation=%s errno=%d", operation, errno)
}

// probeStorageRename proves write, sync and read-back on the fixed file in place,
// then proves rename on a separate fixed marker by moving it to an alternate
// name and back. The written file is never renamed: on a Linux CIFS mount of
// the fleet NAS, a file that was renamed and renamed back refuses write opens
// for minutes, so the installer's second probe of the same root failed with
// EACCES. The marker is created once and never reopened. Neither name is ever
// deleted (#1545). A probe interrupted between the renames leaves only the
// alternate name, which the next probe moves back first.
func probeStorageRename(path string, ops storageProbeOps) error {
	if err := probeStorageFile(path, ops); err != nil {
		return err
	}
	marker := path + ".rename"
	renamed := marker + "d"
	err := ops.run("stat", func() error {
		if _, err := os.Lstat(marker); !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if _, err := os.Lstat(renamed); err == nil {
			return os.Rename(renamed, marker)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		f, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		return f.Close()
	})
	if err != nil {
		return err
	}
	if err := ops.run("rename", func() error { return os.Rename(marker, renamed) }); err != nil {
		return err
	}
	return ops.run("rename", func() error { return os.Rename(renamed, marker) })
}

// recentAvailability holds the queue's last completed checkpoint-root probe.
// It has its own lock: it is read and refreshed before a transaction takes q.mu.
type recentAvailability struct {
	mu        sync.Mutex
	at        time.Time
	available bool
	observed  bool
}

// Idle availability is never refreshed faster than ten measured NAS operation
// stall allowances. Slower worker cadences can extend it further. This is host
// admission evidence, never a source of job status or checkpoint progress.
func (q *Queue) checkpointProbeFreshness() time.Duration {
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	var shortest time.Duration
	for _, c := range q.contacts {
		period := c.period
		if period == 0 {
			period = time.Duration(c.cadence.Mean * float64(time.Second))
		}
		if period > 0 && (shortest == 0 || period < shortest) {
			shortest = period
		}
	}
	return max(checkpointMetadataInterval, shortest)
}

// recentCheckpointAvailability reuses a completed probe within the freshness
// window and otherwise joins or starts the single bounded probe sequence. It
// runs before q.mu; only completed results are retained.
func (q *Queue) recentCheckpointAvailability() bool {
	window := q.checkpointProbeFreshness()
	now := q.clock.Now()
	q.nasRecent.mu.Lock()
	if q.nasRecent.observed && window > 0 && !now.Before(q.nasRecent.at) && now.Sub(q.nasRecent.at) < window {
		available := q.nasRecent.available
		q.nasRecent.mu.Unlock()
		return available
	}
	q.nasRecent.mu.Unlock()
	probe := q.nasProbeIO
	if probe == nil {
		probe = func(root string, progress func()) bool { return checkpointAvailable(root, "queue", progress) }
	}
	available, err := q.nasProbe.runProgress(q.root, func(progress func()) (bool, error) {
		return probe(q.root, progress), nil
	})
	q.nasProbe.mu.Lock()
	// Retained success during a pending refresh is not a completed observation;
	// keep checking its operation-progress fence instead of caching it for 5m.
	if err == nil && q.nasProbe.pending == nil {
		q.nasRecent.mu.Lock()
		q.nasRecent.at, q.nasRecent.available, q.nasRecent.observed = q.clock.Now(), available, true
		q.nasRecent.mu.Unlock()
	}
	q.nasProbe.mu.Unlock()
	return available
}

// removeCreatedProbeDir removes a directory a preflight created this run. Its
// own fixed install probe file and rename marker (at either name, if a probe
// was interrupted) are the only content it can hold, so they go first;
// anything else keeps the directory and reports why (#1545).
func removeCreatedProbeDir(dir string) error {
	probe := storageProbePath(dir, ".myce-install-probe-", "install")
	for _, path := range []string{probe, probe + ".rename", probe + ".renamed"} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return os.Remove(dir)
}
