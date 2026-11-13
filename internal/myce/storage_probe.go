package myce

import (
	"errors"
	"io"
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
// directory entries after the first probe.

const storageProbeContent = "myce storage probe\n"

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

// probeStorageFile truncates and rewrites path in place, optionally syncs it,
// closes it and reads it back. Any failed step reports the store unavailable.
// Content is not compared: probes of the same purpose on one host may overlap,
// and a concurrent truncate must not read as an outage.
func probeStorageFile(path string, syncFile bool, ops storageProbeOps) (err error) {
	operation := "open"
	defer func() {
		if err != nil {
			logStorageProbeFailure(operation, err)
		}
	}()
	f, err := ops.open(path)
	ops.report()
	if err != nil {
		if f != nil {
			_ = f.Close()
		}
		return err
	}
	operation = "write"
	err = ops.run("write", func() error { _, err := f.WriteString(storageProbeContent); return err })
	ops.report()
	if err == nil && syncFile {
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
	err = ops.run("read", func() error {
		r, err := os.Open(path)
		if err != nil {
			return err
		}
		var buf [len(storageProbeContent)]byte
		_, err = r.Read(buf[:])
		// Another probe of this host/purpose can have truncated the fixed
		// file before its write. EOF proves the read completed; it is not a
		// storage fault. Keep real read errors and all close errors.
		if errors.Is(err, io.EOF) {
			err = nil
		}
		return errors.Join(err, r.Close())
	})
	ops.report()
	return err
}

// Log only fixed operation names and numeric errno, never filesystem error
// text: PathError contains private mount paths and may contain credentials.
func storageProbeFailureDetails(operation string, err error) (string, int) {
	var path *os.PathError
	if errors.As(err, &path) {
		operation = path.Op
	}
	switch operation {
	case "open", "stat", "lstat", "write", "fsync", "sync", "close", "read", "rename":
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

// probeStorageRename proves write, sync and read on the fixed file in place,
// then proves rename on a separate fixed marker by moving it to an alternate
// name and back. The written file is never renamed: on a Linux CIFS mount of
// the fleet NAS, a file that was renamed and renamed back refuses write opens
// for minutes, so the installer's second probe of the same root failed with
// EACCES. The marker is created once and never reopened. Neither name is ever
// deleted (#1545). A probe interrupted between the renames leaves only the
// alternate name, which the next probe moves back first.
func probeStorageRename(path string, ops storageProbeOps) error {
	if err := probeStorageFile(path, true, ops); err != nil {
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

// checkpointProbeFreshness is the shortest measured worker check-in period.
// Each worker probes its own checkpoint mount on every check-in and its report
// gates admission per machine, so a queue result younger than one check-in is
// no staler than the fleet's own evidence. Without a measured cadence the
// queue probes on each transaction, as before (#1545).
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
	return shortest
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
	if err == nil {
		q.nasRecent.mu.Lock()
		q.nasRecent.at, q.nasRecent.available, q.nasRecent.observed = q.clock.Now(), available, true
		q.nasRecent.mu.Unlock()
	}
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
