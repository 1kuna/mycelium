package myce

import (
	"bytes"
	"errors"
	"io/fs"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
)

func probeEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// A NAS recycle bin keeps every delete, so repeated probes must reuse one
// directory entry per purpose instead of creating and removing files (#1545).
func TestStorageProbesReuseFixedEntries(t *testing.T) {
	root := t.TempDir()
	probe := func() {
		t.Helper()
		if !checkpointAvailable(root, "queue", func() {}) {
			t.Fatal("queue probe failed on a writable root")
		}
		if err := workerCheckpointAvailable(root, func() {}); err != nil {
			t.Fatal(err)
		}
		if err := probeNASDirectory(root); err != nil {
			t.Fatal(err)
		}
	}
	probe()
	first := probeEntries(t, root)
	// One fixed file per purpose, plus the install probe's rename marker.
	if len(first) != 4 {
		t.Fatalf("one fixed probe file per purpose, got %v", first)
	}
	for i := 0; i < 5; i++ {
		probe()
	}
	if got := probeEntries(t, root); !slices.Equal(got, first) {
		t.Fatalf("repeated probes changed directory entries: %v then %v", first, got)
	}

	// An install probe interrupted between its renames resumes without a delete.
	install := storageProbePath(root, ".myce-install-probe-", "install")
	if err := os.Rename(install+".rename", install+".renamed"); err != nil {
		t.Fatal(err)
	}
	if err := probeNASDirectory(root); err != nil {
		t.Fatal(err)
	}
	if got := probeEntries(t, root); !slices.Equal(got, first) {
		t.Fatalf("interrupted rename was not restored: %v", got)
	}
}

// On a Linux CIFS mount of the fleet NAS, a file renamed and renamed back
// refuses write opens for minutes, so the installer's second probe of a root
// failed with EACCES. The install probe proves rename on its marker only; the
// file it writes keeps its name throughout.
func TestInstallProbeNeverRenamesTheWrittenFile(t *testing.T) {
	root := t.TempDir()
	path := storageProbePath(root, ".myce-install-probe-", "install")
	ops := plainStorageProbeOps(nil)
	renames := 0
	ops.run = func(operation string, fn func() error) error {
		err := fn()
		if operation == "rename" {
			renames++
			if _, statErr := os.Stat(path); statErr != nil {
				t.Fatalf("written probe file left its name during rename: %v", statErr)
			}
		}
		return err
	}
	for i := 0; i < 3; i++ {
		if err := probeStorageRename(path, ops); err != nil {
			t.Fatal(err)
		}
	}
	if renames != 6 {
		t.Fatalf("rename was not exercised on every probe: %d renames", renames)
	}
}

// Health sampling and check-in admission probe the worker's one fixed file
// from the same process. The read-back compares this probe's nonce, so those
// probes must serialize instead of truncating each other mid-check.
func TestStorageProbeConcurrentProbesOfOneFileAllPass(t *testing.T) {
	path := storageProbePath(t.TempDir(), ".myce-probe-", "worker")
	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for range 2 {
		wg.Go(func() {
			for range 20 {
				errs <- probeStorageFile(path, plainStorageProbeOps(nil))
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("overlapping probes of one file reported an outage: %v", err)
		}
	}
}

func captureStorageProbeLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(old) })
	return &buf
}

// #2174: a Linux CIFS client accepted every probe write into its page cache,
// failed each write-back with EBADF and served the read-back from that cache,
// so the worker probe passed for 15 hours. The worker probe must fsync and
// report the failed write-back with its typed operation.
func TestStorageProbeFailsWhenWriteBackFails(t *testing.T) {
	logs := captureStorageProbeLog(t)
	path := storageProbePath(t.TempDir(), ".myce-probe-", "worker")
	ops := plainStorageProbeOps(nil)
	ops.run = func(operation string, fn func() error) error {
		if operation == "fsync" {
			return &os.PathError{Op: "sync", Path: path, Err: syscall.EBADF}
		}
		return fn()
	}
	if err := probeStorageFile(path, ops); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("probe with failed write-back = %v, want EBADF", err)
	}
	if !strings.Contains(logs.String(), "operation=sync errno=9") {
		t.Fatalf("failed write-back not logged with its operation: %q", logs.String())
	}
	if err := probeStorageFile(path, plainStorageProbeOps(nil)); err != nil {
		t.Fatalf("healthy store failed the probe: %v", err)
	}
}

// When the store's copy differs from what the client cached, the read-back
// must see the store's copy and fail. The read hook stands in for the store:
// it replaces the file with the server's bytes before the probe reads.
func TestStorageProbeFailsWhenStoreCopyDiffers(t *testing.T) {
	for name, server := range map[string]string{
		"truncated, write lost": "", // #2174: the server file stayed 0 bytes.
		"earlier probe's bytes": storageProbeRecord(),
		"partial write":         storageProbePrefix,
	} {
		t.Run(name, func(t *testing.T) {
			logs := captureStorageProbeLog(t)
			path := storageProbePath(t.TempDir(), ".myce-probe-", "worker")
			ops := plainStorageProbeOps(nil)
			ops.run = func(operation string, fn func() error) error {
				if operation == "read" {
					if err := os.WriteFile(path, []byte(server), 0600); err != nil {
						return err
					}
				}
				return fn()
			}
			if err := probeStorageFile(path, ops); !errors.Is(err, errStorageProbeMismatch) {
				t.Fatalf("probe over a differing store copy = %v, want mismatch", err)
			}
			if !strings.Contains(logs.String(), "operation=verify errno=0") {
				t.Fatalf("mismatch not logged as verify: %q", logs.String())
			}
		})
	}
}

// The read-back drops the client's cached pages for exactly the written range
// on a fresh descriptor of the probe file, after the writer has closed.
func TestStorageProbeReadBackBypassesClientCache(t *testing.T) {
	path := storageProbePath(t.TempDir(), ".myce-probe-", "worker")
	ops := plainStorageProbeOps(nil)
	closed, calls := false, 0
	run := ops.run
	ops.run = func(operation string, fn func() error) error {
		err := run(operation, fn)
		if operation == "close" {
			closed = true
		}
		return err
	}
	ops.bypass = func(file fs.File, offset, length int64) error {
		calls++
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if !closed || info.Size() != int64(len(storageProbeRecord())) || offset != 0 || length != info.Size() {
			t.Errorf("bypass closed=%v size=%d offset=%d length=%d", closed, info.Size(), offset, length)
		}
		return storageCacheBypass(file, offset, length)
	}
	if err := probeStorageFile(path, ops); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("cache bypass calls = %d, want 1", calls)
	}
	ops.bypass = func(fs.File, int64, int64) error { return syscall.EINVAL }
	if err := probeStorageFile(path, ops); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("failed cache bypass = %v, want the error", err)
	}
}

func TestStorageProbeFailuresReportUnavailable(t *testing.T) {
	root := t.TempDir()
	// A directory in the probe file's place makes the write open fail.
	for _, p := range []string{
		storageProbePath(root, ".myce-probe-", "queue"),
		storageProbePath(root, ".myce-probe-", "worker"),
		storageProbePath(root, ".myce-install-probe-", "install"),
	} {
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if checkpointAvailable(root, "queue", func() {}) {
		t.Fatal("queue probe reported available after a failed write")
	}
	if workerCheckpointAvailable(root, func() {}) == nil {
		t.Fatal("worker probe reported available after a failed write")
	}
	if probeNASDirectory(root) == nil {
		t.Fatal("install probe reported available after a failed write")
	}

	readFail := plainStorageProbeOps(nil)
	readFail.run = func(operation string, fn func() error) error {
		if operation == "read" {
			return errors.New("read failed")
		}
		return fn()
	}
	if probeStorageFile(storageProbePath(t.TempDir(), ".myce-probe-", "queue"), readFail) == nil {
		t.Fatal("probe reported available after a failed read-back")
	}
}

// Control transactions reuse a completed probe for one measured worker
// check-in period instead of each touching the share.
func TestQueueTransactionsReuseRecentCheckpointProbe(t *testing.T) {
	q, clock := fixture(t)
	drainAdmissionWait(t, q)
	var probes atomic.Int32
	var available atomic.Bool
	available.Store(true)
	q.checkpointProbe = nil
	q.nasProbeIO = func(string, func()) bool { probes.Add(1); return available.Load() }

	check(t, q, machine("probe-worker", "fake"), nil) // Measures a 1s check-in period.
	if window := q.checkpointProbeFreshness(); window != checkpointMetadataInterval {
		t.Fatalf("freshness window = %s, want the measured stall-derived idle interval", window)
	}
	clock.Advance(checkpointMetadataInterval)
	before := probes.Load()
	for i := 0; i < 10; i++ {
		if err := q.update(func() error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if got := probes.Load() - before; got != 1 {
		t.Fatalf("10 control transactions within one check-in period ran %d probes, want 1", got)
	}

	available.Store(false)
	clock.Advance(checkpointMetadataInterval)
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if q.checkpointsAvailable() {
		t.Fatal("failed probe after the window did not mark checkpoints unavailable")
	}
}

// A probe failure remains attributable without exposing the mount path or
// trusting arbitrary operation text supplied by an error wrapper.
func TestStorageProbeFailureDetails(t *testing.T) {
	for _, err := range []error{
		&os.LinkError{Op: "rename", Old: "/private-mount/source", New: "/private-mount/target", Err: syscall.EACCES},
		&os.SyscallError{Syscall: "rename", Err: syscall.EACCES},
	} {
		op, errno := storageProbeFailureDetails("stat", err)
		if op != "rename" || errno != int(syscall.EACCES) {
			t.Fatalf("nested marker operation lost: %s/%d", op, errno)
		}
	}
	for _, tc := range []struct {
		op   string
		want string
	}{
		{"read", "read"}, {"private mount detail", "unknown"},
	} {
		err := &os.PathError{Op: tc.op, Path: "/private-mount/secret", Err: syscall.EIO}
		op, errno := storageProbeFailureDetails("open", err)
		if op != tc.want || errno != int(syscall.EIO) {
			t.Fatalf("failure details = %q/%d, want %q/%d", op, errno, tc.want, syscall.EIO)
		}
	}
}
