package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"
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
	if len(first) != 3 {
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
	if err := os.Rename(install, install+".renamed"); err != nil {
		t.Fatal(err)
	}
	if err := probeNASDirectory(root); err != nil {
		t.Fatal(err)
	}
	if got := probeEntries(t, root); !slices.Equal(got, first) {
		t.Fatalf("interrupted rename was not restored: %v", got)
	}
}

// The unmounted install path probes each NAS namespace the same way.
func TestInstallNASProbeReusesFixedEntries(t *testing.T) {
	c := ServiceConfig{NAS: t.TempDir(), Role: "queue"}
	for i := 0; i < 3; i++ {
		if err := prepareInstallNASMode(context.Background(), c, nil, false); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"checkpoints", "models", "environments"} {
		got := probeEntries(t, filepath.Join(c.NAS, "mycelium", name))
		if want := []string{filepath.Base(storageProbePath("", ".myce-install-probe-", "install"))}; !slices.Equal(got, want) {
			t.Fatalf("%s entries after repeated install probes = %v, want %v", name, got, want)
		}
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
	if probeStorageFile(storageProbePath(t.TempDir(), ".myce-probe-", "queue"), true, readFail) == nil {
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
	if window := q.checkpointProbeFreshness(); window != time.Second {
		t.Fatalf("freshness window = %s, want the measured 1s check-in period", window)
	}
	clock.Advance(time.Second)
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
	clock.Advance(time.Second)
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if q.checkpointsAvailable() {
		t.Fatal("failed probe after the window did not mark checkpoints unavailable")
	}
}
