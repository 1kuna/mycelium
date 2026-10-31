package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestStorageAccountingRestartAndLegacyGuards(t *testing.T) {
	const capBytes, reserveBytes = 185 << 30, 16 << 30
	clock := &fakeClock{now: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)}
	dir := t.TempDir()
	database, checkpoints, files := filepath.Join(dir, "queue.db"), filepath.Join(dir, "checkpoints"), filepath.Join(dir, "files")
	var q *Queue
	open := func() {
		t.Helper()
		var err error
		q, err = Open(database, checkpoints, clock, WithFileStore(files, capBytes, true), WithFileStoreProductionReserve(reserveBytes), WithOutputDeliveryRoot(checkpoints))
		if err != nil {
			t.Fatal(err)
		}
		fakeCheckpointAvailability(t, q, true)
		q.files.diskProbe = func(string) (DiskReading, error) {
			// Metadata observation, including the control sample, runs outside q.mu.
			if !q.mu.TryLock() {
				t.Error("physical probe under queue lock")
			} else {
				q.mu.Unlock()
			}
			return DiskReading{TotalBytes: 1 << 40, FreeBytes: 300 << 30}, nil
		}
	}
	open()
	t.Cleanup(func() { q.Close() })
	c := deliveryClient(t, q)
	partial := capacityPartial(t, c, "partial output", jobInputUploadPurpose)
	_, complete := retentionUpload(t, c, "complete")
	s := asyncDeliverySpec()
	s.Disk.OutputBytes = 8
	j := submit(t, q, s)
	read := func(c Client, reserved uint64) *PhysicalStorageStatus {
		t.Helper()
		clock.Advance(time.Second) // Expire only the observational status cache.
		var snap Snapshot
		if err := c.Call(context.Background(), http.MethodGet, "/api/status", nil, &snap); err != nil {
			t.Fatal(err)
		}
		fs := snap.FileStore
		if fs == nil || fs.Physical == nil {
			t.Fatal("missing physical storage observation")
		}
		v := fs.Physical
		remaining := uint64(partial.SizeBytes - 2)
		if fs.LimitBytes != capBytes || fs.ProductionReserveBytes != reserveBytes || fs.UsedBytes != uint64(2+complete.SizeBytes) {
			t.Fatalf("legacy settings/accounting changed: %+v", fs)
		}
		if fs.FreeBytes != capBytes-uint64(partial.SizeBytes+complete.SizeBytes)-reserved {
			t.Fatalf("legacy cap no longer governs admission: %+v", fs)
		}
		if v.Error != "" || v.Mode != "shadow" || !v.SampledAt.Equal(clock.Now()) || v.ProtectedFreeBytes != 0 || v.Qualification == "" {
			t.Fatalf("incorrect shadow/qualification evidence: %+v", v)
		}
		if v.RemainingUploadBytes != remaining || v.ReservedOutputBytes != reserved || v.UncommittedBytes != (300<<30)-remaining-reserved {
			t.Fatalf("existing bytes deducted twice or unwritten bytes missed: %+v", v)
		}
		if v.Control.DatabaseBytes == 0 || v.Control.AllocatedBytes < v.Control.DatabaseBytes || v.UploadMetadataBytes == 0 {
			t.Fatalf("missing allocated SQLite/upload metadata measurement: %+v", v)
		}
		b, err := json.Marshal(v)
		if err != nil || strings.Contains(string(b), dir) {
			t.Fatalf("private storage paths exposed: %s %v", b, err)
		}
		return v
	}
	read(c, 8)
	// Physical room beyond the old ceiling is evidence only. Both configured
	// guards must still refuse ordinary writes until explicit migration (#509).
	for _, size := range []int64{186 << 30, 170 << 30} {
		ctx := WithFileStoreSubmission(context.Background(), Spec{Project: "test", Class: "experiment"})
		err := c.PreflightFileStore(ctx, FileManifest{"too-large": {SHA256: strings.Repeat("a", 64), SizeBytes: size}})
		var he *HTTPError
		if !errors.As(err, &he) || he.Status != 507 || he.Code != "file_store_capacity" {
			t.Fatalf("legacy guard bypassed for size=%d: %v", size, err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	open()
	c = deliveryClient(t, q)
	read(c, 8) // Tus lengths and SQLite output commitments reconstruct on disk.
	m := machine("storage-worker", "fake")
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != j.ID {
		t.Fatal("missing restarted assignment")
	}
	r := result(assigned, false)
	if err := q.Started(r); err != nil {
		t.Fatal(err)
	}
	r.OutputRefs = FileManifest{"out.txt": partial}
	if err := q.reserveDeliveryResult(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	read(c, 0) // Exact pending identity is already covered by the tus length.
}

func TestStorageAccountingAliasesAndFailures(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "database")
	if err := os.WriteFile(path, []byte("small real control allocation"), 0600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "wal-alias")
	if err := os.Link(path, alias); err != nil {
		t.Fatal(err)
	}
	s := &FileStore{root: dir, controlFiles: controlStorageFiles{database: path, wal: alias, shared: filepath.Join(dir, "absent-shm")}}
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	v := s.physicalStorageStatus(context.Background(), FileStoreStatus{DiskFreeBytes: 100}, nil, now)
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	allocated := uint64(st.Sys().(*syscall.Stat_t).Blocks) * 512
	if v.Error != "" || v.Control.AllocatedBytes != allocated || v.Control.WALBytes != 0 || v.Control.SharedBytes != 0 || v.ProtectedFreeBytes != 0 {
		t.Fatalf("alias double-counted or missing optional SQLite files rejected: %+v", v)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	v = s.physicalStorageStatus(context.Background(), FileStoreStatus{DiskFreeBytes: 100}, nil, now)
	if v.Error == "" || v.UncommittedBytes != 0 || v.ProtectedFreeBytes != 0 || strings.Contains(v.Error, dir) {
		t.Fatalf("missing required control state yielded capacity or a private path: %+v", v)
	}
}
