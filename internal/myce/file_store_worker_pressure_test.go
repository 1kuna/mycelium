package myce

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestFileStoreWorkerPressureCountsOnlyUnallocatedGrowth(t *testing.T) {
	q, _ := fixture(t)
	q.fileStoreProductionReserve = 32 << 10
	c := retentionClient(t, q)
	q.mu.Lock()
	q.diskHost = "store-host"
	q.s.Machines["local"] = &Machine{ID: "local", HostID: "store-host", Session: "local-session"}
	q.s.Machines["remote"] = &Machine{ID: "remote", HostID: "remote-host", Session: "remote-session"}
	q.mu.Unlock()
	const total = uint64(10 << 30)
	const disk = "shared-filesystem"
	floor := diskFreeFloor(total)
	free := floor + 1<<20
	q.files.diskProbe = func(string) (DiskReading, error) {
		return DiskReading{Volume: disk, TotalBytes: total, FreeBytes: free}, nil
	}
	partial := capacityPartial(t, c, strings.Repeat("x", 4096), jobInputUploadPurpose)
	newFile := FileRef{SHA256: strings.Repeat("e", 64), SizeBytes: 4096}
	reserve := uint64(q.fileStoreProductionReserve)
	const headroom = uint64(2048)
	remaining := uint64(partial.SizeBytes - 2)
	free = floor + reserve + remaining + headroom
	ctx := WithFileStoreSubmission(context.Background(), Spec{Project: "ordinary", Class: "experiment"})
	err := q.PreflightFileStore(ctx, FileManifest{"resumed": partial, "new": newFile})
	var refusal *HTTPError
	if !errors.As(err, &refusal) || refusal.Cause == nil || refusal.Cause.Predicate != fileCapacityProduction {
		t.Fatalf("expected real reserve refusal, got %v", err)
	}
	volume := diskVolumeID("store-host", disk)
	for _, row := range [][3]string{
		{"remote", "remote-session", volume},
		{"local", "stale-session", volume},
		{"local", "local-session", diskVolumeID("store-host", "different-volume")},
	} {
		if got := q.localFileStorePressure(ctx, row[0], row[1], row[2]); got.ShortfallBytes != 0 {
			t.Fatalf("wrong worker or volume got pressure: %+v", got)
		}
	}
	want := workerFileStorePressure{TargetFreeBytes: floor + remaining + reserve + uint64(newFile.SizeBytes), ShortfallBytes: uint64(newFile.SizeBytes) - headroom}
	var demands []workerDiskDemand
	if err := c.Call(ctx, "POST", "/api/workers/disk-demand?compiler_volume="+volume, workerDiskDemandRequest{Machine: "local", Session: "local-session"}, &demands); err != nil {
		t.Fatal(err)
	}
	if len(demands) != 1 || demands[0].FileStore == nil || *demands[0].FileStore != want {
		t.Fatalf("pressure includes resumed allocation or misses reserve: got %+v want %+v", demands, want)
	}
	if got := q.localFileStorePressure(ctx, "local", "local-session", volume); got.ShortfallBytes != 0 {
		t.Fatalf("one preflight supplied repeated pressure: %+v", got)
	}
	free = floor + reserve + remaining - 4096
	if got := q.localFileStorePressure(ctx, "local", "local-session", volume); got.ShortfallBytes != 4096 {
		t.Fatalf("reserve deficit alone supplies pressure: %+v", got)
	}
	q.files.limit = int64(reserve)
	if got := q.localFileStorePressure(ctx, "local", "local-session", volume); got.ShortfallBytes != 0 {
		t.Fatalf("store allowance deficit cannot be fixed by a worker trim: %+v", got)
	}
}

func TestFileStoreWorkerPressureKeepsLargerBlockedWrite(t *testing.T) {
	q, clock := fixture(t)
	const reserve = uint64(256 << 10)
	q.fileStoreProductionReserve = int64(reserve)
	c := retentionClient(t, q)
	const disk = "shared-filesystem"
	const total = uint64(10 << 30)
	floor := diskFreeFloor(total)
	free := floor + 1<<20
	q.files.diskProbe = func(string) (DiskReading, error) {
		return DiskReading{Volume: disk, TotalBytes: total, FreeBytes: free}, nil
	}

	_, input := retentionUpload(t, c, "worker input")
	s := spec("experiment")
	s.Resumable = false
	s.Files = FileManifest{"input": input}
	s.Outputs = map[string]string{"out.txt": "results"}
	m := machine("publisher", "fake")
	m.HostID = "store-host"
	q.diskHost = m.HostID
	check(t, q, m, nil)
	submitted := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != submitted.ID {
		t.Fatal("missing stored-file attempt")
	}
	if err := q.Started(result(assigned, false)); err != nil {
		t.Fatal(err)
	}
	released := result(assigned, false)
	released.ReleaseOnly = true
	if err := q.Complete(released); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	const headroom = uint64(100)
	free = floor + reserve + headroom
	large := FileRef{SHA256: strings.Repeat("a", 64), SizeBytes: 175161}
	inputCtx := WithFileStoreSubmission(ctx, Spec{Project: "build", Class: "test"})
	assertFilePolicyRefusal(t, c.PreflightFileStore(inputCtx, FileManifest{"bundle": large}), "production reserve")

	clock.Advance(time.Second)
	smallPath, small := policyInput(t, strings.Repeat("x", 330))
	outputCtx := withFileStoreAttempt(ctx, *assigned)
	assertFilePolicyRefusal(t, c.UploadFile(outputCtx, smallPath, small), "production reserve")

	volume := diskVolumeID(m.HostID, disk)
	readPressure := func() workerFileStorePressure {
		t.Helper()
		var demands []workerDiskDemand
		if err := c.Call(ctx, "POST", "/api/workers/disk-demand?compiler_volume="+volume,
			workerDiskDemandRequest{Machine: m.ID, Session: m.Session}, &demands); err != nil {
			t.Fatal(err)
		}
		for _, demand := range demands {
			if demand.FileStore != nil {
				return *demand.FileStore
			}
		}
		return workerFileStorePressure{}
	}
	want := workerFileStorePressure{
		TargetFreeBytes: floor + reserve + uint64(large.SizeBytes),
		ShortfallBytes:  uint64(large.SizeBytes) - headroom,
	}
	if got := readPressure(); got != want {
		t.Fatalf("smaller upload masked a larger blocked write: got %+v want %+v", got, want)
	}
	clock.Advance(time.Second)
	assertFilePolicyRefusal(t, c.UploadFile(outputCtx, smallPath, small), "production reserve")
	if got := readPressure(); got.ShortfallBytes != uint64(small.SizeBytes)-headroom {
		t.Fatalf("smaller output could not contribute fresh pressure after the preflight was consumed: %+v", got)
	}
	free = floor + reserve + uint64(small.SizeBytes)
	if err := c.UploadFile(outputCtx, smallPath, small); err != nil {
		t.Fatal(err)
	}
	free = floor + reserve + headroom
	if got := readPressure(); got.ShortfallBytes != 0 || q.files.pressure.Load() != nil {
		t.Fatalf("already allocated output still drives pressure: %+v", got)
	}
}
