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
	q.diskHost = "store-host"
	c := retentionClient(t, q)
	if err := q.update(func() error {
		q.s.Machines["local"] = &Machine{ID: "local", HostID: "store-host", Session: "local-session"}
		q.s.Machines["remote"] = &Machine{ID: "remote", HostID: "remote-host", Session: "remote-session"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
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
	want := workerFileStorePressure{TargetFreeBytes: floor + remaining + reserve + uint64(newFile.SizeBytes), ShortfallBytes: uint64(newFile.SizeBytes) - headroom, BlockedWrite: true}
	var demands []workerDiskDemand
	if err := c.Call(ctx, "POST", "/api/workers/disk-demand?compiler_volume="+volume, workerDiskDemandRequest{Machine: "local", Session: "local-session"}, &demands); err != nil {
		t.Fatal(err)
	}
	if len(demands) != 1 || demands[0].FileStore == nil || *demands[0].FileStore != want {
		t.Fatalf("pressure includes resumed allocation or misses reserve: got %+v want %+v", demands, want)
	}
	if got := q.localFileStorePressure(ctx, "local", "local-session", volume); got != want {
		t.Fatalf("reading pressure lost the unhandled preflight: got %+v want %+v", got, want)
	}
	free = want.TargetFreeBytes
	if got := q.localFileStorePressure(ctx, "local", "local-session", volume); got.ShortfallBytes != 0 || q.files.pressure.Load() != nil {
		t.Fatalf("preflight that now fits still drives pressure: %+v", got)
	}
	free = floor + reserve + remaining - 4096
	if got := q.localFileStorePressure(ctx, "local", "local-session", volume); got.ShortfallBytes != 4096 || got.BlockedWrite {
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
		BlockedWrite:    true,
	}
	if got := readPressure(); got != want {
		t.Fatalf("smaller upload masked a larger blocked write: got %+v want %+v", got, want)
	}
	clock.Advance(unownedFileStoreDemandWindow - time.Second)
	assertFilePolicyRefusal(t, c.UploadFile(outputCtx, smallPath, small), "production reserve")
	if got := readPressure(); got.ShortfallBytes != uint64(small.SizeBytes)-headroom {
		t.Fatalf("abandoned preflight masked a new smaller refusal after expiry: %+v", got)
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

func TestFileStoreWorkerPressureUsesCommittedReservationsAndSession(t *testing.T) {
	q, _ := fixture(t)
	const reserve, total = uint64(32 << 10), uint64(10 << 30)
	const disk = "shared-filesystem"
	q.fileStoreProductionReserve = int64(reserve)
	q.diskHost = "store"
	c := retentionClient(t, q)
	m := machine("worker", "fake")
	m.HostID = q.diskHost
	m.DiskVolumes = []DiskVolume{{Roots: []string{"data"}, TotalBytes: total, FreeBytes: diskFreeFloor(total)}}
	check(t, q, m, nil)

	type probePause struct{ ready, resume chan struct{} }
	pauses := make(chan probePause, 1)
	floor := diskFreeFloor(total)
	q.files.diskProbe = func(string) (DiskReading, error) {
		select {
		case pause := <-pauses:
			close(pause.ready)
			<-pause.resume
		default:
		}
		return DiskReading{Volume: disk, TotalBytes: total, FreeBytes: floor + reserve + 1024}, nil
	}
	ctx := WithFileStoreSubmission(context.Background(), Spec{Project: "ordinary", Class: "experiment"})
	const growth = uint64(4096)
	files := FileManifest{"input": {SHA256: strings.Repeat("a", 64), SizeBytes: int64(growth)}}
	assertFilePolicyRefusal(t, c.PreflightFileStore(ctx, files), "production reserve")

	volume := diskVolumeID(m.HostID, disk)
	type response struct {
		pressure workerFileStorePressure
		err      error
	}
	poll := func() <-chan response {
		done := make(chan response, 1)
		go func() {
			var demands []workerDiskDemand
			err := c.Call(ctx, "POST", "/api/workers/disk-demand?compiler_volume="+volume,
				workerDiskDemandRequest{Machine: m.ID, Session: m.Session}, &demands)
			for _, demand := range demands {
				if demand.FileStore != nil {
					done <- response{*demand.FileStore, err}
					return
				}
			}
			done <- response{err: err}
		}()
		return done
	}
	checkPressure := func(result response, want workerFileStorePressure) {
		t.Helper()
		if result.err != nil || result.pressure != want {
			t.Fatalf("pressure = %+v, err = %v, want %+v", result.pressure, result.err, want)
		}
	}
	want := workerFileStorePressure{TargetFreeBytes: floor + reserve + growth, ShortfallBytes: growth - 1024, BlockedWrite: true}
	q.mu.Lock()
	select {
	case r := <-poll():
		q.mu.Unlock()
		checkPressure(r, want)
	case <-time.After(2 * time.Second):
		q.mu.Unlock()
		t.Fatal("hygiene route waited on the queue writer")
	}

	pollAcrossCommit := func(update func() error) response {
		t.Helper()
		pause := probePause{make(chan struct{}), make(chan struct{})}
		pauses <- pause
		done := poll()
		select {
		case <-pause.ready:
		case <-time.After(2 * time.Second):
			close(pause.resume)
			t.Fatal("hygiene did not sample file-store capacity")
		}
		err := q.update(update)
		close(pause.resume)
		if err != nil {
			t.Fatal(err)
		}
		return <-done
	}
	const output = uint64(8192)
	r := pollAcrossCommit(func() error {
		s := asyncDeliverySpec()
		s.Disk.OutputBytes = output
		q.putJob("output", &Job{ID: "output", State: "queued", Spec: s, Queued: q.clock.Now()})
		return nil
	})
	want.TargetFreeBytes += output
	want.ShortfallBytes += output
	checkPressure(r, want)
	r = pollAcrossCommit(func() error {
		q.s.Machines[m.ID].Session = "replacement-session"
		return nil
	})
	checkPressure(r, workerFileStorePressure{})
}
