package myce

import (
	"context"
	"errors"
	"strings"
	"testing"
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
