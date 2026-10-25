package myce

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestFileCapacityStatus(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	q.files.limit = 64
	_, expired := retentionJobInput(t, c, "expired")
	partial := capacityPartial(t, c, "partial input", jobInputUploadPurpose)
	_, kept := retentionUpload(t, c, "kept")
	clock.Advance(DefaultFileRetention)
	q.files.diskProbe = func(string) (DiskReading, error) {
		// A filesystem probe must never execute inside the queue lock (#80).
		if !q.mu.TryLock() {
			t.Error("disk I/O under q.mu")
		} else {
			q.mu.Unlock()
		}
		return DiskReading{TotalBytes: 1 << 30, FreeBytes: 1 << 30}, nil
	}
	var snap Snapshot
	if err := c.Call(context.Background(), http.MethodGet, "/api/status", nil, &snap); err != nil {
		t.Fatal(err)
	}
	fs := snap.FileStore
	if fs == nil || fs.Store != "queue-file-store" || fs.UsedBytes != 13 || fs.FreeBytes != 40 || fs.ReclaimableBytes != 9 || fs.DiskFreeBytes != 1<<30 || fs.FloorBytes != 256<<20 {
		t.Fatalf("incorrect byte status: %+v", fs)
	}
	// Read-only status must leave even collectible data alone.
	for _, path := range []string{q.files.blob(expired), q.files.blob(kept), filepath.Join(q.files.root, "uploads", partial.SHA256)} {
		if _, err := os.Stat(path); err != nil {
			t.Error("status collected data", err)
		}
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if err := c.Call(context.Background(), http.MethodGet, "/api/status", nil, &snap); err != nil {
		t.Fatal(err)
	}
	if fs := snap.FileStore; fs.UsedBytes != 4 || fs.FreeBytes != 60 || fs.ReclaimableBytes != 0 {
		t.Fatalf("post-sweep byte status: %+v", fs)
	}
}
