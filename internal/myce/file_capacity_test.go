package myce

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func capacityPartial(t *testing.T, c Client, content, purpose string) FileRef {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	metadata := "sha256 " + base64.StdEncoding.EncodeToString([]byte(ref.SHA256))
	if purpose != "" {
		metadata += ",purpose " + base64.StdEncoding.EncodeToString([]byte(purpose))
	}
	res, err := c.fileRequest(context.Background(), "POST", "/api/files/uploads/", nil, map[string]string{"Tus-Resumable": "1.0.0", "Upload-Length": strconv.FormatInt(ref.SizeBytes, 10), "Upload-Metadata": metadata}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := fileResponse(res, 201); err != nil {
		t.Fatal(err)
	}
	res, err = c.fileRequest(context.Background(), "PATCH", "/api/files/uploads/"+ref.SHA256, strings.NewReader(content[:2]), map[string]string{"Tus-Resumable": "1.0.0", "Upload-Offset": "0", "Content-Type": "application/offset+octet-stream"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := fileResponse(res, 204); err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestFileCapacityAbandonedUploadSweep(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	abandoned := capacityPartial(t, c, "abandoned input", jobInputUploadPurpose)
	recent := capacityPartial(t, c, "resumed input", jobInputUploadPurpose)
	imported := capacityPartial(t, c, "unmarked import", "")
	clock.Advance(DefaultFileRetention - time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(q.files.root, "uploads", abandoned.SHA256)); err != nil {
		t.Fatal("early sweep", err)
	}
	// A durable progress/HEAD observation starts a fresh idle window.
	res, err := c.fileRequest(context.Background(), "HEAD", "/api/files/uploads/"+recent.SHA256, nil, map[string]string{"Tus-Resumable": "1.0.0"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := fileResponse(res, 200); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Nanosecond)
	// Admission uses ordinary maintenance before checking available capacity.
	if err := c.PreflightFileStore(context.Background(), FileManifest{"new": {SHA256: strings.Repeat("a", 64), SizeBytes: 1}}); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", ".info"} {
		if _, err := os.Stat(filepath.Join(q.files.root, "uploads", abandoned.SHA256+suffix)); !os.IsNotExist(err) {
			t.Error("abandoned upload retained", err)
		}
		for _, ref := range []FileRef{recent, imported} {
			if _, err := os.Stat(filepath.Join(q.files.root, "uploads", ref.SHA256+suffix)); err != nil {
				t.Error("protected/recent upload lost", err)
			}
		}
	}
	if _, marked := q.s.JobInputUploads[abandoned.SHA256]; marked {
		t.Error("reclaimed marker retained")
	}
}

func TestFileCapacityTypedRefusal(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	q.files.limit = 8
	_, _ = retentionUpload(t, c, "keep")
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, []byte("large"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	assert := func(err error) {
		t.Helper()
		var he *HTTPError
		if !errors.As(err, &he) || he.Status != 507 || he.Code != "file_store_capacity" {
			t.Fatalf("untyped refusal: %v", err)
		}
		for _, part := range []string{"queue-file-store", "need=5 bytes", "free=4 bytes", "shortfall=1 bytes", "--file-store-bytes"} {
			if !strings.Contains(he.Message, part) {
				t.Errorf("missing %q: %s", part, he.Message)
			}
		}
		if strings.Contains(he.Message, q.files.root) {
			t.Error("private store path exposed")
		}
	}
	assert(c.PreflightFileStore(context.Background(), FileManifest{"input": ref}))
	assert(c.UploadJobInput(context.Background(), path, ref))
	if _, err := os.Stat(filepath.Join(q.files.root, "uploads", ref.SHA256)); !os.IsNotExist(err) {
		t.Error("refused upload wrote payload", err)
	}
	// Reuse #158's per-volume floor even when the configured store has room.
	q.files.limit = 1 << 20
	q.files.diskProbe = func(string) (DiskReading, error) {
		return DiskReading{TotalBytes: 1 << 30, FreeBytes: (256 << 20) + 4}, nil
	}
	assert(c.PreflightFileStore(context.Background(), FileManifest{"input": ref}))
}

func TestFileCapacityProtectedReferencesSurvive(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	live := capacityPartial(t, c, "live recoverable input", jobInputUploadPurpose)
	checkpoint := capacityPartial(t, c, "recoverable checkpoint", jobInputUploadPurpose)
	catalog := capacityPartial(t, c, "catalog model", jobInputUploadPurpose)
	if err := q.update(func() error {
		q.s.Jobs["recovery"] = &Job{State: "lost", Spec: Spec{Files: FileManifest{"input": live}}, FileCheckpoints: map[int]FileCheckpoint{1: {Files: FileManifest{"save": checkpoint}}}}
		q.models.Models = []Model{{SHA256: catalog.SHA256}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(DefaultFileRetention + filePublicationGrace)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []FileRef{live, checkpoint, catalog} {
		for _, suffix := range []string{"", ".info"} {
			if _, err := os.Stat(filepath.Join(q.files.root, "uploads", ref.SHA256+suffix)); err != nil {
				t.Error("protected reference lost", err)
			}
		}
	}
}

func TestFileCapacityStoredInputsBelowDiskFloor(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	_, first := retentionUpload(t, c, "first stored input")
	_, second := retentionUpload(t, c, "second stored input")
	q.files.diskProbe = func(string) (DiskReading, error) {
		return DiskReading{TotalBytes: 1 << 30, FreeBytes: 1}, nil
	}
	if err := c.PreflightFileStore(context.Background(), FileManifest{"first": first, "second": second}); err != nil {
		t.Fatalf("stored inputs need no new space below the disk floor: %v", err)
	}
}

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

func TestFileCapacityInterruptedUploadPublicationGrace(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	q.fileRetention = time.Hour
	ref := capacityPartial(t, c, "grace protected input", jobInputUploadPurpose)
	clock.Advance(filePublicationGrace - time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", ".info"} {
		if _, err := os.Stat(filepath.Join(q.files.root, "uploads", ref.SHA256+suffix)); err != nil {
			t.Fatal("upload lost inside publication grace", err)
		}
	}
	clock.Advance(2 * time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", ".info"} {
		if _, err := os.Stat(filepath.Join(q.files.root, "uploads", ref.SHA256+suffix)); !os.IsNotExist(err) {
			t.Error("upload retained after publication grace", err)
		}
	}
}

func TestFileCapacityMissingUploadMarkerCleared(t *testing.T) {
	q, _ := fixture(t)
	retentionClient(t, q)
	hash := strings.Repeat("a", 64)
	if err := q.update(func() error { q.s.JobInputUploads = map[string]time.Time{hash: {}}; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, marked := q.s.JobInputUploads[hash]; marked {
		t.Fatal("missing upload still takes a reclamation slot")
	}
}
