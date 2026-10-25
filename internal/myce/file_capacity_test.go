package myce

import (
	"context"
	"encoding/base64"
	"errors"
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
	res, err := c.fileRequest(context.Background(), "POST", "/api/files/uploads/", nil, map[string]string{"Tus-Resumable": "1.0.0", "Upload-Length": strconv.FormatInt(ref.SizeBytes, 10), "Upload-Metadata": metadata})
	if err != nil {
		t.Fatal(err)
	}
	if err := fileResponse(res, 201); err != nil {
		t.Fatal(err)
	}
	res, err = c.fileRequest(context.Background(), "PATCH", "/api/files/uploads/"+ref.SHA256, strings.NewReader(content[:2]), map[string]string{"Tus-Resumable": "1.0.0", "Upload-Offset": "0", "Content-Type": "application/offset+octet-stream"})
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
	res, err := c.fileRequest(context.Background(), "HEAD", "/api/files/uploads/"+recent.SHA256, nil, map[string]string{"Tus-Resumable": "1.0.0"})
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
