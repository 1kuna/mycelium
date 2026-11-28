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
	"sync"
	"testing"
	"time"
)

func capacityPartial(t *testing.T, c Client, content, purpose string, contexts ...context.Context) FileRef {
	t.Helper()
	ctx := requestContext(contexts)
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
	res, err := c.fileRequest(ctx, "POST", "/api/files/uploads/", nil, map[string]string{"Tus-Resumable": "1.0.0", "Upload-Length": strconv.FormatInt(ref.SizeBytes, 10), "Upload-Metadata": metadata}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := fileResponse(res, 201); err != nil {
		t.Fatal(err)
	}
	res, err = c.fileRequest(ctx, "PATCH", "/api/files/uploads/"+ref.SHA256, strings.NewReader(content[:2]), map[string]string{"Tus-Resumable": "1.0.0", "Upload-Offset": "0", "Content-Type": "application/offset+octet-stream"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := fileResponse(res, 204); err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestFileCapacityUnsubmittedClaimsProtectPublishedInputs(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	path, ref := policyInput(t, "claimed input without any job reference")
	owner := WithFileStoreSubmission(context.Background(), Spec{Project: "owner"})
	if err := c.UploadJobInput(owner, path, ref); err != nil {
		t.Fatal(err)
	}
	clock.Advance(testFileRetention + filePublicationGrace + time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if status := q.fileStoreStatus(context.Background()); status.ReclaimableBytes != 0 {
		t.Fatal("claimed bytes reported reclaimable", status)
	}
	s := spec("experiment")
	s.Project = "owner"
	s.Files = FileManifest{"input": ref}
	submit(t, q, s)
}

func claimedCapacityPartial(t *testing.T, q *Queue, c Client, content, purpose string) (FileInputClaim, context.Context) {
	t.Helper()
	ref := creationRef(content)
	owner := Spec{Project: "owner", Class: "experiment"}
	claim, err := q.claimFileInput(context.Background(), owner, ref)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(WithFileStoreSubmission(context.Background(), owner), fileInputClaimKey{}, claim.ID)
	capacityPartial(t, c, content, purpose, ctx)
	// The HTTP response can reach the client before the durable idle refresh.
	q.fileLifecycle.Lock()
	q.fileLifecycle.Unlock()
	return claim, ctx
}

func TestFileCapacityAbandonedClaimedUploadSweep(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(strconv.FormatBool(legacy), func(t *testing.T) {
			q, clock := fixture(t)
			c := retentionClient(t, q)
			const idle = time.Hour // Shorter than the seeded publication grace.
			if err := q.ConfigureFileRetention(idle); err != nil {
				t.Fatal(err)
			}
			abandoned, ctx := claimedCapacityPartial(t, q, c, "abandoned claimed input", jobInputUploadPurpose)
			imported, _ := claimedCapacityPartial(t, q, c, "claimed unknown import", "")
			if legacy {
				if err := q.update(func() error { delete(q.s.JobInputUploads, abandoned.File.SHA256); return nil }); err != nil {
					t.Fatal(err)
				}
			} else if at, marked := q.s.JobInputUploads[abandoned.File.SHA256]; !marked || !at.IsZero() {
				t.Fatal("unsettled claim lost the interrupted-upload marker")
			}
			path := filepath.Join(q.files.root, "uploads", abandoned.File.SHA256)
			allocated, err := storageAllocation(ctx, path, false, map[[2]uint64]bool{})
			if err != nil || allocated == 0 {
				t.Fatalf("partial upload did not allocate blocks: %d, %v", allocated, err)
			}
			clock.Advance(idle - time.Nanosecond)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			before := q.fileStoreStatus(ctx)
			if before.Error != "" || before.UsedBytes != 4 || before.Physical.RemainingUploadBytes != uint64(abandoned.File.SizeBytes+imported.File.SizeBytes-4) {
				t.Fatalf("early collection or missing upload commitment: %+v", before)
			}
			clock.Advance(2 * time.Nanosecond)
			if status := q.fileStoreStatus(ctx); status.ReclaimableBytes != 2 {
				t.Fatalf("idle claimed bytes missing from reclaimable accounting: %+v", status)
			}
			if legacy {
				syncDir := q.files.syncDir
				q.files.syncDir = func(string) error { return errors.New("test interrupted cleanup sync") }
				if err := q.reclaimInterruptedUploads(ctx, true); err != nil {
					t.Fatal(err)
				}
				q.files.syncDir = syncDir
				if _, marked := q.s.JobInputUploads[abandoned.File.SHA256]; !marked {
					t.Fatal("failed cleanup lost legacy ownership after unlink")
				}
				if _, ok := q.s.FileInputClaims[abandoned.ID]; !ok {
					t.Fatal("claim released before cleanup completed")
				}
			}
			if err := c.PreflightFileStore(context.Background(), FileManifest{"new": {SHA256: strings.Repeat("a", 64), SizeBytes: 1}}); err != nil {
				t.Fatal(err)
			}
			after := q.fileStoreStatus(ctx)
			if after.Error != "" || after.UsedBytes != 2 || after.Physical.RemainingUploadBytes != uint64(imported.File.SizeBytes-2) {
				t.Fatalf("abandoned upload retained bytes or commitment: %+v", after)
			}
			for _, suffix := range []string{"", ".info"} {
				if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
					t.Errorf("abandoned upload retained %q: %v", suffix, err)
				}
			}
			allocated, err = storageAllocation(ctx, path, true, map[[2]uint64]bool{})
			if err != nil || allocated != 0 {
				t.Fatalf("reclaimed upload still allocates blocks: %d, %v", allocated, err)
			}
			if _, ok := q.s.FileInputClaims[abandoned.ID]; ok {
				t.Error("abandoned upload's claim survived collection")
			}
			if _, ok := q.s.FileInputClaims[imported.ID]; !ok {
				t.Error("import lost its claim")
			}
			owner := WithFileStoreSubmission(context.Background(), Spec{Project: abandoned.Project, Class: abandoned.Class})
			path, ref := policyInput(t, "abandoned claimed input")
			if err := c.UploadJobInput(owner, path, ref); err != nil {
				t.Fatal("cannot restart after collection", err)
			}
		})
	}
}

func TestFileCapacityAbandonedUnclaimedLegacyUploadSweep(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	const content = "abandoned legacy input"
	abandoned := capacityPartial(t, c, content, jobInputUploadPurpose)
	q.fileLifecycle.Lock()
	q.fileLifecycle.Unlock()
	// Replay a transfer whose full body reached disk without publishing the
	// blob. The tus receipt was created with offset zero.
	path := filepath.Join(q.files.root, "uploads", abandoned.SHA256)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if err := q.files.refreshUpload(abandoned.SHA256); err != nil {
		t.Fatal(err)
	}
	imported := capacityPartial(t, c, "abandoned legacy import", fileImportUploadPurpose)
	protected := capacityPartial(t, c, "referenced legacy input", jobInputUploadPurpose)
	expiredOwner := submit(t, q, spec("experiment"))
	activeOwner := submit(t, q, spec("experiment"))
	finished := clock.Now()
	clock.Advance(max(testFileRetention, filePublicationGrace) + time.Nanosecond)
	recent := capacityPartial(t, c, "recent legacy input", jobInputUploadPurpose)
	q.fileLifecycle.Lock()
	q.fileLifecycle.Unlock()
	if err := q.update(func() error {
		past := q.touchJob(expiredOwner.ID)
		past.Spec.SharedInputs = map[string]SharedInput{"video": {SHA256: abandoned.SHA256, SizeBytes: abandoned.SizeBytes}}
		past.State, past.FinishedAt, past.ReleaseConfirmed = "succeeded", &finished, true
		past.FilesExpired, past.FileReclamation = true, &FileReclamation{}
		active := q.touchJob(activeOwner.ID)
		active.Spec.SharedInputs = map[string]SharedInput{"video": {SHA256: protected.SHA256, SizeBytes: protected.SizeBytes}}
		delete(q.s.JobInputUploads, abandoned.SHA256)
		delete(q.s.JobInputUploads, recent.SHA256)
		delete(q.s.JobInputUploads, protected.SHA256)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// An installed queue already indexed this terminal history on open.
	q.mu.Lock()
	q.rebuildRecords()
	q.mu.Unlock()
	ctx := context.Background()
	if status := q.fileStoreStatus(ctx); status.ReclaimableBytes != uint64(abandoned.SizeBytes) {
		t.Fatalf("idle unclaimed job-input bytes missing from reclaimable accounting: %+v", status)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	checkUpload := func(ref FileRef, present bool) {
		t.Helper()
		for _, suffix := range []string{"", ".info"} {
			_, err := os.Stat(filepath.Join(q.files.root, "uploads", ref.SHA256+suffix))
			if present && err != nil || !present && !os.IsNotExist(err) {
				t.Fatalf("unexpected presence for %s%s, want %t: %v", ref.SHA256, suffix, present, err)
			}
		}
	}
	checkUpload(abandoned, false)
	checkUpload(protected, true)
	checkUpload(imported, true)
	checkUpload(recent, true)
	clock.Advance(max(testFileRetention, filePublicationGrace) + time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	checkUpload(recent, false)
	checkUpload(protected, true)
	checkUpload(imported, true)
}

func TestFileCapacityClaimedResumeAtIdleBoundary(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	const content = "claimed input resumed across collection"
	claim, ctx := claimedCapacityPartial(t, q, c, content, jobInputUploadPurpose)
	entered, unblock := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	defer release()
	inputClaim := q.files.inputClaim
	var pause sync.Once
	q.files.inputClaim = func(ctx context.Context, ref FileRef, allocate bool) (context.Context, error) {
		pause.Do(func() { close(entered); <-unblock })
		return inputClaim(ctx, ref, allocate)
	}
	endpoint := "/api/files/uploads/" + claim.File.SHA256
	done := make(chan error, 1)
	go func() {
		res, err := c.fileRequest(ctx, http.MethodHead, endpoint, nil, map[string]string{"Tus-Resumable": "1.0.0"})
		if err == nil {
			if res.Header.Get("Upload-Offset") != "2" {
				err = errors.New("resume lost the accepted offset")
				res.Body.Close()
			} else {
				err = fileResponse(res, http.StatusOK)
			}
		}
		done <- err
	}()
	submitVerificationWait(t, "claimed resume in lifecycle fence", entered)
	clock.Advance(testFileRetention)
	if err := q.reclaimInterruptedUploads(ctx, true); err != nil {
		t.Fatal(err)
	}
	release()
	if err := submitVerificationWait(t, "claimed resume response", done); err != nil {
		t.Fatal(err)
	}
	q.fileLifecycle.Lock()
	q.fileLifecycle.Unlock()
	if err := q.reclaimInterruptedUploads(ctx, true); err != nil {
		t.Fatal(err)
	}
	res, err := c.fileRequest(ctx, http.MethodPatch, endpoint, strings.NewReader(content[2:]), map[string]string{"Tus-Resumable": "1.0.0", "Upload-Offset": "2", "Content-Type": "application/offset+octet-stream"})
	if err != nil {
		t.Fatal(err)
	}
	if err := fileResponse(res, http.StatusNoContent); err != nil {
		t.Fatal("valid resume did not refresh the idle window", err)
	}
	s := spec("experiment")
	s.Project = claim.Project
	s.Files = FileManifest{"input": claim.File}
	submit(t, q, s)
}

func TestFileCapacityAutomaticOrphansAreNotReclaimable(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	q.fileRetention = 0
	_, complete := retentionJobInput(t, c, "unsubmitted published input")
	claim, _ := claimedCapacityPartial(t, q, c, "unsubmitted interrupted input", jobInputUploadPurpose)
	partial := claim.File
	clock.Advance(testFileRetention + filePublicationGrace + time.Second)
	if status := q.fileStoreStatus(context.Background()); status.ReclaimableBytes != 0 {
		t.Errorf("automatic retention overstates reclaimable bytes: %d", status.ReclaimableBytes)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{q.files.blob(complete), filepath.Join(q.files.root, "uploads", partial.SHA256)} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("automatic mode collected unique input", err)
		}
	}
	q.fileRetention = testFileRetention
	if status := q.fileStoreStatus(context.Background()); status.ReclaimableBytes != uint64(complete.SizeBytes+2) {
		t.Fatal("explicit expiry policy lost reclaimable bytes", status)
	}
}

func TestFileCapacityAbandonedUploadSweep(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	abandoned := capacityPartial(t, c, "abandoned input", jobInputUploadPurpose)
	recent := capacityPartial(t, c, "resumed input", jobInputUploadPurpose)
	imported := capacityPartial(t, c, "unmarked import", "")
	clock.Advance(testFileRetention - time.Nanosecond)
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
		q.putJob("recovery", &Job{State: "lost", Spec: Spec{Files: FileManifest{"input": live}}, FileCheckpoints: map[int]FileCheckpoint{1: {Files: FileManifest{"save": checkpoint}}}})
		q.models.Models = []Model{{SHA256: catalog.SHA256}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(testFileRetention + filePublicationGrace)
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
	clock.Advance(testFileRetention)
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
	if err := c.Call(context.Background(), http.MethodGet, "/api/status?sections=storage", nil, &snap); err != nil {
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
	clock.Advance(time.Second) // Allow the observational status cache to expire.
	if err := c.Call(context.Background(), http.MethodGet, "/api/status?sections=storage", nil, &snap); err != nil {
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

func TestFileCapacityDataOnlyRetryDoesNotRefresh(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	ref := capacityPartial(t, c, "crash left only upload data", jobInputUploadPurpose)
	path := filepath.Join(q.files.root, "uploads", ref.SHA256)
	if err := os.Remove(path + ".info"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(testFileRetention + time.Nanosecond)
	for _, method := range []string{"HEAD", "PATCH"} {
		res, err := c.fileRequest(context.Background(), method, "/api/files/uploads/"+ref.SHA256, nil, map[string]string{"Tus-Resumable": "1.0.0", "Upload-Offset": "2", "Content-Type": "application/offset+octet-stream"})
		if err != nil {
			t.Fatal(err)
		}
		if err := fileResponse(res, 404); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("404 retries postponed data-only upload cleanup", err)
	}
}

func TestFileCapacityUnpublishedBlobRecoversMarker(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	ref := capacityPartial(t, c, "crash between blob and marker publication", jobInputUploadPurpose)
	path := filepath.Join(q.files.root, "uploads", ref.SHA256)
	// Recreate the publication crash window with complete, verified bytes.
	if err := os.WriteFile(path, []byte("crash between blob and marker publication"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := q.files.publish(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	clock.Advance(testFileRetention - time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if at := q.s.JobInputUploads[ref.SHA256]; at.IsZero() {
		t.Fatal("published blob still consumes interrupted-upload cleanup slots")
	}
	if err := q.files.verify(context.Background(), ref); err != nil {
		t.Fatal("blob lost inside retention", err)
	}
	clock.Advance(2 * time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(ref)); !os.IsNotExist(err) {
		t.Fatal("recovered publication never reclaimed", err)
	}
	if _, marked := q.s.JobInputUploads[ref.SHA256]; marked {
		t.Fatal("reclaimed publication marker retained")
	}
}
