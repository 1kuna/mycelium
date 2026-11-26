package myce

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Rewrites an upload's tusd metadata as another member would have written it.
func recordForeignUploadPath(t *testing.T, store *FileStore, hash string) {
	t.Helper()
	path := filepath.Join(store.root, "uploads", hash+".info")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var info map[string]any
	if err := json.Unmarshal(data, &info); err != nil {
		t.Fatal(err)
	}
	storage := info["Storage"].(map[string]any)
	storage["Path"] = "/another-member-mount/uploads/" + hash
	storage["InfoPath"] = "/another-member-mount/uploads/" + hash + ".info"
	if data, err = json.Marshal(info); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func uploadHeadOffset(t *testing.T, ctx context.Context, c Client, hash string) string {
	t.Helper()
	res, err := c.fileRequest(ctx, http.MethodHead, "/api/files/uploads/"+hash, nil, map[string]string{"Tus-Resumable": "1.0.0"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	offset := res.Header.Get("Upload-Offset")
	if err := fileResponse(res, http.StatusOK); err != nil {
		t.Fatal(err)
	}
	return offset
}

func writeInput(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// An upload's .info records the absolute data path of the member that
// created it. Capacity, resume and publication find it under this store.
func TestFileStoreUploadRecordedOnAnotherMount(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	ctx := context.Background()
	const content = "bytes recorded on another member's mount"
	partial := capacityPartial(t, c, content, jobInputUploadPurpose)
	recordForeignUploadPath(t, q.files, partial.SHA256)
	if status := q.fileStoreStatus(ctx); status.Error != "" || status.OpenUploadBytes != 2 {
		t.Fatalf("capacity with a foreign upload path: %+v", status)
	}
	if offset := uploadHeadOffset(t, ctx, c, partial.SHA256); offset != "2" {
		t.Fatalf("resume offset = %q", offset)
	}
	if err := c.UploadJobInput(ctx, writeInput(t, content), partial); err != nil {
		t.Fatal(err)
	}
	if err := c.HasFile(ctx, partial); err != nil {
		t.Fatal(err)
	}
}

// A HEAD during a PATCH that holds the tus lock, such as a long publication
// on a network share, reports the durable offset instead of a lock timeout.
func TestFileStoreUploadHeadWhileTransferHoldsLock(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	partial := capacityPartial(t, c, "transfer holds the upload lock", jobInputUploadPurpose)
	lock, err := q.files.uploadLocker.NewLock(partial.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Lock(context.Background(), func() {}); err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if offset := uploadHeadOffset(t, ctx, c, partial.SHA256); offset != "2" {
		t.Fatalf("offset during a locked transfer = %q", offset)
	}
}

// A shared member walks its store once, then capacity snapshots re-read only
// what this process changed. The periodic walk adopts outside writes.
func TestSharedFileInventoryReadsOnlyChanges(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	store := q.files
	store.inventory = &fileInventory{dirty: map[string]bool{}} // As on a shared member.
	ctx := context.Background()
	cached := func() map[string]storedPayload {
		t.Helper()
		_, payloads, err := store.diskCapacity(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return payloads
	}
	walks := uint64(0)
	matches := func(step string) {
		t.Helper()
		got := cached()
		if n := store.inventoryWalks.Load(); n != walks+1 {
			t.Fatalf("%s: %d store walks, want %d", step, n, walks+1)
		}
		want, err := store.inventoryUnlocked(ctx)
		walks++
		if err != nil {
			t.Fatal(err)
		}
		if !maps.Equal(got, want) {
			t.Fatalf("%s: cached inventory\n%+v\nwant\n%+v", step, got, want)
		}
	}
	matches("first snapshot")
	partial := capacityPartial(t, c, "interrupted input", jobInputUploadPurpose)
	matches("upload created and written")
	path, published := retentionUpload(t, c, "published input")
	matches("upload published")
	if err := c.UploadFile(ctx, path, published); err != nil {
		t.Fatal(err)
	}
	if err := c.UploadJobInput(ctx, writeInput(t, "interrupted input"), partial); err != nil {
		t.Fatal(err)
	}
	matches("interrupted upload resumed")
	if _, err := store.trashBlob(published.SHA256); err != nil {
		t.Fatal(err)
	}
	matches("blob moved to trash")
	if err := store.restoreBlob(published.SHA256); err != nil {
		t.Fatal(err)
	}
	matches("blob restored")

	// A write this process did not make waits for the periodic walk.
	outside := FileRef{SHA256: "b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c", SizeBytes: 4}
	if err := os.WriteFile(store.blob(outside), []byte("foo\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, ok := cached()[outside.SHA256]; ok {
		t.Fatal("a snapshot walked the store")
	}
	clock.Advance(fileInventoryRewalk)
	cached()
	walks++
	proofEventually(t, "periodic walk", func() bool {
		store.inventory.read.Lock()
		defer store.inventory.read.Unlock()
		return !store.inventory.walking
	})
	if _, ok := cached()[outside.SHA256]; !ok {
		t.Fatal("the periodic walk did not adopt an outside write")
	}
	matches("after the periodic walk")
}

// Members mount the shared store at different paths. After a handover the
// successor resumes and publishes uploads the predecessor created, accepts
// file submissions and new uploads, and its capacity checks walk only once.
func TestSharedQueuePostgresStoreMountedElsewhere(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	ctx := t.Context()
	clock := proofClock()
	serve := func(q *Queue) Client {
		api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
		t.Cleanup(api.Close)
		return Client{URL: api.URL, Token: "test"}
	}
	firstMount := filepath.Join(dir, "first-mount")
	old, first := proofOpenSharedQueueAt(t, a, dir, "old", firstMount, clock)
	if err := a.renew(ctx, first, time.Minute, true); err != nil {
		t.Fatal(err)
	}
	c := serve(old)
	const content = "interrupted on the first member"
	partial := capacityPartial(t, c, content, jobInputUploadPurpose)
	_, published := retentionJobInput(t, c, "published on the first member")
	if err := a.renew(ctx, first, 0, false); err != nil {
		t.Fatal(err)
	}
	// The successor's host mounts the same share somewhere else.
	secondMount := filepath.Join(dir, "second-mount")
	if err := os.Rename(firstMount, secondMount); err != nil {
		t.Fatal(err)
	}
	next, second := proofOpenSharedQueueAt(t, a, dir, "next", secondMount, clock)
	if err := a.renew(ctx, second, time.Minute, true); err != nil {
		t.Fatal(err)
	}
	c = serve(next)
	if status := next.fileStoreStatus(ctx); status.Error != "" || status.OpenUploadBytes != 2 {
		t.Fatalf("successor capacity: %+v", status)
	}
	if offset := uploadHeadOffset(t, ctx, c, partial.SHA256); offset != "2" {
		t.Fatalf("successor resume offset = %q", offset)
	}
	if err := c.UploadJobInput(ctx, writeInput(t, content), partial); err != nil {
		t.Fatal(err)
	}
	_, fresh := retentionJobInput(t, c, "uploaded on the successor")
	s := proofSpec("mounted-elsewhere")
	s.Files = FileManifest{"resumed": partial, "published": published, "fresh": fresh}
	if _, err := next.Submit(s); err != nil {
		t.Fatal(err)
	}
	if status := next.fileStoreStatus(ctx); status.Error != "" || status.HeldPublishedBytes == 0 {
		t.Fatalf("successor capacity after submission: %+v", status)
	}
	if walks := next.files.inventoryWalks.Load(); walks != 1 {
		t.Fatalf("successor walked its store %d times", walks)
	}
}
