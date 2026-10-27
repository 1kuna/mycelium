package myce

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func gcRecipe(t *testing.T, q *Queue, content string) EnvironmentRecipe {
	t.Helper()
	r := EnvironmentRecipe{Files: map[string][]byte{"requirements.txt": []byte(content)}}
	q.fileLifecycle.RLock()
	err := q.files.freezeRecipe(&r, nil)
	q.fileLifecycle.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestBlobGCRejectedRecipeRetentionAndRestart(t *testing.T) {
	q, clock := fixture(t)
	s := spec("experiment")
	s.Project = "" // Rejected only after recipe publication.
	s.Environment = Environment{LockFile: filepath.Join(t.TempDir(), "requirements.txt"), Python: "3.12", Platforms: []string{"fake"}}
	if err := os.WriteFile(s.Environment.LockFile, []byte("example==1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Submit(s); err == nil {
		t.Fatal("invalid submit accepted")
	}
	entries, err := os.ReadDir(filepath.Join(q.files.root, "blobs"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("rejected submit did not publish one recipe: %v %d", err, len(entries))
	}
	ref := FileRef{SHA256: entries[0].Name()}
	// Eligibility must survive a crash/restart, unlike an in-memory orphan list.
	db, root, store := strings.TrimSuffix(q.lock.Name(), ".lock"), q.root, q.files.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, root, clock, WithCheckpointAvailability(func() bool { return true }), WithFileStore(store, 1<<20, false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	clock.Advance(DefaultFileRetention - time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(ref)); err != nil {
		t.Fatal("recipe lost within retention", err)
	}
	clock.Advance(2 * time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(ref)); !os.IsNotExist(err) {
		t.Fatal("abandoned recipe leaked", err)
	}
}

func TestBlobGCBundlePrunedAfterRetainedAnswer(t *testing.T) {
	q, clock := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	e = q.s.Extensions[extensionKey(e)]
	if e.FilesRef == nil {
		t.Fatal("no frozen bundle")
	}
	if err := q.update(func() error {
		j := &Job{ID: "retained", State: "succeeded", ReleaseConfirmed: true, Extensions: []Extension{e}}
		q.putJob(j.ID, j)
		q.putSubmission("answer", &submissionRecord{Kind: submissionBinding, JobID: j.ID, Job: j})
		delete(q.s.Extensions, extensionKey(e))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(DefaultFileRetention + time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if err := q.files.verify(context.Background(), *e.FilesRef); err != nil {
		t.Fatal("retained job lost bundle", err)
	}
	if err := q.update(func() error { q.deleteJob("retained"); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if err := q.files.verify(context.Background(), *e.FilesRef); err != nil {
		t.Fatal("keyed answer lost bundle", err)
	}
	clock.Advance(submissionRetention + time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(*e.FilesRef)); !os.IsNotExist(err) {
		t.Fatal("pruned bundle leaked", err)
	}
}

func TestBlobGCConcurrentUploadAndReferences(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	r := gcRecipe(t, q, "concurrently imported recipe")
	kept := gcRecipe(t, q, "catalog recipe")
	orphan := gcRecipe(t, q, "unreferenced recipe")
	if err := q.update(func() error {
		q.environments.Environments = map[string]map[string]EnvironmentRecipe{"tools": {"fake": kept}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(DefaultFileRetention + time.Nanosecond)
	// A real concurrent upload has reached its durability barrier while GC runs.
	// It reuses an old internal blob and refreshes mtime before acquiring custody.
	source := filepath.Join(t.TempDir(), "recipe.json")
	data, err := os.ReadFile(q.files.blob(*r.FilesRef))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	entered, unblock := gateSubmitVerification(t, q, nil)
	done := make(chan error, 1)
	go func() { done <- c.UploadJobInput(context.Background(), source, *r.FilesRef) }()
	submitVerificationWait(t, "concurrent upload durability", entered)
	submitVerificationProgress(t, "GC during upload", q.reclaimPublishedFiles)
	for _, recipe := range []EnvironmentRecipe{r, kept, orphan} {
		if err := q.files.verify(context.Background(), *recipe.FilesRef); err != nil {
			t.Fatal("GC crossed publication fence", err)
		}
	}
	unblock()
	if err := submitVerificationWait(t, "upload", done); err != nil {
		t.Fatal(err)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(*orphan.FilesRef)); !os.IsNotExist(err) {
		t.Fatal("unreferenced recipe leaked", err)
	}
	if err := q.files.verify(context.Background(), *r.FilesRef); err != nil {
		t.Fatal("fresh upload lost within retention", err)
	}
	clock.Advance(DefaultFileRetention + time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(*r.FilesRef)); !os.IsNotExist(err) {
		t.Fatal("unreferenced refreshed recipe leaked", err)
	}
	for _, recipe := range []EnvironmentRecipe{kept} {
		if err := q.files.verify(context.Background(), *recipe.FilesRef); err != nil {
			t.Fatal("import/catalog custody lost", err)
		}
	}
}

func TestBlobGCImportAndPublicationGrace(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	if err := q.ConfigureFileRetention(time.Hour); err != nil {
		t.Fatal(err)
	}
	orphan := gcRecipe(t, q, "short retention recipe")
	imported := gcRecipe(t, q, "explicit import")
	data, err := os.ReadFile(q.files.blob(*imported.FilesRef))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "import")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := c.UploadFile(context.Background(), path, *imported.FilesRef); err != nil {
		t.Fatal(err)
	}
	clock.Advance(filePublicationGrace - time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if err := q.files.verify(context.Background(), *orphan.FilesRef); err != nil {
		t.Fatal("publication grace lost", err)
	}
	clock.Advance(2 * time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(*orphan.FilesRef)); !os.IsNotExist(err) {
		t.Fatal("orphan leaked after grace", err)
	}
	if err := q.files.verify(context.Background(), *imported.FilesRef); err != nil {
		t.Fatal("generic import became collectible", err)
	}
}

func TestBlobGCImportStartedBeforeRecipePublication(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	files := map[string][]byte{"requirements.txt": []byte("overlapping generic import")}
	data, err := json.Marshal(files)
	if err != nil {
		t.Fatal(err)
	}
	ref := capacityPartial(t, c, string(data), "")
	r := gcRecipe(t, q, "overlapping generic import")
	if ref != *r.FilesRef {
		t.Fatal("test identities differ")
	}
	res, err := c.fileRequest(context.Background(), "PATCH", "/api/files/uploads/"+ref.SHA256, strings.NewReader(string(data[2:])), map[string]string{"Tus-Resumable": "1.0.0", "Upload-Offset": "2", "Content-Type": "application/offset+octet-stream"})
	if err != nil {
		t.Fatal(err)
	}
	if err := fileResponse(res, 204); err != nil {
		t.Fatal(err)
	}
	clock.Advance(DefaultFileRetention + time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if err := q.files.verify(context.Background(), ref); err != nil {
		t.Fatal("concurrent generic import became collectible", err)
	}
}

func TestBlobGCExpiredInputCannotShortenRecipeRetention(t *testing.T) {
	q, clock := fixture(t)
	r := gcRecipe(t, q, "shared with old expired input")
	if err := q.update(func() error {
		ended := clock.Now().Add(-DefaultFileRetention)
		q.putJob("expired", &Job{ID: "expired", State: "succeeded", ReleaseConfirmed: true, FinishedAt: &ended, Spec: Spec{Files: FileManifest{"input": *r.FilesRef}}})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(filePublicationGrace + time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if err := q.files.verify(context.Background(), *r.FilesRef); err != nil {
		t.Fatal("expired input shortened internal retention", err)
	}
	clock.Advance(DefaultFileRetention)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(*r.FilesRef)); !os.IsNotExist(err) {
		t.Fatal("expired shared recipe leaked", err)
	}
}
