package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	clock.Advance(testFileRetention + time.Nanosecond)
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
	if _, err := os.Stat(q.files.blob(*e.FilesRef)); err != nil {
		t.Fatal("durable replay lost its frozen bundle", err)
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
	clock.Advance(testFileRetention + time.Nanosecond)
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
	clock.Advance(testFileRetention + time.Nanosecond)
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

	{
		originalHTTP := c.HTTP
		files := map[string][]byte{"requirements.txt": []byte("published between POST and HEAD")}
		data, err := json.Marshal(files)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "import")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		ref, err := IdentifyFile(path)
		if err != nil {
			t.Fatal(err)
		}
		c.HTTP = &http.Client{Transport: clientTestTransport(func(req *http.Request) (*http.Response, error) {
			res, err := http.DefaultTransport.RoundTrip(req)
			if err == nil && req.Method == "POST" && strings.HasSuffix(req.URL.Path, "/uploads/") {
				gcRecipe(t, q, "published between POST and HEAD")
			}
			return res, err
		})}
		if err := c.UploadFile(context.Background(), path, ref); err != nil {
			t.Fatal(err)
		}
		clock.Advance(testFileRetention + time.Nanosecond)
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
		if err := q.files.verify(context.Background(), ref); err != nil {
			t.Fatal("deduplicated import lost custody", err)
		}
		c.HTTP = originalHTTP

	}

	{
		path := filepath.Join(t.TempDir(), "input")
		if err := os.WriteFile(path, []byte("download survives unlink"), 0600); err != nil {
			t.Fatal(err)
		}
		ref, err := IdentifyFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = c.UploadFile(context.Background(), path, ref); err != nil {
			t.Fatal(err)
		}
		j := submit(t, q, Spec{Name: "open", Project: "test", Run: RunSpec{Command: []string{"true"}}, Needs: ResourceNeeds{MemoryGB: 1, Platform: Platform{Any: true}}, Files: FileManifest{"input": ref}})
		if err = q.update(func() error {
			q.s.Jobs[j.ID].State = "succeeded"
			ended := clock.Now()
			q.s.Jobs[j.ID].FinishedAt = &ended
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		clock.Advance(testFileRetention)
		if err = q.update(func() error { q.maintain(); return nil }); err != nil {
			t.Fatal(err)
		}
		// The download has opened and verified its descriptor before the directory
		// durability barrier. Unlink there, then finish serving that same descriptor.
		originalSync := q.files.syncDir
		defer func() { q.files.syncDir = originalSync }()
		q.files.syncDir = func(string) error { return q.reclaimJobFiles() }
		if err = c.DownloadFile(context.Background(), ref, filepath.Join(t.TempDir(), "download")); err != nil {
			t.Fatal(err)
		}
		if _, err = os.Stat(q.files.blob(ref)); !os.IsNotExist(err) {
			t.Fatal("test did not unlink", err)
		}

	}
}

func TestBlobGCImportCompletesBeforeEnrollment(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	r := EnvironmentRecipe{Files: map[string][]byte{"requirements.txt": []byte("import racing enrollment")}}
	data, err := json.Marshal(r.Files)
	if err != nil {
		t.Fatal(err)
	}
	ref := capacityPartial(t, c, string(data), "")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	record := q.files.recordBundle
	q.files.recordBundle = func(hash string, created bool, contexts ...context.Context) error {
		close(entered)
		<-release
		return record(hash, created, contexts...)
	}
	frozen := make(chan error, 1)
	go func() {
		q.fileLifecycle.RLock()
		err := q.files.freezeRecipe(&r, nil)
		q.fileLifecycle.RUnlock()
		frozen <- err
	}()
	submitVerificationWait(t, "internal enrollment", entered)
	upload := make(chan error, 1)
	go func() {
		res, err := c.fileRequest(context.Background(), "PATCH", "/api/files/uploads/"+ref.SHA256, strings.NewReader(string(data[2:])), map[string]string{"Tus-Resumable": "1.0.0", "Upload-Offset": "2", "Content-Type": "application/offset+octet-stream"})
		if err == nil {
			err = fileResponse(res, 204)
		}
		upload <- err
	}()
	// With the fix, PATCH reaches tus's lock and requests release. Before the
	// fix, PATCH completes promotion before the paused writer enrolls the blob.
	completed := false
	var uploadErr error
	await(t, func() bool {
		select {
		case uploadErr = <-upload:
			completed = true
			return true
		default:
		}
		_, err := os.Stat(filepath.Join(q.files.root, "uploads", ref.SHA256+".stop"))
		return err == nil
	})
	unblock()
	if err := submitVerificationWait(t, "internal publication", frozen); err != nil {
		t.Fatal(err)
	}
	if !completed {
		uploadErr = submitVerificationWait(t, "import completion", upload)
	}
	if uploadErr != nil {
		t.Fatal(uploadErr)
	}
	clock.Advance(testFileRetention + time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if err := q.files.verify(context.Background(), ref); err != nil {
		t.Fatal("acknowledged import enrolled after promotion", err)
	}
}
