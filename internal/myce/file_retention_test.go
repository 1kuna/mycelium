package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFileRetention(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(filepath.Join(t.TempDir(), "store"), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureFileRetention(time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureFileRetention(0); err == nil {
		t.Fatal("nonpositive retention accepted")
	}
	api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer api.Close()
	c := Client{URL: api.URL, Token: "test"}
	put := func(content string) FileRef {
		t.Helper()
		path := filepath.Join(t.TempDir(), "input")
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		ref, err := IdentifyFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = c.UploadFile(context.Background(), path, ref); err != nil {
			t.Fatal(err)
		}
		return ref
	}
	input, output, shared, checkpoint := put("input"), put("output"), put("shared"), put("checkpoint")
	model, extension, environment, orphan := put("model"), put("extension"), put("environment"), put("orphan")
	original := Spec{Name: "retention", Project: "test", SubmissionKey: "retained-answer", Run: RunSpec{Command: []string{"true"}}, Needs: ResourceNeeds{MemoryGB: 1, Platform: Platform{Any: true}}, Files: FileManifest{"input": input, "shared": shared, "checkpoint": checkpoint, "model": model, "extension": extension, "environment": environment}}
	job := submit(t, q, original)
	active := submit(t, q, Spec{Name: "active", Project: "test", Run: RunSpec{Command: []string{"true"}}, Needs: ResourceNeeds{MemoryGB: 1, Platform: Platform{Any: true}}, Files: FileManifest{"shared": shared}})
	if err := q.update(func() error {
		j := q.s.Jobs[job.ID]
		j.State = "succeeded"
		ended := clock.Now()
		j.FinishedAt = &ended
		j.OutputRefs = FileManifest{"output": output}
		j.FileCheckpoints = map[int]FileCheckpoint{1: {Generation: 1, Files: FileManifest{"save": checkpoint}}}
		q.models.Models = append(q.models.Models, Model{SHA256: model.SHA256})
		q.s.Extensions = map[string]Extension{"kept": {Assets: FileManifest{"asset": extension}}}
		q.environments.Environments = map[string]map[string]EnvironmentRecipe{"kept": {"cpu": {Assets: FileManifest{"asset": environment}}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Hour - time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if err := c.HasFile(context.Background(), output); err != nil {
		t.Fatal("not yet eligible", err)
	}
	// The successful HEAD above also protects reuse by an imminent submission.
	clock.Advance(filePublicationGrace + time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []FileRef{input, output} {
		if _, err := os.Stat(q.files.blob(ref)); !os.IsNotExist(err) {
			t.Fatal("expired bytes retained", err)
		}
		var apiError *HTTPError
		if err := c.HasFile(context.Background(), ref); !errors.As(err, &apiError) || apiError.Status != 410 || apiError.Code != "result_expired" {
			t.Fatal("missing typed expiry", err)
		}
		if err := c.DownloadFile(context.Background(), ref, filepath.Join(t.TempDir(), "expired")); !errors.As(err, &apiError) || apiError.Status != 410 || apiError.Code != "result_expired" {
			t.Fatal("download hid expiry behind identity validation", err)
		}
	}
	for _, ref := range []FileRef{shared, checkpoint, model, extension, environment, orphan} {
		if err := c.HasFile(context.Background(), ref); err != nil {
			t.Fatal("keeping reference lost", ref, err)
		}
	}
	retained, err := c.GetJob(context.Background(), job.ID)
	if err != nil || !retained.FilesExpired || retained.OutputRefs["output"] != output {
		t.Fatal("retained answer lost", retained, err)
	}
	replay, err := q.Submit(original)
	if err != nil || replay.ID != job.ID || !replay.FilesExpired {
		t.Fatal("keyed replay lost expiry", replay, err)
	}
	// A queued/retry job keeps its inputs beyond the terminal window. Once it
	// terminates, it receives its own full window, independent of the first job.
	if err := q.Cancel(active.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if err := c.HasFile(context.Background(), shared); err != nil {
		t.Fatal(err)
	}
	// The HEAD above refreshes publication grace independently of the window.
	clock.Advance(filePublicationGrace)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(shared)); !os.IsNotExist(err) {
		t.Fatal("shared bytes did not expire", err)
	}
}

func TestFileRetentionSubmissionFence(t *testing.T) {
	q, clock := fixture(t)
	s := submitVerificationInput(t, q)
	for _, ref := range s.Files {
		if err := os.Chtimes(q.files.blob(ref), clock.Now(), clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	first := submit(t, q, s)
	if err := q.Cancel(first.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(DefaultFileRetention)
	if err := q.update(func() error { q.maintain(); return nil }); err != nil {
		t.Fatal(err)
	}
	entered, unblock := gateSubmitVerification(t, q, nil)
	defer unblock()
	s.SubmissionKey = "another-job-sharing-input"
	done := startSubmitVerification(q, context.Background(), s)
	submitVerificationWait(t, "new job verification", entered)
	// GC must defer rather than stall control traffic or delete a file between
	// the new job's successful verification and reference publication.
	submitVerificationProgress(t, "retention during verification", q.reclaimJobFiles)
	submitVerificationProgress(t, "queue tick during verification", q.Tick)
	unblock()
	if got := submitVerificationWait(t, "new job admission", done); got.err != nil {
		t.Fatal(got.err)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for _, ref := range s.Files {
		if _, err := os.Stat(q.files.blob(ref)); err != nil {
			t.Fatal("new keeping reference lost", err)
		}
	}
}

func retentionClient(t *testing.T, q *Queue) Client {
	t.Helper()
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	t.Cleanup(api.Close)
	return Client{URL: api.URL, Token: "test"}
}

func retentionUpload(t *testing.T, c Client, content string) (string, FileRef) {
	t.Helper()
	return retentionUploadFile(t, c.UploadFile, content)
}

func retentionJobInput(t *testing.T, c Client, content string) (string, FileRef) {
	t.Helper()
	return retentionUploadFile(t, c.UploadJobInput, content)
}

func retentionUploadFile(t *testing.T, upload func(context.Context, string, FileRef) error, content string) (string, FileRef) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = upload(context.Background(), path, ref); err != nil {
		t.Fatal(err)
	}
	return path, ref
}

func TestFileRetentionOrphanUploads(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	_, orphan := retentionJobInput(t, c, "uploaded but never submitted")
	_, referenced := retentionJobInput(t, c, "submitted input")
	s := spec("experiment")
	s.Files = FileManifest{"input": referenced}
	submit(t, q, s)
	if _, marked := q.s.JobInputUploads[referenced.SHA256]; marked {
		t.Fatal("job admission did not clear input upload marker")
	}
	clock.Advance(DefaultFileRetention - time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(orphan)); err != nil {
		t.Fatal("orphan lost before retention elapsed", err)
	}
	_, recent := retentionJobInput(t, c, "recent pre-submit upload")
	clock.Advance(2 * time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for name, ref := range map[string]FileRef{"referenced": referenced, "recent": recent} {
		if _, err := os.Stat(q.files.blob(ref)); err != nil {
			t.Errorf("%s upload lost: %v", name, err)
		}
	}
	if _, err := os.Stat(q.files.blob(orphan)); !os.IsNotExist(err) {
		t.Errorf("old orphan upload retained: %v", err)
	}
}

func TestFileRetentionUnmarkedImports(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	_, model := retentionUpload(t, c, "operator imported model weights")
	_, operator := retentionUpload(t, c, "operator file put without any job")
	catalog := q.models
	catalog.Models = []Model{{Name: "imported", Engine: "fake", FileStore: true, MemoryGB: 1, Platforms: []string{"fake"}, SHA256: model.SHA256, SizeBytes: model.SizeBytes}}
	if err := q.ConfigureModels(catalog); err != nil {
		t.Fatal(err)
	}
	catalog.Models = nil
	if err := q.ConfigureModels(catalog); err != nil {
		t.Fatal(err)
	}
	clock.Advance(DefaultFileRetention + time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for name, ref := range map[string]FileRef{"model with empty catalog": model, "operator import": operator} {
		if _, err := os.Stat(q.files.blob(ref)); err != nil {
			t.Errorf("%s lost: %v", name, err)
		}
	}
}

func TestFileRetentionOrphanPublicationGrace(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	if err := q.ConfigureFileRetention(time.Hour); err != nil {
		t.Fatal(err)
	}
	_, orphan := retentionJobInput(t, c, "short retention still honors publish grace")
	clock.Advance(filePublicationGrace - time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(orphan)); err != nil {
		t.Fatal("orphan lost within publication grace", err)
	}
	clock.Advance(2 * time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(orphan)); !os.IsNotExist(err) {
		t.Fatal("old orphan retained after publication grace", err)
	}
}

func TestFileRetentionOrphanReferences(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	refs := map[string]FileRef{}
	files := func(name string) FileManifest {
		_, ref := retentionJobInput(t, c, name)
		refs[name] = ref
		return FileManifest{"file": ref}
	}
	// Include retained identities whose terminal reclamation has already
	// completed: they are not candidates for either another job pass or orphans.
	j := &Job{State: "succeeded", FilesExpired: true, FileReclamation: &FileReclamation{}}
	j.Spec.Files = files("expired job input")
	j.OutputRefs = files("expired job output")
	j.FileCheckpoints = map[int]FileCheckpoint{1: {Files: files("checkpoint")}}
	j.ResumeFiles = &FileCheckpoint{Files: files("resume checkpoint")}
	j.Extensions = []Extension{{Assets: files("job extension"), Variants: []ExtensionVariant{{Environments: map[string]EnvironmentRecipe{"env": {Assets: files("job variant environment")}}}}}}
	j.EnvironmentRecipes = map[string]EnvironmentRecipe{"env": {Assets: files("job environment")}}
	j.SidecarEnvironmentRecipes = map[string]map[string]EnvironmentRecipe{"sidecar": {"env": {Assets: files("sidecar environment")}}}
	j.FanOut = &FanOutProgress{Results: map[string]PieceResult{"piece": {OutputRefs: files("fan-out result")}}}
	answer := &Job{State: "succeeded", FilesExpired: true, FileReclamation: &FileReclamation{}, Spec: Spec{Files: files("keyed answer input")}, OutputRefs: files("keyed answer output")}
	files("pending reclamation")
	// The pending list itself is custody even if no retained manifest names it.
	pending := &Job{State: "queued", FileReclamation: &FileReclamation{Pending: []string{refs["pending reclamation"].SHA256}}}
	ext := Extension{Assets: files("catalog extension"), Variants: []ExtensionVariant{{Environments: map[string]EnvironmentRecipe{"env": {Assets: files("catalog variant environment")}}}}}
	env := EnvironmentRecipe{Assets: files("catalog environment")}
	slotEnv := EnvironmentRecipe{Assets: files("model slot environment")}
	files("model")
	files("model bundle file")
	if err := q.update(func() error {
		q.s.Jobs["retained"] = j
		q.s.Jobs["pending"] = pending
		q.s.Submissions = map[string]*submissionRecord{"retained": {Kind: submissionBinding, JobID: "removed", Job: answer}}
		q.s.Extensions = map[string]Extension{"catalog": ext}
		q.environments.Environments = map[string]map[string]EnvironmentRecipe{"env": {"cpu": env}}
		q.s.ModelSlots = map[string]*ModelSlot{"slot": {EnvironmentRecipe: &slotEnv}}
		q.models.Models = []Model{{SHA256: refs["model"].SHA256, Files: []ModelFile{{SHA256: refs["model bundle file"].SHA256}}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(DefaultFileRetention + time.Nanosecond)
	if err := q.reclaimJobFiles(); err != nil {
		t.Fatal(err)
	}
	for name, ref := range refs {
		if _, err := os.Stat(q.files.blob(ref)); err != nil {
			t.Errorf("%s reference lost: %v", name, err)
		}
	}
}

func TestFileRetentionOrphanBatch(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	for i := 0; i < fileReclaimBatch+1; i++ {
		retentionJobInput(t, c, fmt.Sprint("orphan-", i))
	}
	clock.Advance(DefaultFileRetention + time.Nanosecond)
	for _, want := range []int{1, 0} {
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(filepath.Join(q.files.root, "blobs"))
		if err != nil || len(entries) != want {
			t.Fatalf("orphan batch remaining=%d, want %d: %v", len(entries), want, err)
		}
	}
}

func TestFileRetentionOrphanSubmissionFence(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	_, ref := retentionJobInput(t, c, "marked input awaiting verification")
	s := spec("experiment")
	s.Files = FileManifest{"input": ref}
	clock.Advance(DefaultFileRetention + time.Nanosecond)
	entered, unblock := gateSubmitVerification(t, q, nil)
	defer unblock()
	done := startSubmitVerification(q, context.Background(), s)
	submitVerificationWait(t, "orphan input verification", entered)
	submitVerificationProgress(t, "orphan collection during verification", q.Tick)
	unblock()
	if got := submitVerificationWait(t, "orphan input admission", done); got.err != nil {
		t.Fatal(got.err)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for _, ref := range s.Files {
		if _, err := os.Stat(q.files.blob(ref)); err != nil {
			t.Fatal("new reference to old orphan lost", err)
		}
	}
}

func TestFileRetentionUploadPurposeDedup(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	modelPath, model := retentionUpload(t, c, "imported model reused by a job")
	if err := c.UploadJobInput(context.Background(), modelPath, model); err != nil {
		t.Fatal(err)
	}
	inputPath, input := retentionJobInput(t, c, "input later imported with file put")
	if err := c.UploadFile(context.Background(), inputPath, input); err != nil {
		t.Fatal(err)
	}
	if len(q.s.JobInputUploads) != 0 {
		t.Fatal("generic import became collectible", q.s.JobInputUploads)
	}
	clock.Advance(DefaultFileRetention + time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []FileRef{model, input} {
		if _, err := os.Stat(q.files.blob(ref)); err != nil {
			t.Error("deduplicated generic import lost", err)
		}
	}
}

func TestFileRetentionDuplicateUploadReadOnly(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	importPath, imported := retentionUpload(t, c, "duplicate checkpoint bytes")
	inputPath, input := retentionJobInput(t, c, "duplicate marked input")
	handler := API{Queue: q, Token: "test"}.Handler()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			t.Error("duplicate reached POST-side hashing/creation")
			http.Error(w, "unexpected duplicate POST", 500)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer api.Close()
	c.URL = api.URL
	var before, after int64
	if err := q.db.QueryRow("SELECT total_changes()").Scan(&before); err != nil {
		t.Fatal(err)
	}
	revision := q.fileReferenceRevision
	// A slow unrelated creation cannot hold duplicate checkpoint verification.
	q.files.mu.Lock()
	err := c.UploadFile(context.Background(), importPath, imported)
	inputErr := c.UploadJobInput(context.Background(), inputPath, input)
	q.files.mu.Unlock()
	if err != nil || inputErr != nil {
		t.Errorf("duplicate upload failed: generic=%v input=%v", err, inputErr)
	}
	if err := q.db.QueryRow("SELECT total_changes()").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before || q.fileReferenceRevision != revision {
		t.Errorf("duplicate wrote state or ran a queue transaction: writes=%d revision=%d", after-before, q.fileReferenceRevision-revision)
	}
}

func TestFileRetentionCatalogReleasesMarker(t *testing.T) {
	for _, catalog := range []string{"model", "environment"} {
		t.Run(catalog, func(t *testing.T) {
			q, _ := fixture(t)
			c := retentionClient(t, q)
			_, ref := retentionJobInput(t, c, "marked catalog asset")
			var err error
			if catalog == "model" {
				config := q.models
				config.Models = []Model{{Name: "imported", Engine: "fake", FileStore: true, MemoryGB: 1, Platforms: []string{"fake"}, SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}}
				err = q.ConfigureModels(config)
			} else {
				err = q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": {Assets: FileManifest{"asset": ref}}}}})
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, marked := q.s.JobInputUploads[ref.SHA256]; marked {
				t.Error("catalog asset still marked before next tick")
			}
			var data []byte
			if err := q.db.QueryRow("SELECT data FROM queue_state WHERE key='state'").Scan(&data); err != nil {
				t.Fatal(err)
			}
			var durable state
			if err := json.Unmarshal(data, &durable); err != nil {
				t.Fatal(err)
			}
			if _, marked := durable.JobInputUploads[ref.SHA256]; marked {
				t.Error("catalog marker release was not persisted")
			}
		})
	}
}

func TestFileRetentionMarkedUploadRestart(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	_, orphan := retentionJobInput(t, c, "durably marked abandoned job input")
	_, model := retentionUpload(t, c, "model imported before queue restart")
	if published := q.s.JobInputUploads[orphan.SHA256]; !published.Equal(clock.Now()) {
		t.Fatal("publish time not recorded", published)
	}
	dbPath, root, storeRoot := strings.TrimSuffix(q.lock.Name(), ".lock"), q.root, q.files.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dbPath, root, clock, WithCheckpointAvailability(func() bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	if err := reopened.ConfigureFileStore(storeRoot, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	// No model configuration is loaded after restart.
	clock.Advance(DefaultFileRetention + time.Nanosecond)
	if err := reopened.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(reopened.files.blob(orphan)); !os.IsNotExist(err) {
		t.Error("marked orphan retained after restart", err)
	}
	if _, err := os.Stat(reopened.files.blob(model)); err != nil {
		t.Error("unmarked model lost without model-config", err)
	}
	if len(reopened.s.JobInputUploads) != 0 {
		t.Error("collected marker retained", reopened.s.JobInputUploads)
	}
}

func TestFileCacheEvictsExpiredVictims(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint("partial=", partial), func(t *testing.T) {
			q, clock := fixture(t)
			// Keep expiry metadata while testing eviction of expired cache bytes.
			if err := q.ConfigureLogRetention(2 * DefaultFileRetention); err != nil {
				t.Fatal(err)
			}
			c := retentionClient(t, q)
			_, a := retentionUpload(t, c, "aaaaaaaaaaaaaaaa")
			s := spec("experiment")
			s.Files = FileManifest{"input": a}
			j := submit(t, q, s)
			cache := FileCache{Root: t.TempDir(), Limit: 16, Clock: clock, Client: c}
			if err := cache.copyTo(context.Background(), a, filepath.Join(t.TempDir(), "a")); err != nil {
				t.Fatal(err)
			}
			victim := filepath.Join(cache.Root, a.SHA256)
			if partial {
				if err := os.Rename(victim, victim+".partial"); err != nil {
					t.Fatal(err)
				}
				victim += ".partial"
			}
			if err := q.Cancel(j.ID); err != nil {
				t.Fatal(err)
			}
			clock.Advance(DefaultFileRetention)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if err := c.HasFile(context.Background(), a); !isResultExpired(err) {
				t.Fatal("victim did not expire", err)
			}
			_, b := retentionUpload(t, c, "bbbbbbbbbbbbbbbb")
			if err := cache.copyTo(context.Background(), b, filepath.Join(t.TempDir(), "b")); err != nil {
				t.Fatal("expired victim wedged cache", err)
			}
			if _, err := os.Stat(victim); !os.IsNotExist(err) {
				t.Fatal("expired victim retained", err)
			}
		})
	}
}

func TestFileRetentionPublicationGrace(t *testing.T) {
	for _, reuse := range []string{"publish", "head"} {
		for _, reference := range []string{"submit", "complete"} {
			t.Run(reuse+"/"+reference, func(t *testing.T) {
				q, clock := fixture(t)
				c := retentionClient(t, q)
				path, a := retentionUpload(t, c, "reusable terminal content")
				s := spec("experiment")
				s.Files = FileManifest{"input": a}
				first := submit(t, q, s)
				if err := q.Cancel(first.ID); err != nil {
					t.Fatal(err)
				}
				clock.Advance(DefaultFileRetention)
				if err := q.update(func() error { q.maintain(); return nil }); err != nil {
					t.Fatal(err)
				}
				var running *Job
				if reference == "complete" {
					_, input := retentionUpload(t, c, "different live input")
					next := spec("experiment")
					next.Files = FileManifest{"input": input}
					next.Outputs = map[string]string{"result": "result"}
					submit(t, q, next)
					running = check(t, q, machine("worker", "fake"), nil).Job
					if running == nil {
						t.Fatal("no running attempt")
					}
					if err := q.Started(result(running, false)); err != nil {
						t.Fatal(err)
					}
				}
				if reuse == "publish" {
					// Keep the old job's candidate pending while republishing bytes.
					if err := os.Remove(q.files.blob(a)); err != nil {
						t.Fatal(err)
					}
				}
				if err := c.UploadFile(context.Background(), path, a); err != nil {
					t.Fatal(err)
				}
				// The first file must survive hours spent uploading later files or
				// waiting through a queue outage/deploy before submit/complete.
				clock.Advance(6*time.Hour - time.Second)
				if running != nil {
					// Keep worker liveness independent of the file-publication gap.
					check(t, q, machine("worker", "fake"), running)
				}
				if err := q.Tick(); err != nil {
					t.Fatal(err)
				}
				if reference == "submit" {
					submit(t, q, s)
				} else {
					r := result(running, true)
					r.OutputRefs = FileManifest{"result": a}
					if err := q.CompleteContext(context.Background(), r); err != nil {
						t.Fatal("republished output lost before complete", err)
					}
				}
				clock.Advance(filePublicationGrace)
				if err := q.Tick(); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(q.files.blob(a)); err != nil {
					t.Fatal("committed reuse lost", err)
				}
			})
		}
	}
}

func TestFileRetentionRetriesRemovalFailure(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	_, a := retentionUpload(t, c, "temporarily unavailable")
	_, b := retentionUpload(t, c, "collect despite other failure")
	s := spec("experiment")
	s.Files = FileManifest{"a": a, "b": b}
	j := submit(t, q, s)
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	path := q.files.blob(a)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(path, "blocked")
	if err := os.WriteFile(child, []byte("disposable test obstruction"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, clock.Now(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(DefaultFileRetention)
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previous)
	if err := q.Tick(); err != nil {
		t.Fatal("removal failure stopped maintenance", err)
	}
	if !strings.Contains(logs.String(), "file retention deferred hash="+a.SHA256) {
		t.Fatal("missing retry diagnostic", logs.String())
	}
	if _, err := os.Stat(q.files.blob(b)); !os.IsNotExist(err) {
		t.Fatal("one removal failure blocked another candidate", err)
	}
	if err := os.Remove(child); err != nil {
		t.Fatal(err)
	}
	// Removing the child changes the directory's mtime using the real clock.
	if err := os.Chtimes(path, clock.Now().Add(-filePublicationGrace), clock.Now().Add(-filePublicationGrace)); err != nil {
		t.Fatal(err)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("failed candidate was not retried", err)
	}
}

func TestFileRetentionProgressAndBatch(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	s := spec("experiment")
	s.SubmissionKey = "reclamation-batch"
	s.Files = FileManifest{}
	var path string
	var ref FileRef
	for i := 0; i < fileReclaimBatch+1; i++ {
		path, ref = retentionUpload(t, c, fmt.Sprint("batch-input-", i))
		s.Files[fmt.Sprint("input-", i)] = ref
	}
	j := submit(t, q, s)
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(DefaultFileRetention)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if pending := q.retainedJob(j.ID).FileReclamation.Pending; len(pending) != 1 {
		t.Fatal("filesystem work not bounded to one batch", len(pending))
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if pending := q.retainedJob(j.ID).FileReclamation.Pending; len(pending) != 0 {
		t.Fatal("reclamation progress not saved", pending)
	}
	// Progress must survive queue restart, and an old completed reclamation must
	// never sweep newly uploaded bytes just because the retained job names them.
	dbPath, root, storeRoot := strings.TrimSuffix(q.lock.Name(), ".lock"), q.root, q.files.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dbPath, root, clock, WithCheckpointAvailability(func() bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	if err := reopened.ConfigureFileStore(storeRoot, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(API{Queue: reopened, Token: "test"}.Handler())
	defer api.Close()
	if err := (Client{URL: api.URL, Token: "test"}).UploadFile(context.Background(), path, ref); err != nil {
		t.Fatal(err)
	}
	clock.Advance(filePublicationGrace)
	if err := reopened.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(reopened.files.blob(ref)); err != nil {
		t.Fatal("already reclaimed hash scanned again", err)
	}
}

func TestFileRetentionDeferredDiagnostic(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	_, a := retentionUpload(t, c, "deferred publication")
	s := spec("experiment")
	s.Files = FileManifest{"input": a}
	j := submit(t, q, s)
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(DefaultFileRetention)
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previous)
	q.fileLifecycle.RLock()
	for range 2 {
		if err := q.Tick(); err != nil {
			q.fileLifecycle.RUnlock()
			t.Fatal(err)
		}
	}
	q.fileLifecycle.RUnlock()
	if got := strings.Count(logs.String(), "file retention deferred: publication"); got != 1 {
		t.Fatal("expected one warning per deferred streak", got, logs.String())
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if q.fileCollectionDeferred {
		t.Fatal("successful collection did not clear deferral")
	}
}

func TestFileRetentionOpenDownload(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(filepath.Join(t.TempDir(), "store"), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer api.Close()
	c := Client{URL: api.URL, Token: "test"}
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
	clock.Advance(DefaultFileRetention)
	if err = q.update(func() error { q.maintain(); return nil }); err != nil {
		t.Fatal(err)
	}
	// The download has opened and verified its descriptor before the directory
	// durability barrier. Unlink there, then finish serving that same descriptor.
	q.files.syncDir = func(string) error { return q.reclaimJobFiles() }
	if err = c.DownloadFile(context.Background(), ref, filepath.Join(t.TempDir(), "download")); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(q.files.blob(ref)); !os.IsNotExist(err) {
		t.Fatal("test did not unlink", err)
	}
}
