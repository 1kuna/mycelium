package myce

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Err is checked outside q.mu after the publication snapshot and before unlink.
// Interleave a real rejected transaction there without timing a goroutine race.
type retentionInterleaveContext struct {
	context.Context
	once func()
}

func (c *retentionInterleaveContext) Err() error {
	if f := c.once; f != nil {
		c.once = nil
		f()
	}
	return c.Context.Err()
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
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	_, ref := retentionJobInput(t, c, "marked input awaiting verification")
	s.Files["orphan"] = ref
	clock.Advance(testFileRetention)
	clock.Advance(time.Nanosecond)
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
	for _, ref := range s.Files {
		if _, err := os.Stat(q.files.blob(ref)); err != nil {
			t.Fatal("new reference to old orphan lost", err)
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
	clock.Advance(testFileRetention + time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for name, ref := range map[string]FileRef{"model with empty catalog": model, "operator import": operator} {
		if _, err := os.Stat(q.files.blob(ref)); err != nil {
			t.Errorf("%s lost: %v", name, err)
		}
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
	clock.Advance(testFileRetention + time.Nanosecond)
	if err := q.reclaimJobFiles(); err != nil {
		t.Fatal(err)
	}
	for name, ref := range refs {
		if _, err := os.Stat(q.files.blob(ref)); err != nil {
			t.Errorf("%s reference lost: %v", name, err)
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
	clock.Advance(testFileRetention + time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []FileRef{model, input} {
		if _, err := os.Stat(q.files.blob(ref)); err != nil {
			t.Error("deduplicated generic import lost", err)
		}
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
	if err := reopened.ConfigureFileRetention(testFileRetention); err != nil {
		t.Fatal(err)
	}
	if err := reopened.ConfigureFileStore(storeRoot, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	// No model configuration is loaded after restart.
	clock.Advance(testFileRetention + time.Nanosecond)
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
				clock.Advance(testFileRetention)
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
