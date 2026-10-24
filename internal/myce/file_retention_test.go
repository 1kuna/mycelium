package myce

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	clock.Advance(time.Nanosecond)
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
			t.Fatal("keeping reference or orphan lost", ref, err)
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
	clock.Advance(time.Hour)
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
