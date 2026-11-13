package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A busy queue can spend the old whole-finalization budget before PATCH even
// starts. Advance the client/queue clock while holding the real queue mutex;
// ownerError checks that clock against the context deadline, without sleeping.
func TestFinalizationUploadBusyQueue(t *testing.T) {
	q, clock := fixture(t)
	// Go contexts use wall-clock deadlines. Align the injected clock once, then
	// advance only the fake clock to model service time.
	clock.Advance(time.Since(clock.Now()))
	c := retentionClient(t, q)
	_, ref := retentionUpload(t, c, "input")
	s := spec("experiment")
	s.Resumable = false
	s.Files = FileManifest{"input": ref}
	s.Outputs = map[string]string{"short.mp4": "results"}
	submitted := submit(t, q, s)
	m := machine("worker", "fake")
	check(t, q, m, nil)
	j := check(t, q, m, nil).Job
	if j == nil || j.ID != submitted.ID {
		t.Fatal("no assignment")
	}
	j.Spec.Run.Workdir = t.TempDir()
	path := filepath.Join(j.Spec.Run.Workdir, "short.mp4")
	if err := os.WriteFile(path, []byte("finished output"), 0600); err != nil {
		t.Fatal(err)
	}
	blocked, patches := false, 0
	c.Clock = clock
	c.HTTP = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
		if !blocked && r.Method == "HEAD" && strings.HasPrefix(r.URL.Path, "/api/files/blobs/") {
			blocked = true
			q.mu.Lock()
			type response struct {
				res *http.Response
				err error
			}
			done := make(chan response, 1)
			go func() {
				res, err := http.DefaultTransport.RoundTrip(r)
				done <- response{res, err}
			}()
			func() {
				defer q.mu.Unlock()
				await(t, func() bool { return q.mu.waiting.Load() > 0 })
				clock.Advance(time.Minute)
			}()
			got := <-done
			return got.res, got.err
		}
		if r.Method == "PATCH" {
			patches++
		}
		return http.DefaultTransport.RoundTrip(r)
	})}
	w := Worker{Client: c, Clock: clock}
	finished, err := w.finishFileResultContext(context.Background(), *j, result(j, true), t.TempDir())
	if err != nil || !finished.Success || finished.ExecutionCause != nil {
		t.Fatalf("busy queue interrupted finalization: %+v %v", finished, err)
	}
	if !blocked || patches != 1 {
		t.Fatalf("want one publication after queue service, blocked=%t patches=%d", blocked, patches)
	}
	if err := q.Complete(finished); err != nil {
		t.Fatal(err)
	}
	got, err := c.GetJob(context.Background(), j.ID)
	if err != nil || got.State != "succeeded" || got.OutputRefs["short.mp4"].SizeBytes != int64(len("finished output")) {
		t.Fatalf("output not accepted: %+v %v", got, err)
	}
	// Losing the attempt owner still ends publication and retains its bytes.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	finished, err = w.finishFileResultContext(ctx, *j, result(j, true), t.TempDir())
	if !errors.Is(err, context.Canceled) || !finished.Success {
		t.Fatalf("owner cancellation lost: %+v %v", finished, err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "finished output" {
		t.Fatal("owner cancellation lost local output", err)
	}
}

// Exercise the shared publication boundary after an actual exit-zero command.
// The queue must retain it without requiring optional diagnostic artifact I/O.
func TestFinalizationPublicationFailure(t *testing.T) {
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(filepath.Join(t.TempDir(), "files"), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	c := deliveryClient(t, q)
	_, ref := retentionUpload(t, c, "fixture")
	s := spec("experiment")
	s.Resumable = false
	s.Files = FileManifest{"input": ref}
	s.Outputs = map[string]string{"output": "results"}
	submitted := submit(t, q, s)
	m := machine("worker", "fake")
	check(t, q, m, nil)
	j := check(t, q, m, nil).Job
	if j == nil || j.ID != submitted.ID {
		t.Fatal("no assignment")
	}
	j.Spec.Run.Workdir = t.TempDir()
	cmd := exec.Command("sh", "-ec", "printf ok > output")
	cmd.Dir = j.Spec.Run.Workdir
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	original := &os.PathError{Op: "write", Path: "private-output-root", Err: syscall.EACCES}
	err := publishDirectoryJobOutputs(*j, t.TempDir(), os.Rename, func(string, string) error { return original })
	if !errors.Is(err, syscall.EACCES) {
		t.Fatalf("local error identity lost: %v", err)
	}
	r := finalizationResult(result(j, true), "output_publication", "", err, true)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got, err := c.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertFinalization(t, got, "output_publication", "permission_denied", "output")
	snapshot, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertFinalization(t, snapshot.Jobs[0], "output_publication", "permission_denied", "output")
	dest := filepath.Join(t.TempDir(), "summary.json")
	if err := c.DownloadDiagnostic(context.Background(), j.ID, j.Attempt, "summary.json", dest); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "permission_denied") || strings.Contains(string(data), "private-output-root") {
		t.Fatalf("wrong result diagnostics: %s", data)
	}
}

func assertFinalization(t *testing.T, j *Job, step, class, output string) {
	t.Helper()
	if j.State != "failed" || j.ExecutionCause == nil || j.ExecutionCause.Finalization == nil {
		t.Fatalf("cause missing: %+v", j)
	}
	c, d := j.ExecutionCause, j.ExecutionCause.Finalization
	if c.Class != class || d.Step != step || d.Output != output || d.CommandExitCode == nil || *d.CommandExitCode != 0 || !strings.Contains(j.Reason, step) {
		t.Fatalf("wrong cause: %+v detail=%+v reason=%s", c, d, j.Reason)
	}
	if len(j.AttemptHistory) != 1 || j.AttemptHistory[0].ExecutionCause == nil || j.AttemptHistory[0].Diagnostics == nil {
		t.Fatalf("outcome/summary missing: %+v", j.AttemptHistory)
	}
}

// An upload failure remains retryable and keeps bytes and exit-zero evidence.
// An exact release-only update exposes the cause; success clears that pending
// cause. Neither a stale update nor the publication error can change custody.
func TestFinalizationUploadRetry(t *testing.T) {
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(filepath.Join(t.TempDir(), "files"), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
	defer api.Close()
	c := Client{URL: api.URL, Token: "fixture"}
	_, ref := retentionUpload(t, c, "fixture")
	s := spec("experiment")
	s.Resumable = false
	s.Files = FileManifest{"input": ref}
	s.Outputs = map[string]string{"output": "results"}
	submitted := submit(t, q, s)
	m := machine("worker", "fake")
	check(t, q, m, nil)
	j := check(t, q, m, nil).Job
	if j == nil || j.ID != submitted.ID {
		t.Fatal("no assignment")
	}
	j.Spec.Run.Workdir = t.TempDir()
	path := filepath.Join(j.Spec.Run.Workdir, "output")
	if err := os.WriteFile(path, []byte("retained output"), 0600); err != nil {
		t.Fatal(err)
	}
	c.HTTP = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
		if strings.HasPrefix(r.URL.Path, "/api/files/uploads") {
			return nil, &os.PathError{Op: "write", Path: "private-upload-secret", Err: syscall.EACCES}
		}
		return http.DefaultTransport.RoundTrip(r)
	})}
	w := Worker{Client: c}
	pending, err := w.finishFileResult(*j, result(j, true), t.TempDir())
	if err == nil || !errors.Is(err, syscall.EACCES) || !pending.Success || pending.ExecutionCause == nil || pending.ExecutionCause.Finalization.Step != "output_upload" {
		t.Fatalf("upload classification lost: %+v %v", pending, err)
	}
	release := pending
	release.ReleaseOnly = true
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	got, err := c.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "running" || got.ExecutionCause == nil || got.ExecutionCause.Finalization.Output != "output" || !strings.Contains(got.Reason, "output_upload") {
		t.Fatalf("pending cause missing: %+v", got)
	}
	staleUpdate := release
	staleUpdate.Attempt++
	staleUpdate.ExecutionCause = safeRuntimeCause(release.ExecutionCause)
	staleUpdate.ExecutionCause.Finalization.Step = "output_reservation"
	_ = q.Complete(staleUpdate)
	got, err = c.GetJob(context.Background(), j.ID)
	if err != nil || got.Attempt != j.Attempt || got.ExecutionCause == nil || got.ExecutionCause.Finalization.Step != "output_upload" {
		t.Fatal("stale release changed attempt", err)
	}
	data, _ := json.Marshal(got)
	if strings.Contains(string(data), "private-upload-secret") {
		t.Fatal("private transport detail escaped")
	}
	c.HTTP = nil
	w.Client = c
	accepted, err := w.finishFileResult(*j, pending, t.TempDir())
	if err != nil || !accepted.Success || accepted.ExecutionCause != nil {
		t.Fatalf("retry failed: %+v %v", accepted, err)
	}
	if err := q.Complete(accepted); err != nil {
		t.Fatal(err)
	}
	got, err = c.GetJob(context.Background(), j.ID)
	if err != nil || got.State != "succeeded" || got.ExecutionCause != nil {
		t.Fatalf("pending failure survived success: %+v %v", got, err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "retained output" {
		t.Fatal("retry lost local bytes", err)
	}
}

func TestFinalizationPrivacy(t *testing.T) {
	zero := 0
	cause := &RuntimeCause{Operation: "outputs", Class: "not_found", Finalization: &FinalizationDetail{Step: "output_path", Output: "undeclared-secret", Destination: "private-destination", Error: "not_found", CommandExitCode: &zero}}
	safe := safeJobRuntimeCause(Spec{Outputs: map[string]string{"output": "results"}}, cause)
	if safe.Finalization.Output != "" || safe.Finalization.Destination != "" || cause.Finalization.Output != "undeclared-secret" {
		t.Fatal("declaration binding mutated or leaked worker input")
	}
}
