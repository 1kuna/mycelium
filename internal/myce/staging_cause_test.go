package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Prelaunch infrastructure consumes the existing bounded production budget;
// unavailable referenced bytes and invalid archives need consumer action.
func TestStagingCauseClassificationAndRetry(t *testing.T) {
	for _, tc := range []struct {
		name              string
		err               error
		operation, reason string
		retry             bool
	}{
		{"disk_io", &os.PathError{Op: "write", Path: "/private/worker", Err: syscall.EIO}, "copy", "EIO", true},
		{"disk_full", syscall.ENOSPC, "cache_download", "ENOSPC", true},
		{"permission", syscall.EACCES, "publish", "EACCES", true},
		{"timeout", errFileTransferStalled, "cache_verify", "timeout", true},
		{"shared_input_stall", fmt.Errorf("shared input: %w", errCheckpointIOTimeout), "shared_input", "timeout", true},
		{"storage_outage", &HTTPError{Status: 503}, "cache_verify", "blob_unavailable", true},
		{"missing", &HTTPError{Status: 404}, "cache_verify", "blob_missing", false},
		{"expired", resultExpired(), "cache_verify", "blob_expired", false},
		{"archive", errors.New("source archive contains unsupported file type"), "source_extract", "invalid_source", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			spec := submitVerificationInput(t, q)
			spec.Priority = "production"
			spec.Resumable = false
			submitted := submit(t, q, spec)
			m := machine("staging", "fake")
			assigned := check(t, q, m, nil).Job
			if assigned == nil {
				t.Fatal("not assigned")
			}
			w := Worker{StateDir: newTestStateRoot(t), Clock: clock, Machine: m}
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			store.state.Job, store.state.Session = assigned, assigned.Session
			failure := withStagingCause("input", spec.Files["input"], tc.operation, tc.err)
			err = w.stagingFailure(store, *assigned, failure)
			r := startFailureOutcome(result(assigned, false), err)
			if r.Interrupted != tc.retry || r.ExecutionCause == nil || r.ExecutionCause.Staging == nil || r.ExecutionCause.Staging.Error != tc.reason {
				t.Fatalf("classification lost: %+v", r)
			}
			if tc.reason == "timeout" && r.ExecutionCause.Class != "timeout" {
				t.Fatalf("timeout cause lost: %+v", r.ExecutionCause)
			}
			if tc.retry {
				var recorded *recordedStartError
				if !errors.As(err, &recorded) || !recorded.facts.Infrastructure[0].At.Equal(clock.Now()) {
					t.Fatal("infrastructure fact did not use injected clock")
				}
			}
			// The ordinary log replay works even though there was no process/log before staging.
			api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer api.Close()
			w.Client = Client{URL: api.URL, Token: "test"}
			if err := w.replay(context.Background(), store); err != nil {
				t.Fatal(err)
			}
			logs, err := q.Logs(submitted.ID, 0)
			if err != nil || len(logs) == 0 || !strings.Contains(logs[0].Text, tc.reason) {
				t.Fatal("missing staging log", logs, err)
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, submitted.ID)
			want := "failed"
			if tc.retry {
				want = "queued"
			}
			if got.State != want || got.InfrastructureRetries != map[bool]int{false: 0, true: 1}[tc.retry] || len(got.AttemptHistory) != 1 || got.AttemptHistory[0].ExecutionCause.Staging == nil {
				t.Fatalf("wrong durable decision: %+v", got)
			}
			wire, _ := json.Marshal(got)
			if strings.Contains(string(wire), "/private/worker") {
				t.Fatal("expanded path leaked", string(wire))
			}
			stale := r
			stale.Attempt++
			if err := q.Complete(stale); !errors.Is(err, ErrStale) {
				t.Fatal("stale cause accepted", err)
			}
			if tc.retry {
				for retry := 1; retry <= DefaultProductionInfrastructureRetries; retry++ {
					clock.Advance(time.Second)
					assigned = check(t, q, m, nil).Job
					if assigned == nil {
						t.Fatal("retry not assigned", job(t, q, submitted.ID))
					}
					store.state.Job = assigned
					r = startFailureOutcome(result(assigned, false), w.stagingFailure(store, *assigned, failure))
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
				}
				got = job(t, q, submitted.ID)
				if got.State != "failed" || got.Reason != "infrastructure_retries_exhausted" || got.InfrastructureRetries != DefaultProductionInfrastructureRetries {
					t.Fatal("unbounded staging retries", got)
				}
			}
		})
	}
}

// A high attempt number includes planned stops, not just infrastructure losses.
// Reproduce the retained ninth-attempt ending without changing either budget.
func TestStagingSharedInputTimeoutAfterPreemptions(t *testing.T) {
	q, clock := fixture(t)
	s := submitVerificationInput(t, q)
	ref := s.Files["input"]
	s.SharedInputs = map[string]SharedInput{"source": {SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}}
	m := machine("staging", "fake")
	m.Capabilities = append(m.Capabilities, "shared_inputs_v1")
	j := submit(t, q, s)
	w := Worker{StateDir: newTestStateRoot(t), Clock: clock, Machine: m}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	w.Client = Client{URL: server.URL, Token: "test"}
	for attempt := 1; attempt <= 9; attempt++ {
		a := check(t, q, m, nil).Job
		if a == nil || a.Attempt != attempt {
			t.Fatalf("missing attempt %d: %+v", attempt, a)
		}
		clock.Advance(time.Second)
		r := result(a, false)
		r.Interrupted = true
		if attempt >= 2 && attempt <= 7 {
			if err := q.update(func() error {
				q.stopBatchFor(q.s.Jobs[j.ID], &Job{ID: "demand", Spec: spec("deadline")})
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		} else {
			store.state.Job, store.state.Session = a, a.Session
			r = startFailureOutcome(r, w.stagingFailure(store, *a, withStagingCause("shared_inputs/source", ref, "shared_input", errCheckpointIOTimeout)))
			if !r.Interrupted || r.ExecutionCause.Class != "timeout" || r.ExecutionCause.Staging.Error != "timeout" {
				t.Fatalf("staging timeout misclassified: %+v", r)
			}
			if err := w.replay(context.Background(), store); err != nil {
				t.Fatal(err)
			}
		}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		got, err := w.Client.GetJob(context.Background(), j.ID)
		if err != nil {
			t.Fatal(err)
		}
		losses := 1
		if attempt >= 8 {
			losses = attempt - 6
		}
		if got.Losses != losses || got.InfrastructureRetries != 0 || got.State != map[bool]string{false: "queued", true: "failed"}[attempt == 9] {
			t.Fatalf("attempt %d retry accounting: %+v", attempt, got)
		}
		if attempt == 9 {
			last := got.AttemptHistory[len(got.AttemptHistory)-1]
			if !strings.HasPrefix(got.Reason, "stopped after 3 infrastructure interruptions") || last.Reason != got.Reason || !last.Interrupted || last.ExecutionCause.Staging.Input != "shared_inputs/source" || last.ExecutionCause.Staging.Blob != ref.SHA256 || got.FinishedAt == nil || !got.FinishedAt.Equal(clock.Now()) {
				t.Fatalf("terminal staging explanation lost: %+v", got)
			}
		}
	}
	logs, err := q.Logs(j.ID, 0)
	if err != nil || len(logs) == 0 || !strings.Contains(logs[len(logs)-1].Text, "operation=shared_input error=timeout") {
		t.Fatal("missing safe staging timeout log", logs, err)
	}
}

func TestStagingSubmissionRefusalAndRetention(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureLogRetention(2 * testFileRetention); err != nil {
		t.Fatal(err)
	}
	spec := submitVerificationInput(t, q)
	ref := spec.Files["input"]
	data, err := os.ReadFile(q.files.blob(ref))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(q.files.blob(ref)); err != nil {
		t.Fatal(err)
	}
	for _, corrupt := range []bool{false, true} {
		if corrupt {
			if err := os.WriteFile(q.files.blob(ref), []byte("corrupt"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		_, err := q.Submit(spec)
		var response *HTTPError
		code := "input_blob_missing"
		if corrupt {
			code = "input_blob_corrupt"
		}
		if !errors.As(err, &response) || response.Code != code || !strings.Contains(response.Message, ref.SHA256) || !strings.Contains(response.Message, "files/input") {
			t.Fatal("unactionable refusal", err)
		}
		requireNoSubmission(t, q)
	}
	if err := os.WriteFile(q.files.blob(ref), data, 0600); err != nil {
		t.Fatal(err)
	}
	first := submit(t, q, spec)
	assigned := check(t, q, machine("reader", "fake"), nil).Job
	if assigned == nil {
		t.Fatal("not assigned")
	}
	finish(t, q, assigned, true)
	// Reusing available immutable bytes establishes a new live reference. Expiry
	// of the older receipt cannot collect bytes needed by this waiting job.
	spec.SubmissionKey = "reuse-verified-input"
	second := submit(t, q, spec)
	clock.Advance(testFileRetention + filePublicationGrace + time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if !job(t, q, first.ID).FilesExpired || job(t, q, second.ID).FilesExpired {
		t.Fatal("wrong expiry boundary")
	}
	if err := q.files.verify(context.Background(), ref); err != nil {
		t.Fatal("live reused input collected", err)
	}
}

func TestStagingCauseRejectsUndeclaredNames(t *testing.T) {
	s := Spec{Files: FileManifest{"input": {SHA256: strings.Repeat("a", 64)}}}
	for _, d := range []*StagingCause{
		{Input: "UNDECLARED_SECRET", Blob: strings.Repeat("a", 64), Operation: "copy", Error: "EIO"},
		{Input: "input", Blob: strings.Repeat("b", 64), Operation: "copy", Error: "EIO"},
		{Input: "input", Blob: strings.Repeat("a", 64), Operation: "private/path", Error: "EIO"},
	} {
		if c := safeJobRuntimeCause(s, &RuntimeCause{Operation: "input_staging", Class: "io_error", Staging: d}); c.Staging != nil {
			t.Fatal("untrusted staging detail escaped", c)
		}
	}
}

func TestStagingStorageErrorSurvivesHead(t *testing.T) {
	q, _ := fixture(t)
	spec := submitVerificationInput(t, q)
	ref := spec.Files["input"]
	q.files.syncDir = func(string) error { return syscall.EIO }
	api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer api.Close()
	client := Client{URL: api.URL, Token: "test"}
	err := client.HasFile(context.Background(), ref)
	var se *stagingError
	if !errors.As(err, &se) || se.detail.Operation != "store_sync" || se.detail.Error != "EIO" || se.detail.Status != 503 || !stagingRetryable(err) {
		t.Fatal("HEAD lost storage error", err)
	}
}

// #720's incident shape: a shared input larger than an explicit cache limit fails
// admission before any source archive, plan blob, NAS read or environment launch.
func TestStagingSharedInputCapacityBeforeSourceReads(t *testing.T) {
	_, clock := fixture(t)
	w := Worker{StateDir: newTestStateRoot(t), Clock: clock, FileCacheLimit: 10 << 30}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if err := w.initFileTransfers(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	input := SharedInput{NAS: "media/large", SHA256: strings.Repeat("a", 64), SizeBytes: 11 << 30}
	j := Job{Spec: Spec{Files: FileManifest{".myce-source.tar": {SHA256: strings.Repeat("b", 64), SizeBytes: 10}, "plan.json": {SHA256: strings.Repeat("c", 64), SizeBytes: 10}}, SharedInputs: map[string]SharedInput{"large": input}}}
	w.sharedInputs.resolveNAS = func(string, string) (string, error) {
		t.Error("NAS read reached before impossible admission")
		return "", syscall.EIO
	}
	_, release, err := w.acquireSharedInputs(context.Background(), j)
	if release != nil {
		release()
	}
	if err == nil {
		t.Fatal("oversized shared input admitted")
	}
	cause := runtimeDiagnostic(runtimeErrorFor("input_staging", err))
	d := cause.Staging
	if cause.Class != "resource_limit" || d == nil || d.Input != "shared_inputs/large" || d.Blob != input.SHA256 || d.Operation != "shared_input_admission" || d.Error != "cache_space" || d.RequiredBytes != 11<<30 || d.LimitBytes != 10<<30 || d.AvailableBytes > 10<<30 || !stagingRetryable(err) {
		t.Fatalf("lost actual capacity failure: %+v %v", d, err)
	}
}

func TestStagingLogFailureDoesNotBlockCompletion(t *testing.T) {
	q, clock := fixture(t)
	j := submit(t, q, spec("production"))
	m := machine("staging", "fake")
	assigned := check(t, q, m, nil).Job
	w := Worker{StateDir: newTestStateRoot(t), Clock: clock, Machine: m}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Job, store.state.Session = assigned, assigned.Session
	logName := fmt.Sprintf("staging-%s-%d.log", assigned.ID, assigned.Attempt)
	if err := os.Mkdir(filepath.Join(store.dir, logName), 0700); err != nil {
		t.Fatal(err)
	}
	failure := w.stagingFailure(store, *assigned, withStagingCause("", FileRef{}, "publish", syscall.EIO))
	if store.state.Log != "" {
		t.Fatal("uncreated log retained as replay obligation")
	}
	if err := w.replay(context.Background(), store); err != nil {
		t.Fatal("log creation failure blocked completion", err)
	}
	if err := q.Complete(startFailureOutcome(result(assigned, false), failure)); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "queued" || got.InfrastructureRetries != 1 {
		t.Fatal("bounded infrastructure retry stranded", got)
	}
}

func TestStagingDependencyCauseSurvivesCompletion(t *testing.T) {
	for _, stored := range []bool{false, true} {
		t.Run(fmt.Sprint(stored), func(t *testing.T) {
			q, _ := fixture(t)
			submitted := submit(t, q, spec("experiment"))
			assigned := check(t, q, machine("staging", "fake"), nil).Job
			ref := FileRef{SHA256: strings.Repeat("a", 64), SizeBytes: 1}
			dep := &PieceBinding{}
			if stored {
				dep.Files = FileManifest{"upstream/input": ref}
			} else {
				dep.Inputs = map[string]string{"upstream/input": "upstream/output"}
				ref = FileRef{}
			}
			if err := q.update(func() error { q.touchJob(submitted.ID).Dependency = dep; return nil }); err != nil {
				t.Fatal(err)
			}
			r := result(assigned, false)
			r.ExecutionCause = runtimeDiagnostic(runtimeErrorFor("input_staging", withStagingCause("upstream/input", ref, "copy", syscall.EIO)))
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, submitted.ID)
			if got.ExecutionCause == nil || got.ExecutionCause.Staging == nil || got.ExecutionCause.Staging.Input != "upstream/input" || len(got.AttemptHistory) != 1 || got.AttemptHistory[0].ExecutionCause.Staging == nil {
				t.Fatal("resolved dependency source stripped", got)
			}
		})
	}
}

func TestStagingOrdinaryNASMissingIsActionable(t *testing.T) {
	q, clock := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	s := Spec{Name: "missing-input", Project: "test", Priority: "experiment", Kind: "sample", Inputs: map[string]string{"input": "missing"}}
	submitted := submit(t, q, s)
	m := machine("staging", "fake")
	m.HostMemoryMeasured, m.FreeHostMemoryGB, m.HostAvailableMemoryGB = true, 8, 8
	m.ExtensionReports = installQualified(t, t.TempDir(), e, "")
	assigned := check(t, q, m, nil).Job
	if assigned == nil {
		t.Fatal("not assigned", job(t, q, submitted.ID))
	}
	w := Worker{StateDir: newTestStateRoot(t), JobFileRoot: t.TempDir(), Clock: clock, Machine: m}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	err = w.prepareExtensionFilesContext(context.Background(), assigned, store.dir)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing NAS input did not fail", err)
	}
	r := startFailureOutcome(result(assigned, false), w.stagingFailure(store, *assigned, err))
	if r.Interrupted || r.ExecutionCause == nil || r.ExecutionCause.Staging == nil || r.ExecutionCause.Staging.Input != "input" || r.ExecutionCause.Staging.Operation != "nas_read" {
		t.Fatal("authored NAS absence became infrastructure", r)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, submitted.ID); got.State != "failed" || got.Losses != 0 {
		t.Fatal("missing authored input retried", got)
	}
}
