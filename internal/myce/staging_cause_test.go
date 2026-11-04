package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
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
			if strings.Contains(string(wire), "/private/") {
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

func TestStagingSubmissionRefusalAndRetention(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureLogRetention(2 * DefaultFileRetention); err != nil {
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
	clock.Advance(DefaultFileRetention + filePublicationGrace + time.Second)
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
