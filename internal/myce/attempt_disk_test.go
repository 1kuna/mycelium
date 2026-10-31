package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestAttemptDiskDiagnosticsStopRace(t *testing.T) {
	for _, stopAt := range []string{"before-verification", "during-verification", "after-completion"} {
		t.Run(stopAt, func(t *testing.T) {
			q, clock := fixture(t)
			c := retentionClient(t, q)
			_, input := retentionUpload(t, c, "input")
			s := spec("experiment")
			s.Files = FileManifest{"input": input}
			s.Diagnostics = &DiagnosticSpec{}
			submit(t, q, s)
			m := machine("disk-worker", "fake")
			assigned := check(t, q, m, nil).Job
			if assigned == nil {
				t.Fatal("missing assignment")
			}
			r := result(assigned, false)
			if err := q.Started(r); err != nil {
				t.Fatal(err)
			}
			r.Interrupted = true // Reported stop intent cannot hide a quota failure.
			r.ExecutionCause = &RuntimeCause{Operation: "outputs", Class: diskLimitExceeded}
			summary, err := json.Marshal(FailureSummary{Execution: r.Execution, Machine: r.Machine, Session: r.Session, ExecutionCause: r.ExecutionCause, FailedTests: []FailedTest{}})
			if err != nil {
				t.Fatal(err)
			}
			_, summaryRef := retentionUploadFile(t, c.UploadJobInput, string(summary))
			_, tailRef := retentionUploadFile(t, c.UploadJobInput, "quota failure log tail")
			r.Diagnostics = &AttemptDiagnostics{Files: FileManifest{"summary.json": summaryRef, "log-tail.txt": tailRef}}
			syncDir := q.files.syncDir
			stop := func() {
				t.Helper()
				clock.Advance(time.Second)
				if err := q.Leave(m.ID, true); err != nil {
					t.Fatal(err)
				}
			}
			if stopAt == "before-verification" {
				stop()
			}
			if stopAt == "during-verification" {
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				q.files.syncDir = func(path string) error {
					close(entered)
					<-release
					return syncWorkerDir(path)
				}
				done := make(chan error, 1)
				go func() { done <- q.Complete(r) }()
				select {
				case <-entered:
				case <-time.After(testWaitTimeout):
					t.Fatal("diagnostic verification did not reach durability barrier")
				}
				stopped := make(chan error, 1)
				clock.Advance(time.Second)
				go func() { stopped <- q.Leave(m.ID, true) }()
				select {
				case err := <-stopped:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(testWaitTimeout):
					t.Fatal("stop blocked behind diagnostic I/O under q.mu")
				}
				unblock()
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(testWaitTimeout):
					t.Fatal("diagnostic completion did not finish")
				}
			} else if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if stopAt == "after-completion" {
				stop()
			}
			// Terminal replay must leave the same accepted manifest available.
			// The single-use verification barrier also detects needless rehashing.
			for replay := 0; replay < 2; replay++ {
				if replay > 0 {
					if err := q.Complete(r); err != nil {
						t.Fatal("terminal completion replay", err)
					}
				}
				j, err := c.GetJob(context.Background(), assigned.ID)
				if err != nil {
					t.Fatal(err)
				}
				files, err := DiagnosticFiles(j, assigned.Attempt)
				if err != nil || !maps.Equal(files, r.Diagnostics.Files) || j.State != "failed" || len(j.AttemptHistory) != 1 {
					t.Fatalf("quota diagnostics lost/replaced: files=%v err=%v job=%+v", files, err, j)
				}
			}
			q.files.syncDir = syncDir // Downloads have their own durability barrier.
			dest := filepath.Join(t.TempDir(), "tail")
			if err := c.DownloadDiagnostic(context.Background(), assigned.ID, assigned.Attempt, "log-tail.txt", dest); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(dest)
			if err != nil || string(data) != "quota failure log tail" {
				t.Fatalf("diagnostics retrieval changed: %q %v", data, err)
			}
			stale := r
			stale.Attempt++
			if err := q.Complete(stale); err != ErrStale {
				t.Fatalf("stale quota diagnostics acknowledged: %v", err)
			}
		})
	}
}

func TestAttemptDiskLimitFailure(t *testing.T) {
	for _, stopping := range []bool{false, true} {
		t.Run(fmt.Sprint("stopping=", stopping), func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("disk-worker", "fake")
			j := submit(t, q, spec("experiment"))
			assigned := check(t, q, m, nil).Job
			if assigned == nil {
				t.Fatal("missing assignment")
			}
			if err := q.Started(result(assigned, false)); err != nil {
				t.Fatal(err)
			}
			if stopping {
				if err := q.Leave(m.ID, true); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(time.Second)
			denied := fmt.Errorf("write failed: %w", &os.PathError{Op: "write", Path: "/private/attempt/output", Err: syscall.EDQUOT})
			r := result(assigned, false)
			r.Error = denied.Error()
			r.ExecutionCause = runtimeDiagnostic(runtimeErrorFor("outputs", denied))
			r.Interrupted = stopping // A stop cannot disguise a confirmed quota failure.
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, j.ID)
			if got.State != "failed" || got.Attempt != assigned.Attempt || got.Losses != 0 || got.InfrastructureRetries != 0 || got.ExecutionCause == nil || got.ExecutionCause.Class != diskLimitExceeded {
				t.Fatalf("disk denial retried, charged as infrastructure, or lost: %+v", got)
			}
			if got.FinishedAt == nil || !got.FinishedAt.Equal(clock.Now()) || len(got.AttemptHistory) == 0 {
				t.Fatal("disk failure missing clock/attempt evidence")
			}
			outcome := got.AttemptHistory[len(got.AttemptHistory)-1]
			if outcome.Interrupted || outcome.OutOfMemory || outcome.ExecutionCause == nil || outcome.ExecutionCause.Class != diskLimitExceeded {
				t.Fatalf("disk failure poisoned memory or stop evidence: %+v", outcome)
			}
			if strings.Contains(got.Reason, "/private/") || strings.Contains(outcome.Reason, "/private/") {
				t.Fatal("quota diagnostics exposed a private path")
			}
			if err := q.Complete(r); err != nil {
				t.Fatal("lost completion acknowledgement was not replayable", err)
			}
			r.Attempt++
			if err := q.Complete(r); err != ErrStale {
				t.Fatalf("wrong attempt quota report accepted: %v", err)
			}
		})
	}
}

func TestAttemptDiskDenialEvidence(t *testing.T) {
	full := runtimeDiagnostic(runtimeErrorFor("outputs", syscall.ENOSPC))
	if full.Class != "storage_full" {
		t.Fatal("uncontained ENOSPC was mistaken for an attempt quota")
	}
	r := Result{Error: "disk_limit_exceeded: no space left on device", ExecutionCause: full}
	if attemptDiskLimitFailure(r) {
		t.Fatal("text/uncontained ENOSPC established a quota failure")
	}
	r.ExecutionCause = runtimeDiagnostic(runtimeErrorFor("outputs", syscall.EDQUOT))
	at := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	facts := attemptCause{memberOutcome: memberOutcome{Signals: []deliveredSignal{{Signal: "TERM", At: at}}, Exit: &memberExit{Status: 1, ObservedAt: at.Add(time.Second)}}}
	r = classifyAttemptOutcome(r, facts)
	if !attemptDiskLimitFailure(r) || r.Interrupted || r.OutOfMemory {
		t.Fatalf("quota denial became an interruption or memory failure: %+v", r)
	}
}
